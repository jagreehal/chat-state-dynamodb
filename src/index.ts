/**
 * DynamoDB state adapter for Chat SDK.
 *
 * One table, `pk` + `sk` string keys and a `ttl` attribute with DynamoDB TTL
 * enabled. Every kind of state the SDK keeps is a row in it:
 *
 *   subscriptions  pk = <prefix>#sub#<threadId>    sk = "#"
 *   locks          pk = <prefix>#lock#<threadId>   sk = "#"     token, expiresAt
 *   key/value      pk = <prefix>#kv#<key>          sk = "#"     value, expiresAt?
 *   lists          pk = <prefix>#list#<key>        sk = <seq>   one row per entry
 *   queues         pk = <prefix>#queue#<threadId>  sk = <seq>   one row per entry
 *
 * Two DynamoDB facts shape the design:
 *
 *   - TTL deletion is lazy (documented as up to 48 hours late), so `ttl` is
 *     only housekeeping. Every read filters on `expiresAt` itself and every
 *     lock condition compares it, so an expired lock is re-acquirable the
 *     millisecond it expires, exactly as with Redis `PX`.
 *   - An item is at most 400 KB. Lists and queues are therefore one row per
 *     entry under a shared `pk`, not a single array attribute, so a long
 *     thread history never hits the limit. Append + trim is two calls rather
 *     than one Lua script; the trim only ever removes the oldest rows.
 */

import {
  ConditionalCheckFailedException,
  type CreateTableCommandInput,
  DescribeTableCommand,
  DynamoDBClient,
} from '@aws-sdk/client-dynamodb';
import {
  DeleteCommand,
  DynamoDBDocumentClient,
  GetCommand,
  PutCommand,
  QueryCommand,
  UpdateCommand,
} from '@aws-sdk/lib-dynamodb';
import { randomBytes } from 'node:crypto';
import type { Lock, QueueEntry, StateAdapter } from 'chat';

export interface DynamoDBStateOptions {
  /** Table with `pk` (S) hash key, `sk` (S) range key, TTL on `ttl`. */
  tableName: string;
  /** Reuse a client (e.g. with a local endpoint). One is created otherwise. */
  client?: DynamoDBClient;
  /** Namespace for every key; lets several bots share a table. Default `chat`. */
  keyPrefix?: string;
  /** Skip the `DescribeTable` check in `connect()`. Default `false`. */
  skipTableCheck?: boolean;
}

/**
 * The table this adapter expects, in the shape `CreateTableCommand` /
 * `aws dynamodb create-table` take. CDK: `partitionKey: { name: 'pk',
 * type: STRING }, sortKey: { name: 'sk', type: STRING }, timeToLiveAttribute:
 * 'ttl'`.
 */
export const TABLE_DEFINITION = {
  AttributeDefinitions: [
    { AttributeName: 'pk', AttributeType: 'S' },
    { AttributeName: 'sk', AttributeType: 'S' },
  ],
  KeySchema: [
    { AttributeName: 'pk', KeyType: 'HASH' },
    { AttributeName: 'sk', KeyType: 'RANGE' },
  ],
  BillingMode: 'PAY_PER_REQUEST',
} satisfies Omit<CreateTableCommandInput, 'TableName'>;

export const TTL_ATTRIBUTE = 'ttl';

const SINGLE = '#';
const MAX_DEQUEUE_RETRIES = 3;

type Row = {
  pk: string;
  sk: string;
  value?: string;
  token?: string;
  expiresAt?: number;
  ttl?: number;
};

const ttlOf = (expiresAtMs: number) => Math.ceil(expiresAtMs / 1000);

/** Sort key that orders by insertion across processes: ms timestamp + random. */
function nextSeq(): string {
  return `${Date.now().toString().padStart(15, '0')}#${randomBytes(4).toString('hex')}`;
}

function isExpired(row: Pick<Row, 'expiresAt'> | undefined, now = Date.now()): boolean {
  return row?.expiresAt !== undefined && row.expiresAt <= now;
}

export class DynamoDBStateAdapter implements StateAdapter {
  private readonly doc: DynamoDBDocumentClient;
  private readonly ownsClient: boolean;
  private readonly table: string;
  private readonly prefix: string;
  private readonly skipTableCheck: boolean;
  private connected = false;

  constructor(options: DynamoDBStateOptions) {
    const client = options.client ?? new DynamoDBClient({});
    this.ownsClient = !options.client;
    this.doc = DynamoDBDocumentClient.from(client, {
      marshallOptions: { removeUndefinedValues: true },
    });
    this.table = options.tableName;
    this.prefix = options.keyPrefix ?? 'chat';
    this.skipTableCheck = options.skipTableCheck ?? false;
  }

  // ─── lifecycle ────────────────────────────────────────────────────────────

  async connect(): Promise<void> {
    if (this.connected) return;
    if (!this.skipTableCheck) {
      // Fail at startup with the table's name in the message, not on the
      // first webhook with a ResourceNotFoundException from deep inside.
      try {
        await this.doc.send(new DescribeTableCommand({ TableName: this.table }));
      } catch (err) {
        throw new Error(
          `[chat-state-dynamodb] table "${this.table}" is not reachable: ${err instanceof Error ? err.message : String(err)}`,
          { cause: err },
        );
      }
    }
    this.connected = true;
  }

  async disconnect(): Promise<void> {
    this.connected = false;
    if (this.ownsClient) this.doc.destroy();
  }

  // ─── subscriptions ────────────────────────────────────────────────────────

  async subscribe(threadId: string): Promise<void> {
    await this.doc.send(
      new PutCommand({ TableName: this.table, Item: { pk: this.pk('sub', threadId), sk: SINGLE } }),
    );
  }

  async unsubscribe(threadId: string): Promise<void> {
    await this.doc.send(
      new DeleteCommand({ TableName: this.table, Key: { pk: this.pk('sub', threadId), sk: SINGLE } }),
    );
  }

  async isSubscribed(threadId: string): Promise<boolean> {
    const { Item } = await this.doc.send(
      new GetCommand({
        TableName: this.table,
        Key: { pk: this.pk('sub', threadId), sk: SINGLE },
        ConsistentRead: true,
      }),
    );
    return Item !== undefined;
  }

  // ─── locks ────────────────────────────────────────────────────────────────

  async acquireLock(threadId: string, ttlMs: number): Promise<Lock | null> {
    const token = randomBytes(16).toString('hex');
    const now = Date.now();
    const expiresAt = now + ttlMs;
    try {
      await this.doc.send(
        new PutCommand({
          TableName: this.table,
          Item: { pk: this.pk('lock', threadId), sk: SINGLE, token, expiresAt, ttl: ttlOf(expiresAt) },
          // Free, or held but expired — the expiry check is what makes this
          // correct without waiting for DynamoDB's lazy TTL sweep.
          ConditionExpression: 'attribute_not_exists(pk) OR expiresAt <= :now',
          ExpressionAttributeValues: { ':now': now },
        }),
      );
      return { threadId, token, expiresAt };
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) return null;
      throw err;
    }
  }

  async releaseLock(lock: Lock): Promise<void> {
    try {
      await this.doc.send(
        new DeleteCommand({
          TableName: this.table,
          Key: { pk: this.pk('lock', lock.threadId), sk: SINGLE },
          // Only the holder may release; a stale holder's release is a no-op.
          ConditionExpression: '#token = :token',
          ExpressionAttributeNames: { '#token': 'token' },
          ExpressionAttributeValues: { ':token': lock.token },
        }),
      );
    } catch (err) {
      if (!(err instanceof ConditionalCheckFailedException)) throw err;
    }
  }

  async extendLock(lock: Lock, ttlMs: number): Promise<boolean> {
    const now = Date.now();
    const expiresAt = now + ttlMs;
    try {
      await this.doc.send(
        new UpdateCommand({
          TableName: this.table,
          Key: { pk: this.pk('lock', lock.threadId), sk: SINGLE },
          UpdateExpression: 'SET expiresAt = :expiresAt, #ttl = :ttl',
          ConditionExpression: '#token = :token AND expiresAt > :now',
          ExpressionAttributeNames: { '#token': 'token', '#ttl': TTL_ATTRIBUTE },
          ExpressionAttributeValues: {
            ':token': lock.token,
            ':now': now,
            ':expiresAt': expiresAt,
            ':ttl': ttlOf(expiresAt),
          },
        }),
      );
      lock.expiresAt = expiresAt;
      return true;
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) return false;
      throw err;
    }
  }

  async forceReleaseLock(threadId: string): Promise<void> {
    await this.doc.send(
      new DeleteCommand({ TableName: this.table, Key: { pk: this.pk('lock', threadId), sk: SINGLE } }),
    );
  }

  // ─── key/value ────────────────────────────────────────────────────────────

  async get<T = unknown>(key: string): Promise<T | null> {
    const { Item } = await this.doc.send(
      new GetCommand({
        TableName: this.table,
        Key: { pk: this.pk('kv', key), sk: SINGLE },
        ConsistentRead: true,
      }),
    );
    const row = Item as Row | undefined;
    if (!row || row.value === undefined || isExpired(row)) return null;
    return JSON.parse(row.value) as T;
  }

  async set<T = unknown>(key: string, value: T, ttlMs?: number): Promise<void> {
    await this.doc.send(
      new PutCommand({ TableName: this.table, Item: this.kvRow(key, value, ttlMs) }),
    );
  }

  async setIfNotExists(key: string, value: unknown, ttlMs?: number): Promise<boolean> {
    try {
      await this.doc.send(
        new PutCommand({
          TableName: this.table,
          Item: this.kvRow(key, value, ttlMs),
          ConditionExpression: 'attribute_not_exists(pk) OR expiresAt <= :now',
          ExpressionAttributeValues: { ':now': Date.now() },
        }),
      );
      return true;
    } catch (err) {
      if (err instanceof ConditionalCheckFailedException) return false;
      throw err;
    }
  }

  async delete(key: string): Promise<void> {
    await this.doc.send(
      new DeleteCommand({ TableName: this.table, Key: { pk: this.pk('kv', key), sk: SINGLE } }),
    );
  }

  // ─── lists ────────────────────────────────────────────────────────────────

  async appendToList(
    key: string,
    value: unknown,
    options?: { maxLength?: number; ttlMs?: number },
  ): Promise<void> {
    const pk = this.pk('list', key);
    await this.doc.send(
      new PutCommand({ TableName: this.table, Item: this.entryRow(pk, value, options?.ttlMs) }),
    );
    if (options?.maxLength && options.maxLength > 0) await this.trim(pk, options.maxLength);
  }

  async getList<T = unknown>(key: string): Promise<T[]> {
    const rows = await this.entries(this.pk('list', key));
    return rows.map((r) => JSON.parse(r.value!) as T);
  }

  // ─── queues ───────────────────────────────────────────────────────────────

  async enqueue(threadId: string, entry: QueueEntry, maxSize: number): Promise<number> {
    const pk = this.pk('queue', threadId);
    // Same floor as the Redis adapter, so an entry never expires before the
    // lock that will process it.
    const ttlMs = Math.max(entry.expiresAt - Date.now(), 60_000);
    await this.doc.send(
      new PutCommand({ TableName: this.table, Item: this.entryRow(pk, entry, ttlMs) }),
    );
    if (maxSize > 0) await this.trim(pk, maxSize);
    return this.count(pk);
  }

  async dequeue(threadId: string): Promise<QueueEntry | null> {
    const pk = this.pk('queue', threadId);
    // Pop the oldest: read one, then delete it on condition it still exists.
    // Another consumer winning that race just means we read the next one.
    for (let attempt = 0; attempt < MAX_DEQUEUE_RETRIES; attempt++) {
      const [head] = await this.entries(pk, 1);
      if (!head) return null;
      try {
        await this.doc.send(
          new DeleteCommand({
            TableName: this.table,
            Key: { pk, sk: head.sk },
            ConditionExpression: 'attribute_exists(pk)',
          }),
        );
        return JSON.parse(head.value!) as QueueEntry;
      } catch (err) {
        if (!(err instanceof ConditionalCheckFailedException)) throw err;
      }
    }
    return null;
  }

  async queueDepth(threadId: string): Promise<number> {
    return this.count(this.pk('queue', threadId));
  }

  // ─── internals ────────────────────────────────────────────────────────────

  private pk(kind: 'sub' | 'lock' | 'kv' | 'list' | 'queue', id: string): string {
    return `${this.prefix}#${kind}#${id}`;
  }

  private kvRow(key: string, value: unknown, ttlMs?: number): Row {
    const row: Row = { pk: this.pk('kv', key), sk: SINGLE, value: JSON.stringify(value) };
    if (ttlMs !== undefined && ttlMs > 0) {
      row.expiresAt = Date.now() + ttlMs;
      row.ttl = ttlOf(row.expiresAt);
    }
    return row;
  }

  private entryRow(pk: string, value: unknown, ttlMs?: number): Row {
    const row: Row = { pk, sk: nextSeq(), value: JSON.stringify(value) };
    if (ttlMs !== undefined && ttlMs > 0) {
      row.expiresAt = Date.now() + ttlMs;
      row.ttl = ttlOf(row.expiresAt);
    }
    return row;
  }

  /** Live entries under a pk, oldest first. */
  private async entries(pk: string, limit?: number): Promise<Row[]> {
    const now = Date.now();
    const out: Row[] = [];
    let ExclusiveStartKey: Record<string, unknown> | undefined;
    do {
      const page = await this.doc.send(
        new QueryCommand({
          TableName: this.table,
          KeyConditionExpression: 'pk = :pk',
          FilterExpression: 'attribute_not_exists(expiresAt) OR expiresAt > :now',
          ExpressionAttributeValues: { ':pk': pk, ':now': now },
          ConsistentRead: true,
          ExclusiveStartKey,
        }),
      );
      out.push(...((page.Items ?? []) as Row[]));
      ExclusiveStartKey = page.LastEvaluatedKey;
      if (limit !== undefined && out.length >= limit) break;
    } while (ExclusiveStartKey);
    return limit === undefined ? out : out.slice(0, limit);
  }

  private async count(pk: string): Promise<number> {
    return (await this.entries(pk)).length;
  }

  /** Delete the oldest rows so at most `max` remain. */
  private async trim(pk: string, max: number): Promise<void> {
    const rows = await this.entries(pk);
    const excess = rows.slice(0, Math.max(0, rows.length - max));
    await Promise.all(
      excess.map((r) =>
        this.doc.send(new DeleteCommand({ TableName: this.table, Key: { pk, sk: r.sk } })),
      ),
    );
  }
}

export function createDynamoDBState(options: DynamoDBStateOptions): DynamoDBStateAdapter {
  return new DynamoDBStateAdapter(options);
}
