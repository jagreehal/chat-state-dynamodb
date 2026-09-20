// Runs against DynamoDB Local (`pnpm dynamodb:up`), because the conditional
// expressions *are* the adapter — a mock that says "yes" proves nothing.
import { CreateTableCommand, DeleteTableCommand, DynamoDBClient } from '@aws-sdk/client-dynamodb';
import type { QueueEntry } from 'chat';
import { story } from 'executable-stories-vitest';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { createDynamoDBState, DynamoDBStateAdapter, TABLE_DEFINITION } from './index';

story.feature({
  title: 'DynamoDB state adapter',
  narrative: `
    Chat SDK keeps five kinds of state: thread subscriptions, distributed locks,
    key/value, lists and per-thread queues. This adapter stores all of them as
    rows in one DynamoDB table, so a bot on Lambda needs no Redis and no VPC.

    DynamoDB's TTL sweep is lazy (up to 48 hours late), so expiry is enforced
    on every read and in every lock condition rather than left to DynamoDB.
  `,
  glossary: [
    { term: 'lock', definition: 'A per-thread mutex with a holder token; only the holder may release or extend it.' },
    { term: 'ttl', definition: 'Housekeeping attribute for DynamoDB to sweep; never trusted for correctness.' },
    { term: 'keyPrefix', definition: 'Namespace prepended to every key so several bots can share a table.' },
  ],
});

const endpoint = process.env.DYNAMODB_ENDPOINT ?? 'http://localhost:8000';
const client = new DynamoDBClient({
  endpoint,
  region: 'local',
  credentials: { accessKeyId: 'local', secretAccessKey: 'local' },
});
const TableName = `chat-state-test-${process.pid}`;
let state: DynamoDBStateAdapter;
let n = 0;
const id = () => `t${++n}-${Date.now()}`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const entry = (tag: string): QueueEntry =>
  ({ enqueuedAt: Date.now(), expiresAt: Date.now() + 60_000, message: { text: tag } }) as unknown as QueueEntry;

beforeAll(async () => {
  try {
    await client.send(new CreateTableCommand({ TableName, ...TABLE_DEFINITION }));
  } catch (err) {
    throw new Error(
      `DynamoDB Local not reachable at ${endpoint} — run \`pnpm dynamodb:up\` first.\n${err instanceof Error ? err.message : String(err)}`,
      { cause: err },
    );
  }
  state = createDynamoDBState({ tableName: TableName, client });
  await state.connect();
});

afterAll(async () => {
  await client.send(new DeleteTableCommand({ TableName }));
  await state.disconnect();
});

describe('connect', () => {
  it('fails fast with the table name when the table is missing', async ({ task }) => {
    story.init(task, { tags: ['lifecycle'] });
    story.given('an adapter pointed at a table that does not exist');
    const bad = createDynamoDBState({ tableName: 'does-not-exist', client });
    story.when('connect() is called');
    story.then('it rejects with the table name in the message');
    await expect(bad.connect()).rejects.toThrow(/"does-not-exist" is not reachable/);
  });
});

describe('subscriptions', () => {
  it('round-trips', async ({ task }) => {
    story.init(task, { tags: ['subscriptions'] });
    const t = id();
    story.given('a thread nobody is subscribed to');
    expect(await state.isSubscribed(t)).toBe(false);
    story.when('the thread is subscribed');
    await state.subscribe(t);
    story.then('isSubscribed reports true');
    expect(await state.isSubscribed(t)).toBe(true);
    story.when('the thread is unsubscribed');
    await state.unsubscribe(t);
    story.then('isSubscribed reports false again');
    expect(await state.isSubscribed(t)).toBe(false);
  });
});

describe('locks', () => {
  it('only one of many concurrent acquirers wins', async ({ task }) => {
    story.init(task, { tags: ['locks'] });
    const t = id();
    story.when('10 callers race to acquire the same lock');
    const results = await Promise.all(Array.from({ length: 10 }, () => state.acquireLock(t, 5_000)));
    story.then('exactly one gets it');
    expect(results.filter(Boolean)).toHaveLength(1);
  });

  it('is re-acquirable the moment it expires, without waiting for TTL deletion', async ({ task }) => {
    story.init(task, { tags: ['locks', 'expiry'] });
    const t = id();
    story.given('a lock held with a 50ms ttl');
    const lock = await state.acquireLock(t, 50);
    expect(lock).not.toBeNull();
    story.then('a second acquirer is refused while it is live');
    expect(await state.acquireLock(t, 5_000)).toBeNull();
    story.when('the ttl passes');
    await sleep(60);
    story.then('the lock is granted to a new holder with a fresh token');
    const again = await state.acquireLock(t, 5_000);
    expect(again).not.toBeNull();
    expect(again!.token).not.toBe(lock!.token);
  });

  it('release is owner-only: a stale holder cannot free a newer lock', async ({ task }) => {
    story.init(task, { tags: ['locks'] });
    const t = id();
    story.given('a lock that expired and was re-acquired by someone else');
    const first = (await state.acquireLock(t, 50))!;
    await sleep(60);
    const second = (await state.acquireLock(t, 5_000))!;
    story.when('the stale holder releases with its old token');
    await state.releaseLock(first);
    story.then('the newer lock is still held');
    expect(await state.acquireLock(t, 5_000)).toBeNull();
    story.when('the current holder releases');
    await state.releaseLock(second);
    story.then('the lock is free');
    expect(await state.acquireLock(t, 5_000)).not.toBeNull();
  });

  it('extend succeeds for the holder and fails for anyone else or after expiry', async ({ task }) => {
    story.init(task, { tags: ['locks', 'expiry'] });
    const t = id();
    story.given('a lock held with a 200ms ttl');
    const lock = (await state.acquireLock(t, 200))!;
    story.when('the holder extends it by 5s');
    story.then('the extension succeeds and expiresAt moves out');
    expect(await state.extendLock(lock, 5_000)).toBe(true);
    expect(lock.expiresAt).toBeGreaterThan(Date.now() + 4_000);
    story.but('a caller with the wrong token cannot extend it');
    expect(await state.extendLock({ ...lock, token: 'nope' }, 5_000)).toBe(false);
    story.but('an expired lock cannot be extended even by its holder');
    const short = (await state.acquireLock(id(), 30))!;
    await sleep(40);
    expect(await state.extendLock(short, 5_000)).toBe(false);
  });

  it('force release ignores the token', async ({ task }) => {
    story.init(task, { tags: ['locks'] });
    const t = id();
    story.given('a lock held for 60s');
    await state.acquireLock(t, 60_000);
    story.when('it is force-released');
    await state.forceReleaseLock(t);
    story.then('anyone can acquire it');
    expect(await state.acquireLock(t, 1_000)).not.toBeNull();
    story.and('force-releasing a lock that was never held is a no-op');
    await expect(state.forceReleaseLock(id())).resolves.toBeUndefined();
  });
});

describe('key/value', () => {
  it('stores JSON values and returns null for missing keys', async ({ task }) => {
    story.init(task, { tags: ['kv'] });
    const k = id();
    story.given('a key that was never set');
    expect(await state.get(k)).toBeNull();
    story.when('a JSON value is stored under it');
    await state.set(k, { a: 1, b: ['x'] });
    story.then('get returns the same structure');
    expect(await state.get(k)).toEqual({ a: 1, b: ['x'] });
    story.when('the key is deleted');
    await state.delete(k);
    story.then('get returns null');
    expect(await state.get(k)).toBeNull();
  });

  it('honours ttl on read before DynamoDB ever sweeps it', async ({ task }) => {
    story.init(task, { tags: ['kv', 'expiry'] });
    const k = id();
    story.given('a value set with a 50ms ttl');
    await state.set(k, 'v', 50);
    story.then('it is readable straight away');
    expect(await state.get(k)).toBe('v');
    story.when('the ttl passes');
    await sleep(60);
    story.then('get returns null without waiting for the TTL sweep');
    expect(await state.get(k)).toBeNull();
  });

  it('setIfNotExists: first wins, expired counts as absent', async ({ task }) => {
    story.init(task, { tags: ['kv', 'expiry'] });
    const k = id();
    story.when('setIfNotExists is called on an empty key with a 50ms ttl');
    story.then('it succeeds');
    expect(await state.setIfNotExists(k, 1, 50)).toBe(true);
    story.but('a second call while the value is live fails');
    expect(await state.setIfNotExists(k, 2)).toBe(false);
    story.when('the ttl passes');
    await sleep(60);
    story.then('setIfNotExists succeeds again and the new value is read back');
    expect(await state.setIfNotExists(k, 3)).toBe(true);
    expect(await state.get(k)).toBe(3);
  });
});

describe('lists', () => {
  it('keeps insertion order and trims the oldest to maxLength', async ({ task }) => {
    story.init(task, { tags: ['lists'] });
    const k = id();
    story.when('five values are appended with maxLength 3');
    for (const v of [1, 2, 3, 4, 5]) await state.appendToList(k, v, { maxLength: 3 });
    story.then('only the newest three remain, in order');
    expect(await state.getList(k)).toEqual([3, 4, 5]);
  });

  it('expires entries by ttl', async ({ task }) => {
    story.init(task, { tags: ['lists', 'expiry'] });
    const k = id();
    story.given('one entry with a 50ms ttl and one with a 60s ttl');
    await state.appendToList(k, 'old', { ttlMs: 50 });
    await state.appendToList(k, 'new', { ttlMs: 60_000 });
    story.when('50ms passes');
    await sleep(60);
    story.then('only the live entry is returned');
    expect(await state.getList(k)).toEqual(['new']);
  });

  it('does not conflate lists and kv under the same key', async ({ task }) => {
    story.init(task, { tags: ['lists', 'kv'] });
    const k = id();
    story.given('a kv value and a list entry stored under the same key');
    await state.set(k, 'kv');
    await state.appendToList(k, 'list');
    story.then('each is read back from its own namespace');
    expect(await state.get(k)).toBe('kv');
    expect(await state.getList(k)).toEqual(['list']);
  });
});

describe('queues', () => {
  it('is FIFO, reports depth, and drops the oldest beyond maxSize', async ({ task }) => {
    story.init(task, { tags: ['queues'] });
    const t = id();
    story.when('three entries are enqueued with maxSize 2');
    expect(await state.enqueue(t, entry('a'), 2)).toBe(1);
    expect(await state.enqueue(t, entry('b'), 2)).toBe(2);
    expect(await state.enqueue(t, entry('c'), 2)).toBe(2);
    story.then('depth is capped at 2');
    expect(await state.queueDepth(t)).toBe(2);
    story.and('dequeue returns the two newest in FIFO order, then null');
    expect((await state.dequeue(t))!.message).toEqual({ text: 'b' });
    expect((await state.dequeue(t))!.message).toEqual({ text: 'c' });
    expect(await state.dequeue(t)).toBeNull();
    expect(await state.queueDepth(t)).toBe(0);
  });

  it('concurrent dequeuers never hand out the same entry twice', async ({ task }) => {
    story.init(task, { tags: ['queues'] });
    const t = id();
    story.given('a queue with four entries');
    for (const tag of ['a', 'b', 'c', 'd']) await state.enqueue(t, entry(tag), 10);
    story.when('six callers dequeue concurrently');
    const got = await Promise.all(Array.from({ length: 6 }, () => state.dequeue(t)));
    const tags = got.filter(Boolean).map((e) => (e!.message as { text: string }).text);
    story.then('no entry is handed out twice');
    expect(new Set(tags).size).toBe(tags.length);
    story.and('dequeued plus remaining equals four');
    expect(tags.length + (await state.queueDepth(t))).toBe(4);
  });
});

describe('keyPrefix', () => {
  it('isolates two bots sharing one table', async ({ task }) => {
    story.init(task, { tags: ['keyPrefix'] });
    story.given('two adapters on the same table with different keyPrefixes');
    const a = createDynamoDBState({ tableName: TableName, client, keyPrefix: 'bot-a', skipTableCheck: true });
    const b = createDynamoDBState({ tableName: TableName, client, keyPrefix: 'bot-b', skipTableCheck: true });
    await a.connect();
    await b.connect();
    const t = id();
    story.when('bot-a subscribes to a thread');
    await a.subscribe(t);
    story.then('bot-b does not see the subscription');
    expect(await b.isSubscribed(t)).toBe(false);
    story.and('both can hold a lock on the same thread id');
    expect(await a.acquireLock(t, 1_000)).not.toBeNull();
    expect(await b.acquireLock(t, 1_000)).not.toBeNull();
  });
});
