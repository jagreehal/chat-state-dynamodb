# chat-state-dynamodb

DynamoDB [state adapter](https://chat-sdk.dev/docs/state-adapters) for [Chat SDK](https://chat-sdk.dev):
thread subscriptions, distributed locks and caching on **one DynamoDB table**. No Redis, no VPC,
IAM-only auth — the natural fit for a bot on Lambda.

```bash
pnpm add chat-state-dynamodb @aws-sdk/client-dynamodb @aws-sdk/lib-dynamodb
```

```ts
import { Chat } from 'chat';
import { createSlackAdapter } from '@chat-adapter/slack';
import { createDynamoDBState } from 'chat-state-dynamodb';

const bot = new Chat({
  userName: 'my-bot',
  adapters: { slack: createSlackAdapter() },
  state: createDynamoDBState({ tableName: process.env.STATE_TABLE! }),
});
```

## The table

`pk` (string) partition key, `sk` (string) sort key, TTL enabled on `ttl`. Nothing else.

```ts
// CDK
import { AttributeType, BillingMode, Table } from 'aws-cdk-lib/aws-dynamodb';

const table = new Table(this, 'ChatState', {
  partitionKey: { name: 'pk', type: AttributeType.STRING },
  sortKey: { name: 'sk', type: AttributeType.STRING },
  timeToLiveAttribute: 'ttl',
  billingMode: BillingMode.PAY_PER_REQUEST,
});
table.grantReadWriteData(fn);
fn.addEnvironment('STATE_TABLE', table.tableName);
```

```bash
# CLI
aws dynamodb create-table --table-name chat-state \
  --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
  --billing-mode PAY_PER_REQUEST
aws dynamodb update-time-to-live --table-name chat-state \
  --time-to-live-specification Enabled=true,AttributeName=ttl
```

`TABLE_DEFINITION` is exported in `CreateTableCommand` shape for scripts and tests.

## Options

| Option | Default | |
| --- | --- | --- |
| `tableName` | — | required |
| `client` | `new DynamoDBClient({})` | pass your own for a local endpoint or custom config |
| `keyPrefix` | `chat` | namespace, so several bots can share a table |
| `skipTableCheck` | `false` | `connect()` runs `DescribeTable` to fail fast with a useful message |

## How it maps

| Chat SDK | DynamoDB |
| --- | --- |
| `acquireLock` | `PutItem` conditioned on `attribute_not_exists(pk) OR expiresAt <= :now` |
| `releaseLock` / `extendLock` | conditional delete / update on the holder's `token` |
| `get` / `set` / `setIfNotExists` / `delete` | one item per key, `expiresAt` checked on read |
| `subscribe` / `isSubscribed` | one item per thread |
| `appendToList` (+ `maxLength`, `ttlMs`) | one item per entry under a shared `pk`; oldest trimmed after append |
| `enqueue` / `dequeue` / `queueDepth` | same layout; `dequeue` reads the oldest and deletes it on condition it still exists |

Two DynamoDB facts drive that design:

- **TTL deletion is lazy** (up to 48 h late). `ttl` is housekeeping only; every read filters on
  `expiresAt` and every lock condition compares it, so an expired lock is re-acquirable the
  millisecond it expires, exactly like Redis `PX`.
- **Items are ≤ 400 KB.** Lists and queues are a row per entry, so a long thread history never
  hits the limit. Append-then-trim is two calls rather than one Lua script; the trim only ever
  removes the oldest rows.

## Tests

They run against the real thing, because the conditional expressions *are* the adapter:

```bash
pnpm dynamodb:up   # amazon/dynamodb-local on :8000
pnpm test
pnpm dynamodb:down
```

Covered: one winner among concurrent acquirers, re-acquire on expiry without a TTL sweep,
owner-only release and extend, kv expiry, list order and trim, queue FIFO and cap, no duplicate
hand-outs to concurrent dequeuers, prefix isolation.

## Cost note

Every operation is one or two requests; `queueDepth` and the list trim are a `Query`. At
pay-per-request pricing a chat bot's traffic is fractions of a cent a day. If you have a thread
with tens of thousands of history entries, `getList` pages through all of them — cap with
`maxLength`.

## License

MIT
