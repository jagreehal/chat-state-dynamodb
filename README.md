# chat-state-dynamodb

DynamoDB [state adapter](https://chat-sdk.dev/docs/state-adapters) for [Chat SDK](https://chat-sdk.dev).
Thread subscriptions, distributed locks and caching live on one DynamoDB table. You skip Redis,
the VPC and the secrets; IAM covers auth. Built for bots on Lambda.

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

`pk` (string) partition key, `sk` (string) sort key, TTL enabled on `ttl`.

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

The package exports `TABLE_DEFINITION` in `CreateTableCommand` shape for scripts and tests.

## Options

| Option | Default | |
| --- | --- | --- |
| `tableName` | | required |
| `client` | `new DynamoDBClient({})` | pass your own for a local endpoint or custom config |
| `keyPrefix` | `chat` | namespace, so several bots can share a table |
| `skipTableCheck` | `false` | `connect()` runs `DescribeTable` and fails with the table name if it is missing |

## How it maps

| Chat SDK | DynamoDB |
| --- | --- |
| `acquireLock` | `PutItem` conditioned on `attribute_not_exists(pk) OR expiresAt <= :now` |
| `releaseLock` / `extendLock` | conditional delete / update on the holder's `token` |
| `get` / `set` / `setIfNotExists` / `delete` | one item per key, `expiresAt` checked on read |
| `subscribe` / `isSubscribed` | one item per thread |
| `appendToList` (+ `maxLength`, `ttlMs`) | one item per entry under a shared `pk`; oldest trimmed after append |
| `enqueue` / `dequeue` / `queueDepth` | same layout; `dequeue` reads the oldest and deletes it on condition it still exists |

Two DynamoDB limits shape the design.

DynamoDB deletes expired items lazily, up to 48 hours late, so the adapter treats `ttl` as
housekeeping. Every read filters on `expiresAt` and every lock condition compares it. An expired
lock is re-acquirable the millisecond it expires, the same guarantee Redis `PX` gives you.

An item holds at most 400 KB. Lists and queues use a row per entry, so a long thread history
stays under the limit. Append then trim costs two calls instead of one Lua script; the trim
removes only the oldest rows.

## Tests

The tests run against DynamoDB Local. The conditional expressions are the adapter, and a mock
that returns "yes" proves nothing.

```bash
pnpm dynamodb:up   # amazon/dynamodb-local on :8000
pnpm test
pnpm dynamodb:down
```

Covered: one winner among concurrent acquirers, re-acquire on expiry without a TTL sweep,
owner-only release and extend, kv expiry, list order and trim, queue FIFO and cap, no duplicate
hand-outs to concurrent dequeuers, prefix isolation.

`pnpm test` also writes a story report to `reports/test-results.{md,html}`, and CI posts it on
each pull request.

## Cost note

Each operation is one or two requests; `queueDepth` and the list trim are a `Query`. At
pay-per-request pricing a chat bot's traffic costs under a cent a day. `getList` pages through
every entry in a thread, so cap long histories with `maxLength`.

## License

MIT
