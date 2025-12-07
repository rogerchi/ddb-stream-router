# ddb-stream-router

A TypeScript library providing Express-like routing for DynamoDB Stream events. Register type-safe handlers for INSERT, MODIFY, and REMOVE operations using discriminator functions or schema validators like Zod. Defer heavy processing to SQS with a simple `.defer()` chain to keep your stream processing fast and reliable.

## Features

- **Express-like API** - Familiar `.onInsert()`, `.onModify()`, `.onRemove()`, `.use()` methods
- **Type Safety** - Full TypeScript inference from discriminators and parsers
- **Flexible Matching** - Use type guards or schema validators (Zod, etc.)
- **Attribute Filtering** - React to specific attribute changes in MODIFY events
- **Batch Processing** - Group records and process them together
- **Middleware Support** - Intercept records before handlers
- **Deferred Processing** - Automatically enqueue to SQS for async processing
- **Global Tables** - Filter records by region
- **Partial Batch Failures** - Lambda retry support via `reportBatchItemFailures`

## Installation

```bash
npm install ddb-stream-router
# or
yarn add ddb-stream-router
```

**Peer dependencies:**
```bash
npm install @aws-sdk/util-dynamodb @aws-sdk/client-sqs
```

## Quick Start

```typescript
import { StreamRouter } from 'ddb-stream-router';
import type { DynamoDBStreamHandler } from 'aws-lambda';

interface User {
  pk: string;
  sk: string;
  name: string;
  email: string;
}

// Type guard discriminator
const isUser = (record: unknown): record is User =>
  typeof record === 'object' &&
  record !== null &&
  'pk' in record &&
  (record as { pk: string }).pk.startsWith('USER#');

const router = new StreamRouter();

router
  .onInsert(isUser, async (newUser, ctx) => {
    console.log(`User created: ${newUser.name}`);
  })
  .onModify(isUser, async (oldUser, newUser, ctx) => {
    console.log(`User updated: ${oldUser.name} -> ${newUser.name}`);
  })
  .onRemove(isUser, async (deletedUser, ctx) => {
    console.log(`User deleted: ${deletedUser.name}`);
  });

// Simplified export with built-in batch failure support
export const handler = router.streamHandler;

// Or with custom options:
// export const handler: DynamoDBStreamHandler = async (event) => {
//   return router.process(event, { reportBatchItemFailures: true });
// };
```

## Discriminator Matching

The discriminator/parser is matched against different images based on event type:

| Event Type | Image Used for Matching |
|------------|------------------------|
| `INSERT` | newImage |
| `MODIFY` | newImage |
| `REMOVE` | oldImage |

For MODIFY events, the **newImage** is used for matching because you typically want to route based on the current state of the record. If a record's type changed (e.g., `pk` prefix changed from `USER#` to `ADMIN#`), the handler for the new type will be invoked.

## Using Zod Schemas

```typescript
import { z } from 'zod';
import { StreamRouter } from 'ddb-stream-router';

const UserSchema = z.object({
  pk: z.string().startsWith('USER#'),
  sk: z.string(),
  name: z.string(),
  email: z.string(),
});

type User = z.infer<typeof UserSchema>;

const router = new StreamRouter();

// Schema validates and parses data before handler receives it
router.onInsert(UserSchema, async (newUser: User, ctx) => {
  // newUser is guaranteed to match the schema
  console.log(`Valid user: ${newUser.email}`);
});
```

## Attribute Change Filtering

React only when specific attributes change:

```typescript
// Only trigger when email changes
router.onModify(
  isUser,
  async (oldUser, newUser, ctx) => {
    await sendEmailVerification(newUser.email);
  },
  { attribute: 'email', changeType: 'changed_attribute' }
);

// Nested attributes with dot notation
router.onModify(
  isUser,
  async (oldUser, newUser, ctx) => {
    console.log(`Theme: ${oldUser.preferences.theme} -> ${newUser.preferences.theme}`);
  },
  { attribute: 'preferences.theme', changeType: 'changed_attribute' }
);

// Watching parent catches all nested changes
router.onModify(
  isUser,
  async (oldUser, newUser, ctx) => {
    // Triggers when preferences.theme OR preferences.notifications changes
    console.log('Any preference changed');
  },
  { attribute: 'preferences' }
);

// Trigger when tags are added to a collection
router.onModify(
  isUser,
  async (oldUser, newUser, ctx) => {
    console.log('New tags added');
  },
  { attribute: 'tags', changeType: 'new_item_in_collection' }
);

// Multiple change types (OR logic)
router.onModify(
  isUser,
  async (oldUser, newUser, ctx) => {
    console.log('Tags modified');
  },
  { attribute: 'tags', changeType: ['new_item_in_collection', 'remove_item_from_collection'] }
);
```

**Change types:**
- `new_attribute` - Attribute added
- `remove_attribute` - Attribute removed  
- `changed_attribute` - Attribute value changed
- `new_item_in_collection` - Item added to List or Set (SS/NS/BS)
- `remove_item_from_collection` - Item removed from List or Set
- `changed_item_in_collection` - Items both added and removed in List/Set

**Supported collection types:**
- Arrays (DynamoDB Lists)
- Sets (DynamoDB String Sets, Number Sets, Binary Sets)

## Batch Processing

Process multiple records together:

```typescript
// All matching records in one handler call
router.onInsert(
  isInventoryChange,
  async (records) => {
    console.log(`Processing ${records.length} changes`);
    for (const { newImage, ctx } of records) {
      // process each record
    }
  },
  { batch: true }
);

// Group by attribute value
router.onInsert(
  isAuditLog,
  async (records) => {
    const userId = records[0].newImage.userId;
    console.log(`${records.length} logs for user ${userId}`);
  },
  { batch: true, batchKey: 'userId' }
);

// Group by primary key
router.onModify(
  isItem,
  async (records) => {
    // All records for the same pk+sk
  },
  { batch: true, batchKey: { partitionKey: 'pk', sortKey: 'sk' } }
);
```

## Middleware

```typescript
// Logging middleware
router.use(async (record, next) => {
  console.log(`Processing ${record.eventName}: ${record.eventID}`);
  await next();
});

// Skip certain records
router.use(async (record, next) => {
  if (record.eventSourceARN?.includes('test-table')) {
    return; // Don't call next() to skip
  }
  await next();
});

// Error handling
router.use(async (record, next) => {
  try {
    await next();
  } catch (error) {
    await recordMetric('stream.error', 1);
    throw error;
  }
});
```

## Deferred Processing (SQS)

Offload heavy processing to SQS:

```typescript
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { StreamRouter, createSQSClient } from 'ddb-stream-router';

const router = new StreamRouter({
  deferQueue: process.env.DEFER_QUEUE_URL,
  sqsClient: createSQSClient(new SQSClient({}), SendMessageCommand),
});

// Immediate handler
router.onInsert(isOrder, async (order, ctx) => {
  console.log('Order received');
});

// Deferred handler - enqueues to SQS
router
  .onInsert(isOrder, async (order, ctx) => {
    // This runs when processing from SQS
    await sendConfirmationEmail(order);
    await generateInvoice(order);
  })
  .defer({ delaySeconds: 30 });

// Stream handler - simplified export with built-in batch failure support
export const streamHandler = router.streamHandler;

// SQS handler - simplified export with built-in batch failure support
export const sqsHandler = router.sqsHandler;
```

## Global Tables (Region Filtering)

Process only records from the current region:

```typescript
const router = new StreamRouter({ sameRegionOnly: true });
```

## Configuration Options

```typescript
const router = new StreamRouter({
  // Stream view type (default: 'NEW_AND_OLD_IMAGES')
  streamViewType: 'NEW_AND_OLD_IMAGES',
  
  // Auto-unmarshall DynamoDB JSON (default: true)
  unmarshall: true,
  
  // Only process same-region records (default: false)
  sameRegionOnly: false,
  
  // Default SQS queue for deferred handlers
  deferQueue: 'https://sqs...',
  
  // SQS client for deferred processing
  sqsClient: { sendMessage: async (params) => { ... } },
});
```

## Stream View Types

Handler signatures adapt based on your DynamoDB stream configuration:

| Stream View Type | INSERT | MODIFY | REMOVE |
|-----------------|--------|--------|--------|
| `KEYS_ONLY` | `(keys, ctx)` | `(keys, ctx)` | `(keys, ctx)` |
| `NEW_IMAGE` | `(newImage, ctx)` | `(undefined, newImage, ctx)` | `(undefined, ctx)` |
| `OLD_IMAGE` | `(undefined, ctx)` | `(oldImage, undefined, ctx)` | `(oldImage, ctx)` |
| `NEW_AND_OLD_IMAGES` | `(newImage, ctx)` | `(oldImage, newImage, ctx)` | `(oldImage, ctx)` |

## Processing Results

```typescript
const result = await router.process(event);
// { processed: 10, succeeded: 9, failed: 1, errors: [...] }

// Or with partial batch failures for Lambda retry
const result = await router.process(event, { reportBatchItemFailures: true });
// { batchItemFailures: [{ itemIdentifier: 'sequence-number' }] }
```

## API Reference

### StreamRouter

```typescript
class StreamRouter<V extends StreamViewType = 'NEW_AND_OLD_IMAGES'> {
  constructor(options?: StreamRouterOptions);
  
  onInsert<T>(matcher, handler, options?): HandlerRegistration;
  onModify<T>(matcher, handler, options?): HandlerRegistration;
  onRemove<T>(matcher, handler, options?): HandlerRegistration;
  use(middleware): this;
  
  process(event, options?): Promise<ProcessingResult | BatchItemFailuresResponse>;
  processDeferred(sqsEvent, options?): Promise<ProcessingResult | BatchItemFailuresResponse>;
}
```

### HandlerRegistration

```typescript
interface HandlerRegistration {
  defer(options?: { queue?: string; delaySeconds?: number }): StreamRouter;
  onInsert(...): HandlerRegistration;
  onModify(...): HandlerRegistration;
  onRemove(...): HandlerRegistration;
}
```

### HandlerContext

```typescript
interface HandlerContext {
  eventName: 'INSERT' | 'MODIFY' | 'REMOVE';
  eventID?: string;
  eventSourceARN?: string;
}
```

## License

Apache-2.0
