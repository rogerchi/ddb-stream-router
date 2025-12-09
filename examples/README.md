# DynamoDB Stream Router Examples

This directory contains example usage patterns for the DynamoDB Stream Router library.

## Examples

### [basic-usage.ts](./basic-usage.ts)
Simple setup with discriminator functions for INSERT, MODIFY, and REMOVE events.

### [with-zod-parser.ts](./with-zod-parser.ts)
Using Zod schemas for runtime validation and type-safe parsed data.

### [deferred-processing.ts](./deferred-processing.ts)
Deferring heavy processing to SQS to avoid blocking stream processing.

### [batch-processing.ts](./batch-processing.ts)
Collecting multiple records and processing them together for bulk operations.

### [middleware.ts](./middleware.ts)
Using middleware for logging, metrics, filtering, and enrichment.

### [global-tables.ts](./global-tables.ts)
Handling DynamoDB Global Tables with same-region filtering.

### [attribute-filtering.ts](./attribute-filtering.ts)
Filtering MODIFY events based on specific attribute changes.

### [value-based-filtering.ts](./value-based-filtering.ts)
Advanced filtering based on specific field values (old and new) and detecting when fields are cleared.

## Running Examples

These examples are TypeScript files meant to be used as Lambda handlers. They demonstrate patterns and are not meant to be run directly.

To use in your project:

```bash
npm install ddb-stream-router @aws-sdk/util-dynamodb
```

Then import and adapt the patterns to your use case.
