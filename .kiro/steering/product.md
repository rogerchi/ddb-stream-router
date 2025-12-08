# Product Overview

**@rogerchi/ddb-stream-router** is a TypeScript library that provides Express-like routing for AWS DynamoDB Stream events.

## Core Purpose

Simplifies processing DynamoDB Stream events by providing a familiar, type-safe routing API similar to Express.js. Handlers can be registered for INSERT, MODIFY, and REMOVE operations with automatic type inference and validation.

## Key Features

- **Express-like routing API** - `.onInsert()`, `.onModify()`, `.onRemove()`, `.use()` methods
- **Type safety** - Full TypeScript inference from discriminators and schema validators (Zod)
- **Attribute filtering** - React only to specific attribute changes in MODIFY events
- **Batch processing** - Group and process multiple records together
- **Middleware support** - Intercept and transform records before handlers
- **Deferred processing** - Automatically enqueue heavy processing to SQS
- **Global table support** - Filter records by region
- **Partial batch failures** - Lambda retry support via `reportBatchItemFailures`

## Target Use Cases

- Processing DynamoDB Stream events in AWS Lambda
- Type-safe event routing with discriminators or schema validators
- Deferring heavy processing to SQS to keep stream processing fast
- Reacting to specific attribute changes without manual diffing
- Batch processing related records together
