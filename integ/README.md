# Integration Tests

This directory contains CDK infrastructure and test scripts for end-to-end integration testing of the ddb-stream-router library.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Integration Test Stack                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────┐     Stream      ┌──────────────────┐          │
│  │   DynamoDB   │ ──────────────► │  Stream Handler  │          │
│  │    Table     │                 │   (Lambda)       │          │
│  └──────────────┘                 └────────┬─────────┘          │
│                                            │                     │
│                                   ┌────────┴────────┐           │
│                                   │                 │           │
│                                   ▼                 ▼           │
│                          ┌──────────────┐  ┌──────────────────┐ │
│                          │ Defer Queue  │  │ Verification     │ │
│                          │  (Standard)  │  │ Queue (FIFO)     │ │
│                          └──────┬───────┘  └──────────────────┘ │
│                                 │                    ▲          │
│                                 ▼                    │          │
│                          ┌──────────────┐            │          │
│                          │ SQS Handler  │────────────┘          │
│                          │  (Lambda)    │                       │
│                          └──────────────┘                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

## Prerequisites

- AWS CLI configured with appropriate credentials
- Node.js 20+
- CDK CLI (`npm install -g aws-cdk`)

## Usage

### Deploy Infrastructure

```bash
npm run integ:deploy
```

This deploys the CDK stack and writes resource ARNs to `.cdk.outputs.integration.json`.

### Run Integration Tests

```bash
npm run integ:test
```

This runs the test script which:
1. Creates a test item in DynamoDB (triggers INSERT handlers)
2. Updates the item (triggers MODIFY handler)
3. Deletes the item (triggers REMOVE handler)
4. Validates messages in the verification queue

### Destroy Infrastructure

```bash
npm run integ:destroy
```

Removes all AWS resources created by the stack.

## Test Flow

1. **INSERT Test**: Creates an item, expects 2 messages (immediate + deferred INSERT)
2. **MODIFY Test**: Updates the item, expects 1 message (immediate MODIFY)
3. **REMOVE Test**: Deletes the item, expects 1 message (immediate REMOVE)

## Verification Messages

Each handler writes a message to the FIFO verification queue with:

```typescript
{
  operationType: 'INSERT' | 'MODIFY' | 'REMOVE',
  isDeferred: boolean,
  pk: string,
  sk: string,
  timestamp: number,
  eventId?: string
}
```

## Files

- `stack.ts` - CDK stack definition
- `handler.ts` - Lambda handler using StreamRouter
- `run-tests.ts` - Integration test runner
- `cdk.json` - CDK configuration
- `tsconfig.json` - TypeScript configuration for CDK
