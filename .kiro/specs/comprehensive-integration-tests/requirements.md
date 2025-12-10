# Requirements Document

## Introduction

This feature extends the existing integration test infrastructure to provide comprehensive end-to-end testing of all ddb-stream-router library capabilities. The current integration tests cover basic INSERT/MODIFY/REMOVE handlers, deferred processing, and value-based filtering. This enhancement adds coverage for batch processing, middleware, TTL removal handling, nested attribute filtering, Zod schema validation, validation target options, and field_cleared change detection. The goal is to verify all documented library features work correctly in a deployed AWS environment.

## Glossary

- **Integration Test Stack**: A CDK stack containing all AWS resources needed for end-to-end testing
- **Test Table**: A DynamoDB table with streams enabled for triggering Lambda handlers
- **Stream Handler**: A Lambda function that processes DynamoDB stream events using StreamRouter
- **Defer Queue**: A standard SQS queue where deferred handlers enqueue records for async processing
- **Verification Queue**: A FIFO SQS queue where handlers write operation results for test assertions
- **Test Runner**: A script that performs DynamoDB operations and validates handler behavior via the verification queue
- **Batch Handler**: A handler that collects multiple matching records and processes them together
- **Middleware**: Functions that intercept records before handlers for logging, filtering, or enrichment
- **TTL Removal**: A DynamoDB REMOVE event triggered by Time-To-Live expiration rather than explicit deletion
- **Discriminator**: A type guard function that determines if a record matches a specific type
- **Parser**: A schema validator (like Zod) with a safeParse method for runtime validation
- **Validation Target**: Configuration specifying which image(s) to validate (newImage, oldImage, or both)

## Requirements

### Requirement 1

**User Story:** As a library maintainer, I want to test batch processing handlers, so that I can verify records are correctly grouped and processed together.

#### Acceptance Criteria

1. WHEN multiple INSERT events occur for items with the same batch key THEN the Stream Handler SHALL invoke the batch handler once with all matching records
2. WHEN multiple MODIFY events occur for items with the same primary key THEN the Stream Handler SHALL group records by primary key and invoke the batch handler for each group
3. WHEN batch processing completes THEN the Verification Queue SHALL receive a message containing the count of records processed in the batch
4. WHEN records have different batch keys THEN the Stream Handler SHALL invoke the batch handler separately for each batch key group

### Requirement 2

**User Story:** As a library maintainer, I want to test middleware functionality, so that I can verify middleware executes in order and can filter or enrich records.

#### Acceptance Criteria

1. WHEN middleware is registered THEN the Stream Handler SHALL execute middleware in registration order before handlers
2. WHEN middleware does not call next() THEN the Stream Handler SHALL skip all subsequent middleware and handlers for that record
3. WHEN middleware modifies the record THEN the Stream Handler SHALL pass the modified record to subsequent middleware and handlers
4. WHEN middleware completes successfully THEN the Verification Queue SHALL receive a message indicating middleware execution

### Requirement 3

**User Story:** As a library maintainer, I want to test TTL removal handling, so that I can verify TTL-triggered deletions are distinguished from user-initiated deletions.

#### Acceptance Criteria

1. WHEN a TTL-triggered REMOVE event occurs THEN the onTTLRemove handler SHALL execute and write to the Verification Queue
2. WHEN a user-initiated REMOVE event occurs THEN the onTTLRemove handler SHALL NOT execute
3. WHEN a user-initiated REMOVE event occurs with excludeTTL option THEN the onRemove handler SHALL execute and write to the Verification Queue
4. WHEN a TTL-triggered REMOVE event occurs with excludeTTL option THEN the onRemove handler SHALL NOT execute

### Requirement 4

**User Story:** As a library maintainer, I want to test nested attribute filtering, so that I can verify handlers trigger only when specific nested fields change.

#### Acceptance Criteria

1. WHEN a nested attribute changes (e.g., preferences.theme) THEN the handler with matching nested attribute filter SHALL execute
2. WHEN a sibling nested attribute changes (e.g., preferences.notifications) THEN the handler filtering on a different nested path SHALL NOT execute
3. WHEN any nested attribute under a parent path changes THEN the handler filtering on the parent path SHALL execute
4. WHEN the nested attribute value remains unchanged THEN the handler with nested attribute filter SHALL NOT execute

### Requirement 5

**User Story:** As a library maintainer, I want to test field_cleared change detection, so that I can verify handlers trigger when attributes are set to null or removed.

#### Acceptance Criteria

1. WHEN an attribute is set to null THEN the handler with field_cleared changeType SHALL execute
2. WHEN an attribute is removed from the item THEN the handler with field_cleared changeType SHALL execute
3. WHEN an attribute value changes from one non-null value to another THEN the handler with field_cleared changeType SHALL NOT execute
4. WHEN an attribute is added (was undefined, now has value) THEN the handler with field_cleared changeType SHALL NOT execute

### Requirement 6

**User Story:** As a library maintainer, I want to test Zod schema validation, so that I can verify parsers correctly validate and type records.

#### Acceptance Criteria

1. WHEN an INSERT event matches a Zod schema THEN the handler SHALL receive parsed and validated data
2. WHEN an INSERT event does not match a Zod schema THEN the handler SHALL NOT execute for that record
3. WHEN a MODIFY event matches a Zod schema THEN both oldImage and newImage SHALL be parsed and validated
4. WHEN schema validation fails THEN the record SHALL be skipped without error

### Requirement 7

**User Story:** As a library maintainer, I want to test validation target options, so that I can verify handlers can validate against oldImage, newImage, or both.

#### Acceptance Criteria

1. WHEN validationTarget is "newImage" THEN the handler SHALL validate only the new image against the matcher
2. WHEN validationTarget is "oldImage" THEN the handler SHALL validate only the old image against the matcher
3. WHEN validationTarget is "both" THEN the handler SHALL validate both images and execute only when both match
4. WHEN validationTarget is "both" and only one image matches THEN the handler SHALL NOT execute

### Requirement 8

**User Story:** As a library maintainer, I want to test multiple change types with OR logic, so that I can verify handlers trigger on any of the specified change types.

#### Acceptance Criteria

1. WHEN changeType is an array and any specified change type occurs THEN the handler SHALL execute
2. WHEN changeType is an array and none of the specified change types occur THEN the handler SHALL NOT execute
3. WHEN changeType includes both "field_cleared" and "changed_attribute" THEN the handler SHALL execute for either condition

### Requirement 9

**User Story:** As a library maintainer, I want to test deferred batch handlers, so that I can verify batch handlers work correctly with deferred processing.

#### Acceptance Criteria

1. WHEN a deferred batch handler matches multiple records THEN each record SHALL be enqueued individually to SQS
2. WHEN deferred records are processed from SQS THEN the handler SHALL execute for each record
3. WHEN deferred processing completes THEN the Verification Queue SHALL receive messages for each processed record

### Requirement 10

**User Story:** As a library maintainer, I want comprehensive test reporting, so that I can quickly identify which features pass or fail.

#### Acceptance Criteria

1. WHEN all tests complete THEN the Test Runner SHALL report total tests, passed tests, and failed tests
2. WHEN a test fails THEN the Test Runner SHALL display the expected and actual values
3. WHEN tests complete THEN the Test Runner SHALL exit with code 0 for success and code 1 for failure
4. WHEN tests run THEN the Test Runner SHALL organize results by feature category

