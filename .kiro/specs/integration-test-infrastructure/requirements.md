# Requirements Document

## Introduction

This feature provides a CDK-based integration test infrastructure for verifying the ddb-stream-router library works correctly in a deployed AWS environment. The infrastructure deploys a DynamoDB table with streams, Lambda handlers using the router, SQS queues for deferred processing and test verification, and the necessary event source mappings. A test runner performs DynamoDB operations and validates that stream and deferred handlers execute correctly by checking messages in a FIFO verification queue.

## Glossary

- **Integration Test Stack**: A CDK stack containing all AWS resources needed for end-to-end testing
- **Test Table**: A DynamoDB table with streams enabled for triggering Lambda handlers
- **Stream Handler**: A Lambda function that processes DynamoDB stream events using StreamRouter
- **Defer Queue**: A standard SQS queue where deferred handlers enqueue records for async processing
- **Verification Queue**: A FIFO SQS queue where handlers write operation results for test assertions
- **Test Runner**: A script that performs DynamoDB operations and validates handler behavior via the verification queue

## Requirements

### Requirement 1

**User Story:** As a library maintainer, I want to deploy test infrastructure with a single command, so that I can quickly set up an environment for integration testing.

#### Acceptance Criteria

1. WHEN a user runs the CDK deploy command THEN the Integration Test Stack SHALL create a DynamoDB table with stream enabled using NEW_AND_OLD_IMAGES view type
2. WHEN a user runs the CDK deploy command THEN the Integration Test Stack SHALL create a standard SQS queue for deferred processing
3. WHEN a user runs the CDK deploy command THEN the Integration Test Stack SHALL create a FIFO SQS queue for test verification
4. WHEN a user runs the CDK deploy command THEN the Integration Test Stack SHALL create a Lambda function with the StreamRouter handler code bundled
5. WHEN deployment completes THEN the Integration Test Stack SHALL output resource ARNs and URLs to a JSON file at a configurable path

### Requirement 2

**User Story:** As a library maintainer, I want the Lambda handler to use StreamRouter with both immediate and deferred handlers, so that I can verify all routing functionality works in production.

#### Acceptance Criteria

1. WHEN a DynamoDB INSERT event occurs THEN the Stream Handler SHALL route the event to an immediate handler that writes to the Verification Queue
2. WHEN a DynamoDB MODIFY event occurs THEN the Stream Handler SHALL route the event to an immediate handler that writes to the Verification Queue
3. WHEN a DynamoDB REMOVE event occurs THEN the Stream Handler SHALL route the event to an immediate handler that writes to the Verification Queue
4. WHEN a DynamoDB INSERT event occurs THEN the Stream Handler SHALL route the event to a deferred handler that enqueues to the Defer Queue
5. WHEN the Defer Queue receives a message THEN the Stream Handler SHALL process the deferred record and write to the Verification Queue

### Requirement 3

**User Story:** As a library maintainer, I want to run integration tests that verify handler execution, so that I can confirm the library works correctly when deployed.

#### Acceptance Criteria

1. WHEN the Test Runner creates an item in the Test Table THEN the Verification Queue SHALL receive messages for both immediate INSERT and deferred INSERT operations
2. WHEN the Test Runner modifies an item in the Test Table THEN the Verification Queue SHALL receive a message for the immediate MODIFY operation
3. WHEN the Test Runner deletes an item from the Test Table THEN the Verification Queue SHALL receive a message for the immediate REMOVE operation
4. WHEN the Test Runner polls the Verification Queue THEN the Test Runner SHALL validate message contents match expected operation types and item data
5. WHEN all expected messages are received and validated THEN the Test Runner SHALL report success with operation counts

### Requirement 4

**User Story:** As a library maintainer, I want to clean up test infrastructure easily, so that I don't incur unnecessary AWS costs.

#### Acceptance Criteria

1. WHEN a user runs the CDK destroy command THEN the Integration Test Stack SHALL remove all created resources
2. WHEN the Test Table contains items during destroy THEN the Integration Test Stack SHALL allow deletion by configuring removal policy appropriately

### Requirement 5

**User Story:** As a library maintainer, I want verification messages to contain sufficient context, so that I can debug failures and validate correct behavior.

#### Acceptance Criteria

1. WHEN a handler writes to the Verification Queue THEN the message SHALL include the operation type (INSERT, MODIFY, REMOVE)
2. WHEN a handler writes to the Verification Queue THEN the message SHALL include whether it was immediate or deferred
3. WHEN a handler writes to the Verification Queue THEN the message SHALL include the item's primary key values
4. WHEN a handler writes to the Verification Queue THEN the message SHALL include a timestamp for ordering verification
