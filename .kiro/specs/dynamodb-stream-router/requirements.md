# Requirements Document

## Introduction

The DynamoDB Stream Router is a TypeScript library that provides an Express-like API for handling DynamoDB Stream events. It enables developers to register handlers for INSERT, MODIFY, and REMOVE operations with type-safe parsing and discrimination capabilities. The library supports middleware patterns and adapts handler signatures based on the configured stream view type.

## Glossary

- **Stream_Router**: The main application class that manages event routing and handler registration for DynamoDB Stream events
- **Handler**: A callback function that processes a specific type of DynamoDB Stream record
- **Middleware**: A function that intercepts and can modify stream records before they reach handlers
- **Discriminator_Function**: A type guard function that determines if a record matches a specific type without parsing
- **Parser_Function**: A function (such as a Zod schema) that validates and transforms stream record data into a typed object
- **Stream_View_Type**: A DynamoDB configuration that determines which image data is included in stream records (KEYS_ONLY, NEW_IMAGE, OLD_IMAGE, NEW_AND_OLD_IMAGES)
- **Old_Image**: The item attributes before modification in a DynamoDB Stream record
- **New_Image**: The item attributes after modification in a DynamoDB Stream record
- **Attribute_Change_Type**: The type of change that occurred to a specific attribute (new_attribute, remove_attribute, changed_attribute, new_item_in_collection, remove_item_from_collection, changed_item_in_collection)
- **Collection_Type**: A DynamoDB attribute type that contains multiple values (List, Map, or Set)
- **DynamoDB_Local**: A downloadable version of DynamoDB that enables local development and testing without connecting to the DynamoDB web service
- **Batch_Mode**: A handler configuration that collects all matching records and invokes the handler once with an array instead of invoking per-record
- **Batch_Key**: A function or attribute name used to group records together when batching
- **DynamoDB_JSON**: The wire format used by DynamoDB that includes type descriptors (e.g., {S: "value"}, {N: "123"})
- **Unmarshall**: The process of converting DynamoDB JSON format to native JavaScript objects (controlled by unmarshall option, defaults to true)
- **Same_Region_Only**: A configuration option that filters records to only process those originating from the same AWS region as the Lambda function (useful for global tables)
- **Event_Source_ARN**: The Amazon Resource Name identifying the DynamoDB stream that triggered the event, contains the region information

## Requirements

### Requirement 1

**User Story:** As a developer, I want to create a Stream Router instance with configurable options, so that I can customize the behavior based on my DynamoDB Stream configuration.

#### Acceptance Criteria

1. WHEN a developer creates a Stream_Router instance with a streamViewType option THEN the Stream_Router SHALL store the configuration and use it to determine handler signatures
2. WHEN a developer creates a Stream_Router instance without options THEN the Stream_Router SHALL default to NEW_AND_OLD_IMAGES stream view type
3. WHEN a developer provides an invalid streamViewType value THEN the Stream_Router SHALL reject the configuration with a descriptive error

### Requirement 2

**User Story:** As a developer, I want to register handlers for INSERT events, so that I can process new items added to my DynamoDB table.

#### Acceptance Criteria

1. WHEN a developer calls the insert method with a discriminator function and handler THEN the Stream_Router SHALL register the handler for INSERT events matching the discriminator
2. WHEN a developer calls the insert method with a parser function and handler THEN the Stream_Router SHALL register the handler and parse the newImage using the parser before invoking the handler
3. WHEN an INSERT event is received and the discriminator returns true THEN the Stream_Router SHALL invoke the registered handler with the appropriate image data
4. WHEN an INSERT event is received and the parser successfully validates the data THEN the Stream_Router SHALL invoke the handler with the parsed and typed data
5. WHEN an INSERT event is received and the parser fails validation THEN the Stream_Router SHALL skip the handler and continue processing other handlers

### Requirement 3

**User Story:** As a developer, I want to register handlers for MODIFY events, so that I can process updates to existing items in my DynamoDB table.

#### Acceptance Criteria

1. WHEN a developer calls the modify method with a discriminator function and handler THEN the Stream_Router SHALL register the handler for MODIFY events matching the discriminator
2. WHEN a developer calls the modify method with a parser function and handler THEN the Stream_Router SHALL register the handler and parse both oldImage and newImage using the parser before invoking the handler
3. WHEN a MODIFY event is received and the discriminator returns true THEN the Stream_Router SHALL invoke the registered handler with the appropriate image data
4. WHEN a MODIFY event is received and the parser successfully validates the data THEN the Stream_Router SHALL invoke the handler with the parsed and typed data
5. WHEN a MODIFY event is received and the parser fails validation THEN the Stream_Router SHALL skip the handler and continue processing other handlers
6. WHEN a developer calls the modify method with an attribute filter option THEN the Stream_Router SHALL only invoke the handler when the specified attribute has changed
7. WHEN a developer specifies a change type filter (new_attribute, remove_attribute, changed_attribute, new_item_in_collection, remove_item_from_collection, changed_item_in_collection) THEN the Stream_Router SHALL only invoke the handler when the attribute change matches the specified type

### Requirement 4

**User Story:** As a developer, I want to register handlers for REMOVE events, so that I can process deletions from my DynamoDB table.

#### Acceptance Criteria

1. WHEN a developer calls the remove method with a discriminator function and handler THEN the Stream_Router SHALL register the handler for REMOVE events matching the discriminator
2. WHEN a developer calls the remove method with a parser function and handler THEN the Stream_Router SHALL register the handler and parse the oldImage using the parser before invoking the handler
3. WHEN a REMOVE event is received and the discriminator returns true THEN the Stream_Router SHALL invoke the registered handler with the appropriate image data
4. WHEN a REMOVE event is received and the parser successfully validates the data THEN the Stream_Router SHALL invoke the handler with the parsed and typed data
5. WHEN a REMOVE event is received and the parser fails validation THEN the Stream_Router SHALL skip the handler and continue processing other handlers

### Requirement 5

**User Story:** As a developer, I want to register middleware functions, so that I can intercept and process all stream records before they reach specific handlers.

#### Acceptance Criteria

1. WHEN a developer calls the use method with a middleware function THEN the Stream_Router SHALL register the middleware in the order it was added
2. WHEN a stream event is processed THEN the Stream_Router SHALL execute all registered middleware functions in registration order before invoking handlers
3. WHEN middleware calls the next function THEN the Stream_Router SHALL continue to the next middleware or handler in the chain
4. WHEN middleware throws an error THEN the Stream_Router SHALL stop processing the current record and propagate the error

### Requirement 6

**User Story:** As a developer, I want handler signatures to adapt based on the stream view type, so that I receive only the data that is available in my stream configuration.

#### Acceptance Criteria

1. WHEN streamViewType is KEYS_ONLY THEN the Stream_Router SHALL provide handlers with only the key attributes from the record
2. WHEN streamViewType is NEW_IMAGE THEN the Stream_Router SHALL provide handlers with only the newImage data (oldImage will be undefined)
3. WHEN streamViewType is OLD_IMAGE THEN the Stream_Router SHALL provide handlers with only the oldImage data (newImage will be undefined)
4. WHEN streamViewType is NEW_AND_OLD_IMAGES THEN the Stream_Router SHALL provide handlers with both oldImage and newImage data

### Requirement 7

**User Story:** As a developer, I want to process DynamoDB Stream events through the router, so that my registered handlers are invoked for matching records.

#### Acceptance Criteria

1. WHEN the Stream_Router receives a DynamoDB Stream event THEN the Stream_Router SHALL iterate through all records in the event
2. WHEN a record matches a registered handler's discriminator or parser THEN the Stream_Router SHALL invoke that handler with the appropriate data
3. WHEN multiple handlers match a single record THEN the Stream_Router SHALL invoke all matching handlers in registration order
4. WHEN no handlers match a record THEN the Stream_Router SHALL skip the record without error
5. WHEN processing completes successfully THEN the Stream_Router SHALL return a summary of processed records

### Requirement 8

**User Story:** As a developer, I want type-safe handler parameters, so that I can leverage TypeScript's type system when processing stream records.

#### Acceptance Criteria

1. WHEN a discriminator function is provided THEN the Stream_Router SHALL infer the handler parameter types from the discriminator's type guard
2. WHEN a parser function (such as Zod schema) is provided THEN the Stream_Router SHALL infer the handler parameter types from the parser's output type
3. WHEN the streamViewType restricts available images THEN the Stream_Router SHALL reflect this in the handler's type signature by making unavailable images undefined

### Requirement 9

**User Story:** As a developer, I want to detect specific attribute-level changes in MODIFY events, so that I can react to granular changes within my DynamoDB items.

#### Acceptance Criteria

1. WHEN a developer specifies an attribute name in the modify options THEN the Stream_Router SHALL compare the oldImage and newImage to detect changes to that attribute
2. WHEN the change type is new_attribute THEN the Stream_Router SHALL invoke the handler only when the attribute exists in newImage but not in oldImage
3. WHEN the change type is remove_attribute THEN the Stream_Router SHALL invoke the handler only when the attribute exists in oldImage but not in newImage
4. WHEN the change type is changed_attribute THEN the Stream_Router SHALL invoke the handler only when the attribute value differs between oldImage and newImage
5. WHEN the change type is new_item_in_collection THEN the Stream_Router SHALL invoke the handler only when a List, Map, or Set attribute has new items added
6. WHEN the change type is remove_item_from_collection THEN the Stream_Router SHALL invoke the handler only when a List, Map, or Set attribute has items removed
7. WHEN the change type is changed_item_in_collection THEN the Stream_Router SHALL invoke the handler only when items within a List or Map attribute have been modified
8. WHEN multiple attribute filters are specified THEN the Stream_Router SHALL invoke the handler when any of the specified conditions are met

### Requirement 10

**User Story:** As a developer, I want to batch matching records together, so that I can process multiple related records in a single handler invocation.

#### Acceptance Criteria

1. WHEN a developer registers a handler with batch option set to true THEN the Stream_Router SHALL collect all matching records and invoke the handler once with an array of records
2. WHEN a developer provides a batchKey function THEN the Stream_Router SHALL group matching records by the key returned from the function
3. WHEN a developer provides a batchKey string THEN the Stream_Router SHALL group matching records by the value of that attribute in the record
4. WHEN batch mode is enabled and multiple records match THEN the Stream_Router SHALL invoke the handler with all matching records as an array
5. WHEN batch mode is enabled and only one record matches THEN the Stream_Router SHALL invoke the handler with a single-element array

### Requirement 11

**User Story:** As a developer, I want to control whether records are unmarshalled, so that I can work with either DynamoDB JSON format or native JavaScript objects based on my preference.

#### Acceptance Criteria

1. WHEN a developer creates a Stream_Router with unmarshall option set to true THEN the Stream_Router SHALL convert DynamoDB JSON format to native JavaScript objects before passing to discriminators, parsers, and handlers
2. WHEN a developer creates a Stream_Router without specifying the unmarshall option THEN the Stream_Router SHALL default to unmarshalling records (unmarshall: true)
3. WHEN a developer creates a Stream_Router with unmarshall option set to false THEN the Stream_Router SHALL pass records in raw DynamoDB JSON format to discriminators, parsers, and handlers
4. WHEN unmarshalling is enabled THEN the Stream_Router SHALL use the @aws-sdk/util-dynamodb unmarshall function

### Requirement 13

**User Story:** As a developer, I want to filter records by region, so that I can process only records originating from the same region as my Lambda function when using global tables.

#### Acceptance Criteria

1. WHEN a developer creates a Stream_Router with sameRegionOnly option set to true THEN the Stream_Router SHALL skip records where the Event_Source_ARN region differs from the AWS_REGION environment variable
2. WHEN a developer creates a Stream_Router without specifying the sameRegionOnly option THEN the Stream_Router SHALL default to processing all records regardless of region (sameRegionOnly: false)
3. WHEN sameRegionOnly is enabled and a record's Event_Source_ARN region matches AWS_REGION THEN the Stream_Router SHALL process the record normally
4. WHEN sameRegionOnly is enabled and AWS_REGION is not set THEN the Stream_Router SHALL process all records (fail-open behavior)

### Requirement 14

**User Story:** As a developer, I want comprehensive test coverage for the Stream Router, so that I can trust the library behaves correctly in production.

#### Acceptance Criteria

1. WHEN unit tests are executed THEN the test suite SHALL verify all handler registration methods (insert, modify, remove, use) function correctly
2. WHEN unit tests are executed THEN the test suite SHALL verify discriminator and parser functions are invoked correctly
3. WHEN unit tests are executed THEN the test suite SHALL verify attribute change detection logic for all change types
4. WHEN unit tests are executed THEN the test suite SHALL verify middleware execution order and error propagation
5. WHEN integration tests are executed with DynamoDB Local THEN the test suite SHALL verify end-to-end stream event processing
6. WHEN integration tests are executed THEN the test suite SHALL verify INSERT, MODIFY, and REMOVE events trigger appropriate handlers
7. WHEN integration tests are executed THEN the test suite SHALL verify stream view type configurations produce correct handler signatures
8. WHEN unit tests are executed THEN the test suite SHALL verify sameRegionOnly filtering correctly skips cross-region records
