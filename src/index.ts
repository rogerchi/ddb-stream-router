// Main exports

// Utility exports
export {
	diffAttributes,
	getNestedValue,
	hasAttributeChange,
} from "./attribute-diff.js";
export { ConfigurationError } from "./errors.js";
export type { UnmarshalledRecord } from "./middleware.js";
export { unmarshallMiddleware } from "./middleware.js";
export { createSQSClient } from "./sqs-adapter.js";
export { HandlerRegistration, StreamRouter } from "./stream-router.js";
// Type exports
export type {
	AttributeChangeType,
	AttributeDiff,
	BatchHandlerOptions,
	BatchInsertHandler,
	BatchItemFailuresResponse,
	BatchModifyHandler,
	BatchRemoveHandler,
	DeferOptions,
	DeferredRecordMessage,
	DiffResult,
	Discriminator,
	HandlerContext,
	HandlerFunction,
	HandlerOptions,
	InsertHandler,
	Logger,
	Matcher,
	MiddlewareFunction,
	ModifyHandler,
	ModifyHandlerOptions,
	Parser,
	PrimaryKeyConfig,
	ProcessingResult,
	ProcessOptions,
	RegisteredHandler,
	RemoveHandler,
	SQSClient,
	StreamRouterOptions,
	StreamViewType,
} from "./types.js";
