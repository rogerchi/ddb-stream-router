// Main exports

// Utility exports
export {
	diffAttributes,
	getNestedValue,
	hasAttributeChange,
} from "./attribute-diff";
export { ConfigurationError } from "./errors";
export type { UnmarshalledRecord } from "./middleware";
export { unmarshallMiddleware } from "./middleware";
export { HandlerRegistration, StreamRouter } from "./stream-router";
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
} from "./types";
