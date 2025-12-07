import type { DynamoDBRecord } from "aws-lambda";

// Stream view type configuration
export type StreamViewType =
	| "KEYS_ONLY"
	| "NEW_IMAGE"
	| "OLD_IMAGE"
	| "NEW_AND_OLD_IMAGES";

// Router configuration options
export interface StreamRouterOptions {
	streamViewType?: StreamViewType;
	unmarshall?: boolean; // Whether to unmarshall DynamoDB JSON to native JS (default: true)
	sameRegionOnly?: boolean; // Only process records from the same region as the Lambda (default: false)
}

// Attribute change types for MODIFY filtering
export type AttributeChangeType =
	| "new_attribute"
	| "remove_attribute"
	| "changed_attribute"
	| "new_item_in_collection"
	| "remove_item_from_collection"
	| "changed_item_in_collection";

// Options for modify handlers
export interface ModifyHandlerOptions {
	attribute?: string;
	changeType?: AttributeChangeType | AttributeChangeType[];
}

// Generic handler options
export interface HandlerOptions {
	batch?: boolean; // When true, handler receives all matching records as array
}

// Batch handler options with grouping key
export interface BatchHandlerOptions extends HandlerOptions {
	batch: true;
	batchKey?: string | ((record: unknown) => string); // Key to group records by
}

// Discriminator function - type guard pattern
export type Discriminator<T> = (record: unknown) => record is T;

// Parser function - validation + transformation pattern (Zod-compatible)
export interface Parser<T> {
	parse(data: unknown): T;
	safeParse(
		data: unknown,
	): { success: true; data: T } | { success: false; error: unknown };
}


// Union type for matcher parameter
export type Matcher<T> = Discriminator<T> | Parser<T>;

// Base handler context
export interface HandlerContext {
	eventName: "INSERT" | "MODIFY" | "REMOVE";
	eventID?: string;
	eventSourceARN?: string;
}

// Processing result
export interface ProcessingResult {
	processed: number;
	succeeded: number;
	failed: number;
	errors: Array<{
		recordId: string;
		error: Error;
		phase?: "middleware" | "handler";
	}>;
}

// Generic handler function type
export type HandlerFunction = (
	...args: unknown[]
) => void | Promise<void>;

// Internal handler registration
export interface RegisteredHandler<T = unknown> {
	id: string;
	eventType: "INSERT" | "MODIFY" | "REMOVE";
	matcher: Matcher<T>;
	handler: HandlerFunction;
	options: HandlerOptions | ModifyHandlerOptions | BatchHandlerOptions;
	isParser: boolean;
}

// Attribute diff result
export interface AttributeDiff {
	attribute: string;
	changeType: AttributeChangeType;
	oldValue?: unknown;
	newValue?: unknown;
}

export interface DiffResult {
	hasChanges: boolean;
	changes: AttributeDiff[];
}

// Middleware function signature
export type MiddlewareFunction = (
	record: DynamoDBRecord,
	next: () => Promise<void>,
) => void | Promise<void>;

// Handler signatures per stream view type
export type InsertHandler<T, V extends StreamViewType> = V extends "KEYS_ONLY"
	? (keys: Record<string, unknown>, ctx: HandlerContext) => void | Promise<void>
	: V extends "NEW_IMAGE" | "NEW_AND_OLD_IMAGES"
		? (newImage: T, ctx: HandlerContext) => void | Promise<void>
		: V extends "OLD_IMAGE"
			? (oldImage: undefined, ctx: HandlerContext) => void | Promise<void>
			: never;

export type ModifyHandler<T, V extends StreamViewType> = V extends "KEYS_ONLY"
	? (keys: Record<string, unknown>, ctx: HandlerContext) => void | Promise<void>
	: V extends "NEW_IMAGE"
		? (
				oldImage: undefined,
				newImage: T,
				ctx: HandlerContext,
			) => void | Promise<void>
		: V extends "OLD_IMAGE"
			? (
					oldImage: T,
					newImage: undefined,
					ctx: HandlerContext,
				) => void | Promise<void>
			: V extends "NEW_AND_OLD_IMAGES"
				? (oldImage: T, newImage: T, ctx: HandlerContext) => void | Promise<void>
				: never;

export type RemoveHandler<T, V extends StreamViewType> = V extends "KEYS_ONLY"
	? (keys: Record<string, unknown>, ctx: HandlerContext) => void | Promise<void>
	: V extends "OLD_IMAGE" | "NEW_AND_OLD_IMAGES"
		? (oldImage: T, ctx: HandlerContext) => void | Promise<void>
		: V extends "NEW_IMAGE"
			? (newImage: undefined, ctx: HandlerContext) => void | Promise<void>
			: never;

// Batch handler signatures - receive arrays of records
export type BatchInsertHandler<T, V extends StreamViewType> =
	V extends "KEYS_ONLY"
		? (
				records: Array<{ keys: Record<string, unknown>; ctx: HandlerContext }>,
			) => void | Promise<void>
		: V extends "NEW_IMAGE" | "NEW_AND_OLD_IMAGES"
			? (
					records: Array<{ newImage: T; ctx: HandlerContext }>,
				) => void | Promise<void>
			: never;

export type BatchModifyHandler<T, V extends StreamViewType> =
	V extends "KEYS_ONLY"
		? (
				records: Array<{ keys: Record<string, unknown>; ctx: HandlerContext }>,
			) => void | Promise<void>
		: V extends "NEW_AND_OLD_IMAGES"
			? (
					records: Array<{ oldImage: T; newImage: T; ctx: HandlerContext }>,
				) => void | Promise<void>
			: never;

export type BatchRemoveHandler<T, V extends StreamViewType> =
	V extends "KEYS_ONLY"
		? (
				records: Array<{ keys: Record<string, unknown>; ctx: HandlerContext }>,
			) => void | Promise<void>
		: V extends "OLD_IMAGE" | "NEW_AND_OLD_IMAGES"
			? (
					records: Array<{ oldImage: T; ctx: HandlerContext }>,
				) => void | Promise<void>
			: never;
