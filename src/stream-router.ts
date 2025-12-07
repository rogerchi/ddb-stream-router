import { unmarshall } from "@aws-sdk/util-dynamodb";
import type {
	AttributeValue,
	DynamoDBRecord,
	DynamoDBStreamEvent,
} from "aws-lambda";
import { ConfigurationError } from "./errors";
import type {
	BatchHandlerOptions,
	HandlerContext,
	HandlerFunction,
	HandlerOptions,
	InsertHandler,
	Matcher,
	MiddlewareFunction,
	ModifyHandler,
	ModifyHandlerOptions,
	Parser,
	ProcessingResult,
	RegisteredHandler,
	RemoveHandler,
	StreamRouterOptions,
	StreamViewType,
} from "./types";

const VALID_STREAM_VIEW_TYPES: StreamViewType[] = [
	"KEYS_ONLY",
	"NEW_IMAGE",
	"OLD_IMAGE",
	"NEW_AND_OLD_IMAGES",
];

export class StreamRouter<V extends StreamViewType = "NEW_AND_OLD_IMAGES"> {
	private readonly _streamViewType: V;
	private readonly _unmarshall: boolean;
	private readonly _sameRegionOnly: boolean;
	private readonly _handlers: RegisteredHandler[] = [];
	private readonly _middleware: MiddlewareFunction[] = [];

	constructor(options?: StreamRouterOptions) {
		const streamViewType = (options?.streamViewType ??
			"NEW_AND_OLD_IMAGES") as V;

		if (!VALID_STREAM_VIEW_TYPES.includes(streamViewType)) {
			throw new ConfigurationError(
				`Invalid streamViewType: "${streamViewType}". Valid options are: ${VALID_STREAM_VIEW_TYPES.join(", ")}`,
			);
		}

		this._streamViewType = streamViewType;
		this._unmarshall = options?.unmarshall ?? true;
		this._sameRegionOnly = options?.sameRegionOnly ?? false;
	}

	get streamViewType(): V {
		return this._streamViewType;
	}

	get unmarshall(): boolean {
		return this._unmarshall;
	}

	get sameRegionOnly(): boolean {
		return this._sameRegionOnly;
	}

	/** @internal */
	get handlers(): RegisteredHandler[] {
		return this._handlers;
	}

	/** @internal */
	get middleware(): MiddlewareFunction[] {
		return this._middleware;
	}

	/**
	 * Checks if a record originated from the same region as the Lambda function.
	 * Extracts region from eventSourceARN (format: arn:aws:dynamodb:REGION:account:table/...)
	 */
	isRecordFromSameRegion(eventSourceARN: string | undefined): boolean {
		if (!eventSourceARN) {
			return true; // If no ARN, allow processing
		}

		const lambdaRegion = process.env.AWS_REGION;
		if (!lambdaRegion) {
			return true; // If no Lambda region set, allow processing
		}

		// ARN format: arn:aws:dynamodb:region:account-id:table/table-name/stream/timestamp
		const arnParts = eventSourceARN.split(":");
		if (arnParts.length < 4) {
			return true; // Invalid ARN format, allow processing
		}

		const recordRegion = arnParts[3];
		return recordRegion === lambdaRegion;
	}

	/**
	 * Detects if a matcher is a parser (has safeParse method) or a discriminator function.
	 */
	private isParser<T>(matcher: Matcher<T>): matcher is Parser<T> {
		return (
			typeof matcher === "object" &&
			matcher !== null &&
			"safeParse" in matcher &&
			typeof matcher.safeParse === "function"
		);
	}

	/**
	 * Generates a unique handler ID.
	 */
	private generateHandlerId(): string {
		return `handler_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`;
	}

	/**
	 * Register a handler for INSERT events.
	 */
	insert<T>(
		matcher: Matcher<T>,
		handler: InsertHandler<T, V>,
		options?: HandlerOptions | BatchHandlerOptions,
	): this {
		const registration: RegisteredHandler<T> = {
			id: this.generateHandlerId(),
			eventType: "INSERT",
			matcher,
			handler: handler as HandlerFunction,
			options: options ?? {},
			isParser: this.isParser(matcher),
		};
		this._handlers.push(registration as RegisteredHandler);
		return this;
	}

	/**
	 * Register a handler for MODIFY events.
	 */
	modify<T>(
		matcher: Matcher<T>,
		handler: ModifyHandler<T, V>,
		options?:
			| ModifyHandlerOptions
			| (BatchHandlerOptions & ModifyHandlerOptions),
	): this {
		const registration: RegisteredHandler<T> = {
			id: this.generateHandlerId(),
			eventType: "MODIFY",
			matcher,
			handler: handler as HandlerFunction,
			options: options ?? {},
			isParser: this.isParser(matcher),
		};
		this._handlers.push(registration as RegisteredHandler);
		return this;
	}

	/**
	 * Register a handler for REMOVE events.
	 */
	remove<T>(
		matcher: Matcher<T>,
		handler: RemoveHandler<T, V>,
		options?: HandlerOptions | BatchHandlerOptions,
	): this {
		const registration: RegisteredHandler<T> = {
			id: this.generateHandlerId(),
			eventType: "REMOVE",
			matcher,
			handler: handler as HandlerFunction,
			options: options ?? {},
			isParser: this.isParser(matcher),
		};
		this._handlers.push(registration as RegisteredHandler);
		return this;
	}

	/**
	 * Register middleware to be executed for each record before handlers.
	 * Middleware is executed in the order it was registered.
	 */
	use(middleware: MiddlewareFunction): this {
		this._middleware.push(middleware);
		return this;
	}

	/**
	 * Unmarshalls DynamoDB attribute map to native JavaScript object.
	 */
	private unmarshallImage(
		image: { [key: string]: AttributeValue } | undefined,
	): Record<string, unknown> | undefined {
		if (!image) return undefined;
		if (!this._unmarshall) return image as Record<string, unknown>;
		return unmarshall(image);
	}

	/**
	 * Executes the middleware chain for a record.
	 */
	private async executeMiddleware(
		record: DynamoDBRecord,
		index: number,
	): Promise<void> {
		if (index >= this._middleware.length) {
			return;
		}

		const middleware = this._middleware[index];
		await middleware(record, () => this.executeMiddleware(record, index + 1));
	}

	/**
	 * Builds handler context from a DynamoDB record.
	 */
	private buildContext(record: DynamoDBRecord): HandlerContext {
		return {
			eventName: record.eventName as "INSERT" | "MODIFY" | "REMOVE",
			eventID: record.eventID,
			eventSourceARN: record.eventSourceARN,
		};
	}

	/**
	 * Checks if a handler matches the record using discriminator or parser.
	 */
	private matchHandler(
		handler: RegisteredHandler,
		imageData: unknown,
	): { matches: boolean; parsedData?: unknown } {
		if (handler.isParser) {
			const parser = handler.matcher as Parser<unknown>;
			const result = parser.safeParse(imageData);
			if (result.success) {
				return { matches: true, parsedData: result.data };
			}
			return { matches: false };
		}

		// Discriminator function
		const discriminator = handler.matcher as (record: unknown) => boolean;
		return { matches: discriminator(imageData) };
	}

	/**
	 * Gets the appropriate image data for matching based on event type.
	 */
	private getMatchingImage(
		record: DynamoDBRecord,
		eventType: "INSERT" | "MODIFY" | "REMOVE",
	): unknown {
		const newImage = this.unmarshallImage(record.dynamodb?.NewImage);
		const oldImage = this.unmarshallImage(record.dynamodb?.OldImage);

		switch (eventType) {
			case "INSERT":
				return newImage;
			case "REMOVE":
				return oldImage;
			case "MODIFY":
				return newImage; // Use newImage for matching MODIFY events
			default:
				return undefined;
		}
	}

	/**
	 * Invokes a handler with the appropriate arguments based on event type and stream view type.
	 */
	private async invokeHandler(
		handler: RegisteredHandler,
		record: DynamoDBRecord,
		parsedData: unknown | undefined,
		ctx: HandlerContext,
	): Promise<void> {
		const newImage = parsedData ?? this.unmarshallImage(record.dynamodb?.NewImage);
		const oldImage = this.unmarshallImage(record.dynamodb?.OldImage);
		const keys = this.unmarshallImage(record.dynamodb?.Keys);

		switch (ctx.eventName) {
			case "INSERT":
				if (this._streamViewType === "KEYS_ONLY") {
					await handler.handler(keys, ctx);
				} else {
					await handler.handler(newImage, ctx);
				}
				break;
			case "MODIFY":
				if (this._streamViewType === "KEYS_ONLY") {
					await handler.handler(keys, ctx);
				} else if (this._streamViewType === "NEW_IMAGE") {
					await handler.handler(undefined, newImage, ctx);
				} else if (this._streamViewType === "OLD_IMAGE") {
					await handler.handler(oldImage, undefined, ctx);
				} else {
					// NEW_AND_OLD_IMAGES - need to re-parse oldImage if parser
					let parsedOldImage: unknown = oldImage;
					if (handler.isParser && oldImage) {
						const parser = handler.matcher as Parser<unknown>;
						const result = parser.safeParse(oldImage);
						if (result.success) {
							parsedOldImage = result.data;
						}
					}
					await handler.handler(parsedOldImage, parsedData ?? newImage, ctx);
				}
				break;
			case "REMOVE":
				if (this._streamViewType === "KEYS_ONLY") {
					await handler.handler(keys, ctx);
				} else {
					await handler.handler(parsedData ?? oldImage, ctx);
				}
				break;
		}
	}

	/**
	 * Process a DynamoDB Stream event through the router.
	 */
	async process(event: DynamoDBStreamEvent): Promise<ProcessingResult> {
		const result: ProcessingResult = {
			processed: 0,
			succeeded: 0,
			failed: 0,
			errors: [],
		};

		for (const record of event.Records) {
			result.processed++;
			const recordId = record.eventID ?? `record_${result.processed}`;

			try {
				// Check same region filter
				if (this._sameRegionOnly && !this.isRecordFromSameRegion(record.eventSourceARN)) {
					result.succeeded++;
					continue;
				}

				// Execute middleware chain
				await this.executeMiddleware(record, 0);

				const eventType = record.eventName as "INSERT" | "MODIFY" | "REMOVE";
				const ctx = this.buildContext(record);
				const matchingImage = this.getMatchingImage(record, eventType);

				// Find and execute matching handlers
				const matchingHandlers = this._handlers.filter(
					(h) => h.eventType === eventType,
				);

				for (const handler of matchingHandlers) {
					const { matches, parsedData } = this.matchHandler(handler, matchingImage);
					if (matches) {
						await this.invokeHandler(handler, record, parsedData, ctx);
					}
				}

				result.succeeded++;
			} catch (error) {
				result.failed++;
				result.errors.push({
					recordId,
					error: error instanceof Error ? error : new Error(String(error)),
					phase: "handler",
				});
			}
		}

		return result;
	}
}
