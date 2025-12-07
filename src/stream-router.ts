import type { AttributeValue as SDKAttributeValue } from "@aws-sdk/client-dynamodb";
import { unmarshall } from "@aws-sdk/util-dynamodb";
import type {
	DynamoDBRecord,
	DynamoDBStreamEvent,
	AttributeValue as LambdaAttributeValue,
} from "aws-lambda";
import { diffAttributes, hasAttributeChange } from "./attribute-diff";
import { ConfigurationError } from "./errors";
import type {
	AttributeChangeType,
	BatchHandlerOptions,
	BatchInsertHandler,
	BatchItemFailuresResponse,
	BatchModifyHandler,
	BatchRemoveHandler,
	DeferOptions,
	DeferredRecordMessage,
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
	ProcessOptions,
	RegisteredHandler,
	RemoveHandler,
	SQSClient,
	StreamRouterOptions,
	StreamViewType,
} from "./types";

const VALID_STREAM_VIEW_TYPES: StreamViewType[] = [
	"KEYS_ONLY",
	"NEW_IMAGE",
	"OLD_IMAGE",
	"NEW_AND_OLD_IMAGES",
];

/**
 * HandlerRegistration allows chaining .defer() after handler registration.
 */
export class HandlerRegistration<
	V extends StreamViewType = "NEW_AND_OLD_IMAGES",
> {
	constructor(
		private readonly router: StreamRouter<V>,
		private readonly handlerId: string,
	) {}

	/**
	 * Mark this handler as deferred - enqueues to SQS from stream, executes from SQS.
	 */
	defer(options?: DeferOptions): StreamRouter<V> {
		const handler = this.router.handlers.find((h) => h.id === this.handlerId);
		if (!handler) {
			throw new ConfigurationError("Handler not found for defer configuration");
		}

		// Determine the queue URL
		const queueUrl = options?.queue ?? this.router.deferQueue;
		if (!queueUrl) {
			throw new ConfigurationError(
				"Cannot defer handler: no queue specified in defer options and no router-level deferQueue configured",
			);
		}

		handler.deferred = true;
		handler.deferOptions = {
			queue: queueUrl,
			delaySeconds: options?.delaySeconds,
		};

		return this.router;
	}

	// Proxy methods to continue chaining
	insert<T>(
		matcher: Matcher<T>,
		handler: BatchInsertHandler<T, V>,
		options: BatchHandlerOptions,
	): HandlerRegistration<V>;
	insert<T>(
		matcher: Matcher<T>,
		handler: InsertHandler<T, V>,
		options?: HandlerOptions,
	): HandlerRegistration<V>;
	insert<T>(
		matcher: Matcher<T>,
		handler: InsertHandler<T, V> | BatchInsertHandler<T, V>,
		options?: HandlerOptions | BatchHandlerOptions,
	): HandlerRegistration<V> {
		return this.router.insert(
			matcher,
			handler as InsertHandler<T, V>,
			options as HandlerOptions,
		);
	}

	modify<T>(
		matcher: Matcher<T>,
		handler: BatchModifyHandler<T, V>,
		options: BatchHandlerOptions & ModifyHandlerOptions,
	): HandlerRegistration<V>;
	modify<T>(
		matcher: Matcher<T>,
		handler: ModifyHandler<T, V>,
		options?: ModifyHandlerOptions,
	): HandlerRegistration<V>;
	modify<T>(
		matcher: Matcher<T>,
		handler: ModifyHandler<T, V> | BatchModifyHandler<T, V>,
		options?:
			| ModifyHandlerOptions
			| (BatchHandlerOptions & ModifyHandlerOptions),
	): HandlerRegistration<V> {
		return this.router.modify(
			matcher,
			handler as ModifyHandler<T, V>,
			options as ModifyHandlerOptions,
		);
	}

	remove<T>(
		matcher: Matcher<T>,
		handler: BatchRemoveHandler<T, V>,
		options: BatchHandlerOptions,
	): HandlerRegistration<V>;
	remove<T>(
		matcher: Matcher<T>,
		handler: RemoveHandler<T, V>,
		options?: HandlerOptions,
	): HandlerRegistration<V>;
	remove<T>(
		matcher: Matcher<T>,
		handler: RemoveHandler<T, V> | BatchRemoveHandler<T, V>,
		options?: HandlerOptions | BatchHandlerOptions,
	): HandlerRegistration<V> {
		return this.router.remove(
			matcher,
			handler as RemoveHandler<T, V>,
			options as HandlerOptions,
		);
	}

	use(middleware: MiddlewareFunction): StreamRouter<V> {
		return this.router.use(middleware);
	}
}

export class StreamRouter<V extends StreamViewType = "NEW_AND_OLD_IMAGES"> {
	private readonly _streamViewType: V;
	private readonly _unmarshall: boolean;
	private readonly _sameRegionOnly: boolean;
	private readonly _deferQueue: string | undefined;
	private readonly _sqsClient: SQSClient | undefined;
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
		this._deferQueue = options?.deferQueue;
		this._sqsClient = options?.sqsClient;
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

	get deferQueue(): string | undefined {
		return this._deferQueue;
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
	 * @overload Batch mode - handler receives array of records
	 */
	insert<T>(
		matcher: Matcher<T>,
		handler: BatchInsertHandler<T, V>,
		options: BatchHandlerOptions,
	): HandlerRegistration<V>;
	/**
	 * Register a handler for INSERT events.
	 * @overload Standard mode - handler receives single record
	 */
	insert<T>(
		matcher: Matcher<T>,
		handler: InsertHandler<T, V>,
		options?: HandlerOptions,
	): HandlerRegistration<V>;
	insert<T>(
		matcher: Matcher<T>,
		handler: InsertHandler<T, V> | BatchInsertHandler<T, V>,
		options?: HandlerOptions | BatchHandlerOptions,
	): HandlerRegistration<V> {
		const handlerId = this.generateHandlerId();
		const registration: RegisteredHandler<T> = {
			id: handlerId,
			eventType: "INSERT",
			matcher,
			handler: handler as HandlerFunction,
			options: options ?? {},
			isParser: this.isParser(matcher),
		};
		this._handlers.push(registration as RegisteredHandler);
		return new HandlerRegistration(this, handlerId);
	}

	/**
	 * Register a handler for MODIFY events.
	 * @overload Batch mode - handler receives array of records
	 */
	modify<T>(
		matcher: Matcher<T>,
		handler: BatchModifyHandler<T, V>,
		options: BatchHandlerOptions & ModifyHandlerOptions,
	): HandlerRegistration<V>;
	/**
	 * Register a handler for MODIFY events.
	 * @overload Standard mode - handler receives single record
	 */
	modify<T>(
		matcher: Matcher<T>,
		handler: ModifyHandler<T, V>,
		options?: ModifyHandlerOptions,
	): HandlerRegistration<V>;
	modify<T>(
		matcher: Matcher<T>,
		handler: ModifyHandler<T, V> | BatchModifyHandler<T, V>,
		options?:
			| ModifyHandlerOptions
			| (BatchHandlerOptions & ModifyHandlerOptions),
	): HandlerRegistration<V> {
		const handlerId = this.generateHandlerId();
		const registration: RegisteredHandler<T> = {
			id: handlerId,
			eventType: "MODIFY",
			matcher,
			handler: handler as HandlerFunction,
			options: options ?? {},
			isParser: this.isParser(matcher),
		};
		this._handlers.push(registration as RegisteredHandler);
		return new HandlerRegistration(this, handlerId);
	}

	/**
	 * Register a handler for REMOVE events.
	 * @overload Batch mode - handler receives array of records
	 */
	remove<T>(
		matcher: Matcher<T>,
		handler: BatchRemoveHandler<T, V>,
		options: BatchHandlerOptions,
	): HandlerRegistration<V>;
	/**
	 * Register a handler for REMOVE events.
	 * @overload Standard mode - handler receives single record
	 */
	remove<T>(
		matcher: Matcher<T>,
		handler: RemoveHandler<T, V>,
		options?: HandlerOptions,
	): HandlerRegistration<V>;
	remove<T>(
		matcher: Matcher<T>,
		handler: RemoveHandler<T, V> | BatchRemoveHandler<T, V>,
		options?: HandlerOptions | BatchHandlerOptions,
	): HandlerRegistration<V> {
		const handlerId = this.generateHandlerId();
		const registration: RegisteredHandler<T> = {
			id: handlerId,
			eventType: "REMOVE",
			matcher,
			handler: handler as HandlerFunction,
			options: options ?? {},
			isParser: this.isParser(matcher),
		};
		this._handlers.push(registration as RegisteredHandler);
		return new HandlerRegistration(this, handlerId);
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
		image: { [key: string]: LambdaAttributeValue } | undefined,
	): Record<string, unknown> | undefined {
		if (!image) return undefined;
		if (!this._unmarshall) return image as Record<string, unknown>;
		// Cast Lambda AttributeValue to SDK AttributeValue for unmarshall
		return unmarshall(image as Record<string, SDKAttributeValue>);
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
	 * Checks if a MODIFY handler's attribute filter matches the record changes.
	 * Returns true if no attribute filter is specified, or if the filter matches.
	 * Multiple attribute filters use OR logic.
	 */
	private matchesAttributeFilter(
		handler: RegisteredHandler,
		oldImage: Record<string, unknown> | undefined,
		newImage: Record<string, unknown> | undefined,
	): boolean {
		const options = handler.options as ModifyHandlerOptions;

		// No attribute filter specified - match all MODIFY events
		if (!options.attribute) {
			return true;
		}

		// Compute the diff between old and new images
		const diff = diffAttributes(oldImage, newImage);

		// Check if the specified attribute has the required change type(s)
		const changeTypes = options.changeType;

		return hasAttributeChange(
			diff,
			options.attribute,
			changeTypes as AttributeChangeType | AttributeChangeType[] | undefined,
		);
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
	 * Builds a batch record entry for batch mode handlers.
	 */
	private buildBatchRecord(
		handler: RegisteredHandler,
		record: DynamoDBRecord,
		parsedData: unknown | undefined,
		ctx: HandlerContext,
	): Record<string, unknown> {
		const newImage =
			parsedData ?? this.unmarshallImage(record.dynamodb?.NewImage);
		const oldImage = this.unmarshallImage(record.dynamodb?.OldImage);
		const keys = this.unmarshallImage(record.dynamodb?.Keys);

		switch (ctx.eventName) {
			case "INSERT":
				if (this._streamViewType === "KEYS_ONLY") {
					return { keys, ctx };
				}
				return { newImage, ctx };
			case "MODIFY": {
				if (this._streamViewType === "KEYS_ONLY") {
					return { keys, ctx };
				}
				if (this._streamViewType === "NEW_IMAGE") {
					return { oldImage: undefined, newImage, ctx };
				}
				if (this._streamViewType === "OLD_IMAGE") {
					return { oldImage, newImage: undefined, ctx };
				}
				// NEW_AND_OLD_IMAGES - need to re-parse oldImage if parser
				let parsedOldImage: unknown = oldImage;
				if (handler.isParser && oldImage) {
					const parser = handler.matcher as Parser<unknown>;
					const result = parser.safeParse(oldImage);
					if (result.success) {
						parsedOldImage = result.data;
					}
				}
				return {
					oldImage: parsedOldImage,
					newImage: parsedData ?? newImage,
					ctx,
				};
			}
			case "REMOVE":
				if (this._streamViewType === "KEYS_ONLY") {
					return { keys, ctx };
				}
				return { oldImage: parsedData ?? oldImage, ctx };
			default:
				return { ctx };
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
		const newImage =
			parsedData ?? this.unmarshallImage(record.dynamodb?.NewImage);
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
	 * Gets the batch key for a record based on handler options.
	 */
	private getBatchKey(
		handler: RegisteredHandler,
		record: DynamoDBRecord,
		parsedData: unknown | undefined,
	): string {
		const options = handler.options as BatchHandlerOptions;
		if (!options.batchKey) {
			return "__default__";
		}

		const imageData =
			parsedData ??
			this.unmarshallImage(record.dynamodb?.NewImage) ??
			this.unmarshallImage(record.dynamodb?.OldImage);

		if (typeof options.batchKey === "function") {
			return options.batchKey(imageData);
		}

		// batchKey is a string - use it as attribute name
		if (imageData && typeof imageData === "object") {
			const value = (imageData as Record<string, unknown>)[options.batchKey];
			return String(value ?? "__undefined__");
		}

		return "__undefined__";
	}

	/**
	 * Enqueues a record to SQS for deferred processing.
	 */
	private async enqueueDeferred(
		handler: RegisteredHandler,
		record: DynamoDBRecord,
	): Promise<void> {
		if (!this._sqsClient) {
			throw new ConfigurationError(
				"Cannot enqueue deferred record: no SQS client configured",
			);
		}

		const queueUrl = handler.deferOptions?.queue;
		if (!queueUrl) {
			throw new ConfigurationError(
				"Cannot enqueue deferred record: no queue URL configured",
			);
		}

		const message: DeferredRecordMessage = {
			handlerId: handler.id,
			record: record,
		};

		await this._sqsClient.sendMessage({
			QueueUrl: queueUrl,
			MessageBody: JSON.stringify(message),
			DelaySeconds: handler.deferOptions?.delaySeconds,
		});
	}

	/**
	 * Process a DynamoDB Stream event through the router.
	 * @param event The DynamoDB Stream event to process
	 * @param options Processing options
	 * @returns ProcessingResult or BatchItemFailuresResponse based on options
	 */
	async process(
		event: DynamoDBStreamEvent,
		options: ProcessOptions & { reportBatchItemFailures: true },
	): Promise<BatchItemFailuresResponse>;
	async process(
		event: DynamoDBStreamEvent,
		options?: ProcessOptions,
	): Promise<ProcessingResult>;
	async process(
		event: DynamoDBStreamEvent,
		options?: ProcessOptions,
	): Promise<ProcessingResult | BatchItemFailuresResponse> {
		const result: ProcessingResult = {
			processed: 0,
			succeeded: 0,
			failed: 0,
			errors: [],
		};

		// Track first failed record's sequence number for batch item failures
		let firstFailedSequenceNumber: string | undefined;

		// Collect batch records: Map<handlerId, Map<batchKey, Array<batchRecord>>>
		const batchCollector = new Map<
			string,
			Map<string, Array<Record<string, unknown>>>
		>();

		for (const record of event.Records) {
			result.processed++;
			const recordId = record.eventID ?? `record_${result.processed}`;
			const sequenceNumber = record.dynamodb?.SequenceNumber;

			try {
				// Check same region filter
				if (
					this._sameRegionOnly &&
					!this.isRecordFromSameRegion(record.eventSourceARN)
				) {
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

				// Get images for attribute filtering (MODIFY events)
				const oldImage = this.unmarshallImage(record.dynamodb?.OldImage);
				const newImage = this.unmarshallImage(record.dynamodb?.NewImage);

				for (const handler of matchingHandlers) {
					const { matches, parsedData } = this.matchHandler(
						handler,
						matchingImage,
					);
					if (matches) {
						// For MODIFY events, check attribute filter
						if (eventType === "MODIFY") {
							if (!this.matchesAttributeFilter(handler, oldImage, newImage)) {
								continue; // Skip handler if attribute filter doesn't match
							}
						}

						// Check if handler is deferred
						if (handler.deferred && handler.deferOptions?.queue) {
							// Enqueue to SQS instead of executing
							await this.enqueueDeferred(handler, record);
						} else {
							// Check if batch mode is enabled
							const handlerOptions = handler.options as BatchHandlerOptions;
							if (handlerOptions.batch) {
								// Collect for batch processing
								if (!batchCollector.has(handler.id)) {
									batchCollector.set(handler.id, new Map());
								}
								const handlerBatches = batchCollector.get(handler.id);
								if (handlerBatches) {
									const batchKey = this.getBatchKey(
										handler,
										record,
										parsedData,
									);
									if (!handlerBatches.has(batchKey)) {
										handlerBatches.set(batchKey, []);
									}
									const batchRecord = this.buildBatchRecord(
										handler,
										record,
										parsedData,
										ctx,
									);
									const batchRecords = handlerBatches.get(batchKey);
									if (batchRecords) {
										batchRecords.push(batchRecord);
									}
								}
							} else {
								// Immediate execution for non-batch handlers
								await this.invokeHandler(handler, record, parsedData, ctx);
							}
						}
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

				// Track first failed sequence number for batch item failures
				if (!firstFailedSequenceNumber && sequenceNumber) {
					firstFailedSequenceNumber = sequenceNumber;
				}
			}
		}

		// Execute batch handlers after all records are processed
		for (const [handlerId, batchesByKey] of batchCollector) {
			const handler = this._handlers.find((h) => h.id === handlerId);
			if (!handler) continue;

			for (const [, records] of batchesByKey) {
				try {
					await handler.handler(records);
				} catch (error) {
					result.failed++;
					result.errors.push({
						recordId: `batch_${handlerId}`,
						error: error instanceof Error ? error : new Error(String(error)),
						phase: "handler",
					});
				}
			}
		}

		// Return batch item failures response if requested
		if (options?.reportBatchItemFailures) {
			const batchItemFailures: Array<{ itemIdentifier: string }> = [];
			if (firstFailedSequenceNumber) {
				batchItemFailures.push({ itemIdentifier: firstFailedSequenceNumber });
			}
			return { batchItemFailures };
		}

		return result;
	}

	/**
	 * Process deferred records from an SQS event.
	 * Executes only the specific deferred handler that enqueued each record.
	 * @param sqsEvent The SQS event containing deferred records
	 * @param options Processing options
	 * @returns ProcessingResult or BatchItemFailuresResponse based on options
	 */
	async processDeferred(
		sqsEvent: {
			Records: Array<{ body: string; messageId: string }>;
		},
		options: ProcessOptions & { reportBatchItemFailures: true },
	): Promise<BatchItemFailuresResponse>;
	async processDeferred(sqsEvent: {
		Records: Array<{ body: string; messageId?: string }>;
	}): Promise<ProcessingResult>;
	async processDeferred(
		sqsEvent: {
			Records: Array<{ body: string; messageId?: string }>;
		},
		options?: ProcessOptions,
	): Promise<ProcessingResult | BatchItemFailuresResponse> {
		const result: ProcessingResult = {
			processed: 0,
			succeeded: 0,
			failed: 0,
			errors: [],
		};

		// Track all failed message IDs for SQS batch item failures
		const failedMessageIds: string[] = [];

		for (const sqsRecord of sqsEvent.Records) {
			result.processed++;
			const recordId = sqsRecord.messageId ?? `deferred_${result.processed}`;

			try {
				const message: DeferredRecordMessage = JSON.parse(sqsRecord.body);
				const handler = this._handlers.find((h) => h.id === message.handlerId);

				if (!handler) {
					throw new Error(`Handler not found: ${message.handlerId}`);
				}

				const record = message.record as DynamoDBRecord;
				const eventType = record.eventName as "INSERT" | "MODIFY" | "REMOVE";
				const ctx = this.buildContext(record);
				const matchingImage = this.getMatchingImage(record, eventType);

				// Re-match to get parsed data if using a parser
				const { parsedData } = this.matchHandler(handler, matchingImage);

				// Execute the handler
				await this.invokeHandler(handler, record, parsedData, ctx);

				result.succeeded++;
			} catch (error) {
				result.failed++;
				result.errors.push({
					recordId,
					error: error instanceof Error ? error : new Error(String(error)),
					phase: "handler",
				});

				// Track failed message ID for batch item failures
				if (sqsRecord.messageId) {
					failedMessageIds.push(sqsRecord.messageId);
				}
			}
		}

		// Return batch item failures response if requested (all failed messages for SQS)
		if (options?.reportBatchItemFailures) {
			return {
				batchItemFailures: failedMessageIds.map((id) => ({
					itemIdentifier: id,
				})),
			};
		}

		return result;
	}
}
