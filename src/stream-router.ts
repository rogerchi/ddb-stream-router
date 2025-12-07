import { ConfigurationError } from "./errors";
import type {
	BatchHandlerOptions,
	HandlerFunction,
	HandlerOptions,
	InsertHandler,
	Matcher,
	MiddlewareFunction,
	ModifyHandler,
	ModifyHandlerOptions,
	Parser,
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
		options?: ModifyHandlerOptions | (BatchHandlerOptions & ModifyHandlerOptions),
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
}
