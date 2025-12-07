import { ConfigurationError } from "./errors";
import type {
	MiddlewareFunction,
	RegisteredHandler,
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
}
