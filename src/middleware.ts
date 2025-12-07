import { unmarshall } from "@aws-sdk/util-dynamodb";
import type { AttributeValue, DynamoDBRecord } from "aws-lambda";
import type { MiddlewareFunction } from "./types";

/**
 * Extended DynamoDB record with unmarshalled data attached.
 */
export interface UnmarshalledRecord extends DynamoDBRecord {
	unmarshalled?: {
		NewImage?: Record<string, unknown>;
		OldImage?: Record<string, unknown>;
		Keys?: Record<string, unknown>;
	};
}

/**
 * Creates a middleware function that unmarshalls DynamoDB JSON format
 * to native JavaScript objects.
 *
 * The middleware attaches unmarshalled data to the record's `unmarshalled` property,
 * making it available to downstream handlers.
 *
 * @example
 * ```typescript
 * const router = new StreamRouter({ unmarshall: false });
 * router.use(unmarshallMiddleware());
 * ```
 */
export function unmarshallMiddleware(): MiddlewareFunction {
	return async (record: DynamoDBRecord, next: () => Promise<void>) => {
		const extendedRecord = record as UnmarshalledRecord;

		extendedRecord.unmarshalled = {
			NewImage: record.dynamodb?.NewImage
				? unmarshall(record.dynamodb.NewImage as Record<string, AttributeValue>)
				: undefined,
			OldImage: record.dynamodb?.OldImage
				? unmarshall(record.dynamodb.OldImage as Record<string, AttributeValue>)
				: undefined,
			Keys: record.dynamodb?.Keys
				? unmarshall(record.dynamodb.Keys as Record<string, AttributeValue>)
				: undefined,
		};

		await next();
	};
}
