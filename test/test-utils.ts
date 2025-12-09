import { marshall } from "@aws-sdk/util-dynamodb";
import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";

export const TABLE_NAME = "test-table";

/**
 * Helper to create a stream record from DynamoDB items.
 * This creates records in the exact format that DynamoDB streams produce.
 */
export function createStreamRecord(
	eventName: "INSERT" | "MODIFY" | "REMOVE",
	keys: Record<string, unknown>,
	newImage?: Record<string, unknown>,
	oldImage?: Record<string, unknown>,
	userIdentity?: { type: string; principalId: string },
): DynamoDBRecord {
	const record: DynamoDBRecord = {
		eventID: `event_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
		eventName,
		eventVersion: "1.1",
		eventSource: "aws:dynamodb",
		awsRegion: "us-east-1",
		eventSourceARN: `arn:aws:dynamodb:us-east-1:123456789012:table/${TABLE_NAME}/stream/2024-01-01T00:00:00.000`,
		dynamodb: {
			Keys: marshall(keys) as Record<string, { S?: string; N?: string }>,
			NewImage: newImage
				? (marshall(newImage) as Record<string, { S?: string; N?: string }>)
				: undefined,
			OldImage: oldImage
				? (marshall(oldImage) as Record<string, { S?: string; N?: string }>)
				: undefined,
			SequenceNumber: `seq_${Date.now()}`,
			StreamViewType: "NEW_AND_OLD_IMAGES",
		},
	} as DynamoDBRecord;

	// Add userIdentity if provided
	if (userIdentity) {
		(
			record as DynamoDBRecord & {
				userIdentity: { type: string; principalId: string };
			}
		).userIdentity = userIdentity;
	}

	return record;
}

export function createStreamEvent(
	records: DynamoDBRecord[],
): DynamoDBStreamEvent {
	return { Records: records };
}

/**
 * Helper to create a MODIFY event with old and new images.
 */
export function createModifyEvent(
	oldImage: Record<string, unknown>,
	newImage: Record<string, unknown>,
): DynamoDBStreamEvent {
	// Extract keys from the old image (assuming pk and sk, or just pk)
	const keys: Record<string, unknown> = {};
	if ("pk" in oldImage) keys.pk = oldImage.pk;
	if ("sk" in oldImage) keys.sk = oldImage.sk;
	if ("id" in oldImage) keys.id = oldImage.id;

	// If no standard keys found, use first property as key
	if (Object.keys(keys).length === 0 && Object.keys(oldImage).length > 0) {
		const firstKey = Object.keys(oldImage)[0];
		keys[firstKey] = oldImage[firstKey];
	}

	const record = createStreamRecord("MODIFY", keys, newImage, oldImage);
	return createStreamEvent([record]);
}

/**
 * Helper to create a mock stream event from simplified record structures.
 * Accepts an array of objects with eventName and dynamodb properties.
 * The NewImage and OldImage are expected to be in DynamoDB AttributeValue format.
 */
export function createMockStreamEvent(
	records: Array<{
		eventName: "INSERT" | "MODIFY" | "REMOVE";
		dynamodb: {
			Keys: Record<string, { S?: string; N?: string; B?: string }>;
			NewImage?: Record<
			string,
			{
				S?: string;
				N?: string;
				B?: string;
				L?: unknown[];
				M?: Record<string, unknown>;
			}
		>;
			OldImage?: Record<
			string,
			{
				S?: string;
				N?: string;
				B?: string;
				L?: unknown[];
				M?: Record<string, unknown>;
			}
		>;
		};
	}>,
): DynamoDBStreamEvent {
	const dynamoDBRecords: DynamoDBRecord[] = records.map(
		(record) =>
			({
				eventID: `event_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
				eventName: record.eventName,
				eventVersion: "1.1",
				eventSource: "aws:dynamodb",
				awsRegion: "us-east-1",
				eventSourceARN: `arn:aws:dynamodb:us-east-1:123456789012:table/${TABLE_NAME}/stream/2024-01-01T00:00:00.000`,
				dynamodb: {
					Keys: record.dynamodb.Keys,
					NewImage: record.dynamodb.NewImage,
					OldImage: record.dynamodb.OldImage,
					SequenceNumber: `seq_${Date.now()}`,
					StreamViewType: "NEW_AND_OLD_IMAGES",
				},
			}) as DynamoDBRecord,
	);

	return { Records: dynamoDBRecords };
}
