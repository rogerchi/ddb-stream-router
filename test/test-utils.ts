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
        const toAttributeValue = (
                obj: Record<string, unknown> | undefined,
        ): Record<string, { S?: string; N?: string; BOOL?: boolean }> | undefined => {
                if (!obj) return undefined;
                const result: Record<string, { S?: string; N?: string; BOOL?: boolean }> =
                        {};
                for (const [key, value] of Object.entries(obj)) {
                        if (typeof value === "string") {
                                result[key] = { S: value };
                        } else if (typeof value === "number") {
                                result[key] = { N: String(value) };
                        } else if (typeof value === "boolean") {
                                result[key] = { BOOL: value };
                        }
                }
                return result;
        };

        const record: DynamoDBRecord = {
                eventID: `event_${Date.now()}_${Math.random().toString(36).substring(2, 9)}`,
                eventName,
                eventVersion: "1.1",
                eventSource: "aws:dynamodb",
                awsRegion: "us-east-1",
                eventSourceARN: `arn:aws:dynamodb:us-east-1:123456789012:table/${TABLE_NAME}/stream/2024-01-01T00:00:00.000`,
                dynamodb: {
                        Keys: toAttributeValue(keys) as Record<
                                string,
                                { S?: string; N?: string }
                        >,
                        NewImage: toAttributeValue(newImage) as
                                | Record<string, { S?: string; N?: string }>
                                | undefined,
                        OldImage: toAttributeValue(oldImage) as
                                | Record<string, { S?: string; N?: string }>
                                | undefined,
                        SequenceNumber: `seq_${Date.now()}`,
                        StreamViewType: "NEW_AND_OLD_IMAGES",
                },
        } as DynamoDBRecord;

        // Add userIdentity if provided
        if (userIdentity) {
                (record as DynamoDBRecord & { userIdentity: { type: string; principalId: string } }).userIdentity = userIdentity;
        }

        return record;
}

export function createStreamEvent(
        records: DynamoDBRecord[],
): DynamoDBStreamEvent {
        return { Records: records };
}
