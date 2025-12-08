/**
 * Helper to wrap AWS SDK SQSClient for use with StreamRouter.
 *
 * The StreamRouter uses a minimal SQSClient interface to avoid a hard dependency
 * on @aws-sdk/client-sqs. This helper wraps the AWS SDK client to match that interface.
 */
import type { SQSClient } from "./types.js";

/**
 * Interface matching the AWS SDK v3 SQSClient's send method signature.
 * This allows the adapter to work with the real SQSClient without importing it.
 * @internal
 */
export interface AWSSQSClient {
	send(command: unknown): Promise<unknown>;
}

/**
 * Interface matching the AWS SDK v3 SendMessageCommand constructor.
 * @internal
 */
export interface SendMessageCommandConstructor {
	new (input: {
		QueueUrl: string;
		MessageBody: string;
		DelaySeconds?: number;
	}): unknown;
}

/**
 * Creates an SQSClient adapter from the AWS SDK v3 SQSClient.
 *
 * @example
 * ```typescript
 * import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
 * import { StreamRouter, createSQSClient } from "ddb-stream-router";
 *
 * const sqsClient = new SQSClient({});
 *
 * const router = new StreamRouter({
 *   deferQueue: process.env.DEFER_QUEUE_URL,
 *   sqsClient: createSQSClient(sqsClient, SendMessageCommand),
 * });
 * ```
 *
 * @param client - The AWS SDK v3 SQSClient instance
 * @param SendMessageCommand - The SendMessageCommand class from @aws-sdk/client-sqs
 * @returns An SQSClient adapter compatible with StreamRouter
 */
export function createSQSClient(
	client: AWSSQSClient,
	SendMessageCommand: SendMessageCommandConstructor,
): SQSClient {
	return {
		sendMessage: async (params: {
			QueueUrl: string;
			MessageBody: string;
			DelaySeconds?: number;
		}) => {
			return client.send(
				new SendMessageCommand({
					QueueUrl: params.QueueUrl,
					MessageBody: params.MessageBody,
					DelaySeconds: params.DelaySeconds,
				}),
			);
		},
	};
}
