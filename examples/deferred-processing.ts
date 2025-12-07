/**
 * Deferred processing example with SQS
 *
 * This example shows how to defer heavy processing to SQS
 * to avoid blocking the DynamoDB stream processing.
 */
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import type { DynamoDBStreamHandler, SQSHandler } from "aws-lambda";
import { StreamRouter } from "../src";

// Entity types
interface Order {
	pk: string;
	sk: string;
	orderId: string;
	status: string;
	customerEmail: string;
}

const isOrder = (record: unknown): record is Order =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("ORDER#");

// Create SQS client adapter
const sqsClient = new SQSClient({});
const sqsClientAdapter = {
	sendMessage: async (params: {
		QueueUrl: string;
		MessageBody: string;
		DelaySeconds?: number;
	}) => {
		return sqsClient.send(
			new SendMessageCommand({
				QueueUrl: params.QueueUrl,
				MessageBody: params.MessageBody,
				DelaySeconds: params.DelaySeconds,
			}),
		);
	},
};

// Create router with SQS configuration
const router = new StreamRouter({
	deferQueue: process.env.DEFER_QUEUE_URL,
	sqsClient: sqsClientAdapter,
});

// Quick handler - runs immediately during stream processing
router.onInsert(isOrder, async (newOrder, ctx) => {
	console.log(`Order ${newOrder.orderId} received - quick acknowledgment`);
	// Fast operations only: update counters, simple logging
});

// Deferred handler - enqueued to SQS for later processing
router
	.onInsert(isOrder, async (newOrder, ctx) => {
		console.log(`Processing order ${newOrder.orderId} - sending emails, generating PDFs...`);
		// Heavy operations: send emails, generate invoices, call external APIs
		await sendOrderConfirmationEmail(newOrder.customerEmail, newOrder.orderId);
		await generateInvoicePDF(newOrder.orderId);
	})
	.defer({ delaySeconds: 5 }); // Optional delay

// DynamoDB Stream handler - processes stream, enqueues deferred work
export const streamHandler: DynamoDBStreamHandler = async (event) => {
	return router.process(event, { reportBatchItemFailures: true });
};

// SQS handler - processes deferred work
export const sqsHandler: SQSHandler = async (event) => {
	return router.processDeferred(event, { reportBatchItemFailures: true });
};

// Placeholder functions
async function sendOrderConfirmationEmail(email: string, orderId: string) {
	console.log(`Sending confirmation to ${email} for order ${orderId}`);
}

async function generateInvoicePDF(orderId: string) {
	console.log(`Generating invoice PDF for order ${orderId}`);
}
