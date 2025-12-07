/**
 * Deferred processing example with SQS
 *
 * This example shows how to defer heavy processing to SQS
 * to avoid blocking the DynamoDB stream processing.
 */
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { StreamRouter, createSQSClient } from "../src";

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

// Create router with SQS configuration using the helper
const router = new StreamRouter({
	deferQueue: process.env.DEFER_QUEUE_URL,
	sqsClient: createSQSClient(new SQSClient({}), SendMessageCommand),
});

// Quick handler - runs immediately during stream processing
router.onInsert(isOrder, async (newOrder) => {
	console.log(`Order ${newOrder.orderId} received - quick acknowledgment`);
	// Fast operations only: update counters, simple logging
});

// Deferred handler - enqueued to SQS for later processing
router
	.onInsert(isOrder, async (newOrder) => {
		console.log(`Processing order ${newOrder.orderId} - sending emails, generating PDFs...`);
		// Heavy operations: send emails, generate invoices, call external APIs
		await sendOrderConfirmationEmail(newOrder.customerEmail, newOrder.orderId);
		await generateInvoicePDF(newOrder.orderId);
	})
	.defer("order-heavy-processing", { delaySeconds: 5 }); // Explicit ID required for deferred handlers

// DynamoDB Stream handler - processes stream, enqueues deferred work
export const streamHandler = router.streamHandler;

// SQS handler - processes deferred work
export const sqsHandler = router.sqsHandler;

// Placeholder functions
async function sendOrderConfirmationEmail(email: string, orderId: string) {
	console.log(`Sending confirmation to ${email} for order ${orderId}`);
}

async function generateInvoicePDF(orderId: string) {
	console.log(`Generating invoice PDF for order ${orderId}`);
}
