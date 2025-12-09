import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { StreamRouter, createSQSClient } from "../src/index.js";

// Environment variables
const DEFER_QUEUE_URL = process.env.DEFER_QUEUE_URL!;
const VERIFICATION_QUEUE_URL = process.env.VERIFICATION_QUEUE_URL!;

// Verification message format
interface VerificationMessage {
	operationType: "INSERT" | "MODIFY" | "REMOVE";
	isDeferred: boolean;
	pk: string;
	sk: string;
	timestamp: number;
	eventId?: string;
	handlerType?: string; // To distinguish different handler types
}

// Test item interface
interface TestItem {
	pk: string;
	sk: string;
	data?: string;
	status?: string;
	count?: number;
}

// Type guard for test items
const isTestItem = (record: unknown): record is TestItem => {
	if (typeof record !== "object" || record === null) return false;
	const r = record as Record<string, unknown>;
	return (
		typeof r.pk === "string" &&
		r.pk.startsWith("TEST#") &&
		typeof r.sk === "string"
	);
};

// SQS client for sending verification messages
const sqsClient = new SQSClient({});

// Helper to send verification message
async function sendVerification(
	message: VerificationMessage,
): Promise<void> {
	await sqsClient.send(
		new SendMessageCommand({
			QueueUrl: VERIFICATION_QUEUE_URL,
			MessageBody: JSON.stringify(message),
			MessageGroupId: message.pk, // Group by pk for FIFO ordering
		}),
	);
}

// Create router with deferred processing support
const router = new StreamRouter({
	deferQueue: DEFER_QUEUE_URL,
	sqsClient: createSQSClient(sqsClient, SendMessageCommand),
});

// Immediate INSERT handler
router.onInsert(isTestItem, async (newImage, ctx) => {
	await sendVerification({
		operationType: "INSERT",
		isDeferred: false,
		pk: newImage.pk,
		sk: newImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
	});
});

// Immediate MODIFY handler (all changes)
router.onModify(isTestItem, async (_oldImage, newImage, ctx) => {
	await sendVerification({
		operationType: "MODIFY",
		isDeferred: false,
		pk: newImage.pk,
		sk: newImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
		handlerType: "modify-all",
	});
});

// Targeted MODIFY handler - only when 'status' attribute changes
router.onModify(
	isTestItem,
	async (_oldImage, newImage, ctx) => {
		await sendVerification({
			operationType: "MODIFY",
			isDeferred: false,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
			handlerType: "modify-status-change",
		});
	},
	{ attribute: "status" },
);

// Immediate REMOVE handler
router.onRemove(isTestItem, async (oldImage, ctx) => {
	await sendVerification({
		operationType: "REMOVE",
		isDeferred: false,
		pk: oldImage.pk,
		sk: oldImage.sk,
		timestamp: Date.now(),
		eventId: ctx.eventID,
	});
});

// Deferred INSERT handler - enqueues to defer queue, then processes from SQS
router
	.onInsert(isTestItem, async (newImage, ctx) => {
		await sendVerification({
			operationType: "INSERT",
			isDeferred: true,
			pk: newImage.pk,
			sk: newImage.sk,
			timestamp: Date.now(),
			eventId: ctx.eventID,
		});
	})
	.defer("deferred-insert-handler");

// Export handlers for Lambda
export const streamHandler = router.streamHandler;
export const sqsHandler = router.sqsHandler;
