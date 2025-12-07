/**
 * Middleware example
 *
 * This example shows how to use middleware for cross-cutting concerns
 * like logging, metrics, and error handling.
 */
import type { DynamoDBRecord, DynamoDBStreamEvent } from "aws-lambda";
import { StreamRouter } from "../src";

// Entity type
interface User {
	pk: string;
	sk: string;
	name: string;
}

const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("USER#");

const router = new StreamRouter();

// Logging middleware - logs every record
router.use(async (record: DynamoDBRecord, next: () => Promise<void>) => {
	const startTime = Date.now();
	console.log(`Processing ${record.eventName} event: ${record.eventID}`);

	await next(); // Continue to next middleware/handlers

	const duration = Date.now() - startTime;
	console.log(`Completed ${record.eventID} in ${duration}ms`);
});

// Metrics middleware - track processing metrics
router.use(async (record: DynamoDBRecord, next: () => Promise<void>) => {
	const eventType = record.eventName ?? "UNKNOWN";

	try {
		await next();
		// Record success metric
		await recordMetric("stream.record.success", 1, { eventType });
	} catch (error) {
		// Record failure metric
		await recordMetric("stream.record.failure", 1, { eventType });
		throw error; // Re-throw to propagate error
	}
});

// Filtering middleware - skip records from certain sources
router.use(async (record: DynamoDBRecord, next: () => Promise<void>) => {
	// Skip records from test tables
	if (record.eventSourceARN?.includes("test-table")) {
		console.log("Skipping test table record");
		return; // Don't call next() to skip processing
	}

	await next();
});

// Enrichment middleware - add data to the record
router.use(async (record: DynamoDBRecord, next: () => Promise<void>) => {
	// Add processing metadata (modifies the record in place)
	(record as DynamoDBRecord & { _metadata?: object })._metadata = {
		processedAt: new Date().toISOString(),
		lambdaRequestId: process.env.AWS_REQUEST_ID,
	};

	await next();
});

// Register handlers after middleware
router
	.onInsert(isUser, async (newUser) => {
		console.log(`New user: ${newUser.name}`);
	})
	.onModify(isUser, async (oldUser, newUser) => {
		console.log(`User updated: ${oldUser.name} -> ${newUser.name}`);
	});

// Lambda handler
export async function handler(event: DynamoDBStreamEvent) {
	const result = await router.process(event);

	if (result.failed > 0) {
		console.error("Processing errors:", result.errors);
	}
}

// Placeholder function
async function recordMetric(name: string, value: number, dimensions: Record<string, string>) {
	console.log(`Metric: ${name}=${value}`, dimensions);
}
