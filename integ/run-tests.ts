#!/usr/bin/env npx ts-node
import * as fs from "node:fs";
import * as path from "node:path";
import assert from "node:assert";
import {
	DynamoDBClient,
	PutItemCommand,
	UpdateItemCommand,
	DeleteItemCommand,
} from "@aws-sdk/client-dynamodb";
import {
	SQSClient,
	ReceiveMessageCommand,
	DeleteMessageCommand,
	PurgeQueueCommand,
} from "@aws-sdk/client-sqs";

// Configuration interface
interface CdkOutputs {
	IntegrationTestStack: {
		TableArn: string;
		TableName: string;
		DeferQueueUrl: string;
		VerificationQueueUrl: string;
		HandlerFunctionArn: string;
		SqsHandlerFunctionArn: string;
	};
}

// Verification message interface
interface VerificationMessage {
	operationType: "INSERT" | "MODIFY" | "REMOVE";
	isDeferred: boolean;
	pk: string;
	sk: string;
	timestamp: number;
	eventId?: string;
}

// Load CDK outputs
function loadConfig(): CdkOutputs {
	const outputsPath = path.join(
		__dirname,
		"..",
		".cdk.outputs.integration.json",
	);
	if (!fs.existsSync(outputsPath)) {
		throw new Error(
			`CDK outputs file not found at ${outputsPath}. Run 'npm run integ:deploy' first.`,
		);
	}
	const content = fs.readFileSync(outputsPath, "utf-8");
	return JSON.parse(content) as CdkOutputs;
}

// Purge verification queue before tests
async function purgeQueue(
	sqsClient: SQSClient,
	queueUrl: string,
): Promise<void> {
	console.log("  Purging verification queue...");
	try {
		await sqsClient.send(new PurgeQueueCommand({ QueueUrl: queueUrl }));
		// Wait for purge to take effect (AWS recommends 60s, but we'll use less for testing)
		await new Promise((resolve) => setTimeout(resolve, 5000));
	} catch (error) {
		// PurgeQueue can fail if called too recently, ignore
		console.log("  (Queue purge skipped - may have been purged recently)");
	}
}

// Drain all messages from verification queue
async function drainVerificationQueue(
	sqsClient: SQSClient,
	queueUrl: string,
	expectedCount: number,
	timeoutMs = 90000,
): Promise<VerificationMessage[]> {
	const messages: VerificationMessage[] = [];
	const startTime = Date.now();
	let emptyPollCount = 0;
	const maxEmptyPolls = 3; // Stop after 3 consecutive empty polls

	console.log(
		`  Draining queue for ${expectedCount} messages (timeout: ${timeoutMs / 1000}s)...`,
	);

	while (Date.now() - startTime < timeoutMs) {
		const response = await sqsClient.send(
			new ReceiveMessageCommand({
				QueueUrl: queueUrl,
				MaxNumberOfMessages: 10,
				WaitTimeSeconds: 10,
				VisibilityTimeout: 30,
			}),
		);

		if (response.Messages && response.Messages.length > 0) {
			emptyPollCount = 0; // Reset empty poll counter

			for (const msg of response.Messages) {
				if (msg.Body && msg.ReceiptHandle) {
					const parsed = JSON.parse(msg.Body) as VerificationMessage;
					messages.push(parsed);
					console.log(
						`  Received: ${parsed.operationType} (deferred: ${parsed.isDeferred}) - ${parsed.pk}`,
					);

					// Delete message after reading
					await sqsClient.send(
						new DeleteMessageCommand({
							QueueUrl: queueUrl,
							ReceiptHandle: msg.ReceiptHandle,
						}),
					);
				}
			}
		} else {
			emptyPollCount++;
			console.log(
				`  Empty poll ${emptyPollCount}/${maxEmptyPolls} (collected ${messages.length}/${expectedCount})`,
			);

			// If we have enough messages and got empty polls, we're done
			if (messages.length >= expectedCount && emptyPollCount >= maxEmptyPolls) {
				break;
			}
		}

		// Early exit if we have all expected messages
		if (messages.length >= expectedCount) {
			// Do one more poll to make sure we got everything
			await new Promise((resolve) => setTimeout(resolve, 2000));
		}
	}

	return messages;
}

// Main test runner
async function runTests(): Promise<void> {
	console.log("\n🧪 DDB Stream Router Integration Tests\n");
	console.log("=".repeat(50));

	// Load configuration
	console.log("\n📋 Loading configuration...");
	const config = loadConfig();
	const { TableName, VerificationQueueUrl } = config.IntegrationTestStack;
	console.log(`  Table: ${TableName}`);
	console.log(`  Verification Queue: ${VerificationQueueUrl}`);

	// Initialize clients
	const ddbClient = new DynamoDBClient({});
	const sqsClient = new SQSClient({});

	// Test data
	const testPk = `TEST#${Date.now()}`;
	const testSk = "v0";

	try {
		// Purge queue before starting
		await purgeQueue(sqsClient, VerificationQueueUrl);

		// Perform all DynamoDB operations first
		console.log("\n📝 Performing DynamoDB operations...");
		console.log("-".repeat(50));

		// Operation 1: INSERT
		console.log(`  1. INSERT: pk=${testPk}, sk=${testSk}`);
		await ddbClient.send(
			new PutItemCommand({
				TableName,
				Item: {
					pk: { S: testPk },
					sk: { S: testSk },
					data: { S: "initial data" },
				},
			}),
		);

		// Small delay between operations to ensure ordering
		await new Promise((resolve) => setTimeout(resolve, 1000));

		// Operation 2: MODIFY
		console.log(`  2. MODIFY: pk=${testPk}, sk=${testSk}`);
		await ddbClient.send(
			new UpdateItemCommand({
				TableName,
				Key: {
					pk: { S: testPk },
					sk: { S: testSk },
				},
				UpdateExpression: "SET #data = :data",
				ExpressionAttributeNames: { "#data": "data" },
				ExpressionAttributeValues: { ":data": { S: "updated data" } },
			}),
		);

		await new Promise((resolve) => setTimeout(resolve, 1000));

		// Operation 3: REMOVE
		console.log(`  3. REMOVE: pk=${testPk}, sk=${testSk}`);
		await ddbClient.send(
			new DeleteItemCommand({
				TableName,
				Key: {
					pk: { S: testPk },
					sk: { S: testSk },
				},
			}),
		);

		// Wait for stream processing + deferred processing
		console.log("\n⏳ Waiting for stream and deferred processing...");
		console.log("-".repeat(50));

		// Expected messages:
		// - INSERT immediate + INSERT deferred = 2
		// - MODIFY immediate = 1
		// - REMOVE immediate = 1
		// Total = 4
		const expectedMessageCount = 4;

		const allMessages = await drainVerificationQueue(
			sqsClient,
			VerificationQueueUrl,
			expectedMessageCount,
			90000, // 90 second timeout
		);

		// Analyze results
		console.log("\n📊 Analyzing results...");
		console.log("-".repeat(50));
		console.log(`  Total messages received: ${allMessages.length}`);

		// Filter messages for our test pk
		const testMessages = allMessages.filter((m) => m.pk === testPk);
		console.log(`  Messages for test pk: ${testMessages.length}`);

		// Categorize messages
		const insertImmediate = testMessages.filter(
			(m) => m.operationType === "INSERT" && !m.isDeferred,
		);
		const insertDeferred = testMessages.filter(
			(m) => m.operationType === "INSERT" && m.isDeferred,
		);
		const modifyImmediate = testMessages.filter(
			(m) => m.operationType === "MODIFY" && !m.isDeferred,
		);
		const removeImmediate = testMessages.filter(
			(m) => m.operationType === "REMOVE" && !m.isDeferred,
		);

		console.log(`  INSERT immediate: ${insertImmediate.length}`);
		console.log(`  INSERT deferred: ${insertDeferred.length}`);
		console.log(`  MODIFY immediate: ${modifyImmediate.length}`);
		console.log(`  REMOVE immediate: ${removeImmediate.length}`);

		// Run assertions
		console.log("\n✅ Running assertions...");
		console.log("-".repeat(50));

		let totalTests = 0;
		let passedTests = 0;

		// Test 1: INSERT immediate
		totalTests++;
		try {
			assert.strictEqual(
				insertImmediate.length,
				1,
				`Expected 1 immediate INSERT, got ${insertImmediate.length}`,
			);
			console.log("  ✅ INSERT immediate: PASSED");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ INSERT immediate: FAILED - ${(error as Error).message}`);
		}

		// Test 2: INSERT deferred
		totalTests++;
		try {
			assert.strictEqual(
				insertDeferred.length,
				1,
				`Expected 1 deferred INSERT, got ${insertDeferred.length}`,
			);
			console.log("  ✅ INSERT deferred: PASSED");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ INSERT deferred: FAILED - ${(error as Error).message}`);
		}

		// Test 3: MODIFY immediate
		totalTests++;
		try {
			assert.strictEqual(
				modifyImmediate.length,
				1,
				`Expected 1 immediate MODIFY, got ${modifyImmediate.length}`,
			);
			console.log("  ✅ MODIFY immediate: PASSED");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ MODIFY immediate: FAILED - ${(error as Error).message}`);
		}

		// Test 4: REMOVE immediate
		totalTests++;
		try {
			assert.strictEqual(
				removeImmediate.length,
				1,
				`Expected 1 immediate REMOVE, got ${removeImmediate.length}`,
			);
			console.log("  ✅ REMOVE immediate: PASSED");
			passedTests++;
		} catch (error) {
			console.log(`  ❌ REMOVE immediate: FAILED - ${(error as Error).message}`);
		}

		// Test 5: Deferred INSERT has correct data
		totalTests++;
		try {
			assert.ok(insertDeferred[0], "Missing deferred INSERT message");
			assert.strictEqual(insertDeferred[0].pk, testPk);
			assert.strictEqual(insertDeferred[0].sk, testSk);
			console.log("  ✅ Deferred INSERT data: PASSED");
			passedTests++;
		} catch (error) {
			console.log(
				`  ❌ Deferred INSERT data: FAILED - ${(error as Error).message}`,
			);
		}

		// Summary
		console.log("\n" + "=".repeat(50));
		console.log("📊 Test Summary");
		console.log("-".repeat(50));
		console.log(`  Total:  ${totalTests}`);
		console.log(`  Passed: ${passedTests}`);
		console.log(`  Failed: ${totalTests - passedTests}`);

		if (passedTests === totalTests) {
			console.log("\n✅ All integration tests passed!\n");
			process.exit(0);
		} else {
			console.log("\n❌ Some integration tests failed.\n");
			process.exit(1);
		}
	} catch (error) {
		console.error("\n❌ Test execution error:", error);
		process.exit(1);
	}
}

// Run tests
runTests();
