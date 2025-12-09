/**
 * Example: Handling TTL Removal Events
 *
 * This example demonstrates how to handle DynamoDB TTL-triggered removal events
 * separately from user-initiated deletions using the onTTLRemove handler.
 */

import { StreamRouter } from "../src/index.js";

interface Session {
	pk: string;
	sk: string;
	userId: string;
	sessionId: string;
	ttl: number;
}

interface User {
	pk: string;
	sk: string;
	userId: string;
	name: string;
	email: string;
}

// Type guards
const isSession = (record: unknown): record is Session =>
	typeof record === "object" &&
	record !== null &&
	"sk" in record &&
	(record as Session).sk.startsWith("session#");

const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"sk" in record &&
	(record as User).sk === "profile";

// Create router
const router = new StreamRouter();

// Handler for TTL-triggered session removals
// This fires when sessions expire due to TTL
router.onTTLRemove(isSession, async (oldImage, ctx) => {
	console.log("Session expired via TTL:", {
		userId: oldImage.userId,
		sessionId: oldImage.sessionId,
		ttl: oldImage.ttl,
		eventID: ctx.eventID,
	});

	// Clean up related resources
	// e.g., notify caching layer, update metrics, etc.
	await cleanupExpiredSession(oldImage);
});

// Handler for user-initiated session removals (logout)
// Use excludeTTL: true to only handle explicit deletions
router.onRemove(isSession, async (oldImage, ctx) => {
	console.log("Session explicitly deleted (user logout):", {
		userId: oldImage.userId,
		sessionId: oldImage.sessionId,
		eventID: ctx.eventID,
	});

	// Handle logout-specific logic
	await handleUserLogout(oldImage);
}, { excludeTTL: true });

// Handler for user profile deletions
// By default, excludeTTL is false, so this handles both TTL and explicit deletes
// (though user profiles typically wouldn't have TTL)
router.onRemove(isUser, async (oldImage, ctx) => {
	console.log("User profile deleted:", {
		userId: oldImage.userId,
		name: oldImage.name,
		email: oldImage.email,
		eventID: ctx.eventID,
	});

	// Handle user deletion logic
	await deleteUserData(oldImage);
});

// Example with batch processing for TTL removals
// Useful when many sessions expire at similar times
router.onTTLRemove(isSession, async (records) => {
	console.log(`Processing batch of ${records.length} expired sessions`);

	const sessionIds = records.map(r => r.oldImage.sessionId);

	// Batch cleanup operations
	await batchCleanupExpiredSessions(sessionIds);
}, { batch: true });

// Helper functions (implementations not shown)
async function cleanupExpiredSession(session: Session): Promise<void> {
	// Implementation: clear cache, update metrics, etc.
	console.log(`Cleaning up expired session: ${session.sessionId}`);
}

async function handleUserLogout(session: Session): Promise<void> {
	// Implementation: invalidate tokens, log audit trail, etc.
	console.log(`Handling logout for session: ${session.sessionId}`);
}

async function deleteUserData(user: User): Promise<void> {
	// Implementation: delete related data, trigger downstream systems, etc.
	console.log(`Deleting user data for: ${user.userId}`);
}

async function batchCleanupExpiredSessions(sessionIds: string[]): Promise<void> {
	// Implementation: batch operations for efficiency
	console.log(`Batch cleanup of ${sessionIds.length} sessions`);
}

// Export Lambda handler
export const handler = router.streamHandler;

/**
 * Key Points:
 *
 * 1. onTTLRemove() - Only handles removals triggered by DynamoDB TTL
 *    - Identified by userIdentity.type === "Service" and
 *      userIdentity.principalId === "dynamodb.amazonaws.com"
 *
 * 2. onRemove() with excludeTTL: true - Only handles user-initiated deletions
 *    - Excludes TTL-triggered removals
 *
 * 3. onRemove() with excludeTTL: false (default) - Handles all removals
 *    - Includes both TTL-triggered and user-initiated deletions
 *
 * 4. You can register both handlers with different matchers to handle
 *    different types of records differently
 *
 * 5. TTL removal handlers support all the same features:
 *    - Discriminators and parsers
 *    - Batch processing
 *    - Middleware
 *    - Deferred processing via .defer()
 *
 * Use Cases:
 * - Metrics: Track session expirations vs explicit logouts separately
 * - Cleanup: Different cleanup logic for TTL vs explicit deletes
 * - Auditing: Log different event types for compliance
 * - Notifications: Send alerts based on deletion type
 */
