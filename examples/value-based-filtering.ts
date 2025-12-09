/**
 * Value-based filtering example
 *
 * This example demonstrates the enhanced onModify filtering capabilities:
 * - Matching when a field is updated TO a specific value (newFieldValue)
 * - Matching when a field is updated FROM a specific value (oldFieldValue)
 * - Matching when a field is updated FROM one value TO another (both)
 * - Detecting when a field is cleared (field_cleared change type)
 */
import { StreamRouter } from "../src";

// Entity types
interface User {
	pk: string;
	sk: string;
	name: string;
	email: string | null;
	status: "pending" | "active" | "suspended" | "deleted";
	lastLoginDate: string | null;
	subscriptionTier: "free" | "pro" | "enterprise";
}

const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("USER#");

const router = new StreamRouter();

// ============================================================================
// NEW FIELD VALUE FILTERING
// ============================================================================

// Trigger only when status changes TO "active" (from any previous value)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`User ${newUser.name} is now active!`);
		await sendWelcomeEmail(newUser);
		await enableAllFeatures(newUser);
	},
	{
		attribute: "status",
		newFieldValue: "active",
	},
);

// Trigger only when status changes TO "suspended"
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`User ${newUser.name} has been suspended`);
		await notifySecurityTeam(newUser);
		await disableUserAccess(newUser);
	},
	{
		attribute: "status",
		newFieldValue: "suspended",
	},
);

// Trigger when subscription is upgraded TO "enterprise"
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`User ${newUser.name} upgraded to enterprise!`);
		await assignAccountManager(newUser);
		await sendEnterpriseWelcomeKit(newUser);
	},
	{
		attribute: "subscriptionTier",
		newFieldValue: "enterprise",
	},
);

// ============================================================================
// OLD FIELD VALUE FILTERING
// ============================================================================

// Trigger only when status changes FROM "pending" (to any new value)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`User ${newUser.name} moved from pending status to ${newUser.status}`);
		await recordOnboardingCompletion(newUser);
	},
	{
		attribute: "status",
		oldFieldValue: "pending",
	},
);

// Trigger when leaving "free" tier (upgrading to any paid tier)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(
			`User ${newUser.name} upgraded from free to ${newUser.subscriptionTier}`,
		);
		await trackConversion(newUser);
		await unlockPremiumFeatures(newUser);
	},
	{
		attribute: "subscriptionTier",
		oldFieldValue: "free",
	},
);

// ============================================================================
// COMBINED OLD AND NEW VALUE FILTERING
// ============================================================================

// Trigger ONLY when status changes from "pending" to "active"
// (not any other status transitions)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`User ${newUser.name} completed activation from pending state`);
		await sendActivationSuccessEmail(newUser);
		await trackSuccessfulOnboarding(newUser);
	},
	{
		attribute: "status",
		oldFieldValue: "pending",
		newFieldValue: "active",
	},
);

// Trigger ONLY when downgrading from "enterprise" to "pro"
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`User ${newUser.name} downgraded from enterprise to pro`);
		await removeEnterpriseFeatures(newUser);
		await notifyAccountManager(newUser);
	},
	{
		attribute: "subscriptionTier",
		oldFieldValue: "enterprise",
		newFieldValue: "pro",
	},
);

// Trigger when user is suspended from active state (not from pending)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Active user ${newUser.name} was suspended`);
		await alertSecurityTeam("active_user_suspended", newUser);
	},
	{
		attribute: "status",
		oldFieldValue: "active",
		newFieldValue: "suspended",
	},
);

// ============================================================================
// FIELD_CLEARED CHANGE TYPE
// ============================================================================

// Trigger when email is cleared (set to null)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(
			`Email cleared for user ${newUser.name} (was: ${oldUser.email})`,
		);
		await handleEmailRemoval(newUser);
		await notifyComplianceTeam(newUser);
	},
	{
		attribute: "email",
		changeType: "field_cleared",
	},
);

// Trigger when lastLoginDate is cleared
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Last login date cleared for user ${newUser.name}`);
		await markAsNeverLoggedIn(newUser);
	},
	{
		attribute: "lastLoginDate",
		changeType: "field_cleared",
	},
);

// ============================================================================
// COMBINING FIELD_CLEARED WITH VALUE FILTERS
// ============================================================================

// Trigger when a verified email is cleared (not just any email)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		// Only trigger if the old email ended with verified domain
		if (oldUser.email?.endsWith("@verified.com")) {
			console.log(`Verified email was cleared for user ${newUser.name}`);
			await alertVerificationTeam(newUser);
		}
	},
	{
		attribute: "email",
		changeType: "field_cleared",
		// Could also check specific old email value if needed
	},
);

// ============================================================================
// COMBINING WITH EXISTING FEATURES
// ============================================================================

// Combine value filtering with change type
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Status changed to active`);
	},
	{
		attribute: "status",
		changeType: "changed_attribute", // Must be a change (not new or removed)
		newFieldValue: "active", // AND the new value must be "active"
	},
);

// Multiple change types including field_cleared
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Email was cleared or changed`);
	},
	{
		attribute: "email",
		changeType: ["field_cleared", "changed_attribute"], // Either cleared OR changed
	},
);

// Lambda handler - simplified export
export const handler = router.streamHandler;

// ============================================================================
// Placeholder functions for demonstration
// ============================================================================

async function sendWelcomeEmail(user: User) {
	console.log(`Sending welcome email to ${user.email}`);
}

async function enableAllFeatures(user: User) {
	console.log(`Enabling features for ${user.name}`);
}

async function notifySecurityTeam(user: User) {
	console.log(`Notifying security team about ${user.name}`);
}

async function disableUserAccess(user: User) {
	console.log(`Disabling access for ${user.name}`);
}

async function assignAccountManager(user: User) {
	console.log(`Assigning account manager to ${user.name}`);
}

async function sendEnterpriseWelcomeKit(user: User) {
	console.log(`Sending enterprise welcome kit to ${user.name}`);
}

async function recordOnboardingCompletion(user: User) {
	console.log(`Recording onboarding completion for ${user.name}`);
}

async function trackConversion(user: User) {
	console.log(`Tracking conversion for ${user.name}`);
}

async function unlockPremiumFeatures(user: User) {
	console.log(`Unlocking premium features for ${user.name}`);
}

async function sendActivationSuccessEmail(user: User) {
	console.log(`Sending activation success email to ${user.email}`);
}

async function trackSuccessfulOnboarding(user: User) {
	console.log(`Tracking successful onboarding for ${user.name}`);
}

async function removeEnterpriseFeatures(user: User) {
	console.log(`Removing enterprise features from ${user.name}`);
}

async function notifyAccountManager(user: User) {
	console.log(`Notifying account manager about ${user.name}`);
}

async function alertSecurityTeam(reason: string, user: User) {
	console.log(`Alerting security team: ${reason} for ${user.name}`);
}

async function handleEmailRemoval(user: User) {
	console.log(`Handling email removal for ${user.name}`);
}

async function notifyComplianceTeam(user: User) {
	console.log(`Notifying compliance team about ${user.name}`);
}

async function markAsNeverLoggedIn(user: User) {
	console.log(`Marking ${user.name} as never logged in`);
}

async function alertVerificationTeam(user: User) {
	console.log(`Alerting verification team about ${user.name}`);
}
