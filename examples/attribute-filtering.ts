/**
 * Attribute change filtering example
 *
 * This example shows how to filter MODIFY events based on
 * specific attribute changes.
 */
import { StreamRouter } from "../src";

// Entity types
interface User {
	pk: string;
	sk: string;
	name: string;
	email: string;
	status: "active" | "inactive" | "suspended";
	preferences: {
		theme: string;
		notifications: boolean;
	};
	tags: string[];
}

const isUser = (record: unknown): record is User =>
	typeof record === "object" &&
	record !== null &&
	"pk" in record &&
	(record as { pk: string }).pk.startsWith("USER#");

const router = new StreamRouter();

// Only trigger when email changes
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Email changed: ${oldUser.email} -> ${newUser.email}`);
		await sendEmailVerification(newUser.email);
	},
	{ attribute: "email", changeType: "changed_attribute" },
);

// Only trigger when status changes
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Status changed: ${oldUser.status} -> ${newUser.status}`);

		if (newUser.status === "suspended") {
			await notifyAdmins(newUser);
		}
	},
	{ attribute: "status", changeType: "changed_attribute" },
);

// Trigger when a new attribute is added
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log("New attribute added to user profile");
	},
	{ changeType: "new_attribute" },
);

// Trigger when an attribute is removed
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log("Attribute removed from user profile");
	},
	{ changeType: "remove_attribute" },
);

// Trigger when items are added to a collection (list/set/map)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		const newTags = newUser.tags.filter((t) => !oldUser.tags.includes(t));
		console.log(`New tags added: ${newTags.join(", ")}`);
	},
	{ attribute: "tags", changeType: "new_item_in_collection" },
);

// Trigger when items are removed from a collection
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		const removedTags = oldUser.tags.filter((t) => !newUser.tags.includes(t));
		console.log(`Tags removed: ${removedTags.join(", ")}`);
	},
	{ attribute: "tags", changeType: "remove_item_from_collection" },
);

// Trigger on any change to preferences (nested object)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log("User preferences updated");
		await syncPreferencesToExternalSystem(newUser);
	},
	{ attribute: "preferences", changeType: "changed_attribute" },
);

// ============================================================================
// NESTED ATTRIBUTE FILTERING (Dot Notation)
// ============================================================================

// Trigger ONLY when preferences.theme changes (not other preferences)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Theme changed: ${oldUser.preferences.theme} -> ${newUser.preferences.theme}`);
	},
	{ attribute: "preferences.theme", changeType: "changed_attribute" },
);

// Trigger when preferences.notifications changes
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log(`Notifications setting changed to: ${newUser.preferences.notifications}`);
	},
	{ attribute: "preferences.notifications", changeType: "changed_attribute" },
);

// Watching a parent path catches ALL nested changes
// This triggers when preferences.theme OR preferences.notifications changes
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log("Any preference changed - syncing to external system");
		await syncPreferencesToExternalSystem(newUser);
	},
	{ attribute: "preferences" }, // No changeType = any change
);

// Multiple change types - triggers on any of them (OR logic)
router.onModify(
	isUser,
	async (oldUser, newUser, ctx) => {
		console.log("Tags collection modified");
	},
	{
		attribute: "tags",
		changeType: ["new_item_in_collection", "remove_item_from_collection"],
	},
);

// Lambda handler - simplified export
export const handler = router.streamHandler;

// Placeholder functions
async function sendEmailVerification(email: string) {
	console.log(`Sending verification to ${email}`);
}

async function notifyAdmins(user: User) {
	console.log(`Notifying admins about suspended user: ${user.name}`);
}

async function syncPreferencesToExternalSystem(user: User) {
	console.log(`Syncing preferences for user: ${user.name}`);
}
