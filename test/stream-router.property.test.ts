import * as fc from "fast-check";
import { ConfigurationError } from "../src/errors";
import { StreamRouter } from "../src/stream-router";
import type { StreamViewType } from "../src/types";

const VALID_STREAM_VIEW_TYPES: StreamViewType[] = [
	"KEYS_ONLY",
	"NEW_IMAGE",
	"OLD_IMAGE",
	"NEW_AND_OLD_IMAGES",
];

describe("StreamRouter Configuration Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 1: Valid stream view type configuration is stored correctly**
	 * **Validates: Requirements 1.1**
	 *
	 * For any valid StreamViewType value, when a StreamRouter is created with that
	 * configuration, the router's internal configuration should store that exact value.
	 */
	test("Property 1: Valid stream view type configuration is stored correctly", () => {
		fc.assert(
			fc.property(
				fc.constantFrom(...VALID_STREAM_VIEW_TYPES),
				(streamViewType) => {
					const router = new StreamRouter({ streamViewType });
					return router.streamViewType === streamViewType;
				},
			),
			{ numRuns: 100 },
		);
	});

	/**
	 * **Feature: dynamodb-stream-router, Property 2: Invalid stream view type configuration is rejected**
	 * **Validates: Requirements 1.3**
	 *
	 * For any string that is not a valid StreamViewType, when attempting to create
	 * a StreamRouter with that configuration, the router should throw a descriptive error.
	 */
	test("Property 2: Invalid stream view type configuration is rejected", () => {
		fc.assert(
			fc.property(
				fc
					.string()
					.filter(
						(s) => !VALID_STREAM_VIEW_TYPES.includes(s as StreamViewType),
					),
				(invalidType) => {
					try {
						new StreamRouter({ streamViewType: invalidType as StreamViewType });
						return false; // Should have thrown
					} catch (error) {
						return (
							error instanceof ConfigurationError &&
							error.message.includes("Invalid streamViewType")
						);
					}
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Default stream view type is NEW_AND_OLD_IMAGES", () => {
		const router = new StreamRouter();
		expect(router.streamViewType).toBe("NEW_AND_OLD_IMAGES");
	});

	test("Default unmarshall option is true", () => {
		const router = new StreamRouter();
		expect(router.unmarshall).toBe(true);
	});
});


describe("StreamRouter Handler Registration Properties", () => {
	/**
	 * **Feature: dynamodb-stream-router, Property 3: Handler registration preserves handler across all event types**
	 * **Validates: Requirements 2.1, 3.1, 4.1**
	 *
	 * For any event type (INSERT, MODIFY, REMOVE), matcher function, and handler function,
	 * when the corresponding registration method is called, the handler should be stored
	 * in the registry with the correct event type association.
	 */
	test("Property 3: Handler registration preserves handler across all event types", () => {
		const eventTypes = ["INSERT", "MODIFY", "REMOVE"] as const;

		fc.assert(
			fc.property(
				fc.constantFrom(...eventTypes),
				(eventType) => {
					const router = new StreamRouter();
					const discriminator = (record: unknown): record is { id: string } =>
						typeof record === "object" &&
						record !== null &&
						"id" in record &&
						typeof (record as { id: unknown }).id === "string";
					const handler = jest.fn();

					// Register handler based on event type
					if (eventType === "INSERT") {
						router.insert(discriminator, handler);
					} else if (eventType === "MODIFY") {
						router.modify(discriminator, handler);
					} else {
						router.remove(discriminator, handler);
					}

					// Verify handler is registered with correct event type
					expect(router.handlers).toHaveLength(1);
					expect(router.handlers[0].eventType).toBe(eventType);
					expect(router.handlers[0].matcher).toBe(discriminator);
					expect(router.handlers[0].handler).toBe(handler);
					expect(router.handlers[0].isParser).toBe(false);

					return true;
				},
			),
			{ numRuns: 100 },
		);
	});

	test("Parser detection correctly identifies parser vs discriminator", () => {
		const router = new StreamRouter();

		// Discriminator function
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;

		// Parser object (Zod-like)
		const parser = {
			parse: (data: unknown) => data as { id: string },
			safeParse: (data: unknown) => ({ success: true as const, data: data as { id: string } }),
		};

		router.insert(discriminator, jest.fn());
		router.insert(parser, jest.fn());

		expect(router.handlers[0].isParser).toBe(false);
		expect(router.handlers[1].isParser).toBe(true);
	});

	test("Method chaining works for all registration methods", () => {
		const router = new StreamRouter();
		const discriminator = (record: unknown): record is { id: string } =>
			typeof record === "object" && record !== null && "id" in record;
		const handler = jest.fn();

		const result = router
			.insert(discriminator, handler)
			.modify(discriminator, handler)
			.remove(discriminator, handler);

		expect(result).toBe(router);
		expect(router.handlers).toHaveLength(3);
	});
});
