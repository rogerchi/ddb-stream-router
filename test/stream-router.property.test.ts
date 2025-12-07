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
