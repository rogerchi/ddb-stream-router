import {
	ConfigurationError,
	diffAttributes,
	HandlerRegistration,
	hasAttributeChange,
	StreamRouter,
	unmarshallMiddleware,
} from "../src";

describe("Public API exports", () => {
	test("StreamRouter is exported", () => {
		expect(StreamRouter).toBeDefined();
		const router = new StreamRouter();
		expect(router).toBeInstanceOf(StreamRouter);
	});

	test("HandlerRegistration is exported", () => {
		expect(HandlerRegistration).toBeDefined();
	});

	test("ConfigurationError is exported", () => {
		expect(ConfigurationError).toBeDefined();
		const error = new ConfigurationError("test");
		expect(error).toBeInstanceOf(ConfigurationError);
		expect(error).toBeInstanceOf(Error);
	});

	test("unmarshallMiddleware is exported", () => {
		expect(unmarshallMiddleware).toBeDefined();
		const middleware = unmarshallMiddleware();
		expect(typeof middleware).toBe("function");
	});

	test("diffAttributes is exported", () => {
		expect(diffAttributes).toBeDefined();
		const result = diffAttributes({ a: 1 }, { a: 2 });
		expect(result).toHaveProperty("hasChanges");
		expect(result).toHaveProperty("changes");
	});

	test("hasAttributeChange is exported", () => {
		expect(hasAttributeChange).toBeDefined();
	});
});
