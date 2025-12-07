/**
 * Tests for Zod schema integration
 */
import { z } from "zod";
import { StreamRouter } from "../src/stream-router";
import { createStreamEvent, createStreamRecord } from "./test-utils";

describe("Zod Schema Integration", () => {
	test("INSERT event processing with Zod schema", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const UserSchema = z.object({
			pk: z.string(),
			sk: z.string(),
			name: z.string(),
			email: z.string().email(),
		});

		router.onInsert(UserSchema, handler);

		const newItem = {
			pk: "user#1",
			sk: "profile",
			name: "Test User",
			email: "test@example.com",
		};
		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			newItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({
				pk: "user#1",
				name: "Test User",
				email: "test@example.com",
			}),
			expect.any(Object),
		);
	});

	test("Zod schema validation rejects invalid records", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const UserSchema = z.object({
			pk: z.string(),
			sk: z.string(),
			name: z.string(),
			email: z.string().email(), // Requires valid email
		});

		router.onInsert(UserSchema, handler);

		// Invalid email format
		const newItem = {
			pk: "user#1",
			sk: "profile",
			name: "Test User",
			email: "not-an-email",
		};
		const record = createStreamRecord(
			"INSERT",
			{ pk: "user#1", sk: "profile" },
			newItem,
		);
		const event = createStreamEvent([record]);

		const result = await router.process(event);

		// Handler should NOT be called because validation failed
		expect(handler).not.toHaveBeenCalled();
		// Record should still be processed successfully (just skipped)
		expect(result.succeeded).toBe(1);
		expect(result.failed).toBe(0);
	});

	test("Zod schema with MODIFY event", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const OrderSchema = z.object({
			pk: z.string(),
			sk: z.string(),
			orderId: z.string(),
			status: z.enum(["pending", "shipped", "delivered"]),
			total: z.number(),
		});

		router.onModify(OrderSchema, handler);

		const oldItem = {
			pk: "order#1",
			sk: "details",
			orderId: "ORD-123",
			status: "pending",
			total: 99,
		};
		const newItem = {
			pk: "order#1",
			sk: "details",
			orderId: "ORD-123",
			status: "shipped",
			total: 99,
		};
		const record = createStreamRecord(
			"MODIFY",
			{ pk: "order#1", sk: "details" },
			newItem,
			oldItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		const [oldImage, newImage] = handler.mock.calls[0];
		expect(oldImage.status).toBe("pending");
		expect(newImage.status).toBe("shipped");
	});

	test("Zod schema with REMOVE event", async () => {
		const router = new StreamRouter();
		const handler = jest.fn();

		const ProductSchema = z.object({
			pk: z.string(),
			sk: z.string(),
			productId: z.string(),
			name: z.string(),
			price: z.number().positive(),
		});

		router.onRemove(ProductSchema, handler);

		const oldItem = {
			pk: "product#1",
			sk: "info",
			productId: "PROD-456",
			name: "Widget",
			price: 29,
		};
		const record = createStreamRecord(
			"REMOVE",
			{ pk: "product#1", sk: "info" },
			undefined,
			oldItem,
		);
		const event = createStreamEvent([record]);

		await router.process(event);

		expect(handler).toHaveBeenCalledTimes(1);
		expect(handler).toHaveBeenCalledWith(
			expect.objectContaining({
				productId: "PROD-456",
				name: "Widget",
				price: 29,
			}),
			expect.any(Object),
		);
	});

	test("Multiple Zod schemas matching different entity types", async () => {
		const router = new StreamRouter();
		const userHandler = jest.fn();
		const orderHandler = jest.fn();

		const UserSchema = z.object({
			pk: z.string().startsWith("USER#"),
			sk: z.string(),
			name: z.string(),
		});

		const OrderSchema = z.object({
			pk: z.string().startsWith("ORDER#"),
			sk: z.string(),
			orderId: z.string(),
		});

		router.onInsert(UserSchema, userHandler);
		router.onInsert(OrderSchema, orderHandler);

		// User record
		const userRecord = createStreamRecord(
			"INSERT",
			{ pk: "USER#1", sk: "profile" },
			{ pk: "USER#1", sk: "profile", name: "John" },
		);

		// Order record
		const orderRecord = createStreamRecord(
			"INSERT",
			{ pk: "ORDER#1", sk: "details" },
			{ pk: "ORDER#1", sk: "details", orderId: "ORD-789" },
		);

		const event = createStreamEvent([userRecord, orderRecord]);

		await router.process(event);

		expect(userHandler).toHaveBeenCalledTimes(1);
		expect(userHandler).toHaveBeenCalledWith(
			expect.objectContaining({ pk: "USER#1", name: "John" }),
			expect.any(Object),
		);

		expect(orderHandler).toHaveBeenCalledTimes(1);
		expect(orderHandler).toHaveBeenCalledWith(
			expect.objectContaining({ pk: "ORDER#1", orderId: "ORD-789" }),
			expect.any(Object),
		);
	});
});
