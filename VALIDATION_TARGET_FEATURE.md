# ValidationTarget Feature

## Overview
The `validationTarget` option allows you to specify which DynamoDB image(s) to validate when matching records against discriminators or parsers.

## Configuration Options

### `validationTarget: "newImage"` (Default)
Validates only the new image. This is the default behavior and maintains backward compatibility.

**Best for**: Standard MODIFY handlers, INSERT handlers

```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Executes if newImage matches User type
});
```

### `validationTarget: "oldImage"`
Validates only the old image.

**Best for**: Processing records based on their previous state, cleanup operations

```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Executes if oldImage matches User type
  // Useful when you care about what the record WAS, not what it IS
}, { validationTarget: "oldImage" });
```

### `validationTarget: "both"`
Validates both images - BOTH must match for the handler to execute.

**Best for**: Type migration validation, ensuring consistency, strict schema enforcement

```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Executes ONLY if BOTH oldImage and newImage match User type
  // Perfect for validating that records maintain required structure
}, { validationTarget: "both" });
```

## Behavior by Event Type

| Event Type | Available Images | "newImage" | "oldImage" | "both" |
|------------|-----------------|-----------|-----------|--------|
| INSERT     | newImage only   | ✅ Validates newImage | ✅ Falls back to newImage | ✅ Falls back to newImage |
| MODIFY     | both images     | ✅ Validates newImage | ✅ Validates oldImage | ✅ Validates both (AND logic) |
| REMOVE     | oldImage only   | ✅ Falls back to oldImage | ✅ Validates oldImage | ✅ Falls back to oldImage |

## Real-World Use Cases

### 1. Type Migration Validation
Ensure records maintain required structure during schema changes:

```typescript
interface UserV2 {
  id: string;
  name: string;
  email: string;
  createdAt: string; // New required field
}

// Only process if BOTH old and new have createdAt
router.onModify(isUserV2, (oldImage, newImage, ctx) => {
  console.log("Both states have createdAt field");
  // Safe to process migration
}, { validationTarget: "both" });
```

### 2. Legacy Record Detection
Find and process records that need migration:

```typescript
interface LegacyUser {
  id: string;
  fullName: string; // Old field name
}

// Detect records that HAD the old structure
router.onModify(isLegacyUser, (oldImage, newImage, ctx) => {
  console.log("Found legacy record being updated");
  // Track or migrate legacy records
}, { validationTarget: "oldImage" });
```

### 3. Strict Type Enforcement
Only process changes where both states are valid:

```typescript
// Complex validation with Zod
const StrictUserSchema = z.object({
  id: z.string().uuid(),
  email: z.string().email(),
  age: z.number().min(18),
});

// Both old and new must pass strict validation
router.onModify(StrictUserSchema, (oldImage, newImage, ctx) => {
  // Guaranteed both images pass validation
  // Safe to perform operations requiring both states
}, { validationTarget: "both" });
```

### 4. Role Change Detection
Process records where role changed, but both states are valid users:

```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  if (oldImage.role !== newImage.role) {
    console.log(`Role changed: ${oldImage.role} -> ${newImage.role}`);
    // Update permissions, send notifications, etc.
  }
}, { validationTarget: "both" });
```

## Works with All Handler Types

### Standard Handlers
```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Process single record
}, { validationTarget: "both" });
```

### Batch Handlers
```typescript
router.onModify(isUser, (records) => {
  // All records in batch have both images validated
  for (const record of records) {
    console.log(record.oldImage, record.newImage);
  }
}, { batch: true, validationTarget: "both" });
```

### With Parsers (Zod, etc.)
```typescript
import { z } from "zod";

const UserSchema = z.object({
  id: z.string(),
  name: z.string(),
  email: z.string().email(),
});

router.onModify(UserSchema, (oldImage, newImage, ctx) => {
  // Both images are parsed and fully typed
}, { validationTarget: "both" });
```

### With Attribute Filtering
```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  console.log("Email changed and both states are valid users");
}, {
  validationTarget: "both",
  attribute: "email",
  changeType: "changed_attribute"
});
```

## API Reference

### Type Definition
```typescript
type ValidationTarget = "oldImage" | "newImage" | "both";

interface HandlerOptions {
  batch?: boolean;
  validationTarget?: ValidationTarget; // Default: "newImage"
}
```

### Available on Methods
- `onInsert(matcher, handler, options?)`
- `onModify(matcher, handler, options?)`
- `onRemove(matcher, handler, options?)`
- `onTTLRemove(matcher, handler, options?)`

## Testing
Comprehensive test suite available in `test/validation-target.test.ts`

## Examples
Full working examples in `examples/validation-target.ts`

## Backward Compatibility
✅ 100% backward compatible - defaults to `"newImage"` (existing behavior)
