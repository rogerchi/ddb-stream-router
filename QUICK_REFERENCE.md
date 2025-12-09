# ValidationTarget Quick Reference

## Syntax

```typescript
router.onModify(
  matcher,           // Discriminator or Parser
  handler,           // Handler function
  {
    validationTarget: "newImage" | "oldImage" | "both"  // Optional, default: "newImage"
  }
);
```

## Quick Comparison

| Option | Validates | Use When |
|--------|-----------|----------|
| `"newImage"` (default) | New image only | Standard processing, current state matters |
| `"oldImage"` | Old image only | Processing based on previous state |
| `"both"` | Both images (AND) | Both states must be valid, migration checks |

## Code Snippets

### Validate New Image (Default)
```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Runs if newImage is a User
});
```

### Validate Old Image
```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Runs if oldImage is a User
}, { validationTarget: "oldImage" });
```

### Validate Both Images
```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Runs ONLY if BOTH images are Users
}, { validationTarget: "both" });
```

### With Zod Parser
```typescript
import { z } from "zod";

const UserSchema = z.object({
  id: z.string(),
  email: z.string().email(),
});

router.onModify(UserSchema, (oldImage, newImage, ctx) => {
  // Both images validated and typed by Zod
}, { validationTarget: "both" });
```

### Batch Processing
```typescript
router.onModify(isUser, (records) => {
  // All records have validated images
}, { 
  batch: true, 
  validationTarget: "both" 
});
```

### Combined with Attribute Filtering
```typescript
router.onModify(isUser, (oldImage, newImage, ctx) => {
  // Email changed AND both states are valid Users
}, {
  validationTarget: "both",
  attribute: "email",
  changeType: "changed_attribute"
});
```

## Decision Tree

```
Do you need to validate...
  └─ Only the NEW state?
     └─ Use: { validationTarget: "newImage" } or omit (default)
  
  └─ Only the OLD state?
     └─ Use: { validationTarget: "oldImage" }
  
  └─ BOTH states must be valid?
     └─ Use: { validationTarget: "both" }
```

## Common Patterns

### Pattern 1: Type Migration Safety
Ensure records have required fields before and after:
```typescript
router.onModify(hasRequiredFields, handler, { 
  validationTarget: "both" 
});
```

### Pattern 2: Legacy Detection
Find records that HAD old structure:
```typescript
router.onModify(isLegacyFormat, handler, { 
  validationTarget: "oldImage" 
});
```

### Pattern 3: State Transition
Process when both states are valid:
```typescript
router.onModify(isValidState, (old, new) => {
  // Safe to compare and process transition
}, { validationTarget: "both" });
```

## Applies To

✅ `onInsert()` - only newImage available, "both"/"oldImage" fall back to "newImage"
✅ `onModify()` - all three options work as expected
✅ `onRemove()` - only oldImage available, "both"/"newImage" fall back to "oldImage"
✅ `onTTLRemove()` - same as `onRemove()`

## Backward Compatibility

✅ Existing code continues to work without changes
✅ Default behavior unchanged (validates newImage)
✅ Opt-in feature via options parameter
