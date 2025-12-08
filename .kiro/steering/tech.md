# Technology Stack

## Build System

**Projen** - Project configuration and build management
- All project configuration is managed through `.projenrc.ts`
- Run `npx projen` to regenerate configuration files
- DO NOT manually edit generated files (marked with projen comments)

## Language & Runtime

- **TypeScript 5.9+** - Strict mode enabled
- **Node.js** - Target: ES2020
- **Module system** - CommonJS (for Lambda compatibility)

## Key Dependencies

- `@aws-sdk/util-dynamodb` - DynamoDB data unmarshalling
- `@aws-sdk/client-sqs` - SQS client for deferred processing (peer dependency)

## Development Tools

- **Biome** - Linting and formatting (replaces ESLint/Prettier)
- **Jest** - Testing framework with ts-jest
- **fast-check** - Property-based testing
- **aws-sdk-client-mock** - Mocking AWS SDK clients
- **AWS CDK** - Integration test infrastructure

## Code Style

- **Indentation** - Tabs (configured in biome.jsonc)
- **Quotes** - Double quotes for JavaScript/TypeScript
- **Strict TypeScript** - All strict compiler options enabled
- **No unused variables** - Enforced by compiler

## Common Commands

```bash
# Build the project
npm run build

# Run tests
npm run test

# Run tests in watch mode
npm run test:watch

# Format and lint code
npm run biome

# Compile TypeScript
npm run compile

# Run integration tests
npm run integ:deploy   # Deploy test infrastructure
npm run integ:test     # Run integration tests
npm run integ:destroy  # Clean up test infrastructure

# Package for distribution
npm run package

# Upgrade dependencies
npm run upgrade
```

## TypeScript Configuration

- **rootDir** - `src/`
- **outDir** - `lib/`
- **Target** - ES2020
- **Module** - CommonJS
- **Source maps** - Separate .map files (not inline)
- **Strict mode** - All strict checks enabled
- **Declaration files** - Generated for type definitions
