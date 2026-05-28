# @lix-js/sdk

JavaScript SDK for Lix, backed by the native Rust SDK.

## Install

```bash
npm install @lix-js/sdk
```

## Usage

```ts
import { openLix, SqliteBackend } from "@lix-js/sdk";

const lix = await openLix({
	backend: new SqliteBackend({ path: "app.lix" }),
});

await lix.execute(
	"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
	["hello", "world"],
);

const result = await lix.execute(
	"SELECT value FROM lix_key_value WHERE key = $1",
	["hello"],
);

console.log(result.rows[0]?.get("value"));

await lix.close();
```

## Branches

```ts
const main = await lix.activeBranchId();
const draft = await lix.createBranch({ name: "Draft" });

await lix.switchBranch({ branchId: draft.id });
await lix.execute(
	"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
	["status", "draft"],
);

await lix.switchBranch({ branchId: main });
const preview = await lix.mergeBranchPreview({ sourceBranchId: draft.id });
const merge = await lix.mergeBranch({ sourceBranchId: draft.id });
```

## Transactions

```ts
const tx = await lix.beginTransaction();

try {
	await tx.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["a", "1"],
	);
	await tx.execute(
		"INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
		["b", "2"],
	);
	await tx.commit();
} catch (error) {
	await tx.rollback();
	throw error;
}
```

## Notes

- The SDK currently opens Lix through `SqliteBackend`.
- The native addon is built from Rust and loaded by the TypeScript wrapper.
- SQL parameters use normal JavaScript values: `string`, `number`, `boolean`, `Uint8Array`, `null`, arrays, and objects.
