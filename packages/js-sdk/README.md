# @lix-js/sdk

JavaScript SDK for Lix, backed by the native Rust SDK.

## Install

```bash
npm install @lix-js/sdk
```

## Usage

```ts
import { FsBackend, openLix } from "@lix-js/sdk";

const lix = await openLix({
	backend: new FsBackend({
		path: "./workspace",
		syncAllFiles: true,
	}),
});

await lix.execute(
	"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
	["/hello.txt", new TextEncoder().encode("world")],
);

const result = await lix.execute("SELECT data FROM lix_file WHERE path = $1", [
	"/hello.txt",
]);
const bytes = result.rows[0]?.value("data").asBytes();

console.log(bytes && new TextDecoder().decode(bytes));

await lix.close();
```

## Branches

```ts
const main = await lix.activeBranchId();
const draft = await lix.createBranch({ name: "Draft" });

await lix.switchBranch({ branchId: draft.id });
await lix.execute(
	"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
	["/status.txt", new TextEncoder().encode("draft")],
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
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		["/a.txt", new TextEncoder().encode("1")],
	);
	await tx.execute(
		"INSERT INTO lix_file (path, data) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET data = excluded.data",
		["/b.txt", new TextEncoder().encode("2")],
	);
	await tx.commit();
} catch (error) {
	await tx.rollback();
	throw error;
}
```

## Notes

- `openLix()` opens a fresh in-memory Lix. Pass `new FsBackend({ path, syncAllFiles: true })` for a filesystem workspace directory backed by `<path>/.lix/.internal/rocksdb`.
- Pass `new FsBackend({ path, lixDir, syncAllFiles: true })` for filesystem sync with repository metadata in an external `.lix` directory and no workspace `.lix` directory.
- Pass `syncAllFiles: false` to start filesystem sync with no regular workspace files, then call `lix.importFilesystemPaths(["notes/today.md"])` to sync selected files. Imported paths are exact workspace-relative file paths, not directories or globs.
- Use `new SqliteBackend({ path })` when a single SQLite-backed `.lix` file is the application document itself, for example when defining a new file format and using Lix as the application's file format.
- The SDK is Node/native only right now; it is not browser-compatible.
- The package is ESM-only.
- The native addon is built from Rust and loaded by the TypeScript wrapper.
- SQL parameters use normal JavaScript values: `string`, finite `number`, `boolean`, `Uint8Array`, `null`, JSON-compatible arrays, and JSON-compatible plain objects.
- Use `Value.integer(...)`, `Value.real(...)`, `Value.text(...)`, `Value.json(...)`, or `Value.blob(...)` only when you need to pass an explicit native Lix value.
