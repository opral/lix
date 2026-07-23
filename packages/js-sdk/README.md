# @lix-js/sdk

JavaScript SDK for Lix. It uses the native Rust addon in Node.js and the same
Rust SDK compiled to WebAssembly in browsers.

## Install

```bash
npm install @lix-js/sdk
```

## Usage

The default in-memory storage works in browsers and Node.js:

```ts
import { openLix } from "@lix-js/sdk";

const lix = await openLix();
const result = await lix.execute("SELECT $1 AS message", ["hello"]);
console.log(result.rows[0]?.get("message"));
await lix.close();
```

## Remote workspaces

Use the same Lix client as a thin client against a hosted workspace:

```ts
const lix = await openLix({
	server: {
		mode: "remote",
		url: "https://lixray.com/@namespace/workspace",
		headers: async () => ({
			Authorization: `Bearer ${await accessToken()}`,
		}),
	},
});

const files = lix.observe("SELECT path FROM lix_file ORDER BY path");
const initial = await files.next();

await lix.execute(
	"INSERT INTO lix_file (path, data) VALUES ($1, $2)",
	["/hello.txt", new TextEncoder().encode("hello")],
);
const update = await files.next();

files.close();
await lix.close();
```

Remote mode uses the server for persistence and does not open a local engine,
so it does not take a client `storage`. Dynamic headers are resolved for every
request and observation reconnect. An injected `fetch` can route requests
through a service binding or another authorized server-side transport.

Agent-style clients that always operate on one branch can request immutable,
server-owned branch scope. This avoids reading the workspace's shared branch
selector on every operation and prevents another client from moving the
session to a different branch:

```ts
const lix = await openLix({
	server: {
		mode: "remote",
		url: "https://lixray.com/@namespace/workspace",
		branchId: "agent-draft-id",
	},
});
```

`switchBranch()` is rejected for a branch-pinned session; open a new remote
client to work on another branch. The SDK fails closed if the server does not
confirm the requested immutable scope.

Browser deployments that only use remote mode can import the same API from the
remote-only entrypoint. This keeps the local worker and engine WASM out of the
bundle:

```ts
import { openLix } from "@lix-js/sdk/remote";

const lix = await openLix({
	server: {
		mode: "remote",
		url: "https://lixray.com/@namespace/workspace",
	},
});
```

Filesystem sync and SQLite persistence use native Node.js dependencies:

```ts
import { LocalFilesystem, openLix } from "@lix-js/sdk";

const lix = await openLix({
	storage: new LocalFilesystem({
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

- `openLix()` opens a fresh in-memory Lix. Pass `new LocalFilesystem({ path, syncAllFiles: true })` for a filesystem workspace directory backed by `<path>/.lix/.internal/rocksdb`.
- Pass `new LocalFilesystem({ path, lixDir, syncAllFiles: true })` for filesystem sync with repository metadata in an external `.lix` directory and no workspace `.lix` directory.
- Pass `syncAllFiles: false` to start filesystem sync with no regular workspace files, then call `storage.importPaths(["notes/today.md"])` on the `LocalFilesystem` instance to sync selected files. Imported paths are exact workspace-relative file paths, not directories or globs.
- Use `new SQLite({ path })` when a single SQLite-backed `.lix` file is the application document itself, for example when defining a new file format and using Lix as the application's file format.
- In browsers, `openLix()` loads the Rust engine as WebAssembly and uses the
  in-memory storage.
- `LocalFilesystem` and `SQLite` are Node.js-only. Constructing them is safe in
  shared code, but passing one to `openLix()` in a browser throws an error.
- The package is ESM-only.
- The package uses conditional ESM imports internally: Node.js resolves the
  native N-API binding, while browsers and other runtimes resolve the portable
  WebAssembly binding. Vite follows this split without consumer configuration.
- Every `openLix()` owns one dedicated worker. The engine, storage, and
  installed WASM plugin components all run in that worker in both Node.js and
  browsers, so database and plugin work does not block the page's main thread.
- Installed WASM plugin components are transpiled with JCO and executed by the
  worker's WebAssembly runtime in both environments. Plugin execution does not
  yet enforce the declared fuel, timeout, or memory limits, so only install
  trusted plugins.
- A page Content Security Policy only needs to permit the package's same-origin
  worker. WebAssembly compilation and JCO's generated `data:` module imports
  happen inside that worker, so they can be scoped to the worker script's HTTP
  response instead of being allowed by the document:

  ```http
  # HTML document response
  Content-Security-Policy: default-src 'self'; script-src 'self'; worker-src 'self'

  # Lix worker response (Vite emits assets/entry.browser-<hash>.js)
  Content-Security-Policy: default-src 'none'; script-src 'self' data: 'wasm-unsafe-eval'; connect-src 'self'
  ```

  Hosts that apply one policy to every response can use
  `script-src 'self' data: 'wasm-unsafe-eval'; worker-src 'self'` globally
  instead. Worker-scoped headers keep those permissions out of the page.
- SQL parameters use normal JavaScript values: `string`, finite `number`, `boolean`, `Uint8Array`, `null`, JSON-compatible arrays, and JSON-compatible plain objects.
- Use `Value.integer(...)`, `Value.real(...)`, `Value.text(...)`, `Value.json(...)`, or `Value.blob(...)` only when you need to pass an explicit native Lix value.

## Browser development

The browser suite runs the published package shape in a real headless Chromium
page through Vite/Vitest Browser Mode:

```bash
rustup target add wasm32-unknown-unknown
cargo install wasm-bindgen-cli --version 0.2.122 --locked
npx playwright install chromium
npm run test:browser
```

`npm run test:browser:production` additionally packs the SDK, installs the
tarball into a minimal Vite app, makes a production build, and exercises SQL
plus both bundled plugins in Chromium. It runs with both worker-scoped and
global strict CSP headers.

Use `npm run build:wasm:dev` while iterating on the Rust bridge when release
optimization is unnecessary.
