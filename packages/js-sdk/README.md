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

## Remote repositories

Use the same Lix client as a thin client against a hosted repository:

```ts
const lix = await openLix({
  server: {
    mode: "remote",
    url: "https://example.com/repositories/acme",
    headers: async () => ({
      Authorization: `Bearer ${await accessToken()}`,
    }),
  },
});

const files = lix.observe("SELECT path FROM lix_file ORDER BY path");
const initial = await files.next();

await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/hello.txt",
  new TextEncoder().encode("hello"),
]);
const update = await files.next();

files.close();
await lix.close();
```

Without `storage`, remote mode uses the server for all persistence and does not
open a local engine. Dynamic headers are resolved for every request and
observation reconnect. An injected `fetch` can route requests through a service
binding or another authorized server-side transport.

Remote server sessions are branch-pinned, so switching one client does not
switch another client. Browser-local application state belongs to the
application rather than the remote Lix handle.

Filesystem sync uses native Node.js dependencies:

```ts
import { openLix } from "@lix-js/sdk";
import { FilesystemStorage } from "@lix-js/storage-filesystem";

const lix = await openLix({
  storage: new FilesystemStorage({ path: "./repository" }),
});

await lix.execute(
  "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET content = excluded.content",
  ["/hello.txt", new TextEncoder().encode("world")],
);

const result = await lix.execute(
  "SELECT content FROM lix_file WHERE path = $1",
  ["/hello.txt"],
);
const bytes = result.rows[0]?.value("content").asBytes();

console.log(bytes && new TextDecoder().decode(bytes));

await lix.close();
```

## Discover the SQL contract

Lix extends the standard `information_schema.columns` relation with
`lix_value_kind` and `lix_insert_policy`. Inspect it before generating writes:

```sql
SELECT table_name, column_name, data_type, is_nullable, column_default,
       lix_value_kind, lix_insert_policy
FROM information_schema.columns
WHERE table_name = 'lix_file'
ORDER BY ordinal_position;
```

`lix_insert_policy` distinguishes `REQUIRED`, `DEFAULT`, `CONDITIONAL`, and
`READ_ONLY` columns. For the complete table and history-function map, see
[SQL Surfaces](https://lix.dev/docs/surfaces).

## Branches

```ts
const main = await lix.activeBranchId();
const draft = await lix.createBranch({ name: "Draft" });

await lix.switchBranch({ branchId: draft.id });
await lix.execute(
  "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET content = excluded.content",
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
    "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET content = excluded.content",
    ["/a.txt", new TextEncoder().encode("1")],
  );
  await tx.execute(
    "INSERT INTO lix_file (path, content) VALUES ($1, $2) ON CONFLICT (path) DO UPDATE SET content = excluded.content",
    ["/b.txt", new TextEncoder().encode("2")],
  );
  await tx.commit();
} catch (error) {
  await tx.rollback();
  throw error;
}
```

## Notes

- `openLix()` opens a fresh in-memory Lix. Install `@lix-js/storage-filesystem` and pass `new FilesystemStorage({ path })` for a filesystem repository directory backed by `<path>/.lix/.internal/rocksdb`.
- In browsers, pass `new IndexedDbStorage({ name })` to persist a complete local Lix across reloads.
- Only one Lix handle may open an IndexedDB storage name at a time, including across browser tabs.
- Pass `syncAllFiles: false` to start filesystem sync with no regular repository files, then call `storage.importPaths(["notes/today.md"])` on the `FilesystemStorage` instance to sync selected files. Imported paths are exact repository-relative file paths, not directories or globs.
- In browsers, local mode and remote mode with IndexedDB storage load the Rust
  engine as WebAssembly. In remote mode, the local engine contains only client
  state.
- `FilesystemStorage` is Node.js-only. Constructing it is safe in
  shared code, but passing one to `openLix()` in a browser throws an error.
- The package is ESM-only.
- The package uses conditional ESM imports internally: Node.js resolves the
  native N-API binding, while browsers and other runtimes resolve the portable
  WebAssembly binding. Vite follows this split without consumer configuration.
- If the native addon cannot load in Node.js, in-memory Lix instances fall back
  to the bundled WebAssembly engine. Filesystem storage and Component API v1
  plugin execution still require the native addon.
- Every browser `openLix()` owns one dedicated worker, so database work does
  not block the page's main thread. Node.js uses the native binding's actor.
- Node.js executes installed Component API v1 plugins with the Rust SDK's
  Wasmtime runtime. The browser and Workerd bindings currently open without a
  component runtime: they can use ordinary Lix storage and SQL, but do not
  execute installed plugins. A browser Component host is a separate follow-up.
- A page Content Security Policy only needs to permit the package's same-origin
  worker. WebAssembly compilation happens inside that worker, so the required
  permission can be scoped to the worker script's HTTP response instead of
  being allowed by the document:

  ```http
  # HTML document response
  Content-Security-Policy: default-src 'self'; script-src 'self'; worker-src 'self'

  # Lix worker response (Vite emits assets/entry.browser-<hash>.js)
  Content-Security-Policy: default-src 'none'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'
  ```

  Hosts that apply one policy to every response can use
  `script-src 'self' 'wasm-unsafe-eval'; worker-src 'self'` globally
  instead. Worker-scoped headers keep those permissions out of the page.

- SQL parameters use normal JavaScript values: `string`, finite `number`, `boolean`, `Uint8Array`, `null`, JSON-compatible arrays, and JSON-compatible plain objects.
- Use `Value.integer(...)`, `Value.real(...)`, `Value.text(...)`, `Value.jsonb(...)`, `Value.timestamptz(...)`, or `Value.blob(...)` only when you need to pass an explicit native Lix value.

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
plus bundled-plugin archive loading in Chromium. It runs with both
worker-scoped and global strict CSP headers.

Use `npm run build:wasm:dev` while iterating on the Rust bridge when release
optimization is unnecessary.
