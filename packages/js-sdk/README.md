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
console.log(result.rows[0]?.message);
await lix.close();
```

### File-qualified row references

Construct row references in SQL with the relation, file scope, and typed
primary-key values. The file-scope argument is always present: use the owning
file ID for a file-scoped plugin row and SQL `NULL` for fileless rows such as
`lix_file` and `lix_directory`.

```ts
const result = await lix.execute(
	"SELECT lix_row_ref('acme_task', $1, $2) AS row_ref",
	[fileId, taskId],
);
```

For compiled plugin downloads, installation, updates, and uninstalling, see
[Installing and managing plugins](../../docs/plugins.md).

## Hosted lifecycle

### Automatic upgrades and progress

Opening a supported older repository automatically upgrades it in Rust, including
filesystem and OPFS storage. The SDK waits inside `openLix()`; applications do not
implement migration or retry logic. Hosted upgrades run on the server, where
concurrent opens share the upgrade operation. The normal native and WASM engines
include this capability; opening never loads a separate migration artifact.

Use `onProgress` to display status for local, remote, or synchronized opening.
Local events emit `scope: "local"`. `lix.openReport.migrations` records the
scoped upgrades completed during opening.
Authority upgrades emit `scope: "authority"` and `phase: "migrating"`, followed
by opening and completion. The source format and work totals may be unknown:
show “Upgrading repository” with an indeterminate indicator instead of inventing
a percentage. Progress callbacks are observational and cannot change the result
of opening.

### Compatibility metadata

Raw HTTP integrations can obtain this SDK's protocol versions without loading
the engine:

```ts
import { compatibility } from "@lix-js/sdk/compatibility";

const headers = {
  "Lix-Server-Protocol-Version": String(compatibility.serverProtocolVersion),
  "Lix-Sync-Protocol-Version": String(compatibility.syncProtocolVersion),
};
```

The object also includes `storageFormatVersion`. It is generated from the Rust
engine constants during the SDK build. Source-checkout CI can use
`node scripts/compatibility.mjs` before building the SDK.

### Execution modes

`openLix()` selects execution from storage and the explicit server mode:

| Options | Behavior |
| --- | --- |
| Neither | Fresh in-memory repository |
| `storage` | Local repository, initialized if empty |
| `server`, mode omitted or `"remote"` | Execute SQL remotely against an existing hosted repository; no local storage |
| `storage` and `server.mode: "partial_replica"` | Partial replica with on-demand sync; reads and writes whose dependencies are resident execute locally |

`server.mode` defaults to `"remote"`. Supplying storage requires explicitly opting
into `"partial_replica"`; remote mode rejects storage. Partial-replica mode requires
storage. These are the only supported server modes. `"replica"` may be added later;
the former `"sync"` mode is not an alias.

Creation and deletion are explicit server operations:

```ts
import { createLix, deleteLix, openLix } from "@lix-js/sdk";

const repository = await createLix({
  server: { url: "https://example.com", headers: getAuthHeaders },
  from: localLix, // Omit to create an empty repository.
});

const remote = await openLix({
  server: { url: repository.url, headers: getAuthHeaders },
});
await remote.close();
await deleteLix({ server: { url: repository.url, headers: getAuthHeaders } });
```

`from` captures a consistent repository snapshot, including history and untracked
rows. The source remains open. Creation returns `{ id, url }`, not a session.
Use the same optional `idempotencyKey` when retrying creation after an uncertain
response. Deletion removes the hosted repository and leaves local copies intact. Opening
or synchronizing never implicitly creates a missing hosted repository.

Browser creation from a local repository requires Fetch request streaming.
Browsers without that support return `LIX_UNSUPPORTED_OPERATION`; Lix does not
buffer the complete repository as a fallback. Browser Fetch may also require an
HTTP/2 or HTTP/3 connection for streaming uploads. Empty creation and remote
execution do not require streaming uploads.

## Synchronized local repositories

Provide storage and set `server.mode: "partial_replica"` to create a
**partial replica with on-demand sync**. Opening loads bounded metadata. SQL fetches missing native inputs on
demand and retains them locally; background synchronization adopts remote branch
updates atomically:

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
  storage: new OpfsStorage({ name: "acme" }),
  server: {
    mode: "partial_replica",
    url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
    headers: async () => ({
      Authorization: `Bearer ${await accessToken()}`,
    }),
  },
});
```

A successful mutation confirms a local commit; it does not confirm server
acceptance. Pending commits upload in the background. Covered reads and writes
whose dependencies are resident work offline, with immediate local visibility.
A statement requiring missing inputs needs a connection; missing data is never
silently treated as an empty result. Current data means the coherently applied
server state plus pending local writes.

Remote adoption does not first replay every previous read. Queries and observers
fetch the inputs they need when evaluating the new state, so a previously cached
query may need network data after an update. Mount the workspace after `openLix`
resolves and handle pending queries in the views that use them.

Prefetch a view on hover using the same ordinary SELECT it will display:

```ts
const sql = "SELECT content FROM lix_file WHERE path=$1";
const params = ["/notes.txt"];
await lix.execute(sql, params); // On hover: fetch missing read inputs.
const result = await lix.execute(sql, params); // On open: resident reads stay local.
```

Use ordinary `execute()` for mutations. Reading a file does not promise that every
later write's validation or commit dependencies are resident; cold operations can
fetch additional inputs while connected.

See [Collaboration and Sync](https://lix.dev/docs/collaboration-and-sync).

### Upgrading a local replica

Keep the same storage name across SDK upgrades. Opening with storage and a server
upgrades supported local formats and reconciles a full replica into the current
partial replica representation in Rust:

```ts
const lix = await openLix({
  storage: new OpfsStorage({ name: "acme" }),
  server: {
    url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
    headers: async () => ({ Authorization: `Bearer ${await accessToken()}` }),
  },
  onProgress: (event) => console.log(event.scope, event.phase),
});
```

Opening authenticates the repository and account, retains source generations,
and publishes the upgraded replica only after reconciliation and validation
succeed. Unsupported pending work returns a recovery error with the source
preserved. Do not clear storage to bypass that error.

If authority cleanup is interrupted after conversion, retry it explicitly while
storage is closed. Ordinary opening does not scan migration journals:

```ts
import { retryReplicaMigrationCleanup } from "@lix-js/sdk";
const completed = await retryReplicaMigrationCleanup({ storage, server });
```

This returns the number of newly completed cleanup records; repeating a
successful cleanup returns zero. It preserves the active replica and retained
source generation.

For retained pre-native sources, opening with a server can report that recovery
is required. Open the retained local
recovery repository without `server`, then inspect and restore retained work:

```ts
const recovery = await openLix({ storage });
try {
  const sources = await recovery.replicaRecoverySources();
  for (const source of sources.filter((source) => source.recoveryRequired)) {
    const exported = await recovery.exportReplicaRecovery(source.id);
    console.log(exported.unresolved); // Save a portable copy when needed.

    const receipt = await recovery.recoverReplicaWithServer(source.id, server);
    console.log(receipt.branchIds, receipt.restoredRows, receipt.unresolved);
  }
} finally {
  await recovery.close();
}
```

`recoverReplicaWithServer()` authenticates the same repository and account and
fetches missing recovery history or chunks explicitly. It does not start a
background sync worker or upload restored branches. Dynamic headers and custom
fetch remain scoped to this operation in the browser. The native Node binding
supports headers and rejects custom fetch.

`exportReplicaRecovery()` captures available logical rows, blob contents, and
original branch/checkpoint coordinates. `recoverReplica()` performs local
restoration; its server variant can hydrate missing dependencies. Both restore
supported tracked rows into separate recovery branches without inheriting
unrelated active-branch rows. Inspect `unresolved`: unavailable history/content
and local-only rows remain in the retained source. Neither operation deletes
that source, and retries reuse durable recovery receipts.

A recovery receipt confirms local restoration, not server acceptance or
eligibility for partial conversion. Conversion of every retained pending branch
is still a release gate: restored additional branches and unresolved retained
sources must not be silently discarded or marked acknowledged. The current
selected-branch native reconciliation path does not establish that broader
migration guarantee.

Recovery export currently allows up to 100,000 logical rows across all branches,
64 MiB per blob, and 128 MiB of blob content in total. Unfinished upload parts
have a separate 128 MiB content budget. A row-limit or upload-limit error leaves
the source intact; omitted blob content is identified in `unresolved`. Exported
JSON can be larger than these content budgets because binary data uses base64.
This recovery file is not a complete repository backup.

These methods require a local storage-backed handle; remote-only handles reject
with `LIX_ERROR_LOCAL_STORAGE_REQUIRED`. Do not clear browser storage to resolve
upgrade or recovery errors. Browser storage eviction or an unsupported old format
can still require external recovery; retaining bytes alone is not proof that all
work has been recovered. Standalone and authoritative repositories continue to
use history-preserving format migrations.

## Remote repositories

Use the same Lix client as a thin client against a hosted repository:

```ts
const lix = await openLix({
  server: {
    url: "https://example.com/lix/01936f4e-7b6c-7c3d-8f9a-123456789abc",
    headers: async () => ({
      Authorization: `Bearer ${await accessToken()}`,
    }),
  },
});

const files = lix.observe("SELECT path FROM lix_file ORDER BY path");
const initial = await files.next(); // { value: ObserveEvent, done: false }

await lix.execute("INSERT INTO lix_file (path, content) VALUES ($1, $2)", [
  "/hello.txt",
  new TextEncoder().encode("hello"),
]);
const update = await files.next();

await files.return?.();
await lix.close();
```

Remote mode is the default when `server.mode` is omitted. It rejects `storage`,
uses the server for all persistence and does not
open a local engine. Dynamic headers are resolved for every request and
observation reconnect. An injected `fetch` can route requests through a service
binding or another authorized server-side transport.

Remote server sessions are branch-pinned, so switching one client does not
switch another client. Browser-local application state belongs to the
application rather than the remote Lix handle.

Remote handles support branch creation, merge preview and merge through the
server's Rust engine. `openAnotherSession()` inherits the active branch unless
one is supplied, retains the authenticated account, and returns an independent
handle with the same operations—including streaming `exportSnapshot()`. Snapshot
export covers the complete repository, not only the active branch. Cancelling
the returned stream releases the HTTP request.

Internally, local and connected-sync bindings call `Lix`; remote bindings call
the Rust protocol client. Both implement the required Rust session operation
contract, and the WASM bindings share SQL and branch-operation forwarding and
value conversion. Host-specific storage, telemetry, stream, and actor cleanup
remain in the bindings. This does not make network availability or authentication
constraints identical to an offline local engine.

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
const bytes = result.rows[0]?.content as Uint8Array | undefined;

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

`beginTransaction()` captures the current branch and account in an independent
transaction context. Use `tx.execute()` for transaction work; its reads see staged
writes. Ordinary reads and observers on `lix` continue to see committed data while
the transaction is open. Commit or roll back before closing `lix`.

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
- In browsers, install a storage provider such as `@lix-js/storage-opfs` and pass its
  storage registration to `openLix()`.
- JavaScript storage packages register a worker-loadable module URL. That module exports
  `createLixStorageProvider(options)` and returns the SDK's Rust-shaped `LixStorageProvider`:
  `beginRead`, `beginWrite`, read/scan handles, write mutation methods, `commit`, and `rollback`.
  The provider module is loaded beside the Lix Wasm engine in its dedicated worker; it does not
  bundle or select a Lix engine version.
- Pass `syncAllFiles: false` to start filesystem sync with no regular repository files, then call `storage.importPaths(["notes/today.md"])` on the `FilesystemStorage` instance to sync selected files. Imported paths are exact repository-relative file paths, not directories or globs.
- Browser-local storage loads the Rust engine as WebAssembly. Remote mode does
  not open a local storage provider.
- `FilesystemStorage` is Node.js-only. Constructing it is safe in
  shared code, but passing one to `openLix()` in a browser throws an error.
- The package is ESM-only.
- The package uses conditional ESM imports internally: Node.js resolves the
  native N-API binding, while browsers and other runtimes resolve the portable
  WebAssembly binding. Vite resolves these conditional imports automatically.
  Configure Vite to emit ES module workers because the Component compiler uses
  top-level await:

  ```js
  // vite.config.js
  export default { worker: { format: "es" } };
  ```
- If the native addon cannot load in Node.js, in-memory Lix instances fall back
  to the bundled WebAssembly engine. Filesystem storage still requires the native addon.
- Browser database work runs off the page's main thread. OPFS handles share one
  elected dedicated worker per physical repository; it owns both engine and
  storage. On owner loss, surviving tabs restore sessions and observations within
  a bounded recovery window. Interrupted transactions and snapshot streams must
  be restarted. In-flight writes may reject with `LIX_WRITE_OUTCOME_UNKNOWN`; do
  not blindly replay them. Node.js uses the native binding's actor.
- Node.js and browsers execute installed Component API v2 plugins through the
  same JavaScript Component host. The host adapts Component interfaces to the
  platform's built-in WebAssembly runtime and connects them to Lix's shared Rust
  host resources. Node.js retains its native engine and filesystem adapter;
  the browser engine runs inside its dedicated worker.
- Components are compiled on first use. Each file actor gets an isolated guest
  instance. Guest functions and loops check execution deadlines, and core memory
  declarations are capped before instantiation. Unsupported memory forms are
  rejected rather than executed without limits.
- A page Content Security Policy only needs to permit the package's same-origin
  worker. Component bindings are generated as data URL modules and WebAssembly
  compilation happens inside that worker, so the required
  permission can be scoped to the worker script's HTTP response instead of
  being allowed by the document:

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
plus bundled-plugin installation and CSV row extraction in Chromium. It runs with both
worker-scoped and global strict CSP headers.

Use `npm run build:wasm:dev` while iterating on the Rust bridge when release
optimization is unnecessary.
