# `@lix-js/storage-opfs`

Durable browser storage for Lix using SQLite Wasm and the Origin Private File System (OPFS).

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
	storage: new OpfsStorage({ name: "my-repository" }),
});
```

When bundling with Vite, emit ES module workers for the SDK's Component compiler:

```js
// vite.config.js
export default { worker: { format: "es" } };
```

Combine OPFS with `server: { url: repositoryUrl, mode: "partial_replica" }`
to keep a durable **partial replica with on-demand sync** of an existing hosted
repository. Opening loads bounded metadata; SQL fetches missing native inputs
and retains them locally. Reads and writes whose dependencies are resident run locally, including
offline, and local commits upload in the background. The default `"remote"` mode
rejects local storage. See
[Collaboration and Sync](https://lix.dev/docs/collaboration-and-sync).

All OPFS sessions for a physical repository share one elected dedicated worker.
That worker owns both the Lix engine and the SQLite/OPFS connection. Each page
starts a candidate, and a repository-scoped Web Lock permits only one candidate
to open storage. Session requests cross a BroadcastChannel; storage reads and
writes stay inside the engine's worker. SharedWorker is no longer required.

The public `openLix({ storage: new OpfsStorage({ name }) })` API is unchanged.
Each handle gets an independent session. Closing a handle does not terminate an
owner that still serves other tabs. A page-scoped client lock detects abrupt
client loss, cancels its credential/transport callbacks, and closes its sessions.
Partial replicas retain their existing authority and account admission checks;
sharing an engine does not authorize another account or admit additional branches.

When the document hosting the owner disappears, a surviving tab's candidate
acquires ownership. The SDK reopens each acknowledged session with its last
acknowledged branch/account context and re-registers observations. An observation
emits a fresh snapshot after recovery and retains its logical sequence. Network
and credential callbacks are fenced by owner generation and fresh callback IDs.
Interrupted transactions and snapshot exports fail explicitly; start new ones.
In-flight writes are never replayed. Every request and response is fenced by the
owner generation.

If a potentially writing operation loses its acknowledgement, it rejects with
`LIX_WRITE_OUTCOME_UNKNOWN`. The operation may already have committed: inspect
repository state before deciding whether to retry it. SQL is conservatively
classified as potentially writing, including SQL submitted through `execute()`.

Worker opening has a 30-second budget, credential callbacks a 15-second budget,
and other finite worker requests a 60-second budget. The close RPC has a
5-second budget after the public handle drains its in-flight operations.
`observe().next()` remains long-lived until a change, closure, or unrecoverable
owner failure.
A failed or timed-out connection is disposed, rather than leaving a reusable
half-open session. Cleanup that cannot drain is stopped at the worker boundary;
physical storage locks remain the final fence until the browser releases them.
A suspended live owner is not replaced by stealing its locks.

OPFS sync access handles, dedicated workers, BroadcastChannel, and Web Locks are
required. There is no silent single-tab fallback. Test actual Safari on the
supported macOS/iOS versions; Linux Playwright WebKit is not Safari qualification.
The generic storage protocol still exposes `watchForChanges()`/`changed()`.
SQLite read handles retain a coherent view using bounded owner-local undo history
(512 generations and 32 MiB); expired reads retry within the engine's bounded
read budget. Durable writes retain the existing SQLite WAL checkpoint fence.

The separate `@lix-js/storage-opfs/migration` entry copies retained source stores
to unpublished namespaces. It is loaded before ordinary opening only when a
browser profile needs routing/migration. It never clears a failed source.

## Development

Run `npm run test:browser` for adapter conformance and persistence. The retired
storage-RPC implementation lives under `tests/legacy-rpc` and is built only for
conformance tests;
public `OpfsStorage` never selects it. Run `npm run test:repository-owner` for
production-bundled cross-tab sessions, observations, transactions, owner loss,
and repeated warm reopen. `npm run test:browser:production` tests packed
SDK and storage artifacts in a minimal Vite application and runs the repository
ownership/recovery regression suite. `npm run benchmark` reports raw samples plus p50/p95 for warm Lix
reopen, local execute-through-observer delivery, and the 10k/1M-row storage
scorecards. `npm run benchmark:multi-tab` reports cross-tab observer delivery
and owner-failover recovery from packed production artifacts.

Run these benchmarks manually when investigating performance. CI/CD runs the
functional browser tests and publishes the tested SDK artifact without running
the OPFS performance budgets.
