# `@lix-js/storage-opfs`

Durable browser storage for Lix using SQLite Wasm and the Origin Private File System (OPFS).

```ts
import { openLix } from "@lix-js/sdk";
import { OpfsStorage } from "@lix-js/storage-opfs";

const lix = await openLix({
	storage: new OpfsStorage({ name: "my-repository" }),
});
```

Combine OPFS with `server: { url: repositoryUrl, mode: "partial_replica" }`
to keep a durable **partial replica with on-demand sync** of an existing hosted
repository. Opening loads bounded metadata; SQL fetches missing native inputs
and retains them locally. Reads and writes whose dependencies are resident run locally, including
offline, and local commits upload in the background. The default `"remote"` mode
rejects local storage. See
[Collaboration and Sync](https://lix.dev/docs/collaboration-and-sync).

Partial replicas coordinate one Lix engine per physical OPFS database through an
SDK-owned SharedWorker. Each tab receives an independent session on that engine,
so loaded data and pending offline edits are shared. Closing one tab releases its
sessions without closing the other tabs' engine. This is automatic with the
existing `openLix()` options and requires SharedWorker support in addition to
OPFS and Web Locks. The physical engine-owner lock remains the final fence across
different SDK builds. Sessions still obey the partial replica's admitted branch
scope; sharing an engine does not admit every repository branch.

`OpfsStorage` starts one package-owned dedicated worker in the page. The Lix
engine workers use a package-internal `BroadcastChannel` RPC client, while the
owner worker holds the SQLite Wasm OPFS SAH-pool connection. This keeps the
SQLite connection and OPFS sync handles in one worker, while multiple tabs and
multiple Lix workers attach to the same repository. Writes are batched before
crossing the channel and commits are serialized by the owner.

The SQLite owner is deliberately a dedicated worker rather than a SharedWorker:
SQLite's OPFS sync-access-handle VFS is only available in dedicated workers.
The partial-engine SharedWorker uses this provider protocol and does not host
the SQLite connection itself.

After every committed write, the owner broadcasts a package-private storage
position. Each attached provider turns a changed position into the SDK's
payload-free storage invalidation signal, so `lix.observe()` reruns in other Lix
workers and browser tabs. Periodic heartbeats announce the current position as
well, allowing a client to recover if a `BroadcastChannel` message was missed.
Read handles are invalidated when the owner generation changes rather than
materializing a full historical SQLite snapshot.
If a commit response is lost after SQLite has accepted the transaction, the
client reports `LIX_STORAGE_COMMIT_OUTCOME_UNKNOWN` rather than replaying it.

OPFS and Web Locks are required. A Web Lock is the split-brain fence: one owner
worker serves a repository name and other tabs remain listeners until the owner
goes away. The generic `@lix-js/sdk` storage protocol exposes only
`watchForChanges()`/`changed()`; OPFS owner epochs and generations stay private
to this package. Browsers without workers or BroadcastChannel use the package's
direct-worker fallback; that fallback is single-owner and does not provide
multi-tab attachment.

## Development

Run `npm run test:browser` for provider persistence, multi-client attach, and
cross-engine observation tests. `npm run test:browser:production` tests packed
SDK and storage artifacts, including cross-tab observation, in a minimal Vite
application. `npm run benchmark` reports raw samples plus p50/p95 for warm Lix
reopen, local execute-through-observer delivery, and the 10k/1M-row storage
scorecards. `npm run benchmark:multi-tab` reports cross-tab observer delivery
and owner-failover recovery from packed production artifacts.

Run these benchmarks manually when investigating performance. CI/CD runs the
functional browser tests and publishes the tested SDK artifact without running
the OPFS performance budgets.
