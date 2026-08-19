# RFC 004: Repository sync for `mode: sync`

## Goal

`openLix({ storage, server: { mode: "sync" } })` is a durable local replica.
Normal reads and writes use only local storage. Synchronization exchanges the
repository facts Lix already owns instead of maintaining a second projection
model.

## Protocol

The repository URL scopes every endpoint to one repository. The protocol has
one ordered live cursor:

- `POST /lix/v1/sync/push` atomically publishes complete commits and
  compare-and-swap branch ref updates.
- `GET /lix/v1/sync/pull` returns pinned branch metadata when query parameters
  are omitted, returns bounded current-row pages pinned to an immutable branch
  head when given `snapshotBranchId` and `snapshotHeadCommitId`, and otherwise
  long-polls commit/ref events after `after`.
- `GET /lix/v1/sync/history?commitId=...` fetches exact immutable commit bodies
  plus bounded topology certificates and never changes the live cursor.
- `GET|POST /lix/v1/sync/blob` negotiates one canonical BLAKE3/FastCDC
  manifest.
- `GET|PUT /lix/v1/sync/chunk` transfers authenticated raw chunks.

There are no per-branch cursors, query scopes, row packs, file projections,
admission receipts, compatibility versions, or polling intervals.

## Data model

A live event contains complete immutable commits plus branch ref moves. A
commit contains its semantic header, exact identity-ordered row membership,
authored change facts, and the core commit-level selected-source identity used
by merges. Physical columnar pages, jump indexes, and current-state roots are
derived locally.

Files remain rows. `lix_binary_blob_ref` rows name BLAKE3-addressed content;
the manifest/chunk endpoints move those bytes without embedding them in commit
JSON or exposing the CAS storage layout.

## Bootstrap and history

A fresh replica performs one handshake and one metadata request. The metadata
pins the durable default branch, every branch head, and the repository cursor.
The worker then concurrently fetches the distinct head commit bodies and their
bounded topology certificates through `sync/history`, and bounded current-row
pages through `sync/pull`, each row scan pinned to its immutable branch head.
Each branch's `hotStateRootId` verifies the assembled live, tombstone-filtered
row stream independently of the physical state-root certificates used for
commit topology. The complete pinned result chooses the local default branch
and initializes the replica atomically. Reopening a durable replica does no
network work on the open path.

Historical row membership is not part of bootstrap. It is fetched as complete
immutable commits through `sync/history` when a history operation needs it.
Blob manifests and chunks use their independent content-addressed lane.

## Runtime

```text
shared repository state machine
├── first bootstrap and reconnect
├── durable local-head outbox discovery
├── BLAKE3 manifest/chunk transfer
├── push and long-pull orchestration
├── branch reconciliation
└── retry and shutdown policy

platform adapter
├── native: Tokio + reqwest
└── browser: spawn_local + fetch + AbortController
```

The immutable local commits and refs are the outbox. A separate serialized
transaction queue is unnecessary. Replica state stores only the last applied
authority cursor and authoritative branch heads. Retry is safe because commit
identity is immutable and ref movement is compare-and-swap.

When a remote ref advances while local commits are pending, reconciliation
preserves both heads and creates an ordinary deterministic Lix merge before
retrying the ref move. No conflict object or sync-specific application API is
exposed.

## Consistency

Local reads return a transactionally consistent local snapshot plus completed
local commits. Local writes are durable and immediately readable. Remote
changes may be in transit, but connected replicas converge in repository
cursor order. A cached read does not imply that the server has no newer event.

Normal SQL never waits for the network. A history request may fetch immutable
history that is not local yet. Missing uncached data while offline fails
clearly rather than appearing as an empty result.

## Acceptance criteria

- Warm reads and writes perform no network requests.
- Fresh bootstrap, immediate file creation, and checkpointing never produce a
  missing checkpoint cursor.
- Offline commits survive restart and publish after reconnect.
- Lost acknowledgements and duplicate delivery are idempotent.
- Two replicas converge for different-row and same-row concurrent writes.
- Branch creation, deletion, switching, merge topology, and refs converge.
- Binary files retain exact bytes through BLAKE3 manifest/chunk transfer.
- Browser sync uses long polling and has no fixed-interval poll loop.
- Native and browser runtimes execute the same policy state machine.
- Snapshot size, cached-read latency, local-write latency, replication lag,
  reconnect behavior, and retained storage have explicit measured gates.

## Non-goals

- Backward compatibility with the unshipped prototype protocol.
- A public sync, conflict-resolution, or query-scope API.
- Server validation on the local interaction path.
- Encoding Lix's physical storage layout on the wire.
