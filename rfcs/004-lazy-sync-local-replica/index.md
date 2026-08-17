# RFC 004: Lazy local replica for `mode: sync`

## Goal

Make `openLix({ server: { mode: "sync" } })` a transparent, lazy local
replica of `mode: "remote"`. The existing Lix APIs and repository semantics
remain unchanged; synchronization is an internal implementation detail.

The local replica must support the same SQL and API surfaces, including
`execute`, `observe`, branches, commits, diff, working diff, merge, plugins,
and files. Cached reads execute against local storage with zero network calls.
Synchronization runs in the background, and data that is not materialized is
hydrated lazily when first requested.

## Consistency contract

Local reads return a transactionally consistent, monotonic local snapshot plus
pending local writes. Local writes are durable and immediately readable.
Remote changes may be briefly in transit, but replicas converge while
connected. A successful cached read does not prove the server has no newer
commit; globally linearizable reads would put the network back in the hot path.

If a query requires an unmaterialized scope, sync may hydrate it before the
query completes. If the required scope is cached, the query must not perform a
network request. When offline, cached scopes remain readable; an uncached scope
fails clearly instead of returning an incomplete result.

No conflict object, conflict API, or sync-specific application API is exposed.

## Internal architecture

```text
existing Lix API
      |
      +--> infer required scope
      |        |
      |        +--> cached: execute locally
      |        +--> missing: hydrate once, then execute locally
      |
      +--> observe local state and background-applied remote changes

background pull/stream --> atomic apply --> replay pending local writes
```

The private materialization manifest tracks coverage and cursors for:

- the global branch catalog, branch refs, schemas, and plugin metadata;
- row and commit scopes for active queries;
- commit topology, including merge parents;
- file/blob projections, which remain lazy.

Branch creation, deletion, switching, refs, and listings are replicated as
repository metadata. Merge commits replicate their parents and resulting row
state so history, diff, working diff, undo/redo, and merge behavior remain
equivalent across replicas. Plugin rows are canonical synchronization data;
files are projections loaded only when requested. For a fresh plugin row
scope, a certified event that combines a source-file mutation with its rows
currently retains that one plugin-owned source payload so the local runtime
can establish ownership and regenerate the rows; ordinary row-only events
remain byte-free. A future ownership/CAS lane can remove this bootstrap
exception.

The current prototype has one deliberate topology constraint: live polling
pulls canonical events unfiltered so a commit is materialized as one graph
node with all parents. Cold scope hydration is filtered at event granularity
(unrelated commits transfer only their cursor identity; a matching commit is
kept atomically). A future commit-skeleton/row-pack split is required before
live polling can be filtered without risking a missing parent or a divergent
local graph.

Internal branch-catalog reconciliation runs in a sync-suppressed worker
session, so catalog maintenance cannot echo a remote branch back into the
outbox. After a successful flush it removes local non-default, non-active
branches absent from an explicitly authoritative catalog; the pre-flush pass
leaves offline local branches intact long enough to be admitted. Embedded or
custom transports that cannot enumerate branches never trigger destructive
pruning.

## Acceptance criteria

- Cached `execute()` performs zero network requests and stays within 10% of
  ordinary local Lix latency.
- SQL queries infer, hydrate, and retain their required scopes automatically.
- `observe()` emits local changes and changes applied by background sync.
- Local writes survive restart and provide read-your-writes while offline.
- Remote writes converge without exposing conflicts.
- A branch created by one client appears through the normal branch APIs of
  another client; switching and branch history work without manual setup.
- Diff, working diff, commit history, undo/redo, and merge preserve the same
  graph and row results on every synchronized client.
- Plugin-backed rows synchronize canonically; files and blobs are lazy.
- Retry, lost acknowledgement, restart, reconnect, offline, and overlapping
  writes are covered by deterministic tests.
- Benchmarks report cached-read p50/p95, cold-hydration latency, replication
  lag, bytes transferred, retained storage, and scope hit rate.
- Correctness, durability, security, and performance receive independent
  sub-agent review before the implementation is considered production-ready.

## Prototype measurements

The checked-in `sync_lazy_profile` benchmark measures the protocol's JSON
framing and scope projection, not database throughput. On the seeded mixed
trace, scoped payload bytes were reduced by 86.2% (16×64 events/rows), 88.5%
(128×64), and 89.2% (512×128). The strategy scorecard likewise reports
convergence, duplicate-admission safety, offline replay, branch isolation, and
wire/storage counters for transaction-admission/event-pull versus commit-pack
variants. Those numbers are deterministic simulator evidence; production
gates still require real FilesystemStorage/RocksDB runs measuring cached-read
latency, cold hydration p50/p95, replication lag, and retained bytes.

## Remaining production work

The 90% path is implemented and covered by Rust unit tests, 20 protocol tests,
and a 13-test two-replica/filesystem/plugin matrix. The following are explicit
follow-ups rather than hidden semantics:

- replace unfiltered live pulls with a topology skeleton plus independently
  hydrated row/blob packs;
- make pending-overlay replay idempotent without repeatedly generating local
  projection commits;
- provide a certified bootstrap for a fresh replica whose server already has
  history (the current safe behavior rejects an uncertified divergent start);
- make ambiguous durable-receipt recovery work with RocksDB-backed
  FilesystemStorage after a server restart;
- paginate branch catalogs and broaden SQL/API scope inference for every
  history/system surface, including prepared-DML and more complex query
  shapes;
- audit `observe()` binding for every plugin/query shape and extend the
  branch-control barrier to any API that resolves a remote source branch;
- make branch deletion safe when the deleted ref is currently selected;
- run real backend benchmarks and independent security/performance review
  before calling the protocol production-ready.

## Non-goals

- A new public `sync(shape)` or conflict-resolution API.
- Synchronous server validation on every local read.
- Downloading the complete repository before the local client can start.
- Treating uncached offline data as an empty or partial result.
