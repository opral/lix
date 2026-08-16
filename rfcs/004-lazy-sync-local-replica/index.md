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
files are projections loaded only when requested.

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

## Non-goals

- A new public `sync(shape)` or conflict-resolution API.
- Synchronous server validation on every local read.
- Downloading the complete repository before the local client can start.
- Treating uncached offline data as an empty or partial result.
