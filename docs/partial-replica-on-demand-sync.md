# Partial replica with on-demand sync implementation plan

**Decision: implement a partial replica with on-demand sync. Opening installs a bounded session descriptor; SQL loads missing native inputs on demand; covered reads and prepared writes execute locally; background synchronization maintains that coverage.** Server SQL fallback is an optional optimization for cold reads, with strict consistency checks. A result-cache wrapper alone does not meet the local-write requirement.

This document records the reviewed design and target contract. The core implementation is complete in the development worktree: bounded opening, native SQL hydration, local commits, scoped publication and explicit migration pass native, adapter and browser verification. This is not a production deployment. Optional server SQL result fallback, broader preparation forms and cache-eviction policy below remain follow-up capabilities; current cold reads hydrate native inputs, and unsupported inventories fail explicitly. See [measured performance](partial-replica-performance.md) and [migration support and remaining boundaries](partial-replica-migration.md) for current evidence; planned capabilities below must not be read as already delivered. Three independent sub-agents reviewed correctness, engine integration and performance; their required changes are incorporated below.

## JavaScript opening configuration

`server.mode` defaults to `"remote"`: SQL executes on the server and local storage
is not accepted. A browser such as Lixray opts into a **partial replica with
on-demand sync** by supplying both storage and `server.mode: "partial_replica"`:

```ts
const lix = await openLix({
  storage: new OpfsStorage({ name: repositoryId }),
  server: { url: repositoryUrl, mode: "partial_replica", headers: getAuthHeaders },
});
```

Partial-replica mode requires storage. Storage alone still opens a standalone
local repository. The only supported server modes are `"remote"` and
`"partial_replica"`; a future full `"replica"` mode is not implemented, and the
former `"sync"` spelling is rejected. Existing JavaScript callers that combined
server and storage must add the explicit opt-in. This selector describes the
JavaScript API; Rust continues to select the topology through its typed builders.

## Contract

Protocol and storage changes may break compatibility. Migrate existing repositories to the new native format explicitly; do not maintain compatibility shims or run the old full-bootstrap path behind an on-demand request. Measure migration separately from ordinary opening. Migration must preserve repository identity, history, branch/checkpoint references, content and pending local edits, and support resumption after interruption.

| Operation | Contract |
|---|---|
| `openLix` in on-demand sync mode | Bounded control metadata; no repository-wide enumeration, row transfer or content hydration |
| First SQL access to missing data | May fetch dependencies and take time; reports loading and supports prefetch |
| Read with resident, valid coverage | Executes locally with no foreground network request |
| Write with resident read/validation/publication dependencies | Commits after local durability; no authority acknowledgment required |
| Immediate read after local write | Sees that write locally without rehydration |
| SQL referencing new dependencies | Cold operation; hydrate before execution/commit or report unavailable offline |
| Background sync | Updates retained scopes transactionally without downloading unrelated row/content payloads |
| Offline | Covered operations work; uncovered operations fail explicitly rather than returning incomplete results |

“Current” means the browser’s latest coherently applied authority state plus its pending local commits. Globally latest server state cannot be guaranteed without communication. Coverage remains usable at its local version during a disconnect; reconnect updates it in the background according to the existing reconciliation policy.

Opening independence means bounded request count, descriptor bytes and application work as repository rows, content, history, branches and schemas grow. It does not promise constant network latency or eliminate ordinary indexed-lookup cost. The authority’s descriptor path must avoid repository-wide scans too; moving a bootstrap scan to the server does not satisfy the objective.

**The local guarantee applies to prepared dependencies, not arbitrary future SQL.** Reading one file cannot make a later repository-wide update or a rename into an unseen directory warm. A cold read also does not automatically prepare every possible write on the returned rows. The prefetch/preparation API makes intended editing operations warm before the user invokes them.

## 1. Replace the opening boundary

Implementation review identified the existing root-backed HOT generation as the preferred serving mechanism: an immutable native base plus local rows, tombstones and collection-generation rules. Reuse this mechanism through a dedicated partial installer. Do not reuse the lifecycle publisher unchanged: some lifecycle paths materialize complete current/checkpoint snapshots or preload catalogs. Missing immutable inputs must produce explicit on-demand dependencies, while a missing object in a full repository remains a corruption error.

Add a versioned partial-replica session bootstrap response containing only repository identity, the selected/default branch ID, its head/checkpoint references, opaque catalog/schema references, authorization/session context and a synchronization starting point. References must remain bounded descriptors; do not expand a commit’s entire mutation inventory or embed all schema definitions.

Open must not wait for branch enumeration, checkpoint inventory, current rows, baseline rows, blob manifests, chunks, a full live-state hash, or a catch-up stream to drain. Queries for branch lists, catalogs and history become ordinary demand-driven SQL. Other branch heads load only when requested. Warm reopen reads the descriptor and restores query/storage objects lazily, rather than loading every retained row into memory first.

Remove repository-sized work from the selected-branch admission path, including accidental account/catalog scans. Server recovery or format migration must be measured separately and must not masquerade as routine bounded opening. A protocol capability mismatch fails explicitly; an on-demand sync request must not silently fall back to the old full bootstrap.

The first tree query is separate from opening. Use projected, indexed, cursor-paginated directory SQL. Returning a million root entries is intrinsically proportional to that output; a fixed first page prevents it from replacing bootstrap as the UI bottleneck.

## 2. Add native data coverage

Represent local availability independently from logical database contents. Every lookup has three possibilities: present, proven absent at a version, or unknown. Existing native storage treats missing keys/spaces as empty, so partial-replica semantics must be introduced deliberately at query/provider and native-state boundaries. Do not change every storage miss into an ad hoc HTTP request.

Start with a small set of coverage types:

| Coverage | Initial use |
|---|---|
| Exact logical row/key, including absence | File or entity lookup |
| Complete indexed equality scope | All children of a specified directory |
| Bounded ordered index range and continuation | Large listings and history pages |
| Referenced schema/plugin/catalog object | Planning, validation and rendering |
| Immutable object or content chunk | File contents and historical state |
| Native index/tree/manifest dependencies | Local validation and commit publication |

For the first implementation, use complete native row records as the editable hydration unit, with external content still independently lazy. UI projections can avoid content loads without introducing partially writable native rows. Column-level row coverage is a later optimization, if measurements justify its additional complexity.

A hydrated scope records its baseline identity, exact predicate/range, completion or page boundary, owned rows/absences, and the stream position that makes the snapshot coherent. Overlapping scopes share immutable objects but retain ownership metadata. Publish coverage and its inputs atomically. A page is not a complete relation; a tombstone is not an unknown key; absent content is not SQL NULL.

Coverage describes what can execute locally. It is not a new copy of application data or a replacement commit language. Native provenance, row identity and untouched canonical state must remain intact.

## 3. Make SQL drive hydration

Extend the existing sync-demand retry mechanism with current-row, indexed-range, schema, content and validation/publication demands. Native SQL providers and transaction planning emit structured requests for unknown inputs. The execution boundary gathers/batches those requests, fetches at a pinned logical baseline, installs them durably, then retries the statement coherently.

```text
SQL execute / observe / prefetch
             |
       local plan + coverage
          /          \
     covered        missing inputs
        |                 |
 local execution    hydrate pinned native scope
        |                 |
 local result       atomic install + coherent retry
        |
 writes commit locally; upload runs in background
```

Do not use regexes to derive dependencies from SQL text. Preserve the existing engine’s planning and validation behavior. Unsupported operators may demand a broader native scope; that can be slow on first use and is consistent with the target. A narrowly expressed query should not trigger broad loading when an appropriate native index/provider path exists.

Hydration does not hold a mutable storage transaction open across the network. If the baseline expires, restart the complete read under a new coherent baseline; never mix pages from different heads. When local pending commits exist, hydration supplies missing inputs from their compatible base and preserves the local overlay. Fetching “latest server rows” and merging them blindly is incorrect.

Mutation retries must distinguish pre-commit dependency misses from a commit that already became durable. Hydrate before mutation admission; after local commit, preserve the outcome and never replay the SQL merely because background publication or response delivery failed.

The authority must retain or lease baseline objects that a partial replica may still demand. A clean read can restart after expiry; pending local commits cannot silently switch bases. Define an expired-base recovery state that preserves pending work when compatible inputs are no longer available. Expiry alone is not authorization to discard local commits. Prepare and pin the dependencies needed for promised offline operations, and test authority GC/lease expiry explicitly.

Keep default `execute` behavior automatic: cold statements hydrate and retry, warm statements stay local. Expose SQL-level prefetch/preparation and a local-only execution option. Public names require API review; the intended behaviors are:

- **Prefetch a read:** execute its dependency-loading path ahead of use.
- **Prepare a write or transaction workload:** load its read set, validation inputs and publication dependencies without performing the mutation or side effects.
- **Execute locally only:** return a typed missing-dependency error instead of initiating a fetch.

Preparation readiness belongs to a dependency set and context. A new parameter that introduces an unseen uniqueness target, foreign key, plugin dependency or directory is a new cold demand. Do not advertise a generic “editable file” as covering every possible future mutation.

Explicit transactions require preparation before opening their fixed snapshot. A missing dependency inside such a transaction returns a typed preparation error; callers can prepare and retry the whole transaction. Never replay arbitrary external side effects or partially publish a mutation while hydrating.

## 4. Prove partial native writes before broad implementation

This is the first engineering gate, because it determines whether the architecture satisfies the central requirement.

Today, initial sync verifies a complete live-value root by hashing supplied rows, and imported-state certification can materialize the complete target state. Queried subsets cannot pass that contract, and weakening the check would allow accidental data loss. The old full-snapshot receipt/certificate must stay distinct from a new partial-current availability contract.

Retain the untouched authority baseline by reference and produce ordinary native commits from local changes. Reuse existing immutable manifest/tree mutation primitives where the actual publication path permits it. The existing BLAKE3-addressed tree has overlay/frontier update operations, but its root is not interchangeable with the current full live-value sync hash. Establish the exact correspondence in code before selecting a scope-proof or incremental-root format.

Prototype this narrow lifecycle first:

1. Open with only the partial-replica descriptor.
2. Load one existing file/entity and its actual mutation dependencies.
3. Disconnect and perform several covered updates with ordinary SQL.
4. Verify immediate local reads, native commit creation and local durability.
5. Reconnect and have the authority accept the commits through native sync.
6. Compare the authority and a fresh full replica, including untouched rows.
7. Exercise checkpoint, reopen, reset and GC without hydrating the full repository.
8. Expire an authority baseline lease while local commits are pending; either retain compatible inputs or enter the defined recovery state without losing local work.

Passing requires zero foreground requests for the prepared mutation, no hidden whole-base materialization, and preservation of canonical state and pending-work recovery. If current publication cannot achieve this, fix that engine path before presenting a result-cache prototype as fulfillment of the task.

Foreign-key, uniqueness, filesystem ancestry, delete restrictions, plugin extraction and checkpoint dependencies are part of write readiness. Full predicates may require broad coverage. Preserve the existing local provisional commit/server admission policy; do not silently convert validation into deferred speculative checks to make an operation appear local.

## 5. Keep the working set warm through background sync

Add server-side scoped delivery. A client subscribes to retained native scopes; the server delivers row changes, new matches, departures, tombstones and ordering-boundary information. It must not send complete unrelated commit bodies or file chunks for the browser to discard. Preserve transaction and commit identity through bounded authenticated control/provenance information and selectively fetched native dependencies.

A scope snapshot and its following changes need a race-free boundary. Apply cross-scope changes as a coherent transaction, with rows, provenance and coverage updates installed together. A move between two loaded directories must not transiently disappear from both or be counted twice in a combined query.

**Do not invalidate every scope on each head change.** Covered local mutations update local indexes, coverage and observation results immediately. Their acknowledgments advance confirmation metadata without discarding newer local descendants or reloading the scope. Remote changes outside a scope must allow that scope’s unchanged state to advance without downloading their payloads.

Maintain a coherent applied working-set version; the latest authority notification is not automatically the version visible to SQL. Dependencies and coverage from incompatible versions cannot be joined as though they were one snapshot. Gaps/resets retain the last coherent view until repaired, or produce a defined unavailable state; they never bless incomplete newer data.

Some scopes may remain retained without an active subscription, but then they describe an older complete snapshot. The system cannot unsubscribe and still promise latest-server values immediately on return. Pin scopes requiring continuously warm editing; bound other subscriptions and retained bytes explicitly.

Pending commits and their base/validation/recovery dependencies are not evictable cache. Garbage collection must understand remote references and incomplete local availability; it cannot assume local reachability is a complete inventory of authority state. Reset must not trigger an implicit full-repository bootstrap.

## 6. Permit server SQL fallback under a precise rule

Yes, a cold `COUNT(*)` or complex join can execute remotely without downloading all its base rows. Store its answer separately as a versioned result entry. That entry does not establish native row coverage or make arbitrary subsequent mutations local.

| Situation | Behavior |
|---|---|
| Aggregate inputs already covered | Execute locally |
| Cold read, no pending local writes, authority can evaluate the same logical baseline/context | Remote fallback allowed |
| Relevant pending local writes | Hydrate required inputs and execute locally |
| Pending writes whose irrelevance has not been proved | Treat them as relevant; do not guess |
| Explicit local transaction | No transparent remote fallback |
| Offline cold aggregate | Typed unavailable error unless a correct retained result/aggregate state exists |

Start conservatively by disabling fallback whenever the replica has pending writes. Later, planner-certified disjointness can relax this. The server must support evaluating the replica’s pinned baseline for automatic fallback; a normal latest-server SQL request is not equivalent. Immutable versioned SQL still needs compatible catalog and session context.

Example: after an offline local insertion, server `COUNT(*)` excludes the insertion. Flushing that insertion first adds a round trip and changes foreground behavior. A general remote evaluator accepting the exact local overlay is possible, but is a separate feature and unnecessary for the initial plan.

Cached scalar results remain local only while valid. Do not globally invalidate all native scopes because an aggregate becomes stale. Incremental local maintenance can be added for specifically supported aggregate forms; arbitrary joins/aggregates require more than adjusting the count by the number of local writes.

Server fallback is a read optimization after native partial-replica semantics are correct, not a dependency for making open bounded.

## Delivery order

| Milestone | Deliverable | Exit gate |
|---|---|---|
| A: instrumentation and partial-replica write feasibility | Bootstrap counters and a native partial-replica edit lifecycle spike | Prepared offline edit, accepted upload and preserved untouched state without full-base reads |
| B: bounded opening | Versioned descriptor and distinct partial-replica availability/receipt mode | Constant-size bootstrap as rows, branches, history, schemas and blobs grow |
| C: SQL-driven current hydration | Point, directory/range, schema and content demand paths; prefetch/local-only behavior | Cold load followed by correct warm offline reads and prepared writes |
| D: scoped live synchronization | Membership-aware transactional updates, confirmation and recovery | Covered local writes and unrelated remote writes do not cause rehydration |
| E: SQL fallback and broader operators | Safe versioned result fallback and additional dependency planning | Counts/joins remain correct with local writes and fixed-snapshot transactions |
| F: browser rollout | OPFS persistence, multi-tab coordination, eviction, telemetry and feature-gated rollout | Full correctness/performance matrix passes |

Milestones B–D are required together before calling this production on-demand sync. Lazy blobs alone or a cached remote file tree is useful evidence, but does not complete the goal.

## Implementation map

The inspected engine revision is `4d1c219e5f08a6b342e7a013f72f9097ad7261e4`; browser revision is `402dea800908d3fee73aa127a069f1479659f7a1` in the landing-use-cases checkout. These are local code references, not deployment provenance.

| Seam | Planned work |
|---|---|
| [Bootstrap runtime](../packages/lix/src/sync/runtime.rs) and [installation](../packages/lix/src/sync/bootstrap.rs) | Replace all-branch/current/checkpoint loading with partial-replica descriptor admission |
| [Complete live-value hash](../packages/lix/src/sync/repository.rs) and [snapshot verification](../packages/lix/src/sync/repository.rs) | Keep full-snapshot certification separate from partial-current coverage |
| [Imported-state certification](../packages/lix/src/sync/repository.rs) | Eliminate whole-state reconstruction from partial-replica publication paths |
| [Demand retry](../packages/lix/src/sync/runtime.rs), [handle retry](../packages/lix/src/handle.rs), [observations](../packages/lix/src/session/observe.rs) | Add current-data demands and coherent retry behavior |
| [SQL providers](../packages/lix/src/sql2/providers/mod.rs) | Derive missing input scopes from actual plans |
| [Validation](../packages/lix/src/transaction/validation.rs) | Make validation dependencies explicit and prepare them locally |
| [Immutable tree updates](../packages/lix/src/tracked_state/tree.rs) | Evaluate reuse for partial-replica publication; do not conflate root types |
| [Browser open](../../lixray-landing-use-cases/web-app/src/lib/repository-lix-session.ts) | Use on-demand sync mode and separate session readiness from query readiness |

## Required regression and performance gates

Use matched repositories that independently scale unopened rows/content, history, branches and schemas by 100×. Test both browser OPFS and canonical engine simulations. Do not claim success from only one small fixture or from elapsed time alone.

- Bootstrap request count, decoded bytes and materialized objects remain bounded. No loop drains any repository inventory.
- First directory page downloads no unrelated content chunks and performs indexed bounded work.
- Repeat warmed reads with transport disabled. Results match a complete reference replica.
- Perform prepared writes at 0/50/150 ms injected RTT and offline. Completion waits only for local work/durability; request counters prove no foreground network dependency.
- Local edit followed by observation, read and own acknowledgment preserves coverage and read-your-writes without hydration.
- Empty-scope fetch followed by remote insertion produces the new match; deletion, rename, move and top-N replacement preserve completeness.
- A cold aggregate after local insertion never returns the authority-only answer.
- Unknown uniqueness target, foreign key, directory ancestry and broad update predicate demand preparation instead of false success.
- Interrupted scope installation, delta gap, schema change, branch switch, reset and multi-tab races cannot publish incomplete state as complete.
- Small edits with large untouched state do not materialize or transmit that state during commit, upload, checkpoint, reset or GC.
- Unsynced commits and their dependency closure survive close/reopen and cache pressure.

Measure authority scans, browser allocations, OPFS writes, foreground request counts, transferred bytes and main-thread stalls alongside p50/p95 latency. Separate opening, first query, warm query, local commit and background catch-up. The invariant is bounded opening and dependency-proportional operations, rather than a promised speedup inferred from the 23.5-second baseline.

## Independent review disposition

**Correctness reviewer:** supports native partial replicas with on-demand sync with explicit completeness and prepared writes. Required safe aggregate fallback, read-your-writes, three-state availability, coherent scope updates, transaction preparation and explicit baseline-retention/expiry behavior with pending edits. Incorporated after final document review.

**Performance reviewer:** supports the direction. Required removing all inventories from opening, preserving coverage across writes, filtering changes at the server, and auditing publication/reset for whole-repository work. Incorporated.

**Architecture reviewer:** supports the direction subject to the native partial-replica write proof. Identified complete-state certification as the central blocker and existing native demand/tree operations as possible reuse seams. Required a distinct partial-current contract and proof of accepted partial-replica writes without data loss. Incorporated.

These reviews support implementing the feasibility milestone; they do not establish that the unimplemented partial-replica publication protocol is correct. The first decision gate is a tested native partial-replica edit, not a UI cache demo.
