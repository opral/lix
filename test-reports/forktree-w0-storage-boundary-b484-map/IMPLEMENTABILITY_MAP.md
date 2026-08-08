# W0 storage-boundary implementability map for b484

Status: TEST/REPORT-only planning evidence. This package makes no production
change, does not compile or run adapters, and is not an approval of b484.
The b484 lineage is explicitly blocked/compiler-red in the assignment.

## Immutable anchors

The map is authored from the frozen W0 binding package, while the source being
mapped is a separate exact object:

```text
W0 binding ref=origin/codex/forktree-w0-storage-boundary-fd2-binding
W0 binding head=846981ead666eda465d358368f73cf93e2c9339f
W0 binding parent=e2503fd1d43b95d3ebfd133b9868a4be0647ee3d
W0 binding tree=8731e9a4c4239ab175d938b069870703fc5affb4
W0 binding parent..head full-index SHA-256=c02da9270b9679f19fad5de813678ac27dc29dc3f71fa85a8c7719d0051f52a3
W0 binding stable patch ID=96e2a4602257a0684e7712eff1cfe4e1593e364e
W0 binding report SHA-256=4f01a664de349802a0d85293a8dcac4fb59f466b73ae5ff7424be7a97e8ac76d

W0 v3 contract commit=6a91df3f88177e9b6d53d20d5ba6554df8fd6b9a
W0 v3 tree=0d194d75190caca4219779edd87469c57f9db8b8
W0 v3 parent..commit full-index SHA-256=847d6f8e8554c21933d5a89238dbca9ae36bdadb64ce761d80c669e59399067e
W0 v3 stable patch ID=f89895331cb4b7c18db0c79b9ff47a8261a076b2

fd2 anchor=fd2be256d763f17e9f127d4c984e36fba191cb82
fd2 tree=20110ca5e3c33d34217630fff0a2b784b545317a
fd2 parent=cd91b9b90f7f468158b4df154adbed9551eb5d60
fd2 parent..commit full-index SHA-256=1a410542cff54e3b1c83a5cfb2cdea568dc9f1f71fc0c3f8598e8936d944a277
fd2 stable patch ID=c275ab15f3306c503e6830afee2a66bacf1fb974

b484 mapped commit=b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
b484 tree=4477c83b246bddac09cd972564bd4ccd67f90f7b
b484 parent=fd2be256d763f17e9f127d4c984e36fba191cb82
b484 subject=Stage2: close file history materialization authority
fd2..b484 full-index binary SHA-256=d36495fc406cc213bb5729babae761916f97bd515221de14c1f3ae114ec22610
fd2..b484 stable patch ID=e90c9dd93db7c343f67887218049406640a77631
```

`b484` is not attached to a local branch in the review checkout. Its only
source delta from fd2 is:

```text
packages/lix/src/sql2/providers/file_history.rs
packages/lix/src/sql2/providers/filesystem_working_diff.rs
```

The W0 binding package itself is the provenance anchor for this report; the
report does not claim that b484 is descended from the W0 binding commit. The
common source boundary is fd2.

## W0 contract applied to b484

The only physical spaces permitted after the boundary cut are:

| Space | Owner | Allowed role |
| --- | --- | --- |
| `OBJECT_SPACE` | ForkTree object store | authenticated immutable objects and blob/object domains |
| `SELECTOR_SPACE` | ForkTree selectors | branch/global selector, epoch, checkpoint and publication control facts |
| `UNTRACKED_ROW_SPACE` | ForkTree untracked rows | engine-owned mutable rows that are not chronology facts |

`StorageSpace` is engine-declared with private identity/brand. The raw
`mutable` and `immutable` constructors are absent from b484's `lix` source;
`SpaceId`'s tuple field is private. Therefore all old direct constructor and
tuple-constructor uses are compiler-deletion residue, not an invitation to
restore public constructors. `StorageSpace::engine_declared` is the only
remaining declaration path and must remain crate/internal-only for the three
allowed spaces and narrowly controlled test fixtures.

The W0 rule is not “delete every distinct fact immediately.” A live semantic
owner must first be represented as an authenticated object, selector, or
untracked row with an explicit key/domain and the required atomicity. Deleting
its old space before that migration would be a data-loss or semantic change.

## Raw constructor and registry inventory

### Direct `StorageSpace` calls

There is no `StorageSpace::mutable` or `StorageSpace::immutable` definition in
`packages/lix/src/storage/types.rs` at b484. The relevant source locations are:

| Location | Count/role | Classification |
| --- | --- | --- |
| `packages/lix/src/gc.rs:45-74` | seven production GC/checkpoint spaces; the grouped eight also includes a test fixture near `:8008` | live W5/R7 owner plus one test residue |
| `packages/lix/src/session/media_upload.rs:18-22` | upload state and manifest-leaf spaces | live W4 upload owner |
| `packages/lix/src/engine.rs:538-543,597-600` | three old-layout spaces under tests | stale no-compatibility calibration; independently removable after preserving the negative intent in the report |
| `packages/lix/src/storage/in_memory.rs:617,685` | generic mutable-space tests | fixture/compiler fallout; retarget or delete |
| `packages/lix/src/storage_adapter/conformance.rs:316,327,338,342` | conformance fixtures | fixture/compiler fallout; retarget or delete |
| `packages/lix/src/storage_adapter/reader.rs:40,168,178` | reader fixtures | fixture/compiler fallout; retarget or delete |

Support-only grouped calls remain in SlateDB tests/source, engine benchmarks,
`rs-sdk-tests`, and adapter tests. They are not production storage owners, but
they will fail once the constructor is unnameable. The compiler-driven wave
must migrate them to a test-only engine-declared fixture or remove them; it
must not make the production constructor public again.

`StorageSpace::immutable` has no `packages/lix/src` production use at b484.
The remaining occurrences are SlateDB and benchmark/test fixtures.

### Direct `SpaceId(...)` calls

The tuple constructor is private, so these are the same hard-cut residue in a
different spelling. The largest groups are:

```text
packages/slatedb-storage/src/slatedb.rs             23
packages/engine-benchmarks/.../storage_v2          15
packages/slatedb-storage/tests/storage.rs           6
packages/engine-benchmarks/.../large_blob           5
packages/lix/src/gc.rs                              7
packages/lix/src/storage_adapter/conformance.rs     4
packages/rs-sdk-tests/tests/e2e.rs                   3
```

The `lix/src/gc.rs` entries are live GC space-key construction and wait for
W5/R7. The other listed groups are fixtures/bench support or compile fallout,
except where a test intentionally asserts old-layout rejection. No direct
constructor should be re-exposed.

## Remaining engine-declared spaces and live callers

Every row below is a real semantic owner or control fact. It is not safe to
delete the space in isolation. The target column names the first wave allowed
to replace it; the replacement must be one of the three W0 planes.

| Current declaration and source | Live callers / fact | Classification and first dependency |
| --- | --- | --- |
| `catalog/revision.rs:9` `CATALOG_REVISION_SPACE` | `load_catalog_revision`/`stage_catalog_revision` at `:35,58` | schema/catalog generation. Move to selector/control or an authenticated derived projection with one transaction publication; coordinate with schema resolver and selector/W3. |
| `filesystem/path_index.rs:34` `FILESYSTEM_PATH_REVISION_SPACE` | load/stage at `:1403,1426` | path-index revision/cache fence. Wait current file-history correction and W4 path/blob authority. |
| `init.rs:49-55` `REPOSITORY_PROTOCOL_SPACE` | `init.rs:68-93`; protocol checks in `engine.rs:648,671,695,719,743,767,791,815,839,867,892,1556,1602` | boot/protocol gate. Preserve exact protocol rejection and cold reopen; fold into selector/control publication in W3, not an independent deletion. |
| `json_store/store.rs:11-16` `JSON_SPACE` | store reads/writes at `:325,401,706,737,744,812`; context and GC callers | content-addressed JSON payload owner. Map to OBJECT_SPACE/ObjectDomain or untracked rows after object authority is accepted; not independent. |
| `json_store/context.rs:24-31` `UNTRACKED_JSON_RECLAIM_CANDIDATE_SPACE` | context `:95,273,306`; GC `:6485,6851,6898,7501,7514`; benchmark `:2228` | durable reclaim hints. Wait W5/R7 full-root closure and owner-scoped pins; then fold into the GC/object plane. |
| `session/idempotency.rs:73-77` `EXECUTE_IDEMPOTENCY_RECEIPT_SPACE` | idempotency `:214`; transaction/context `:1636,1645` | atomic idempotency receipt. Move into untracked/selector rows in the same transaction prepare/commit under W3. |
| `storage_adapter/spaces.rs:9,14` mutation and tracked-mutation revision spaces | `storage_adapter/context.rs:135,139,148,163,191,260` | mutation/epoch fences. Replace with selector-space epoch/control facts in W3; preserve stale same-owner and unrelated-owner CAS behavior. |
| `gc.rs:45-74` seven GC/checkpoint spaces | checkpoint refs, reachability delta/queue, sweep epoch/mark/cursor | live W5/R7 publication/GC owner. Keep until full transitive-root closure, progress debt/no-spin, poisoned cursor, and reopen are implemented. |
| `session/media_upload.rs:18-22` upload state/manifest-leaf spaces | resumable upload and part/manifest publication | live W4 owner. Preserve authenticated manifests, 16 MiB part semantics, atomic publication and W5 final-reference reclamation. |

`packages/lix/src/storage/conformance/*` `engine_declared` calls are generic
test fixtures, not additional production registries. They can remain only as
test-only construction behind an internal helper, or be deleted when the
conformance suite uses the declared constants.

## Legacy owner planes and exact caller closures

### Tracked/columnar state

`packages/lix/src/tracked_state/mod.rs:12-40` still reexports
`TrackedStateContext`, `TrackedStateStoreReader`, merge planning, diff,
materialization and related types. `tracked_state/context.rs` still owns
tracked roots/diffs and writers and imports changelog/storage/tracked-tree
components. This is a live old authority, not dead compiler fallout.

`live_state/context.rs:47-60,124-130,137-155,243-261` still stores and uses
`TrackedHeadContext`/tracked readers for packed identity membership and
collection generation, while `:199-212` already has a ForkTree facade path.
`live_state/context.rs:303-370` retains branch selector/cache loading.
`live_state/reader.rs:24` and `live_state/entity_columnar.rs` remain active
projection/writer support. The current b484 two-file file-history correction
does not delete this closure.

Required order: current file-history/provider correction first; then direct
current-state reader and selector migration; then transaction/W3 publication;
only after all callers are moved can `TrackedStateContext`, its spaces,
columnar writer, reexports and fixtures be deleted. Do not add a wrapper or a
ForkTree fallback around the old reader.

### Changelog

`changelog/context.rs:17-108` contains reader/writer wrappers, but its read
delegations at `:81,108` already call ForkTree commit/change loaders. It is a
transitional facade, not a justification for a second physical owner.

`changelog/mod.rs:7-27` still reexports old types. The old `CHANGE_SPACE`,
`COMMIT_SPACE`, and `COMMIT_CHANGE_ID_SPACE` names are already absent from the
b484 changelog module, so remaining imports are compiler fallout where the
logical caller has not yet migrated:

| Caller | Use | Cut dependency |
| --- | --- | --- |
| `sql2/providers/change.rs:11,329` | commit/change identity provider | independent semantic provider migration; preserve identity/order/NULL/tombstone before deleting import |
| `gc.rs:21,1337,2394,2410,3154,3206,3212,3419,3421,3425` | GC deletion/reachability | W5/R7 only |
| `storage_bench.rs:1089-1092,2242-2244,2393,2397,2597-2599` | benchmark accounting | stale support fallout; remove/update after semantic closure |
| `tracked_state/context.rs:7022` | legacy tracked commit lookup | current file-history/whole tracked-state migration |

### Branch selector

`branch/refs.rs:10-77` (`BranchRefContext`, reader, `load_head` and
`load_head_commit_id`) delegates to ForkTree loading, but
`live_state/context.rs:303+` still constructs `BranchHeadControlContext` and
`BranchHeadControlCache`. These are selector semantics, not dead residue.
Replace them with authenticated `SELECTOR_SPACE`/ForkTree selectors under the
selector/W3 cut, with one retained coherent view and atomic epoch/CAS. Delete
the old context/cache/reexports only after all branch/history/checkpoint
callers are closed.

### Binary CAS

`binary_cas/context.rs:13-21,71-116,139-157` owns `BlobDataReader`,
`BinaryCasContext`, `BinaryCasStoreReader`, and the existing-chunk-aware
writer. Reads are used by plugin/filesystem paths; writes are used by
multimedia/file upload transactions. This is a live payload/chunk authority,
not a lexical residue. It must wait for the accepted W4 object/blob manifest
cut and W5 final-reference GC. Delete the module, reexports in
`binary_cas/mod.rs:5-6`, and its registry only after every reader/writer caller
is moved to authenticated ForkTree object/blob ownership.

## Dependency-ordered hard-cut plan

### Wave A — independently removable compiler/support residue

This wave must not change production semantics:

1. Remove or retarget old-layout negative fixtures in `engine.rs:538-543,597-600`
   while retaining the assertion as a raw backend/corruption fixture in the
   report-only oracle. Remove generic `StorageSpace::mutable` fixtures in
   `storage/in_memory.rs`, `storage_adapter/conformance.rs`, and
   `storage_adapter/reader.rs`, or bind them to the declared constants.
2. Migrate/delete support-only raw `SpaceId(...)` and `StorageSpace::mutable`/
   `immutable` uses in SlateDB tests, engine benchmarks, `rs-sdk-tests`, and
   Lix adapter tests. Do not widen the production constructor.
3. Remove missing old-space imports/usages from `storage_bench.rs` after
   retaining equivalent accounting labels. These are not runtime owners.
4. Delete stale changelog reexports/wrapper imports only where the caller has
   already moved; do not delete the GC/tracked-state callers in this wave.

Acceptance: source residue scanner, negative Rust/TS/native compile probes,
`cargo fmt --all -- --check`, `git diff --check`, and the changed support/test
packages. No compatibility decoder, public raw constructor, or fallback is
allowed.

### Wave B — current file-history and current-state closure

The b484 correction is in `sql2/providers/file_history.rs` and
`filesystem_working_diff.rs`, but it is not the whole closure. Finish the
five history/working-diff providers and current-state reader migration:

* one retained authenticated ForkTree view;
* no `TrackedStateContext`/`TrackedStateStoreReader`/historical batch factory;
* exact file/directory identity, ordering, limit-before/after grouping,
  NULL/tombstone and checkpoint selection;
* missing commit/root is typed failure, while valid absence is an empty result;
* metadata-only and content-bearing rows authenticate identically;
* cold reopen and corruption fail closed with zero partial publication.

Only after this closure can the tracked/columnar read facade be removed from
SQL/provider paths. This wave is the prerequisite for the TrackedHead whole
module deletion and must not be claimed by W0 alone.

### Wave C — selector and transaction publication (W3)

Move `BranchHeadControl*`, mutation revisions, catalog/schema revision,
repository protocol, and idempotency receipts into authenticated
`SELECTOR_SPACE`/`UNTRACKED_ROW_SPACE` rows. Every supported transaction must
use one caller-owned `CoherentView`, one `PreparedPublication`/
`into_storage_plan`, the existing one `prepare_write_set`, and one backend
commit. Selector/global epoch/checkpoint/recovery refs and idempotency facts
must share that batch. Preserve true no-op, rollback/savepoint, stale
same-owner/unrelated-owner, branch-first/GC-first and cold-reopen behavior.

Direct `PreparedPublication::commit`, legacy checkpoint writers, and independent
retry/commit paths must be unnameable. File/upload/checkpoint/multi-branch
cohorts that are not yet supported must fail closed before plan creation; do
not add a dual path.

### Wave D — object/blob/upload and path authority (W4)

Migrate `BinaryCasContext`/writer, media upload spaces, path-index revision,
and JSON payload ownership into ForkTree object/blob domains and untracked rows.
The manifest and visible `BlobId` must remain authenticated authority;
unchanged chunks may be reused only after full named-base authentication. Keep
16 MiB parts, partial-read binding, zero unchanged payload reads/writes for a
valid reuse, atomic publication, and malformed/missing/wrong-domain failures.

### Wave E — GC and recovery (W5/R7)

Replace GC/checkpoint/recovery spaces and reclaim-candidate hints only after
the full authenticated transitive closure exists for every live selector,
checkpoint/history/serving root. Keep a live checkpoint selector's closure
protected after view close, owner-scope reader pins, 64+suffix progress,
blocked-head debt/no-spin/release cadence, publication↔GC race ordering,
poisoned cursor restart, corruption and reopen. Then delete old GC spaces,
writers and reachability registries.

### Wave F — whole-module deletion

After Waves B–E and all caller closure, delete `tracked_state`, remaining
columnar/live-state authority, changelog reader/writer wrappers and stale
branch-head contexts/reexports. Re-run full source/API residue scans and
consumer compile-negative probes. No compatibility alias, migration reader,
cache, fallback or second durable authority may survive.

## What can and cannot wait

| Item | Independent now? | Required dependency |
| --- | --- | --- |
| Test/benchmark raw constructor and private `SpaceId` fallout | Yes | Wave A only |
| Old-layout negative fixtures and stale changelog benchmark imports | Yes, as support cleanup | Preserve report-only calibration |
| `sql2/providers/change.rs` commit/change identity migration | No semantic deletion yet | ForkTree history provider/source-ID semantics |
| `file_history` and filesystem working diff | Candidate correction in b484, but closure not complete | Wave B acceptance and current-state reader cut |
| TrackedState/columnar module | No | Waves B and C; then whole caller closure |
| BranchHeadControl/selector cache | No | Wave C selector/W3 |
| catalog/path-index/protocol/idempotency spaces | No | W3 or W4 according to owner |
| Binary CAS and upload spaces | No | W4 then W5 |
| GC/recovery/reclaim spaces | No | W5/R7 full closure |

## Acceptance command order

The commands below are frozen as future gates. They were not run for this
report, and no build/runtime result is implied.

### Source/provenance gate

```bash
git show -s --format='%H%n%T%n%P%n%s' b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
git diff --name-status fd2be256d763f17e9f127d4c984e36fba191cb82..b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
git diff --binary --full-index --no-ext-diff \
  fd2be256d763f17e9f127d4c984e36fba191cb82..b484e20d845aee3f8137bfa3496f9b3cd0e8cd35 | sha256sum
git diff --no-ext-diff \
  fd2be256d763f17e9f127d4c984e36fba191cb82..b484e20d845aee3f8137bfa3496f9b3cd0e8cd35 | git patch-id --stable
git diff --check
```

### W0/source residue and API negative gate

```bash
python3 test-reports/forktree-w0-storage-boundary-b484-map/verify_map.py \
  --repo "$PWD" \
  --w0 846981ead666eda465d358368f73cf93e2c9339f \
  --candidate b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
git grep -n -E 'StorageSpace::(mutable|immutable)|SpaceId\\(' \
  -- packages/lix packages/slatedb-storage packages/engine-benchmarks packages/rs-sdk-tests
```

The expected result is a classified inventory, not zero text matches. The
production hard cut must reject raw construction and classify only the three
declared spaces; support fixtures must be migrated/deleted rather than used to
weaken the gate.

### Compile and semantic gates for the future production waves

```bash
cargo fmt --all -- --check
git diff --check
cargo clippy -p lix --lib --all-targets -- -D warnings
cargo test -p lix --lib --no-run
cargo test -p lix --tests --no-run
```

Use isolated, exact-SHA `CARGO_TARGET_DIR` values per candidate. Cap every
compile/test cell at 20 minutes. Runtime order, only after compile-green, is:

```text
Memory: source, corruption, stale/race, no-op, rollback, reopen
RocksDB: same correctness package, then cold reopen and publication accounting
SlateDB: same correctness package, then cold reopen and publication accounting
```

The W3/W4/W5 runtime gates must record one view/plan/prepare/commit, backend
reads/writes/bytes, logical rows/bytes, allocation/RSS and settled disk, and
must stop on a first digest, corruption, authority, or critical-regression
failure. No current-main performance comparison is part of this map.

## Terminal classification

This package is a dependency map, not a candidate approval:

```text
W0 binding: accepted report-only boundary
b484: blocked/compiler-red source lineage; not runtime-qualified
independently deletable: Wave A support/fixture/compiler fallout only
semantic owners: Waves B–E, in order, with no deletion before replacement
```
