# SQL changelog readiness binding — e1af

This directory is TEST/REPORT-only. It binds the frozen SQL changelog v4
oracle and the read-only implementation map to immutable production head
`e1af471b9ab0f598dafa7c2ddec7867667c81740`. It is intentionally expected to
be RED on e1af. It does not accept e1af, edit production, add a runtime path,
or replace the independent v4 approval.

## Immutable binding

| item | value |
| --- | --- |
| e1af ref | `origin/codex/forktree-stage2-fd2-nine-seam-correction` |
| e1af head/tree | `e1af471b9ab0f598dafa7c2ddec7867667c81740` / `bfa0d271a723da8250ab76ada16fda90926f1099` |
| e1af parent | `b484e20d845aee3f8137bfa3496f9b3cd0e8cd35` |
| e1af parent..head full-index binary SHA-256 | `9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c` |
| e1af stable patch-id | `31cc575644bf17e65c59d558a03acffc848c2e20` |
| e1af production scope | `packages/lix/src/sql2/providers/file_history.rs`, `packages/lix/src/sql2/providers/filesystem_working_diff.rs` |
| inherited compiler frontier | fd2 baseline `136 errors / 9 warnings`; no new compiler run here |

Frozen v4 binding:

* head/tree `d36e1fe34c4bf93c01478b876a5f73a8dccd33de` /
  `7fcaeb5f965d5c8b641e5e82a741c75cc18365b6`;
* parent `e6ca79542b11245ba5f1ed31b2f62d4a492e035a`;
* parent..head full-index SHA-256
  `4f73b5a944ea15b9d0ed6c040ef80fa60f7631437c2fd22ce1bf5ae6a97798d0`;
* patch-id `212a1ff4a6f050905e5d62f94f78ce2e76d5fc24`;
* exact fd2 RED SHA-256
  `74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5`;
* independent v4 TEST/REPORT-only approval is external evidence, not a claim
  made by this binding.

Implementation-map evidence bound into this package:

* report SHA-256 `249bbc8c6980862421e6475a38d1fa9c36bbab97db84000de0e5fd3368b159f0`;
* manifest SHA-256 `58f88f4bd3758f16950679dcdcd3ec934de5313d6041d94e32687ecc6cbae600`.

## Expanded source and call-graph contract

The full route under review is:

```text
session/context.rs::changelog_query_source
transaction/context.rs::changelog_query_source
sql2/exec/datafusion.rs::DummySqlExecutionContext::changelog_query_source
  -> sql2/context.rs::ChangelogQuerySource
  -> sql2/providers/change.rs::ChangeSpec::plan_scan
       -> one query_source.forktree_reader
       -> authenticated ChangeCatalog/CommitCatalog serving
  -> sql2/providers/diff.rs::register_diff_function
       -> DiffFunction/DiffSpec carrying that same facade
       -> authenticated ForkTree state history
```

`sql2/providers/diff.rs` is a concrete consumer of the same
`ChangelogQuerySource`, not an optional or historical path. Its current
`query_source.store` extraction at `:28-44` and closure-local
`ForkTreeReadFacade::new(store)` at `:138-152` are part of the RED boundary.
The first implementation must carry the already-created
`ForkTreeReadFacade` through `DiffFunction` and `DiffSpec` and call its
historical methods directly. It may not construct another facade or reader.

### Exact allowed production path set

Only the following production paths may change in the first implementation
successor, plus focused tests adjacent to those paths:

```text
packages/lix/src/sql2/context.rs
packages/lix/src/sql2/providers/change.rs
packages/lix/src/sql2/providers/diff.rs
packages/lix/src/sql2/exec/datafusion.rs       # test-only dummy constructor
packages/lix/src/session/context.rs
packages/lix/src/transaction/context.rs
packages/lix/src/forktree/view.rs
packages/lix/src/forktree/serving.rs
packages/lix/src/forktree/mod.rs               # only if an existing seam needs export
```

The e1af two-file production delta is not part of the successor scope and must
remain unchanged. No writer, selector, GC, storage format, checkpoint
chronology, unrelated SQL provider, or public API change is authorized.

### Required hard deletion negatives

The reachable `change.rs` and `diff.rs` closure must contain none of:

```text
tracked_state::scan_change_records_from_commit_deltas
tracked_state::load_change_record_by_id
COMMIT_CHANGE_ID_SPACE
ChangelogContext::new().reader
ChangelogReader
ChangeScanRequest / ChangeLoadRequest
CommitGraphContext::new().reader
query_source.store as a lookup/read authority
ForkTreeReadFacade::new(store) in diff.rs
begin_read, fresh open, cache, compatibility reader, fallback, or reset
```

`ChangelogContext` and the legacy spaces have other writer/GC/test owners;
this binding does not claim whole-module deletion. The SQL provider closure
must nevertheless stop reaching them.

The source gate must also require:

* `ChangelogQuerySource` carries one `ForkTreeReadFacade` bound to the
  operation-retained read;
* both change scan and exact lookup use the exact
  `&query_source.forktree_reader` identity;
* session, transaction, and the test-only dummy each construct one facade;
* transaction construction reuses `opening_read()` / the existing
  `forktree_read_facade()` helper and does not call `begin_read`;
* diff carries the same facade instead of constructing one in its closure;
* direct and derived ChangeCatalog/CommitCatalog rows authenticate key,
  embedded ID, kind/domain, membership, owner/ordinal back-edge, duplicates,
  ordering, and limit-after-merge; malformed required records fail closed.

## First successor replay order

No build or runtime is part of this binding. After a candidate implementation
is independently source-reviewed, replay the frozen v4 verifier plus this
expanded diff-consumer binding, then compile and run semantic gates in order:

```text
python3 test-reports/stage2-sql-change-readiness-e1af/verify_source_binding.py <candidate-worktree>
python3 test-reports/stage2-sql-change-reader-fd2/verify_contract_v2.py <candidate-worktree>
git diff --check
cargo fmt --all -- --check
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-compile timeout 1200 \
  cargo check -p lix --lib --message-format=short
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-compile timeout 1200 \
  cargo test -p lix --lib --no-run --message-format=short
```

Memory is first, using the existing integration SQL tests:

```text
CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-memory timeout 1200 \
  cargo test -p lix --test integration --features all-simulations lix_change_ \
  -- --nocapture --test-threads=1
```

The adapter-specific command contract must be frozen in the first runnable
test-only harness before execution; no such target exists in the current
repository, so these are not claimed results:

```text
LIX_CHANGE_BACKEND=rocksdb CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-rocks \
  timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_reader --features 'storage-benches slatedb' \
  -- --exact rocksdb_lix_change --nocapture --test-threads=1

LIX_CHANGE_BACKEND=slatedb CARGO_TARGET_DIR=/tmp/lix-stage2-sql-change-slate \
  timeout 1200 cargo test -p lix_benchmarks \
  --test sql_changelog_reader --features 'storage-benches slatedb' \
  -- --exact slatedb_lix_change --nocapture --test-threads=1
```

Required semantic order is direct row, derived `lix_commit`, authenticated
absence, missing/malformed/wrong-kind/substituted object, wrong embedded key,
duplicate logical ID, canonical merged order, limit after merge, exact-ID
routing, cold reopen, and identical result digests on Memory/RocksDB/SlateDB.
Stop before widening on any corruption, identity, second-view, or scope
mismatch.

## Consumer-closure audit folded into this binding

The exact e1af tree was audited read-only. The complete production SQL
consumer closure is:

```text
sql2/session.rs::{build_read_session_with_active_head,build_transaction_read_session}
  -> SqlExecutionContext::{changelog_query_source,history_query_source,commit_graph}
  -> sql2/providers/mod.rs::register_read_from_catalog
       -> providers/change.rs::register_lix_change_read_provider
       -> providers/diff.rs::register_diff_function
       -> providers/{checkpoint,working_diff,filesystem_working_diff,
                     file_history,directory_history,entity_history,entity}.rs
            -> history_route.rs::load_history_entries
                 -> CommitGraphReader::{change_history_from_commit,
                                        load_commit_records}
                 -> HistoryQuerySource::forktree_reader
```

Concrete constructor/caller sites are `session/context.rs:727-748`,
`transaction/context.rs:8215-8235`, and the test-only DataFusion dummy at
`sql2/exec/datafusion.rs:3366-3391`. The read source type is declared in
`sql2/context.rs:50-67`; e1af's `ChangelogQuerySource` still contains only
`store` and `json_reader`, while `HistoryQuerySource` separately contains a
`ForkTreeReadFacade`.

The direct changelog-row authority is
`sql2/providers/change.rs:151-367`: it currently reaches tracked-state
delta scans, `ChangelogContext`/`ChangelogReader`, the raw
`COMMIT_CHANGE_ID_SPACE`, and a fresh `CommitGraphContext` for both scan and
exact lookup. The direct diff authority is
`sql2/providers/diff.rs:28-223`: it extracts `query_source.store` and builds
`ForkTreeReadFacade::new(store)` inside the scan closure. Those are the
first-slice blockers.

History is a separate, real consumer and must not be silently omitted from
the closure. `history_route.rs:339-535` consumes a caller-passed
`CommitGraphReader` for chronology and commit records, then consumes
`query_source.forktree_reader` for certified state rows. The session/provider
fan-out is at `sql2/providers/mod.rs:247-370` and `:402-469`; concrete
providers are checkpoint (`checkpoint.rs:21-48`), working diff
(`working_diff.rs:27-55`), filesystem working diff
(`filesystem_working_diff.rs:29-65`), file history, directory history,
entity history, and entity history registration. This is the next chronology
authority boundary, not an excuse to add a second reader to the changelog
slice.

Other production `ChangeRecord`/changelog consumers remain legitimate owners
outside the first SQL cut: changelog context/materialization/types, commit
graph context/types, ForkTree serving/view, filesystem read, functions/state,
GC, init, live-state context, session execute/undo-redo, SQL change
materialization, tracked-state context/row materialization, and transaction
commit/context/cohort. Writer, publication, GC, and non-SQL history owners
must remain until their own authorized cuts.

Direct facade sites were classified rather than globally forbidden:

* SQL production duplicate: `sql2/providers/diff.rs:151`.
* SQL history retained-view wrappers: session and transaction history source
  constructors; provider clones of that facade do not call `begin_read`.
* Same-read, non-SQL serving wrappers: `filesystem/read.rs:29`,
  `live_state/context.rs:209`, `plugin/registry.rs:533`, and
  `live_state/forktree_reader.rs:279` consume caller-retained reads or an
  explicitly retained read for GC/serving roles; they are not SQL changelog
  acquisitions.
* Canonical acquisition/serving APIs: `forktree/view.rs:555-575`,
  `forktree/serving.rs:297-305`, and `transaction/commit.rs:187` use the
  caller-owned read protocol. Their existence is not a pass for a SQL
  provider to call `begin_read`.
* `sql2/history_route.rs:1244-1258`, `sql2/providers/mod.rs:1107-1121`,
  and the DataFusion dummy are test-only source constructors.

The smallest source acceptance delta for the forthcoming production child is
therefore: add one `ForkTreeReadFacade` field to `ChangelogQuerySource`, bind
it from the session/transaction retained read (using the existing transaction
`forktree_read_facade()` helper), update the test-only dummy, migrate
`change.rs` scan/exact lookup to the typed authenticated facade, and carry the
same field through `diff.rs` without a closure-local constructor. No history
chronology rewrite, writer/GC change, storage change, cache, fallback,
compatibility reader, or second authority is in this delta. The v4 ten-case
semantic oracle remains the required next gate, unchanged.

## Corrected package gate

The v2 successor to the blocked predecessor adds three executable package
guards. `verify_source_binding.py` now compares
`e1af471b9ab0f598dafa7c2ddec7867667c81740..HEAD` with rename/copy awareness
and rejects every changed path outside this file's exact `ALLOWED` set. It
also extracts the concrete diff registration, `call`, and `plan_scan` bodies
with balanced-brace parsing, proving this identity chain:

```text
query_source.forktree_reader
  -> DiffFunction.forktree_reader
  -> self.forktree_reader.clone()
  -> first scan closure tuple element
  -> first closure parameter
  -> every authenticated chronology receiver
```

The mismatched-argument fixture passes the correct reader as the first tuple
element but calls `other_reader`; the verifier rejects it. The path fixture
contains one allowed and one unauthorized path; the verifier rejects it.
Both fixtures execute on every verifier invocation and are not production
code. The e1af source worktree remains expected `SOURCE_BINDING=RED`; the
new checks do not convert calibration RED into acceptance.
