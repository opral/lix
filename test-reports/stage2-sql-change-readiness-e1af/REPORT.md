# SQL changelog readiness binding report — e1af

This is a TEST/REPORT-only calibration artifact. It binds the frozen v4
oracle and the b484 implementation map to immutable e1af. It is not an e1af
acceptance report.

## Provenance

```text
e1af head:       e1af471b9ab0f598dafa7c2ddec7867667c81740
e1af tree:       bfa0d271a723da8250ab76ada16fda90926f1099
e1af parent:     b484e20d845aee3f8137bfa3496f9b3cd0e8cd35
parent..head:    9795ee3da81a06657a45a47a50417522a6a6bd7057e21eeb75597096417c9f3c
stable patch-id: 31cc575644bf17e65c59d558a03acffc848c2e20
production paths: sql2/providers/file_history.rs,
                  sql2/providers/filesystem_working_diff.rs
```

The exact e1af parent is b484, and the e1af production delta remains exactly
the two file-history paths above. No source or runtime files were changed by
this package.

The binding retains these independent inputs without modifying them:

```text
v4 head/tree: d36e1fe34c4bf93c01478b876a5f73a8dccd33de /
              7fcaeb5f965d5c8b641e5e82a741c75cc18365b6
v4 diff:      4f73b5a944ea15b9d0ed6c040ef80fa60f7631437c2fd22ce1bf5ae6a97798d0
v4 patch-id:  212a1ff4a6f050905e5d62f94f78ce2e76d5fc24
fd2 RED:      74d2a1d2512ece658aa213e235142935c161a81bd3d859b2c1ffa8ae2006c0a5
map report:   249bbc8c6980862421e6475a38d1fa9c36bbab97db84000de0e5fd3368b159f0
map manifest: 58f88f4bd3758f16950679dcdcd3ec934de5313d6041d94e32687ecc6cbae600
```

The inherited fd2 library frontier remains `136 errors / 9 warnings`; this
package did not run or recompute a compiler frontier.

## Expanded source-gate calibration

The new binding gate was run read-only against the exact e1af worktree:

```text
python3 test-reports/stage2-sql-change-readiness-e1af/verify_source_binding.py \
  /root/repos/lix-e1af-consumer-closure-audit
```

Observed result: exit `1`, expected `SOURCE_BINDING=RED`.

```text
RED: change provider does not bind both routes to query_source.forktree_reader
RED: change provider still consumes query_source.store
RED: diff provider still consumes query_source.store
RED: diff provider constructs a second facade from store
RED: diff identity: diff registration does not bind DiffFunction to query_source.forktree_reader
RED: diff identity: diff registration retains a raw store field
RED: diff identity: DiffFunction::call does not propagate its reader into DiffSpec
RED: diff identity: diff plan body acquires a second reader/facade
RED: diff identity: diff scan tuple reader is not the exact self.forktree_reader identity
RED: diff identity: diff chronology call receiver is not the reader-first closure identity: historical != store
RED: diff identity: diff chronology call receiver is not the reader-first closure identity: historical != store
RED: change retains forbidden legacy token: tracked_state::scan_change_records_from_commit_deltas
RED: change retains forbidden legacy token: tracked_state::load_change_record_by_id
RED: change retains forbidden legacy token: COMMIT_CHANGE_ID_SPACE
RED: change retains forbidden legacy token: ChangelogContext::new().reader
RED: change retains forbidden legacy token: ChangelogReader
RED: change retains forbidden legacy token: ChangeScanRequest
RED: change retains forbidden legacy token: ChangeLoadRequest
RED: change retains forbidden legacy token: CommitGraphContext::new().reader
RED: session changelog constructor lacks forktree_reader
RED: session changelog constructor must have exactly one ForkTreeReadFacade::new
RED: transaction changelog constructor lacks forktree_reader
RED: transaction changelog constructor must have exactly one ForkTreeReadFacade::new
RED: dummy changelog constructor lacks forktree_reader
RED: dummy changelog constructor must have exactly one ForkTreeReadFacade::new
```

`git diff --check` passed for the report-only additions. No Cargo, compiler,
runtime, adapter, or SQL semantic command was run. The RED is expected: e1af
corrects file-history authority but does not yet implement the SQL changelog
reader cut.

## Binding decision boundary

The first production successor may change only the exact paths listed in
`README.md`: the SQL source type, change provider, diff provider, session and
transaction constructors, the test-only DataFusion dummy, and the narrow
ForkTree serving/facade seam. It must preserve e1af's two file-history files.

The successor must pass this expanded gate and the frozen v4 verifier before
any compile or Memory/RocksDB/SlateDB execution. It must remove the tracked
state, certified/legacy changelog, direct reverse-index, raw-store, and
closure-local second-facade routes; preserve one operation-owned
`ForkTreeReadFacade`; and fail closed for malformed, substituted, missing,
wrong-kind, duplicate, and mismatched catalog/object records. No compatibility
reader, cache, fallback, second authority, or new persisted index is allowed.

This report freezes readiness evidence only. It neither approves nor rejects
the unimplemented e1af successor.

## Correction-I successor to the package blocker

R1's independent review blocked the predecessor package for three test-gate
defects: the declared production allowlist was documentation-only, the diff
reader check did not prove call-argument identity, and there were no
discriminating negatives. This immutable successor closes only those package
defects; it does not alter e1af or claim a production result.

The verifier now compares
`e1af471b9ab0f598dafa7c2ddec7867667c81740..HEAD` using
`git diff --name-status --find-renames`. Every path, including both sides of
a rename/copy, must be in the exact `ALLOWED` set. The exact e1af calibration
worktree has an empty e1af..HEAD delta and is therefore scope-clean; its
source checks remain RED as expected.

For `diff.rs`, the verifier is function-scoped and requires this identity
chain:

```text
query_source.forktree_reader
  -> DiffFunction.forktree_reader
  -> self.forktree_reader.clone()
  -> first scan closure tuple element
  -> first closure parameter
  -> every authenticated chronology receiver
```

The mismatched fixture intentionally passes the correct reader as the first
tuple element but calls `other_reader` for both chronology lookups; it is
rejected. The path fixture contains one allowed and one unauthorized path and
is rejected. These are executable checks, not token-presence assertions.

The consumer-closure audit found the same two direct SQL blockers and one
separate history boundary. `providers/change.rs:151-367` still has the
tracked-state/changelog/raw-space/commit-graph scan and exact-lookup routes;
`providers/diff.rs:28-223` still owns a raw store and closure-local facade.
`history_route.rs:339-535` independently consumes a passed CommitGraphReader
for chronology and the HistoryQuerySource ForkTree reader for certified
state, fanned out by `providers/mod.rs:247-469`. Session and transaction
constructors are at `session/context.rs:727-748` and
`transaction/context.rs:8215-8235`; the test-only dummy is at
`sql2/exec/datafusion.rs:3366-3391`. The forthcoming changelog child must
close only the authorized SQL change/diff/source-constructor slice; the
history chronology boundary remains separately visible and may not be hidden
by a second read or fallback.

The direct-facade classification is recorded in README: the SQL diff
constructor is a blocker; retained-read filesystem/live-state/plugin/serving
wrappers and canonical `open_coherent_view_on_read` callers are legitimate
non-SQL owners; test-only SQL constructors are fixtures. No new broad caller
or authority is introduced by this package.

The ten v4 semantic cases, exact fd2 RED calibration, and all identity,
ordering, deduplication, limit-after-merge, malformed/missing/wrong-kind,
substitution, and cold-reopen requirements remain unchanged. This successor
did not build, run adapters, or execute SQL.
