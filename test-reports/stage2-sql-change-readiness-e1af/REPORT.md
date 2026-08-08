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
  /root/repos/lix-e1af-sql-change-binding
```

Observed result: exit `1`, expected `SOURCE_BINDING=RED`.

```text
RED: change provider does not bind both routes to query_source.forktree_reader
RED: change provider still consumes query_source.store
RED: diff provider still consumes query_source.store
RED: diff provider constructs a second facade from store
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
