# 1f742 CommitRecord fail-closed red control

Status: frozen SOURCE BLOCKER. This control supersedes any promotion of the 5C/current-state reader package. No R5 mutable worktree was inspected or modified.

## Immutable object reviewed

- Commit: `1f742a382c755399b8a49ab536c4f6dc55fffdd8`
- Tree: `860a047b98eaa38368a3d889497628e244c2e0ec`
- Worktree: `/root/repos/lix-stage2-milestone5c-review`
- File: `packages/lix/src/sql2/providers/change.rs`
- Existing source package remains frozen; reader acceptance is held.

## Causal source finding

In `scan_changelog_changes`:

```text
189 let mut graph_reader = crate::commit_graph::CommitGraphContext::new().reader(store);
190 let commits = graph_reader.all_nodes().await?;
191 let commit_ids = commits
192     .iter()
193     .map(|commit| commit.commit_id)
194     .collect::<Vec<_>>();
195 for record in graph_reader
196     .load_commit_records(&commit_ids)
197     .await?
198     .into_iter()
199     .flatten()
200 {
201     changes.push(LixChangeRow::DerivedCommit(
202         crate::commit_graph::canonical_commit_change(&record),
203     ));
204 }
```

`load_commit_records` returns `Vec<Option<CommitRecord>>`. The `flatten()` at line 199 drops an absent record after authenticated commit-graph enumeration. The provider can therefore sort and return a partial changelog result instead of failing closed. This remains a correctness blocker even though the exact index route below already rejects an absent record at lines 312–323.

The failure is observable before any `limit` is applied (lines 205–207): deleting one reachable `CommitRecord` while retaining its authenticated `CommitCatalog`/graph node must produce an error, but 1f742 silently omits that commit. It also permits an incomplete result when the missing record is outside the first requested rows.

## Frozen negative-control gate

Test-only verifier: `verify_commit_record_fail_closed.sh`

Verifier SHA-256: `d63d202d64a6ea28e797aea5a948f6b7a3b42087bd8a6228cd81653b6788d788`

Command:

```text
bash /root/repos/lix-evidence/stage2-current-state-reader-1f742/verify_commit_record_fail_closed.sh /root/repos/lix-stage2-milestone5c-review
```

Expected immutable-base output and status:

```text
BLOCKER: CommitRecord scan flattens missing records in scan_changelog_changes
... lines 188–210, including load_commit_records(...).into_iter().flatten() ...
STATUS=2
```

The gate is intentionally a red control: a corrected successor must remove this flattening and introduce an explicit, authenticated missing-record error before row assembly/truncation. The verifier is test/report-only and is not production code.

## Required correction contract

For the same retained reader and already-authenticated `commit_ids`:

1. Require one `CommitRecord` for every enumerated `CommitCatalog`/graph node; absence is `LixError`, never `None` omission.
2. Validate returned record identity against the requested `commit_id` at the same boundary. Reordered, substituted, duplicate, malformed, or missing records must fail closed.
3. Perform all record validation before appending any derived row and before applying `limit`; no partial result may escape.
4. Preserve the existing exact-route behavior at lines 312–323, including the missing-record error.
5. Do not add a fallback to changelog/packed rows, reverse index, cache, compatibility decoder, second authority, writer, selector, or retry. CommitCatalog/graph identity plus the authenticated CommitRecord remains the sole semantic source.
6. Keep current-state reader scope held until this correction is present and a successor proves the R1–R19 runtime oracle, including missing/malformed semantic records and cold reopen.

## Focused oracle matrix for the immutable successor

- valid all-route scan: exact ordered/deduplicated rows and digest;
- remove one CommitRecord while retaining its CommitCatalog node: error, zero partial rows;
- substitute a different CommitRecord under the requested identity: identity error;
- reorder the returned record vector: order/identity error;
- malformed CommitRecord payload: decode/authentication error;
- missing exact-route record: existing explicit error remains;
- `limit=1` with a missing later record: still error, proving validation precedes truncation;
- cold reopen/replay on RocksDB and SlateDB: same fail-closed result and no writes/selector/epoch mutation.

The exact-base runtime remains unavailable at the known compiler frontier (library check 190 errors/7 warnings; focused lib-test no-run 463 errors/8 warnings), so this report makes no runtime-pass claim. The next immutable successor must be reviewed source-first and stopped on any new authority or partial-result path.
