# CommitRecord fail-closed red control

Status: immutable TEST/REPORT-ONLY blocker control. It is rooted at exact
1f742 and contains no production change.

## Provenance

- Base/head before package commit: `1f742a382c755399b8a49ab536c4f6dc55fffdd8`
- Base tree: `860a047b98eaa38368a3d889497628e244c2e0ec`
- Production paths changed by this package: none.
- Package paths: `test-reports/stage2-commit-record-fail-closed/{verify_commit_record_fail_closed.sh,SOURCE_CHECKLIST.md,RED_CONTROL_REPORT.md,MANIFEST.md}`.

## Exact source blocker

Immutable `packages/lix/src/sql2/providers/change.rs` contains:

```text
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
205 changes.sort_by_key(LixChangeRow::change_id);
206 if let Some(limit) = limit {
207     changes.truncate(limit);
208 }
```

`load_commit_records` returns `Vec<Option<CommitRecord>>`. `flatten()` drops
an absent record after authenticated graph/catalog enumeration, allowing a
partial sorted changelog result. The later `limit` can hide the omission.
The exact lookup path already errors for a missing indexed record; the all
route must have the same fail-closed property.

## Exact red-control replay

Run from a clean checkout of the immutable base:

```text
bash test-reports/stage2-commit-record-fail-closed/verify_commit_record_fail_closed.sh /root/repos/lix-stage2-milestone5c-review
```

Observed output:

```text
BLOCKER: CommitRecord scan flattens missing records in scan_changelog_changes
    }
    let mut graph_reader = crate::commit_graph::CommitGraphContext::new().reader(store);
    let commits = graph_reader.all_nodes().await?;
    let commit_ids = commits
        .iter()
        .map(|commit| commit.commit_id)
        .collect::<Vec<_>>();
    for record in graph_reader
        .load_commit_records(&commit_ids)
        .await?
        .into_iter()
        .flatten()
    {
        changes.push(LixChangeRow::DerivedCommit(
            crate::commit_graph::canonical_commit_change(&record),
        ));
    }
    changes.sort_by_key(LixChangeRow::change_id);
    if let Some(limit) = limit {
        changes.truncate(limit);
    }
    Ok(changes)
}
STATUS=2
```

## Expected corrected behavior

The narrow successor must require one valid `CommitRecord` for every
enumerated commit, validate requested-ID identity and ordering, and return a
typed error before appending any output or applying `limit`. Missing,
substituted, reordered, duplicate, malformed, wrong-domain, or cyclic records
must fail closed. No changelog fallback, reverse index, cache, compatibility
reader, retry, writer, selector, epoch, or second semantic authority is
permitted. The parked current-state reader slice must remain absent.

## Bounded review evidence

The exact base compiler controls remain known red at this frontier:

- `cargo check -p lix --lib`: status 101, 190 errors/7 warnings,
  log SHA-256 `f289abd16fb3863b972e00a3279c1f418c8bcf904e8e3ba723e55fe283d6b359`.
- `cargo test -p lix --lib coherent_state_point_and_range_preserve_overlay_semantics --no-run`:
  status 101, 463 errors/8 warnings,
  log SHA-256 `90a72b58ba68b958033cbfb66fa8f4a7e829ed4191eed7dcf1c30d9ff11db465`.

These are build-boundary controls, not runtime acceptance. A corrected
immutable head must pass `git diff --check`, the negative source control,
focused compile/test, and the missing/identity/order/limit oracle before any
broader adapter suite.
