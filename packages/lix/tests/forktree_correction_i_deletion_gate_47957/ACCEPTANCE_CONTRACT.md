# Correction I: checkpoint chronology/deletion gate

This is a test/report-only acceptance package anchored to immutable
`47957d30ae7c16c89c3c523feea23e2f98461fed` (`b2e0c8a355fcee64d24cd5fcf77d2351d6fe4170`).
It makes no production change and performs no runtime qualification.

## Required successor contract

Correction I passes only when all of the following are true:

1. Exactly one ForkTree-owned checkpoint chronology seam exists under
   `packages/lix/src/forktree/`, and it consumes one caller-retained,
   authenticated view/read. The seam must classify the exact authenticated
   checkpoint marker commit and the implicit root; an ordinary commit after a
   checkpoint is not a checkpoint.
2. Checkpoint SQL and filesystem working-diff consumers call that seam through
   the retained view. They must not construct a second read, call `begin_read`,
   or reconstruct chronology locally.
3. `packages/lix/src/checkpoint.rs` contains no production checkpoint-history
   chronology owner, `TrackedStateStoreReader`, `checkpoint_history_from_head`,
   `checkpoint_history_for_branch`, or equivalent local marker walk.
4. `packages/lix/src/sql2/providers/working_diff.rs` contains no tracked-state
   checkpoint scan, `TrackedStateContext` reader factory,
   `latest_checkpoint_for_branch`, or equivalent legacy route.
5. The five historical providers and SQL history route contain no
   `CertifiedHistoryStoreReader`, `CertifiedHistoryReader`,
   `TrackedStateScanRequest`, `TrackedStateReadColumns`, tracked-state reader,
   typed chronology deferral, fallback, compatibility route, or parallel
   chronology owner.
6. The marker/root oracle in `correction_i_marker_oracle.rs` passes, including
   exact-marker selection, implicit-root selection, ordinary-commit exclusion,
   wrong-commit rejection, wrong-branch rejection, and duplicate-marker
   rejection.
7. The normalized library and library-with-tests compiler delta against the
   supplied predecessor logs adds no diagnostic or warning and does not
   increase the remaining reverse-dependency counts. A reduction is allowed;
   a new diagnostic, warning, owner, reader, cache, fallback, or authority is
   a blocker.
8. The candidate diff contains only this test/report package. No production
   path is accepted in this acceptance ref.
9. The e26 baseline in `BASELINE_REVERSE_DEPENDENCIES_E26.md` is reproduced
   exactly. Every listed owner/writer/chronology token must have a non-positive
   signed delta; a new token or any increase is a blocker. Direct fallback,
   compatibility, legacy, authority, and writer text is scanned in the
   production route scope, not inferred from chronology seam names.
10. The checkpoint and filesystem working-diff provider structs each carry the
    same `forktree_reader: crate::forktree::ForkTreeReadFacade<S>` field and
    bind it exactly as `query_source.forktree_reader.clone()`. Their production
    scan closures must consume that field; neither may call `begin_read` or
    construct a local ForkTree reader. This is the structural shared-view
    identity proof.

## Exact 479 calibration

The unchanged 479 production bytes fail the gate for the known reasons:

* `checkpoint.rs` still contains the old `TrackedStateStoreReader` chronology
  owner and marker walk;
* `session/checkpoint.rs` and transaction checkpoint helpers still call it;
* `sql2/providers/working_diff.rs` still constructs a tracked-state reader and
  calls `latest_checkpoint_for_branch`;
* no ForkTree checkpoint chronology seam is present;
* checkpoint and filesystem working-diff providers return typed “deferred until
  its sole ForkTree chronology owner is wired” errors.

The calibrated compiler frontier is 138 errors/9 warnings for the library and
381 errors/16 warnings for library tests. The package runner must report RED
when these exact 479 sources are present, while still proving the standalone
marker/root oracle is green and the candidate production diff is empty.

## Allowed scope and commands

The runner is
`forktree_correction_i_deletion_gate_47957.sh`. It accepts a candidate root,
exact head/tree/parent, predecessor library and test-aware logs, and candidate
library and test-aware logs. The compiler logs are attribution-only; the runner
does not build or run the product. It runs only the dependency-free Rust marker
oracle and static source/diff checks.

The intended future order is: source ownership/deletion proof; marker/root
oracle; normalized compiler delta; only after those pass, separate Memory,
RocksDB, and SlateDB semantic/runtime qualification by the owning lane.
