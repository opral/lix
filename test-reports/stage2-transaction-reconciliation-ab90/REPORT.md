# Frozen transaction-reconciliation oracle report

Status: **TEST/REPORT-ONLY — RED prerequisite hold**

## Immutable anchor

| Item | Value |
|---|---|
| ref at calibration | detached `HEAD` |
| production anchor | `ab90fc51e148611f5fdacde173dd6789ab22ab88` |
| tree | `5bcf259918f86e5b439c1bc50a3e198f87826adc` |
| parent | `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` |
| worktree | `/root/repos/lix-stage2-transaction-reconciliation-oracle-ab90` |
| package directory | `test-reports/stage2-transaction-reconciliation-ab90` |
| production changes | none |
| builds/benchmarks | none |

The package is anchored to ab90, whose direct SQL reader successor is not
accepted for the duplicate invariant.  The historical prerequisite is bound
to the prior test/report ref
`origin/codex/forktree-stage2-sql-entity-semantic-oracle-413` at
`6c7e3c4d67256b5e7e91b763081c7831e1f22cc7`, parent
`413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`; a future green run must supply an
independently accepted immutable prerequisite SHA rather than treating ab90’s
local validators as proof.

## Static calibration

Command, from the package directory:

```sh
bash verify_transaction_reconciliation.sh \
  /root/repos/lix-stage2-transaction-reconciliation-oracle-ab90 candidate \
  > calibration.stdout 2> calibration.stderr
```

Source is loaded only with `git show HEAD:path`; no mutable author worktree is
read.  The output below is the complete stdout captured for the calibration
(stderr was empty).  Paths in this report are absolute where a filesystem path
is needed; source identity is commit/tree based.

```text
PASS	provenance	HEAD=ab90fc51e148611f5fdacde173dd6789ab22ab88 TREE=5bcf259918f86e5b439c1bc50a3e198f87826adc descends from ab90fc51e148611f5fdacde173dd6789ab22ab88
RED	historical_fail_closed_prerequisite	no independently accepted duplicate/order/member oracle supplied; ab90 remains held
PASS	stale_owner_classifier	same-key overlap, disjoint-owner composition, and unsafe mixed conflicts have one classifier
PASS	undo_redo_owner	durable undo/redo marker owns target and redo chronology
PASS	savepoint_rollback_owner	statement checkpoints restore staged and function state before commit
PASS	idempotency_boundary	receipt lookup/staging is coupled to the existing transaction commit boundary
PASS	checkpoint_undo_root_owner	checkpoint and undo/redo roots are represented by existing ForkTree roles/plans
PASS	one_transaction_commit_boundary	existing transaction lowerer has one plan/prepare/backend-commit sequence
PASS	no_direct_forktree_commit	PreparedPublication exposes storage-plan lowering, not an independent backend commit
RED	one_retained_view	ab90 still has inherited tracked-state/cache seams; transaction migration must thread one operation-owned view
PASS	unsupported_fail_before_plan_marker	unsupported publication families have typed rejection markers
RED	old_named_reader_absent	TrackedStateStoreReader remains in inherited transaction reader paths; the future migration must delete it rather than add a facade around it
RED	inherited_reader_cache_frontier	inherited tracked-state/control/cache reader remains; do not treat absence of one old name as deletion proof
PASS	historical_member_fail_closed_shape	historical member path has authenticated validation/selection hooks
RED	historical_tombstone_prerequisite	ab90 rejects tombstone-inclusive terminal reads before acquisition; prerequisite correction is not accepted
NOT_RUN	runtime_matrix	TEST/REPORT-ONLY package: no Memory/RocksDB/SlateDB build or runtime was run
SUMMARY	mode=candidate	pass=10	red=5	fail=0	not_run=1	head=ab90fc51e148611f5fdacde173dd6789ab22ab88	tree=5bcf259918f86e5b439c1bc50a3e198f87826adc
```

This is a terminal calibrated RED, not a production approval.  The verifier
exits successfully only because all five REDs are expected and there are zero
unexpected `FAIL` records.  A future candidate may not waive them by rejecting
supported semantics, adding a fallback, or making a detached cache appear to
be a view.

## Review classification

- **Historical fail-closed prerequisite:** blocked.  Duplicate, reorder,
  missing-member, wrong-owner, and malformed authority must be accepted by an
  independent immutable oracle first.
- **Transaction ownership:** existing stale classification, undo/redo marker,
  statement checkpoint, idempotency receipt, checkpoint-root, and one
  transaction commit boundary are reusable owners.
- **Required future correction:** thread one operation-owned retained view
  through opening snapshot, stale reconciliation, undo/redo transition, and
  publication planning; delete the inherited `TrackedStateStoreReader` path
  and cache rather than wrapping or duplicating it.
- **Unsupported cohorts:** file/upload/checkpoint-only, ref-only, selected
  history, multi-branch, reachability, and GC-only requests reject before
  plan/write unless a later explicitly authorized slice adds their lowering.
- **No-op:** a genuinely unadvanced deterministic transaction returns an empty
  write set with no selector, epoch, or receipt change.
- **No claim:** no adapter runtime, reopen, corruption, or resource result is
  supplied by this package.

## a9dd successor source-only calibration

This successor preserves the 16-case baseline (R01-R15 and P0) and adds D01-D08
for the H2 gaps. It is TEST/REPORT-ONLY: no production file was edited, no
Cargo build was run, and no adapter/runtime command was run.

The verifier now scans all tracked source artifacts in the Git tree and reports
legacy/read/compatibility locations with path scope. Negative compatibility
rules are applied only to the exact lowerer function bodies, so inherited
checkpoint/GC owners do not create unrelated failures. The required future
source proof covers one retained read across alternate opening helpers,
owner_epoch+view_id in both plan and commit, publication-side reconcile_owner,
immutable captured history/tombstones, explicit desired local state,
content-authenticated root identity, and same-prefix transplant rejection.

Calibration command:

```sh
bash test-reports/stage2-transaction-reconciliation-ab90/verify_transaction_reconciliation.sh \
  /root/repos/lix-stage2-transaction-reconciliation-oracle-ab90 candidate \
  > /tmp/a9dd-successor.stdout 2> /tmp/a9dd-successor.stderr
```

At the source checkpoint the verifier produced 11 PASS, 12 expected RED, 0
FAIL, and 1 NOT_RUN. The complete non-normalized stdout SHA-256 was
`f50a0a0cc849f663e63f116b5fc45bc108b10df07d6a33f98a3ea37a879cf101`; stderr
was empty (`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`).
With `NORMALIZE_OUTPUT=1`, which removes moving HEAD/tree values while keeping
all semantic rows, stdout SHA-256 was
`5b8ac5cdc2358fee8ed48a5acae257582810f893f5575b82bf4c7b440362dcc1`; this is
the stable final evidence hash. The immutable successor ref, head, tree,
diff, and patch identities are supplied in the freeze handoff; no moving-ref
value is used as semantic evidence.
