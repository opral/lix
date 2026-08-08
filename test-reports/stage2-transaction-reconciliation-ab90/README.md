# Stage2 transaction reconciliation / undo-transition oracle

TEST/REPORT-ONLY acceptance material anchored to immutable ab90:

- production anchor: `ab90fc51e148611f5fdacde173dd6789ab22ab88`
- anchor tree: `5bcf259918f86e5b439c1bc50a3e198f87826adc`
- parent: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d`

This package does not compile, benchmark, modify, or authorize production
code.  It freezes the transaction migration contract for the future
`TrackedStateStoreReader` replacement.  It is intentionally calibrated RED on
ab90: the historical duplicate-invariant/fail-closed prerequisite is not yet
accepted, and ab90 still contains inherited reader/cache/transaction seams.
That RED is a safety hold, not a request to weaken the prerequisite.

This successor preserves the original 16-case baseline (R01-R15 plus the
historical prerequisite P0) and adds D01-D08 discriminators for the H2 gaps:
full-workspace source classification, one retained read across alternate
opening helpers, owner_epoch/view_id plan-and-commit binding, publication-side
reconcile_owner enforcement, immutable captured history/tombstones, explicit
desired local state, content-authenticated root identity, and precise
function-scoped compatibility rules.

Run the static discriminator from this directory:

```sh
bash verify_transaction_reconciliation.sh /root/repos/lix-stage2-transaction-reconciliation-oracle-ab90 candidate
```

The verifier reads committed source with `git show HEAD:path`; it never reads
or edits a moving author worktree.  It accepts only expected `RED` statuses and
fails on an unexpected source failure.  A future candidate must provide the
historical fail-closed prerequisite explicitly:

```sh
HISTORICAL_FAIL_CLOSED_ORACLE=accepted:<immutable-oracle-sha> \
  bash verify_transaction_reconciliation.sh /path/to/frozen-successor candidate
```

No command in this package is a production acceptance claim.  Runtime replay
is deliberately staged Memory → RocksDB → SlateDB in `CONTRACT.md` and is to
be run only on a future compiler-runnable immutable successor.

The full source policy and its false-positive boundaries are frozen in
`SOURCE_POLICY.md`. The verifier scans all tracked source artifacts and only
applies negative compatibility rules inside the explicitly mapped lowerer
function bodies.
