# Correction-I calibration: exact 479 is RED

This package is anchored to production parent
`47957d30ae7c16c89c3c523feea23e2f98461fed` / tree
`b2e0c8a355fcee64d24cd5fcf77d2351d6fe4170`.

The package runner was calibrated against a test/report-only commit whose
parent is that exact production head. It correctly rejects the unchanged
production source for all of these independent reasons:

* no production checkpoint chronology function exists under `forktree/`;
* `packages/lix/src/checkpoint.rs` still imports and uses
  `TrackedStateStoreReader` and contains the local marker chronology walk;
* `packages/lix/src/sql2/providers/working_diff.rs` still creates a
  `TrackedStateContext` reader and calls `latest_checkpoint_for_branch`;
* checkpoint and filesystem working-diff providers still use typed chronology
  deferrals instead of a retained-view ForkTree seam.

The standalone marker/root oracle remains green: exact marker plus implicit root
is selected, the ordinary post-checkpoint commit is excluded, and wrong branch
and duplicate markers fail closed.

## Compiler calibration

The exact 479 diagnostic logs used by the runner are:

| target | result | log SHA-256 |
|---|---:|---|
| `cargo check -p lix --lib` | 138 errors / 9 warnings | `c0db307adba99b1bbd464da9cb9b3d0dd25393b2113dab123ba3d70f61087450` |
| `cargo check -p lix --lib --tests` | 381 errors / 16 warnings | `e2f6f789d3b20681331a9160d2dc0ff5b37bcfa6030b7720b213ce37260420dc` |

The predecessor 39b normalized references are:

* library errors `22ba78779c90b943090136f47b68d5dfe2ac452f4321e2fc523dc1da1c1442f4`;
* library-with-tests errors `17c1da26ee8108e34f6e75304d4fed03a7a249ad5975062f7ddeaa069f4d9775`;
* library warnings `4f8e8a2ea9193abe58660300ee7733587a70bdac86c4bcec1bd125b04ca7327a`;
* library-with-tests warnings `d5d673e2d3d9c7da229188125b8277d0383e88d425b72d9d9bc7bd9a2f3bfb42`.

The 479 candidate removed one stale `scan_certified_history_rows` diagnostic
without adding a warning, but remains RED on the ownership/deletion contract.

## Successor pass definition

A successor may report PASS only when the runner returns `RESULT=PASS` with
compiler logs for the same predecessor/candidate comparison. A production
source change is intentionally required for that future pass; this ref itself
contains no such change.
