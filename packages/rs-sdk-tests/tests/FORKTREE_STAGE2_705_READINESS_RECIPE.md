# Stage2 readiness recipe anchored to 705440

Status: read-only metadata, **BLOCKED**, not an approval or runtime recipe.

The exact reader correction frontier is:

```text
head:   705440f55eccba9e2d55c0951d6a684737005d76
tree:   2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d
parent: 9f3c703e953440cde1d60b1511467c4337648c8f
full-index parent..head: c68b9338562aee6c9d08de60c447a5e8bd4520696aa275ad9be7599e10b6f6df
ordinary parent..head:    ef540ea92eb6787af4f8b9dc266d60d7fb32055c834654424f1e3d2dd612233e
stable patch ID:          7504d3c10c1f38cc6bb6c124de5af5a4d9ea4e4e
```

The object is verified locally and is not promoted. Terminal R2, R4, H1, and
H4 approval is required. The 9f3 predecessor remains blocked for empty-success
derived/history scans and legacy `TrackedHead`/control acquisition in
`load_exact_batch`; d6b remains the last approved base. The 705 transport ref
and report are not supplied and remain `UNBOUND`.

## Inventory

`FORKTREE_STAGE2_705_READINESS_INVENTORY.tsv` is the authoritative
metadata-only inventory for the current minimum landing package: 705 reader,
scalar SQL, R1 checkpoint/GC, W2 tracked Blob/CAS, Cut B reader, W3
version-control, W4, parsed-file+64MiB, and W5/R7. It records exact object
identity and source/report status without claiming runtime qualification.

Every row is still a separate base-bound artifact. No a12, d6b, e92, or 9f3
test/build verifier may be applied to 705 until its source paths and SPI are
rebased/rebound and the four required approvals are present.

## Stale verifier/path audit

The existing `forktree_stage2_seven_stage_overlay.sh` is not a 705 verifier:
it hard-codes the 1f742 anchor, its parent/tree/diff, R1 binding, and the d6b
R5 hold. Its existing artifact rows also retain a12, d6b, e92, and 9bace
assumptions. It may be used only to audit the historical readiness package;
using it to qualify 705 would be a provenance error.

The W2, Cut B, W4, parsed-file+64MiB, and W5/R7 verifiers are likewise
test/report-only and base-specific. W2, Cut B, and W4 have RED source gates or
no-run/runtime holds; W5/R7 has an immutable package but its Cargo no-run is
blocked by inherited d6b symbols. Scalar SQL and W3 are a12-based and require
rebind. The reader prerequisite named by Cut B/file+64MiB is
`8b0cf91387ffc86851b99029bdd8942938ba2be6` / tree
`5bfc6d63011789c85b70fd0675ffb8a2216210c0`, which is not 705-bound.

## Execution fence

Do not build, apply, benchmark, or run any row from this package. The only
permitted work before the four approvals is immutable object verification and
metadata/hash checking. After approval, a new candidate-specific verifier must
be produced from the 705 ancestry, then the minimum order remains static
residue/source gate, scalar SQL/transaction, W3 branch/history, parsed-file+
BlobRef, checkpoint/GC, W2/Cut B/W4/W5 corruption/recovery controls, and only
then any adapter runtime. A missing ref, report, SPI, or source rebind is a
blocker, not a reason to reuse a stale verifier.
