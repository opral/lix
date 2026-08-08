# Frozen P0 + W1a acceptance package

Frozen: 2026-08-08 UTC

## Baseline identity

- head `a1cf8f7fd55ac21ef7e5bfe7f385c49d99140737`
- tree `d8326da2b1d38bd51b8ac7229d00684a6865bce2`
- detached baseline worktree
  `/root/repos/lix-stage2-production-review-a1cf`
- worktree clean at freeze

## Frozen artifacts

- `P0_W1A_ACCEPTANCE.md`
  - SHA-256 `cfd25a6064aa1c5fd3ad06558c43f79c2169ac88f7b80bd9dab05a90f739d249`
- `P0_W1A_CASES.tsv`
  - SHA-256 `77af0924a86cf023a2924075507545b52035739e8c5bfc33accc080e8f4a9b17`
- `verify_p0_w1a_successor.sh`
  - SHA-256 `35dfbedc0373f5292d96d9e0ab2feafbc11b3f35618adcaa2d5c921514304550`

The TSV has exactly eleven fields in every row. The shell gate passes
`bash -n`.

## Calibration against exact a1cf

The source gate was run against the immutable baseline itself. It first passed
the frozen BlobRef/atomic-writer negative checks, then correctly rejected P0:

```text
BLOCKER: PreparedPublication direct commit remains nameable
CALIBRATION_EXIT=1
```

The reported first P0 residue includes direct fixture calls and
`forktree/mod.rs:153`'s `PreparedPublication::commit::<S>` compile anchor. The
contract also independently forbids production `begin_write`/commit in both
reachability writer paths and requires fixture publication through the same
transaction-owned plan/prepare/commit seam.

## W1a authority decision

The acceptance package does not assume that current
`ChangeCatalogOwner::CommitMember` can represent selected history. That owner
is single-valued. Reassigning it to a target commit breaks the source commit;
keeping it on the source makes the target ordinal back-edge invalid.

The frozen gate therefore requires one hard-cut, many-membership-safe
authenticated projection while preserving one immutable Change object and one
ChangeCatalog/object identity. It permits implementation choice, but rejects a
second catalog, dual old/new owner, mutable membership index, scan fallback,
or unauthenticated member reuse. Every source and target commit+ordinal edge,
parent edge, object domain, embedded ID, and catalog back-edge is covered by a
corruption case.

## Scanner binding

- canonical oracle commit
  `1dbbf3d206540d36f5912eab8372a42819778b47`
- source SHA-256
  `f71e91fcbccbb7d6df676a95e9d747725856b77f7e3177ec42f12ca8b28736cc`
- accepted reviewer binary SHA-256
  `2aaf81d937110b5a248621420f0b3cbc7b5a116da8fbec0bb66453dde4e91585`
- a1cf finding count `166`
- a1cf output SHA-256
  `3891a48613e5d6ebd3d0ab2780aed13c6dd0236f1c2ff343320dd73fb2158a0d`

The successor gate reruns baseline and candidate. Candidate residue may only
decrease: no new scanner key and no per-key increase.

## Scope

No production source, candidate/ref/PR, or mutable author worktree was read or
changed. No build, Cargo check, runtime test, benchmark, or broad scanner suite
was run. This package becomes review evidence only after an authorized clean
immutable successor handoff.
