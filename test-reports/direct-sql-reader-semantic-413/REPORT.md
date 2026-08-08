# Semantic discriminator calibration — 413e08a

Status: **RED control; no production verdict**.

This package is test/report-only and is anchored to the immutable production
candidate:

| Item | Value |
|---|---|
| Base/anchor | `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` |
| Anchor tree | `820fe560da3bbd2b00b788b0b1759c409048cd6e` |
| Prior candidate parent | `11442c1e0023e20307a7231d88cd557bc704fd13` |
| Prior parent diff | `e9be5053f44fa9e009aaa665b69d328f6ee0ac718b18e773fb79a2eb6d7af8d4` |
| Prior patch ID | `02310ae525c028488e654d3cb26eb7d1f85974cb` |

## Replay

```sh
bash verify_entity_semantics.sh \
  /root/repos/lix-direct-reader-successor-413 candidate
```

No compiler, runtime, storage write, or production edit is required. The
oracle reads source from the supplied worktree and emits a deterministic
candidate-mode RED when the anchor is missing the semantic contract.

Calibrated output on the exact anchor:

```text
SUMMARY mode=candidate pass=11 red=4 fail=0
HEAD=413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d
TREE=820fe560da3bbd2b00b788b0b1759c409048cd6e
```

The four RED discriminators are:

1. `projected_deleted_marker`: `Vec<Option<Bytes>>` cannot distinguish an
   authenticated SQL NULL from a deletion tombstone.
2. `tombstone_eligibility`: the corrected terminal capability accepts a
   tombstone request without an explicit deletion/null projection contract.
3. `complete_retention_overlay`: ForkTree selects tracked or untracked mode;
   it does not compose the complete tracked+untracked retention overlay on one
   coherent view.
4. `no_old_row_fallback`: the provider still retains a materialized-row
   capability fallback. A successor must implement the supported semantics in
   the canonical ForkTree reader rather than hide them behind that fallback.

The source-positive controls passed for deleted `scan_entity_rows`, one
`scan_batch` per snapshot/PK projection, one retained coherent view,
branch/global ordered overlay, filter-before-LIMIT, canonical tombstone
filtering, decode propagation, and duplicate/malformed authority rejection.

## Required successor behavior

The successor must preserve absent identity, SQL NULL, and deletion tombstone
as distinct projected states; merge tracked and untracked rows on the same
authenticated view with branch/global replacement and tombstone precedence;
apply typed identity ordering and filtering before LIMIT; and reject malformed,
duplicate, substituted, or wrong-domain authority before partial output. It may
not pass unsupported-but-required cases to the old materialized-row reader or
return capability `None` merely to conceal a missing ForkTree semantic.
