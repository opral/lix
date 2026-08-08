# BranchRef v4 independent review

Terminal verdict: **APPROVE**

## Immutable identity

~~~text
ref: origin/codex/branch-ref-selector-correction-oracle-6eba-v4
head: 32200a21f4cb7a77276ff619179b2c05687ffd2a
tree: 5eef6528fcde96417c0303d3c1df78c48e257ffb
parent: 877cd2aacc3401fe50090d3634e2a9868cac26d2
parent..head full-index binary diff: d22f5d9b4529b9372acc768e26dee375e980efb2a322005d83a69766e2cd4de8
stable patch-id: 07763890b5739ab75d7ea0e019433c26e88b2b06
MANIFEST.json SHA-256: d2e5af4ccd6580fb04b057264bd7e64bed11d2101b90afd8c1c3d554a16c179f
SHA256SUMS SHA-256: be1026f1cf27a51fd436b08bb42db247840e8e9c836d7cc8ebaf49aeec35c27c
~~~

The remote ref, parent/tree/diff/patch identities, and clean worktree match
the handoff. The parent delta contains exactly MANIFEST.json and SHA256SUMS;
no production, Cargo, model, scanner, or adapter path changed.

## Required predecessor binding

MANIFEST.json now binds the exact immutable predecessor v3 HANDOFF:

~~~text
8177bfde4c1732281e66eb30c507ed69b11eccaa65f2511147d243ea03e9d0e0
~~~

It also retains the prior v2 HANDOFF and R1 blocker report/manifest hashes.
The manifest explicitly defines v4 transport provenance as commit/ref/tree/diff
identity and correctly does not require a recursive post-commit v4 HANDOFF.

## Replayed calibration and model

Using the documented immutable v2 input at
/tmp/branch-ref-selector-correction-v2-review:

~~~text
scanner exit status: 1 (expected RED)
required-missing=0
legacy-residue=460
old-closure-paths=4
lix-branch-ref-occurrence-files=15
non-derived-lix-branch-ref-files=4
authority-use-lines=331
raw SHA-256: aa50ca96ffe94bc3917f2ba065edce9de2aa1843c442ac72fa37aeaf230b7232
normalized SHA-256: 34d516017699c2a8cdc39d74ed52a037aa5787fca688d5bac7f4955b8fc0698b
~~~

SHA256SUMS, JSON parsing, shell syntax, rustfmt, and diff-check pass. The
model source remains byte-identical to v2:

~~~text
3bb638608e491b1feeebfdc10adb4b083dcd61e0755a11e421392bc786ef2640
~~~

Standalone rustc warnings-denied execution passes all 15/15 model tests,
including canonical global acceptance and same-size forged global key/root
rejection before view/write/commit. Independent model log SHA-256:

~~~text
f3988dddb6163e4da6160c67fee6ae31a7c21541f8c421abefea9955d6c06fc2
~~~

The unchanged model preserves stale/unrelated owner CAS, missing-root/cycle/
epoch/catalog/dual-authority controls, lifecycle/GC/cold-reopen behavior, and
the one-retained-view/one-publication/one-commit assertions.

No production build, adapter runtime, PR, or source mutation was performed.
