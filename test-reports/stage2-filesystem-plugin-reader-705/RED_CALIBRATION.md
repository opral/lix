# Cut B path-policy RED calibration on exact 705

Status: frozen TEST/REPORT-ONLY calibration. No production file was changed
in the candidate worktree, and no compiler, adapter, or runtime was invoked.

## Anchors

- Base frontier: `705440f55eccba9e2d55c0951d6a684737005d76`
- Base tree: `2b8dcb45a2d06bdda86d0fa5add5ea8c12d18c2d`
- Verifier: `test-reports/stage2-filesystem-plugin-reader-705/verify_source_contract.sh`
- Combined stdout/stderr capture: `/tmp/cut-b-705-red.log`
- Capture SHA-256: `34c097bcb54c01cf7d9cad26c974cebd1480c71e8db7efa0e6537832ce0e66d4`

The verifier resolves its repository root from its own package path unless
`CUT_B_REPO_ROOT` is supplied. Path-policy comparisons use normalized
repo-relative `git diff --name-only` output. All calibration commands captured
stdout and stderr together.

## Exact 705 RED

Command:

```text
CUT_B_BASE=705440f55eccba9e2d55c0951d6a684737005d76 CUT_B_HEAD=705440f55eccba9e2d55c0951d6a684737005d76 bash test-reports/stage2-filesystem-plugin-reader-705/verify_source_contract.sh > /tmp/cut-b-705-red.log 2>&1
```

Result: exit `1`, capture SHA-256
`34c097bcb54c01cf7d9cad26c974cebd1480c71e8db7efa0e6537832ce0e66d4`.
The path policy has no changed paths at base=head; the source gate then
correctly reports missing `CoherentView` and legacy
`TrackedHeadContext`/`TrackedState*`/`BranchHeadControl` acquisition in
the two primary readers. This is the expected pre-Cut-B RED control.

## Authorized reader-path control

Disposable unpushed overlay commit:
`74215bf0f266c0ec7242d2dfa1d1926edd03c0eb`, changing only
`packages/lix/src/filesystem/mod.rs`. The same verifier command shape used:

```text
CUT_B_REPO_ROOT=/root/repos/lix-cut-b-policy-allowed-705 CUT_B_BASE=705440f55eccba9e2d55c0951d6a684737005d76 CUT_B_HEAD=74215bf0f266c0ec7242d2dfa1d1926edd03c0eb bash /root/repos/lix-cut-b-reader-acceptance-705/test-reports/stage2-filesystem-plugin-reader-705/verify_source_contract.sh
```

Result: exit `1`, with no `FORBIDDEN Cut B widening` line; the failure is
only the inherited exact-705 source RED. Combined output SHA-256 is the same
`34c097bcb54c01cf7d9cad26c974cebd1480c71e8db7efa0e6537832ce0e66d4`.
This proves the authorized filesystem reader plumbing path is admitted by the
path policy without weakening the semantic source gate.

## Forbidden GC-widening control

Disposable unpushed overlay commit:
`422f291c824ab92387c3ba20364ac7982f9260aa`, changing only
`packages/lix/src/gc.rs`. The verifier was run with the same base and:

```text
CUT_B_REPO_ROOT=/root/repos/lix-cut-b-policy-gc-705 CUT_B_BASE=705440f55eccba9e2d55c0951d6a684737005d76 CUT_B_HEAD=422f291c824ab92387c3ba20364ac7982f9260aa bash /root/repos/lix-cut-b-reader-acceptance-705/test-reports/stage2-filesystem-plugin-reader-705/verify_source_contract.sh > /tmp/cut-b-gc-705.log 2>&1
```

Result: exit `1`; the first policy diagnostic is
`FORBIDDEN Cut B widening [GC orchestration]:
packages/lix/src/gc.rs`. Combined output SHA-256:
`17327724fb84b4803471c23ad91e5c35b138033976db77f1e62f75cc4286851f`.
The subsequent inherited source RED is retained, so this is not a
short-circuit or permissive policy.

The same allowlist categorizes scalar/entity/W2, transaction/publication/
writer, CAS/storage, selector/ForkTree, and compatibility/fallback paths as
forbidden. No such overlay is part of the frozen ref.
