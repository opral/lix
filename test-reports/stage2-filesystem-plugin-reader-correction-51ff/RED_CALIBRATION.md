# Cut B correction oracle RED calibration

Status: frozen TEST/REPORT-ONLY evidence. No production source, PR, merge,
compiler, adapter, or runtime was changed or invoked.

## Exact anchor

- Base/head: `51ff5dbc353cb0322bcedcd191d6e2082e7ed479`
- Tree: `e3b8d765cee51d61744fabb4e54c9143c04257dc`
- Verifier: `test-reports/stage2-filesystem-plugin-reader-correction-51ff/verify_source_contract.sh`
- Capture mode: stdout and stderr combined with `2>&1`
- Path mode: normalized repo-relative `git diff --name-only`

## Exact 51ff source RED

Command:

```text
CUT_B_BASE=51ff5dbc353cb0322bcedcd191d6e2082e7ed479 CUT_B_HEAD=51ff5dbc353cb0322bcedcd191d6e2082e7ed479 bash test-reports/stage2-filesystem-plugin-reader-correction-51ff/verify_source_contract.sh > /tmp/cut-b-correction-51ff-red.log 2>&1
```

Exit: `1`.
Combined capture SHA-256: `d5c743d71990c06ddab65c5580be141e02da9c9679e1201153ba8c90e6ebeb3e`.

The red proves missing direct `CoherentView` ownership in both primary
filesystem/plugin readers; per-branch `scan_forktree_branch`; raw `store: &S`
and `StorageAdapterRead` collectors; raw historical loaders; the tracked-state
`store()` accessor and merge `reader.store()` escape; `unwrap_or_default()`;
and optional-empty mapping of missing plugin state.

## Allowed read-facade path control

Disposable unpushed commit `2e56051937a134c2fd2651bbc0bff2e97a6a8b17` changed
only `packages/lix/src/forktree/serving.rs`, an allowlisted read-facade path.
The verifier exited `1` only for inherited 51ff source residue and emitted no
`FORBIDDEN Cut B correction path` diagnostic. Combined capture SHA-256:
`d5c743d71990c06ddab65c5580be141e02da9c9679e1201153ba8c90e6ebeb3e`.

## Forbidden GC path control

Disposable unpushed commit `20ee1e48eea05d03677fa93b100b4ef65e490c72` changed
only `packages/lix/src/gc.rs`. The verifier exited `1` and first emitted:

```text
FORBIDDEN Cut B correction path [GC/selector/publication algorithm]: packages/lix/src/gc.rs
```

Combined capture SHA-256:
`dd388aad50ec7e36ff64b72fb51d3bc2985ee9e7c2e79c4e44038f51fd5c5356`.
The inherited source RED remains in the same capture; policy does not short
circuit into a permissive result.

## Required successor behavior

The successor must eliminate every red discriminator, retain one operation
owned coherent read/view across all branch and untracked loops, fail closed on
missing selected retained roots, preserve explicit authenticated empty bootstrap
only, and stay within the manifest path allowlist. No runtime qualification is
claimed by this calibration.
