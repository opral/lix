# Independent review: TrackedHead GC/current-generation oracle

Verdict: **BLOCKER**. This is a read-only TEST/REPORT-ONLY review of
Hetzner-II's immutable oracle. No production source, adapter, benchmark, or
runtime matrix was changed or built.

## Immutable subject

- oracle ref: `origin/codex/tracked-head-gc-oracle-413e`
- head/tree: `35a62f8a66c46616d2c4d29edeb1de3ee4c5cadc` /
  `d729c9592ce53183c62f13119ec55e83e1d52da8`
- parent/source anchor: `413e08a75ad3bbcbd749bfa7ec97a82b9f1f098d` /
  `820fe560da3bbd2b00b788b0b1759c409048cd6e`
- parent→head full-index binary diff:
  `4fd8d8d4d1da53d46b8d4ff8ac73cf8abb496e53965bd93250a86338808be161`
- parent→head ordinary diff:
  `7172170f2e7941600b7d9d1b52be39a786700c469b180a420b7c7b0dc60817a1`
- stable patch ID: `a116613096584d7e82ce987db44132fcd6cb3f01`
- prerequisite whole-module gate: `0b9ab47f7ac7da600b215d0b8aa2ab04db6bd36d`

Subject file hashes:

```text
ACCEPTANCE_ORACLE.md       22871ad0b28890c270c0280156728b830ad3d15797a92b79698d9de73604fe80
FUTURE_GATE_COMMANDS.md    32bdd2ee9e7acf1aec44b0320564a68449c05e6c2b0bf99dd2a74f126a98d154
MANIFEST.md                 1f32ced343ad6befb5bdb45ee06d35faa584b6cde09b294eeba74b83f1a2c97d
gc_migration_model.rs       65d7351abf74b01a924dc9cb4d0aa66a3fd49a1ed75736642eb731efe2c33902
verify_gc_migration_source.sh a36af3aaf98cb77a599576db2130a68711589f396584250e75ef7d12308c0bff
```

## Independent gates run

The model compiled warnings-denied as a standalone test-only binary and ran:

```text
11 passed; 0 failed
```

- model binary SHA-256: `f492a901f3fc90a35b576309b1da1b20f2ed15f451d280902fa4429dc9e07ad2`
- model log SHA-256: `19375e06ba9739e5fbd8ca52ae32dc8a061b6b5839d86f4967dd2b74a47f2f52`

The exact source verifier was run against a disposable worktree at exact
`413e08a` with the exact whole-module prerequisite object:

```text
source_status=1
```

- source log SHA-256: `98c135d4feb0aa69f3a8c4310c6c814c4fd1f1fa2e14c69dba9a4bfe57fe328e`

The source gate reports remaining legacy closure in root observation:
`TrackedHeadContext`, `tracked_serving_commit_dependencies`, and
`tracked_reachability(`; native-part validation still references
`CURRENT_STATE_DATA_PART_SPACE`; recovery/current-generation still references
`TrackedHeadContext`, `untracked_json_refs`, and stale-generation/index
staging. This is a direct source blocker before any runtime qualification.

## Blocking correctness gaps

1. The model does not model selectors, catalogs, or authenticated object edges.
   `GcView` has `branch_root` and `global_root` fields, but `fence` ignores
   both. The `owner: u64` scalar is not a `GlobalSelectorV1`/
   `BranchSelectorV1` identity, and no CommitCatalog/ChangeCatalog or
   recovery/retention root is authenticated. Therefore sole generation
   ownership and branch/global-root isolation are unproven.

2. Same-view fencing is only a synthetic equality of three integers. There is
   no retained `CoherentView`, raw selector/view ID, catalog-root binding, or
   proof that selector, progress, owner, and epoch were read from one storage
   snapshot. `GcPlan` is caller-supplied counters; `validate_plan` validates a
   fabricated struct rather than a persisted publication plan.

3. The 65-entry test proves only `Vec<u64>` slicing into 64 plus one suffix.
   It does not prove persisted progress, sequence identity, duplicate/gap
   rejection, blocked one-debt/no-spin behavior, crash between drain phases,
   or cold-reopen resume.

4. There are no model tests for branch/global root retention, checkpoint/
   recovery/undo/redo roots, shared/final references, malformed or missing
   selector/catalog/progress objects, wrong-kind roots, duplicate/back-edge
   corruption, or cold reopen. `Decision::FailClosed` is a zeroed synthetic
   result, not a decoder/traversal failure.

5. The source verifier checks only selected GC regions and old path strings.
   It does not require selector/catalog/root authentication, same-read
   acquisition, progress persistence, 65-entry persisted state, or zero
   `BranchHeadControl`/`TrackedHead` fallback across the whole production
   source. Its whole-module prerequisite check accepts the existence of a
   commit object rather than independently verifying that exact gate's source
   result/tree/ref.

## Smallest correction required

Keep the oracle test/report-only, but replace the synthetic scalar model with a
stateful model whose view contains authenticated global/branch selector raw
bytes, repository/catalog roots, branch/global root IDs, and persisted
progress. Add fail-closed decode fixtures for missing, malformed, wrong-kind,
identity-mismatched, duplicate, and cyclic edges. Make `fence` compare the
complete same-view selector/progress/root identity, and model crash/reopen
after each 64-entry and suffix publication phase, blocked one-debt resume,
shared/final-reference release, and branch/global publication-first/GC-first
races. Extend the source verifier to require those owner APIs and to verify
the exact prerequisite gate/ref/tree rather than only its object existence.

Until those corrections and the whole-module source gate are green, this
oracle cannot approve the TrackedHead current-generation/GC migration.
