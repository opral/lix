# BranchHeadControl acceptance oracle at e166

Status: frozen test/report-only oracle. It is bound to the exact e166
ForkTree authority and does not modify production code, add a compatibility
reader, or create a second selector/state authority. The current e166 source
gate is intentionally RED because the superseded BranchHeadControl callers
and physical space have not yet been deleted. No Cargo build, adapter runtime,
benchmark, or production matrix was run.

## Immutable binding

- architecture/source base: `e1666edd0b4d814a88d985086ecc5a477b5d32e6`
- base tree: `c680bd7e7f7b70cd784676515839af2dcbbc7917`
- prior hard-cut contract: `c8d5b17f7c313c8302310f90bdae4e2e8e76d48c`
- prior contract tree: `48c4dbbccdd6abef69e49b734c182131357783ec`
- prior contract: `packages/lix/tests/FORKTREE_BRANCH_HEAD_CONTROL_HARD_CUT_E166.md`
- prior contract SHA-256: `ff0e3fc1da9c3dbc29d8a333cd440edeaa83275f88390d790970dc5332e73f83`

The model is deliberately independent of Lix production modules. Its sole
durable authority is a `GlobalSelector` plus per-branch `BranchSelector`
objects, authenticated `BranchSnapshot` objects, and one global epoch/CAS.
It exercises the accepted e166 contract rather than the deleted control
implementation.

## Frozen oracle files

- `packages/lix/tests/forktree_branch_head_control_acceptance.rs`
  - SHA-256: `2abfc31b5d754d2d6a1ad37ba8ed5ad36b05502a267dc423f85b3946e8cdb532`
- `packages/lix/tests/forktree_branch_head_control_source_gate.sh`
  - SHA-256: `826e65b38be79b2f997984f70666f9b30a9c53fda714da549a37a0a456f1de72`

The source verifier rejects the old control type/cache/context, tracked
reachability helper, staging names, untracked lifecycle generation, and
`BRANCH_HEAD_CONTROL_SPACE`; it requires the ForkTree selector/publication/
coherent-view/GC symbols and the absence of `branch/control.rs`. Its match
printing is item-bounded without a `pipefail`/SIGPIPE truncation hazard.

## Model coverage and result

The standalone model covers seven deterministic cases:

1. initialization, session-local switch, true no-op, cold image/reopen;
2. branch create/delete and preservation of an unrelated branch owner;
3. global sequence publication, one epoch rotation, and zero-rotation no-op;
4. same-owner stale branch publication with byte-for-byte no partial mutation;
5. branch-publication-first versus GC-first stale races;
6. malformed, missing, wrong-kind selector and missing-snapshot fail-closed;
7. GC observation, branch final release, sweep, and surviving-main readback.

The model's publication operations exact-check the raw global and branch
selector values. Global-only sequence publication exact-checks the global
selector. Every successful durable operation rotates the single epoch; no-op,
stale, malformed, and wrong-kind operations do not write. GC observes the
typed branch selector universe and exact-CASes the raw global selector before
reclaiming unreachable snapshots.

Standalone commands, run without Cargo:

```text
rustc --edition=2021 -D warnings \
  packages/lix/tests/forktree_branch_head_control_acceptance.rs \
  -o /root/repos/evidence/forktree-branch-head-control-acceptance-e166/model
/root/repos/evidence/forktree-branch-head-control-acceptance-e166/model
```

Result: `MODEL=GREEN cases=7`.

- model binary SHA-256: `332d32a96bf0dea150a7640b03825cfca7258f5fb8ab44c73023b91936f25216`
- model log SHA-256: `cfb41461068872fa542a113a1d63146570f6cd776071688c54564b67d45affc8`

## e166 source-gate result

Exact invocation:

```text
timeout 1200 bash packages/lix/tests/forktree_branch_head_control_source_gate.sh
```

Result: exit `1`, `RESULT=RED`. Required ForkTree symbols and the deleted
control path check pass, but e166 still contains forbidden production
references in live-state, transaction, functions, init, GC, tests/fixtures,
and storage-bench support. This is the intended compiler/deletion frontier,
not a runtime verdict.

- captured log: `/root/repos/evidence/forktree-branch-head-control-acceptance-e166/source-gate-final.log`
- source-gate log SHA-256: `5d00358c9d8826fe8c2e9cc4d10ff27f9074262b28d163a76e324e5aa3097052`

The first accepted runnable candidate must make this gate green after
reader-first/writer-last migration and physical deletion. Public
`BranchHead`/`BranchRefReader` vocabulary is permitted only as a facade that
delegates to ForkTree; it must not contain raw storage, a control codec, a
cache authority, or an alternate publication path.

## Future adapter commands (dormant)

These are exact future commands only. They were not run on e166. Execute in
the stated order against a compile-green immutable candidate and a fresh
isolated target/database path, stopping at the first failure:

```text
# Memory
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  timeout 1200 cargo test -p lix_tests \
  --test forktree_branch_head_control_acceptance \
  forktree_branch_head_control_memory --exact --nocapture --test-threads=1

# RocksDB
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  timeout 1200 cargo test -p lix_tests \
  --test forktree_branch_head_control_acceptance \
  forktree_branch_head_control_rocksdb --exact --nocapture --test-threads=1

# SlateDB
CARGO_TARGET_DIR=<isolated-target> CARGO_BUILD_JOBS=2 \
  timeout 1200 cargo test -p lix_tests \
  --test forktree_branch_head_control_acceptance \
  forktree_branch_head_control_slatedb --exact --nocapture --test-threads=1
```

The future adapter harness must preserve the same seven model obligations:
one coherent read, one global/branch selector pair, exact raw preconditions,
no-op zero writes/rotation, same-owner stale atomicity, unrelated-owner
success without branch-wide copying, branch-first/GC-first fencing,
malformed/missing/wrong-kind fail-closed behavior, cold reopen, and final
reference reclamation. It must use public typed owner/publication APIs only;
it may not inspect or recreate raw selector-space tokens.

## Hard-cut deletion gate

Before any adapter command is accepted, the source gate must be green and
the physical `BRANCH_HEAD_CONTROL_SPACE`/namespace, codec, readers, writers,
cache, reachability helper, lifecycle-generation helper, and old tests/bench
fixtures must be deleted. No compatibility reader, migration, fallback,
dual write, or second branch-state index is acceptable. The first runnable
state is therefore after the complete deletion wave, not an intermediate
compile checkpoint.
