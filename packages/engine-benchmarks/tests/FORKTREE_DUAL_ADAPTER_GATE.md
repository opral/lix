# ForkTree minimum dual-adapter acceptance gate

This is test-only runner glue for the production successor composed from:

- coverage base `a73072bbef9b061bdbdd01add495f179d55972d7`, tree
  `c9d045d164fb1cac0d32b3fd0e2467676466c48f`;
- approved adapter correction `cc9a13280de293be7ea546e9c0d2dfcd167bc529`;
- composed test branch commit for the correction is recorded by Git, while the
  only new files in this gate commit are this runner and this map.

The runner reuses existing public Lix integration tests and existing adapter
qualification tests. It does not add a storage implementation, a publication
path, a fallback, a compatibility decoder, or a second authority. Every cell
has a hard 1200-second timeout and the shell exits on the first failure. The
default is compile-only so this package can be prepared before a production
successor exists; runtime is opt-in with `FORKTREE_GATE_RUN=1`.

## Exact invocation

Use a target directory that is not shared with another SHA:

```bash
cd /root/projects/lix-forktree-dual-adapter-gate
chmod +x packages/engine-benchmarks/tests/forktree_dual_adapter_gate.sh
FORKTREE_GATE_RUN=1 \
  CARGO_TARGET_DIR=/root/repos/target-forktree-dual-adapter-gate \
  CARGO_BUILD_JOBS=2 \
  packages/engine-benchmarks/tests/forktree_dual_adapter_gate.sh "$PWD"
```

The exact order is source diff check, Memory library/integration compilation,
Memory runtime, RocksDB runtime, then SlateDB runtime. A compile or test
failure stops the sequence; no later adapter result is treated as qualified.
The compile-only preparation command is the same invocation without
`FORKTREE_GATE_RUN=1`; it compiles the focused Memory target, checkpoint and
replacement/reopen adapter targets, then stops before runtime or the first
legacy test-support blocker.

## Coverage map

| Contract | Existing test selection |
| --- | --- |
| repository initialization and OLTP insert/update/delete/point/range/transaction | Memory `transaction::` and `sql::lix_key_value::`; durable backend restart cells |
| parsed-file mutation and file projection/BlobRef identity | Memory `sql::lix_file::lix_file_insert_on_conflict_path_updates_existing_content_and_preserves_id`; Rocks/Slate media restart |
| branch, diff, merge, history | Memory `branching::merge_branch_`, `sql::diff_commands::`, `sql::lix_commit::`, `sql::lix_file_history::` |
| checkpoint and GC | Memory `checkpoint_gc::checkpoint_gc_`; Rocks/Slate `checkpoint_gc_replay_reopen` and `cas_gc_history_retention` |
| corruption and fail-closed recovery | Memory `corruption_fuzz::`; Rocks/Slate `corruption_recovery_qualification` |
| cold reopen and final-reference reclamation | Rocks/Slate replacement/delete/reopen and CAS history-retention cells |
| large BlobRef identity and bounded range read | Rocks/Slate `large_media_foreground_lifecycle` (existing ignored focused fixture) |

The runner names each backend cell separately, so a future production
successor cannot pass on Memory or RocksDB evidence while SlateDB is skipped.
The large-media cell is an acceptance correctness fixture, not a benchmark
matrix; no 512 MiB or performance sweep is part of this package.

## Pinned preparation frontier

On the composed base (`a73072bb` plus approved adapter correction `cc9a1328`),
the Memory library, all-simulations integration test, checkpoint adapter test,
and replacement/delete/reopen adapter test compile successfully in the
dedicated target. The next cell is intentionally a red control:

```text
cargo test -p lix_benchmarks --features storage-benches,slatedb \
  --test cas_gc_history_retention --no-run
```

It fails before test execution because that old test imports
`collect_repository_gc_for_bench`, `read_binary_cas_for_bench`, and
`write_binary_cas_for_bench`. The pinned production source deliberately places
those raw-space/CAS benchmark helpers in the uncompiled
`obsolete_benchmark_support` module. The same removed helper family is also
imported by `corruption_recovery_qualification.rs`. This is test-support
fallout, not a production authority defect; no helper was restored and no
production file was edited. The shell stops at this first blocker, so no
runtime result is claimed. A future production successor can replace those
two stale test consumers with public/ForkTree test glue before rerunning the
exact sequence; it must not re-enable the deleted raw-space helper module.

Frozen preparation result from the dedicated target
`/root/repos/target-forktree-dual-adapter-gate`:

- runner exit: `101`;
- compile log: `/root/repos/lix-evidence/forktree-dual-adapter-gate/COMPILE_FRONTIER.log`, SHA-256 `3372dd312ec8f566a3014e23690e2958bc75e42911fe36cda230bc659a42aa3e`;
- exit record SHA-256 `39b8dc3fc8b44765c8e6f1adee04c5b465e555ab791cc42d0d9e810d5b64297c`;
- compiled `lix` test binary SHA-256 `7a002be3623bf9fe89041525912eed91422ca036d53dfe6034b4111f0e0727c5`;
- compiled all-simulations integration binary SHA-256 `f95711efb3db4a3867470fd56c6c81e1b441dbc3f3c220703a8d0e7a9b856e87`;
- compiled checkpoint adapter binary SHA-256 `79e4f0ab8c2f9d8ede3723eff403ea2b99a2608a6f3bceb623f26f46543cd378`;
- compiled replacement/delete adapter binary SHA-256 `347fa8ad1ea53d79bb2fbdaa831898dbc9b564ad1424e900dc6af02c8a9df37b`.

The binaries are evidence of this exact target and toolchain run, not a
runtime approval. The next permitted action is a fresh-target rerun of the
same command after stale test consumers are replaced by test-only public API
glue or after the production successor explicitly supplies the required
public/ForkTree test seam.

## Current bounded preparation status

The branch is intentionally expected to stop at the first current initializer,
checkpoint, or adapter compiler blocker. The resulting command output, exit
status, and SHA-256 log are evidence of the frontier only; they are not a
runtime verdict.
After a runnable production successor is composed, rerun the exact invocation
above in a fresh target directory and preserve the per-cell logs and binary
hashes.
