# ForkTree Stage2 multimedia compile/run manifest

Status: **frozen test-only / intentionally compiler-red until the exact root
facade lands**. Do not run any compiler-red or non-runnable Stage2 head. In
particular, do not execute the historical `c238` milestone or current
`cbe48835f` / tree `36ffe0ff867cd31bf52263675de2d16fc54e9b4f`.

## Immutable inputs

- Public-operation source: `stage2_multimedia_acceptance.rs`
- Standalone compile manifest: `Cargo.toml`
- Residue/format oracle: `verify_frozen_contract.sh`
- Comparator: exact main `a12b76c8690130df5f9cb44a51e9cf3a3bcdb6b3`,
  tree `9a705d36392e88d8f5f363b2b23d373deec3321d`
- Comparator report:
  `/root/repos/lix-evidence/multimedia-closeout-a12/REPORT.md`
- Comparator report SHA-256:
  `7bd123c1ea7d39bf8ecb315d21dbcb30c41235367df203b386134c02fe63d0de`
- Accepted bounded-GC model report SHA-256:
  `ad35a8fc07c51d1f93578ab05e27b47aaf54f3ddf0e60ff3156058b5528b2d77`
- Accepted mixed-lifecycle model report SHA-256:
  `84968abad5ff84eccbbd3d952f6033444148b2335286cfcacb7224c323df1414`

The source uses public Engine/Session operations for upload, exact/range read,
branch, diff, merge, checkpoint, history, undo/redo, branch release, close and
reopen. Its only physical-layout observer is the cfg(`storage-benches`) facade
`lix::storage_bench::forktree`. It must never import internal `lix::forktree`,
raw selector/object spaces, `SpaceId`, or a synthetic presence table.

## Required first-runnable API

Compilation is intentionally rejected until the immutable Stage2 head exposes
exactly:

```text
lix::storage_bench::forktree::GcBudget::default()
lix::storage_bench::forktree::GcTerminalStatus::Complete
lix::storage_bench::forktree::ForkTreeInventory
lix::storage_bench::forktree::inventory(&Engine)
lix::storage_bench::forktree::ForkTreeGcRunSummary
lix::storage_bench::forktree::run_gc_to_completion(&Engine, GcBudget)
```

The facade must authenticate typed roots and objects through the sole ForkTree
owner. A raw object scan, selector scan, external owner index, compatibility
reader, or model-produced inventory is a hard rejection.

## Bind the first runnable immutable head

Only after Ryzen-V advertises an immutable runnable head/ref/tree:

```bash
git -C /root/repos/lix-multimedia-closeout-a12 fetch origin <advertised-ref>
git -C /root/repos/lix-multimedia-closeout-a12 cat-file -e <exact-head>^{commit}
git -C /root/repos/lix-multimedia-closeout-a12 worktree add --detach \
  /root/repos/lix-forktree-stage2-runnable <exact-head>
test "$(git -C /root/repos/lix-forktree-stage2-runnable rev-parse HEAD)" = "<exact-head>"
test -z "$(git -C /root/repos/lix-forktree-stage2-runnable status --porcelain=v1)"
git -C /root/repos/lix-forktree-stage2-runnable rev-parse HEAD^{tree}
/root/repos/lix-evidence/forktree-stage2-multimedia-oracle/verify_frozen_contract.sh
```

Record the advertised ref, commit, tree, all parents, and source-package hashes
before compiling. The fixed standalone manifest resolves its three path
dependencies through `/root/repos/lix-forktree-stage2-runnable`; no Stage2
production or test file is copied or edited.

## Compile probe

All target and database data stays on the workspace filesystem, never `/tmp`.

```bash
mkdir -p /root/projects/forktree-stage2-media-target
timeout 20m env \
  CARGO_TARGET_DIR=/root/projects/forktree-stage2-media-target \
  CARGO_BUILD_JOBS=2 \
  cargo test \
    --manifest-path /root/repos/lix-evidence/forktree-stage2-multimedia-oracle/Cargo.toml \
    --test stage2_multimedia_acceptance \
    --no-run
```

The first accepted compile must be warnings-clean. Any missing exact facade
symbol is a Stage2 readiness failure; do not patch the harness or substitute a
raw scanner.

## First execution: one 64 MiB RocksDB gate only

```bash
mkdir -p /root/projects/forktree-stage2-media-db
db=$(mktemp -d -p /root/projects/forktree-stage2-media-db rocks-image-64-1.XXXXXX)
timeout 20m env \
  CARGO_TARGET_DIR=/root/projects/forktree-stage2-media-target \
  CARGO_BUILD_JOBS=2 \
  LIX_STAGE2_MEDIA_BACKEND=rocksdb \
  LIX_STAGE2_MEDIA_PROFILE=image-64-1 \
  LIX_STAGE2_MEDIA_DB="$db/database" \
  cargo test \
    --manifest-path /root/repos/lix-evidence/forktree-stage2-multimedia-oracle/Cargo.toml \
    --test stage2_multimedia_acceptance \
    stage2_multimedia_lifecycle \
    -- --ignored --exact --nocapture
```

Stop immediately on any correctness, inventory, corruption/GC, cold-reopen, or
20-minute-cap failure and route the exact immutable-head attribution to
Ryzen-V/coordinator. Do not widen on failure.

## Widening order after 64 MiB Rocks GREEN

Run the identical command with only the listed environment values changed:

1. `slatedb` + `image-64-1`
2. `rocksdb` + `audio-64-1`
3. `slatedb` + `audio-64-1`
4. `rocksdb` + `archive-512-10`
5. `slatedb` + `archive-512-10`
6. `rocksdb` + `video-512-10`
7. `slatedb` + `video-512-10`

Each cell gets a fresh explicit database directory and a 20-minute cap. The
512 MiB cells are forbidden until both 64 MiB adapters are correctness-green.

