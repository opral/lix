# Git Compare Benchmark

This benchmark answers a narrower question than `exp git-replay`:

- a repo already exists
- a user changes files
- the user finalizes one commit
- how long do `write` and `commit` take for Git vs Lix?

It cuts replay noise by:

- selecting real first-parent commits from a production repo as workloads
- building Git and Lix parent-state templates outside the timed section
- timing only `apply workload` and `finalize commit`
- interleaving Git and Lix runs
- verifying the final Git tree and final Lix `lix_file` state after each trial

## What It Measures

For each selected workload commit:

- `write_ms`
  - Git: apply the commit's file mutations into a clean checkout
  - Lix: apply equivalent `lix_file` mutations inside an open transaction
- `commit_ms`
  - Git: `git add -A` + `git commit`
  - Lix: `COMMIT`
- `total_ms`
  - end-to-end write + commit

## Usage

```bash
cargo run --release -p git_compare_benchmark -- \
  --repo-path /Users/samuel/git-repos/paraglide-js \
  --output-dir artifact/benchmarks/git-compare/paraglide-js \
  --max-workloads 5 \
  --runs 5 \
  --warmups 1 \
  --force
```

With the benchmark-tuned SQLite settings:

```bash
cargo run --release -p git_compare_benchmark -- \
  --repo-path /Users/samuel/git-repos/paraglide-js \
  --output-dir artifact/benchmarks/git-compare/paraglide-js-tuned \
  --sqlite-benchmark-tuned \
  --max-workloads 5 \
  --runs 5 \
  --warmups 1 \
  --force
```

Reports are written to:

- `report.json`
- `report.md`

inside the chosen output directory.

## Notes

- The current seed mode is hybrid on purpose:
  - Git uses a local parent checkout so the baseline tree is exact.
  - Lix seeds a fresh DB from the parent tree snapshot outside the timer.
- Lix path seeding percent-encodes Git path characters that `lix_file` does not currently accept raw, so the benchmark still exercises the same file set even when the repo contains paths like `+layout.svelte` or `[locale]`.
- Workloads are filtered to regular-file content changes. Mode-only or symlink-heavy commits are skipped because `lix_file` currently benchmarks `path + data`, not full Git file mode semantics.
