# Frozen target identity and evidence

This is the exact external reference run for the package. It is not a Lix
benchmark and contains no ForkTree result.

## Comparator binding

| item | value |
| --- | --- |
| approved SPI source | `b59e1f11a51153e0a787a81f0f25bf104d150aaf` |
| approved SPI tree | `700fd04d21bc40c05425c9fc9e10d65c9e1eda24` |
| SPI operation | `lix exp git-replay --plugins all` |
| required input | hydrated Git checkout with exact commit and required Git LFS objects |
| adapter controls | `--storage rocksdb` and `--storage slatedb` are future comparator controls |
| status | no Lix/ForkTree invocation in this package; b59 was compiler-red and is reference-only |

The future comparator invocation shape is:

```sh
cargo run --release -p lix_cli -- exp git-replay \
  --repo-path /bench/vscode-docs \
  --output-path /bench/output/vscode-docs-replay \
  --storage rocksdb \
  --plugins all \
  --branch main \
  --from-commit a68095505a536ca8cd80c378f40d901fcde5080b \
  --num-commits 1 \
  --force \
  --profile-json /bench/profiles/vscode-docs.json
```

The adapter, output, commit window, and profile path are parameters of a
future run. This package does not claim those runs occurred.

## Primary repository

```text
remote=https://github.com/microsoft/vscode-docs.git
commit=74f6c45c91823e59b72d0a60787fccf482900023
tree=15729cd85f5434cc7e056db8cbbf6f7ae6e6cd63
parent=a68095505a536ca8cd80c378f40d901fcde5080b
tracked_paths=8380
lfs_tracked_paths=7215
lfs_logical_bytes=2782593092
```

The exact recursive tree listing SHA-256 is
`4ae75b6f67938cca3de0f25005b5b1d9ef92c1013efdf2631cd0951a82a9517d`.
The canonical `git lfs ls-files --long` stream SHA-256 is
`fe136b5c8aedc2f5ee8487547e60c9410b3150b5757ee15ae3254bd57bf6bdbc`.

Git object counters before the lifecycle cells were:

```text
count=0 size=0 in-pack=159474 packs=1 size-pack=288079 KiB
```

After `git gc --prune=now` the counters were:

```text
count=0 size=0 in-pack=159474 packs=1 size-pack=288064 KiB
```

The clean checkout storage counters were unchanged across the workload:

```text
git_dir_bytes=3023382605
lfs_objects_bytes=2725411726
working_tree_bytes=2807841146
```

The LFS object store was populated by the exact pinned-commit fetch, not by a
historical `--all` fetch. `git lfs fsck --pointers` passed. The repeat fetch
cell emitted no transfer and its filesystem-output counter was 8 bytes.

## Frozen result identity

The workload stdout digest, formed from the ordered cell labels and stdout
hashes, is:

```text
0e4d7f8d6f0a3fc3905f087b8c167068386d97065ac922b583a4d7f5cbc18fc7
```

The complete ordered stdout/stderr/time hash manifest is:

```text
a140dd14aa41d1503eb03bf2c30bb4bfeef5654e313dbca7c6a12362a2d6d8a2
```

All rows in `REFERENCE_RESULTS.csv` have status 0. The largest workload cell
was the cold reopen-equivalent local clone plus full LFS checkout: 3.84 s wall,
11.95 s user CPU, 4.16 s system CPU, 110,648 KiB peak RSS, 72 filesystem input
blocks, and 8,634,504 filesystem output blocks. Every cell was capped at 1,200
seconds by the script; no cell approached the cap.

## Raw evidence locations

The raw, uncommitted evidence remains on the measurement host at:

```text
checkout=/tmp/git-target-vscode-docs-clean-74f6c45c
trace=/tmp/git-target-vscode-docs-trace-clean-74f6-v2
```

The trace contains raw stdout, stderr, `/usr/bin/time -v`, per-cell result
status, metadata, and the cold `reopen-checkout` fixture. The setup logs are:

```text
/tmp/git-target-vscode-docs-clean-clone.log
  sha256=b814fed01c65a47fb669ebd69f82badb289f869411a55bd9c121308b2f2194e9
/tmp/git-target-vscode-docs-clean-lfs-fetch.log
  sha256=3e323ade677e3611a1908c1147dae7f2faf0eaca6cb12774b84c6beaa9dffe5f
/tmp/git-target-vscode-docs-clean-lfs-checkout-final.log
  sha256=7dd7b60253e597b9ab9ac7c9ee1ea490eac9172df4a3a0914de14f03f069f3c0
```

The trace-driver outer log is `/tmp/git-lfs-workload-clean-v2-run.log`,
SHA-256 `d3bc873a2ec96343a13bcb68385b60267c2ab862acc8676a2b38a709c23e1590`.
The earlier contaminated full-smudge checkout and the interrupted
historical-`--all` trace are explicitly excluded from this evidence.
