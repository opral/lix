# Raw trace manifest

The frozen measurement command was:

```sh
CELL_TIMEOUT_SECONDS=1200 timeout 1200s test-reports/git-lfs-optimization-target-b59/trace_git_lfs_workload.sh /tmp/git-target-vscode-docs-clean-74f6c45c /tmp/git-target-vscode-docs-trace-clean-74f6-v2
```

The source checkout was created and hydrated with:

```sh
GIT_LFS_SKIP_SMUDGE=1 git clone --no-tags https://github.com/microsoft/vscode-docs.git /tmp/git-target-vscode-docs-clean-74f6c45c
git -C /tmp/git-target-vscode-docs-clean-74f6c45c lfs fetch origin 74f6c45c91823e59b72d0a60787fccf482900023
git -C /tmp/git-target-vscode-docs-clean-74f6c45c lfs checkout
```

The trace output directory is intentionally not copied into the Git package:
it contains the 2.8 GB cold reopen worktree. Every compact result row and every
raw output hash is preserved in `REFERENCE_RESULTS.csv`; the corresponding
raw files are `cells/<workload>/{stdout.txt,stderr.txt,time.txt}` under the
host-local trace path in `TARGET_METADATA.md`. The metadata files are also
hash-bound:

```text
repository.txt       11f98e7f54ce52bd1e74c3b13eaff87387700953e2a7fa0ae18dd6bc9cb14516
tree-list.sha256     4ae75b6f67938cca3de0f25005b5b1d9ef92c1013efdf2631cd0951a82a9517d
lfs-pointers.txt     fe136b5c8aedc2f5ee8487547e60c9410b3150b5757ee15ae3254bd57bf6bdbc
lfs-count.txt        580d0594a4e68c01d9ef89af2e3937a7ab1785f8150c5562aba6bdac920e8349
storage-before.txt   fe7e47548e89c86da9f7ad7fb2df0e45a07f41d6c7b2ec51f9f5908eda863b69
storage-after.txt    fe7e47548e89c86da9f7ad7fb2df0e45a07f41d6c7b2ec51f9f5908eda863b69
count-after.txt      702913cd416077151e801f2357a8180400f1dc5481532c31a31096dff8c4910d
```

The exact trace-driver source SHA-256 is
`476d2f6253e77aa2a3e463089273c45f4650248b77128e9e5bef601bfa36f03d`.
The ordered stdout digest and the path-independent complete
`label stdout_sha256 stderr_sha256 time_sha256` digest are listed in
`TARGET_METADATA.md`; neither depends on host-local absolute paths.

The trace is deliberately Git/Git-LFS only. It does not call Lix, ForkTree,
the b59 binary, or a current-main binary; therefore it cannot be misread as a
Lix performance result.
