# Git + Git LFS optimization-target workload package

Status: `TEST/REPORT-ONLY; reference measurements only`.

This package is bound to the approved real-plugin/ForkTree comparator SPI at
`b59e1f11a51153e0a787a81f0f25bf104d150aaf` (tree
`700fd04d21bc40c05425c9fc9e10d65c9e1eda24`). It contains no Lix production
change and does not benchmark Lix or current main.

## Pinned target

The primary target is the public `microsoft/vscode-docs` repository:

```text
remote=https://github.com/microsoft/vscode-docs.git
commit=74f6c45c91823e59b72d0a60787fccf482900023
```

The repository has honest Git LFS content (`7,215` LFS-tracked paths at the
pinned commit), so no synthetic replacement fixture is used. The checkout,
LFS object identities, tree digest, and raw command logs are hash-bound in
the frozen results.

## Workload contract

The trace covers import/checkout, commit replay/object walk, status, history,
branch create/delete, parent-to-head diff, fast-forward merge publication,
cold reopen-equivalent clone+checkout, and GC/repack. Git LFS controls cover
inventory, pointer/object verification, fetch status, checkout, and prune
dry-run. Each operation records exit status, wall/user/system CPU, peak RSS,
filesystem I/O, object counts, LFS object counts/bytes, and output hashes.

The exact commands and ordering are frozen in `WORKLOAD_CONTRACT.md` and
`trace_git_lfs_workload.sh`. Every workload cell is capped at 20 minutes.
Results are optimization-target reference measurements only; they are not
ForkTree results and do not imply a current-main comparison.
