# ForkTree historical-provider acceptance package

This is a transportable TEST/REPORT-ONLY package for the first production
caller migration after the historical CommitCatalog fail-closed correction.
It is anchored to the exact accepted semantic frontier:

| item | value |
| --- | --- |
| base/head | `b59e1f11a51153e0a787a81f0f25bf104d150aaf` |
| tree | `700fd04d21bc40c05425c9fc9e10d65c9e1eda24` |
| parent | `713455a3557907ce705d06f720fcdc4486bddd4a` |
| scope | SQL historical/file-system read semantics only |

The package does not change production code, does not add a compatibility
reader, and makes no compile, runtime, or adapter claim. It freezes the H4
acceptance contract for these public surfaces:

* `lix_file_history`
* `lix_directory_history`
* `lix_diff`
* `lix_checkpoint`
* `lix_file_working_diff`, `lix_file_working_diff_by_branch`,
  `lix_directory_working_diff`, and `lix_directory_working_diff_by_branch`

Run the source gate and pure model from the repository root:

```bash
bash test-reports/forktree-historical-provider-acceptance-b59/source_verifier.sh
rustc --edition 2021 --test \
  test-reports/forktree-historical-provider-acceptance-b59/model.rs \
  -o /tmp/forktree-historical-provider-model
/tmp/forktree-historical-provider-model --nocapture
```

The adapter commands are intentionally future commands only; see
`FUTURE_COMMANDS.md`.

`SHA256SUMS` freezes the content hash of every package artifact.
