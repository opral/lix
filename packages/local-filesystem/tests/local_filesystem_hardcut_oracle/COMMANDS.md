# LocalFilesystem hard-cut oracle commands

```sh
git fetch origin
git worktree add --detach /tmp/local-filesystem-candidate <candidate-sha>
cd /tmp/local-filesystem-candidate
bash packages/local-filesystem/tests/local_filesystem_hardcut_oracle/run_acceptance.sh \
  /tmp/local-filesystem-candidate /tmp/local-filesystem-oracle-target
```

Focused Rust discriminators can be replayed independently:

```sh
env CARGO_TARGET_DIR=/tmp/local-filesystem-oracle-target CARGO_BUILD_JOBS=2 \
  RUSTFLAGS='-D warnings' cargo test -p lix-storage-filesystem \
  --test local_filesystem_hardcut_oracle \
  positional_open_imports_workspace_but_never_physical_lix_metadata \
  -- --exact --nocapture --test-threads=1

env CARGO_TARGET_DIR=/tmp/local-filesystem-oracle-target CARGO_BUILD_JOBS=2 \
  RUSTFLAGS='-D warnings' cargo test -p lix-storage-filesystem \
  --test local_filesystem_hardcut_oracle \
  background_disk_changes_cover_create_modify_delete_rename_nested_binary_without_loop \
  -- --exact --nocapture --test-threads=1

env CARGO_TARGET_DIR=/tmp/local-filesystem-oracle-target CARGO_BUILD_JOBS=2 \
  RUSTFLAGS='-D warnings' cargo test -p lix-storage-filesystem \
  --test local_filesystem_hardcut_oracle \
  acknowledged_lix_writes_are_on_disk_before_close_and_cold_reopen_is_exact \
  -- --exact --nocapture --test-threads=1
```

The source gate is independently runnable:

```sh
python3 packages/local-filesystem/tests/local_filesystem_hardcut_oracle/source_gate.py . candidate
```
