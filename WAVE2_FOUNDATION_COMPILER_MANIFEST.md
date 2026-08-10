# Wave 2 foundation compiler manifest

This manifest is the complete `cargo check --message-format short` error
output for the foundation cut from `b573f814d918a1a76b271bc67f51f1822bb43c3e`.
The command was run with `CARGO_TARGET_DIR=/root/repos/.target-wave2-foundation-b573`,
`CARGO_BUILD_JOBS=2`, and `CARGO_INCREMENTAL=0`.

- Result: 54 errors, 22 warnings; exit 101.
- Full log: `/root/repos/evidence/wave2-foundation-b573-check.log`
- Full-log SHA-256: `2fc2e803cc1ba43986295b828ef990abbfd85394b5193d5d9e25697ad01c5d80`
- Error class: consumer fallout from deleting `crate::live_state`; no diagnostic
  names `crate::state`, `ForkTreeStateView`, or `TransactionStateView`.
- All errors are unresolved imports/paths for the deleted owner. No replacement
  boundary error was emitted.

## Errors grouped by owning path

### `packages/lix/src/catalog/context.rs` (1)

```text
packages/lix/src/catalog/context.rs:16:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/commit_graph/context.rs` (6)

```text
packages/lix/src/commit_graph/context.rs:26:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/commit_graph/context.rs:96:20: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/commit_graph/context.rs:473:20: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/commit_graph/context.rs:540:24: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/commit_graph/context.rs:546:31: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/commit_graph/context.rs:600:27: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/domain.rs` (1)

```text
packages/lix/src/domain.rs:4:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/engine.rs` (3)

```text
packages/lix/src/engine.rs:8:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/engine.rs:302:26: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/engine.rs:361:38: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/filesystem/path_index.rs` (1)

```text
packages/lix/src/filesystem/path_index.rs:17:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/filesystem/planner.rs` (2)

```text
packages/lix/src/filesystem/planner.rs:15:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/filesystem/planner.rs:1425:19: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/filesystem/read.rs` (1)

```text
packages/lix/src/filesystem/read.rs:20:23: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/filesystem/visibility.rs` (1)

```text
packages/lix/src/filesystem/visibility.rs:7:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/functions/state.rs` (2)

```text
packages/lix/src/functions/state.rs:5:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/functions/state.rs:193:23: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/plugin/create_context.rs` (1)

```text
packages/lix/src/plugin/create_context.rs:18:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/plugin/incremental.rs` (1)

```text
packages/lix/src/plugin/incremental.rs:21:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/plugin/materializer.rs` (1)

```text
packages/lix/src/plugin/materializer.rs:1:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/plugin/registry.rs` (1)

```text
packages/lix/src/plugin/registry.rs:21:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/session/context.rs` (2)

```text
packages/lix/src/session/context.rs:20:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/session/context.rs:47:23: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/session/execute.rs` (4)

```text
packages/lix/src/session/execute.rs:1157:48: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/session/execute.rs:2332:52: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/session/execute.rs:2407:40: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/session/execute.rs:2693:32: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/context.rs` (1)

```text
packages/lix/src/sql2/context.rs:18:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/entity_batch.rs` (1)

```text
packages/lix/src/sql2/entity_batch.rs:14:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/exec/bound_public_write.rs` (2)

```text
packages/lix/src/sql2/exec/bound_public_write.rs:15:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/sql2/exec/bound_public_write.rs:1495:28: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/providers/branch.rs` (1)

```text
packages/lix/src/sql2/providers/branch.rs:24:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/providers/directory.rs` (2)

```text
packages/lix/src/sql2/providers/directory.rs:30:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/sql2/providers/directory.rs:33:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/providers/entity.rs` (4)

```text
packages/lix/src/sql2/providers/entity.rs:28:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/sql2/providers/entity.rs:32:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/sql2/providers/entity.rs:292:38: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/sql2/providers/entity.rs:322:38: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/providers/file.rs` (1)

```text
packages/lix/src/sql2/providers/file.rs:48:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/sql2/providers/mod.rs` (1)

```text
packages/lix/src/sql2/providers/mod.rs:11:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/transaction/context.rs` (6)

```text
packages/lix/src/transaction/context.rs:53:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/transaction/context.rs:8249:24: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/transaction/context.rs:8317:20: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/transaction/context.rs:8485:27: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/transaction/context.rs:8493:24: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/transaction/context.rs:8549:34: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/transaction/schema_resolver.rs` (1)

```text
packages/lix/src/transaction/schema_resolver.rs:7:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/transaction/staging.rs` (4)

```text
packages/lix/src/transaction/staging.rs:35:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
packages/lix/src/transaction/staging.rs:596:25: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/transaction/staging.rs:3411:20: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
packages/lix/src/transaction/staging.rs:3512:13: error[E0433]: cannot find `live_state` in `crate`: could not find `live_state` in the crate root
```

### `packages/lix/src/transaction/types.rs` (1)

```text
packages/lix/src/transaction/types.rs:18:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```

### `packages/lix/src/transaction/validation.rs` (1)

```text
packages/lix/src/transaction/validation.rs:29:12: error[E0432]: unresolved import `crate::live_state`: could not find `live_state` in the crate root
```
