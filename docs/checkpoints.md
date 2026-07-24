---
description: Create restore points and query changes since the latest checkpoint.
---

# Checkpoints

Lix automatically commits tracked changes. A checkpoint marks one of those
states as a user-meaningful restore point. The changes after the newest
checkpoint are the branch's working changes.

Create a checkpoint with the Rust SDK:

```rust
let checkpoint = lix.create_checkpoint().await?;
println!("created checkpoint {}", checkpoint.commit_id);
```

`create_checkpoint()` checkpoints every working change on the active branch and
returns the new checkpoint commit ID. It does not take a name or comment.

## Complete example

The runnable
[`checkpoints.rs`](https://github.com/opral/lix/blob/main/packages/rs-sdk/examples/checkpoints.rs)
example writes a tracked row, inspects its working change, creates a checkpoint,
reads checkpoint history, and verifies that no working changes remain:

```rust
use lix_sdk::{LixError, OpenLixOptions, Value, open_lix};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    let lix = open_lix(OpenLixOptions::default()).await?;

    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("checkpoint-demo".to_string()),
            Value::Text("draft".to_string()),
        ],
    )
    .await?;

    let working = lix
        .execute(
            "SELECT entity_pk, schema_key, change_kind
             FROM lix_working_change",
            &[],
        )
        .await?;

    for row in working.rows() {
        let entity_pk = row.get::<serde_json::Value>("entity_pk")?;
        let schema_key = row.get::<String>("schema_key")?;
        let change_kind = row.get::<String>("change_kind")?;
        println!("{change_kind} {schema_key} {entity_pk}");
    }

    let checkpoint = lix.create_checkpoint().await?;

    let history = lix
        .execute(
            "SELECT commit_id, created_at, lixcol_depth
             FROM lix_checkpoint
             ORDER BY lixcol_depth",
            &[],
        )
        .await?;
    let newest_commit_id = history.rows()[0].get::<String>("commit_id")?;
    let newest_depth = history.rows()[0].get::<i64>("lixcol_depth")?;
    assert_eq!(newest_commit_id, checkpoint.commit_id);
    assert_eq!(newest_depth, 0);

    let remaining = lix
        .execute(
            "SELECT COUNT(*) AS count FROM lix_working_change",
            &[],
        )
        .await?;
    assert_eq!(remaining.rows()[0].get::<i64>("count")?, 0);

    lix.close().await?;
    Ok(())
}
```

From the repository root, run it with:

```sh
cargo run -p lix_sdk --example checkpoints --no-default-features
```

`ExecuteResult::rows()` returns rows whose `get::<T>()` method checks and
extracts the requested Rust type. Prefer typed extraction such as
`get::<String>()`, `get::<i64>()`, and `get::<serde_json::Value>()` over
matching raw `Value` variants at every call site.

## SQL surfaces

Checkpointing has four read-only SQL surfaces:

| Surface | Scope | Columns |
| :-- | :-- | :-- |
| `lix_working_change` | Active branch | `entity_pk`, `schema_key`, `file_id`, `change_kind`, `before_change_id`, `after_change_id` |
| `lix_working_change_by_branch` | All branches | The same columns plus `lixcol_branch_id` |
| `lix_checkpoint` | Active branch | `commit_id`, `created_at`, `lixcol_depth` |
| `lix_checkpoint_by_branch` | All branches | The same columns plus `lixcol_branch_id` |

Use the unqualified surfaces for the common active-branch workflow. Use their
`_by_branch` counterparts to inspect multiple branches in one query;
`lixcol_branch_id` identifies the branch represented by each row.

`change_kind` is `added`, `modified`, or `removed`. Working changes compare the
current branch head with that branch's newest checkpoint. Creating a checkpoint
makes the current head the new baseline, so `lix_working_change` is empty until
another tracked change is committed.

Checkpoint depth is newest-first within each branch: `lixcol_depth = 0` is the
latest checkpoint, `1` is the previous checkpoint, and larger values walk
further back. SQL row order is not implicit, so request it explicitly:

```sql
SELECT commit_id, created_at, lixcol_depth
FROM lix_checkpoint
ORDER BY lixcol_depth;
```

For cross-branch history, order within each branch:

```sql
SELECT lixcol_branch_id, commit_id, created_at, lixcol_depth
FROM lix_checkpoint_by_branch
ORDER BY lixcol_branch_id, lixcol_depth;
```

All four relations are read-only. Create checkpoints through
`lix.create_checkpoint().await?`; `INSERT`, `UPDATE`, and `DELETE` against the
checkpoint or working-change surfaces are rejected.
