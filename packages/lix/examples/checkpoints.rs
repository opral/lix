use lix::{LixError, Value, open_lix};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    let lix = open_lix().await?;
    let initial_checkpoint = lix
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await?
        .rows()[0]
        .get::<String>("commit_id")?;

    // Writes to a tracked SQL surface create ordinary working diffs.
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("checkpoint-demo".to_string()),
            Value::Text("draft".to_string()),
        ],
    )
    .await?;

    let working_diffs = lix
        .execute(
            "SELECT row_ref, key, diff_type, from_value, to_value
             FROM lix_diff('lix_key_value', $1, lix_active_branch_commit_id())
             ORDER BY key",
            &[Value::Text(initial_checkpoint)],
        )
        .await?;

    for row in working_diffs.rows() {
        // Row::get<T> performs typed extraction from ExecuteResult.
        let row_ref = row.get::<lix::RowRef>("row_ref")?;
        let key = row.get::<String>("key")?;
        let diff_type = row.get::<String>("diff_type")?;
        println!("{diff_type} lix_key_value {key} ({row_ref})");
    }
    let checkpoint = lix
        .execute("SELECT commit_id FROM lix_create_checkpoint()", &[])
        .await?
        .rows()[0]
        .get::<String>("commit_id")?;
    println!("created checkpoint {checkpoint}");

    // `lix_checkpoint` holds the checkpoint rows and carries no ordering column.
    // `lix_history('lix_checkpoint')` exposes `lixcol_depth`, so ascending depth is
    // newest-first.
    let checkpoints = lix
        .execute(
            "SELECT commit_id, lixcol_depth
             FROM lix_history('lix_checkpoint')
             ORDER BY lixcol_depth",
            &[],
        )
        .await?;

    for row in checkpoints.rows() {
        let commit_id = row.get::<String>("commit_id")?;
        let depth = row.get::<i64>("lixcol_depth")?;
        println!("depth {depth}: {commit_id}");
    }
    let remaining = lix
        .execute(
            "SELECT COUNT(*) AS count
             FROM lix_diff('lix_key_value', $1, lix_active_branch_commit_id())",
            &[Value::Text(checkpoint)],
        )
        .await?;
    let remaining_count = remaining.rows()[0].get::<i64>("count")?;
    println!("working diffs after checkpoint: {remaining_count}");

    lix.close().await?;
    Ok(())
}
