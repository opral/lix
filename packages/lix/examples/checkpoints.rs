use lix::{LixError, Value, open_lix};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    let lix = open_lix().await?;

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
            "SELECT row_pk, schema_key, diff_type
             FROM lix_working_diff()
             ORDER BY schema_key, row_pk",
            &[],
        )
        .await?;

    for row in working_diffs.rows() {
        // Row::get<T> performs typed extraction from ExecuteResult.
        let row_pk = row.get::<serde_json::Value>("row_pk")?;
        let schema_key = row.get::<String>("schema_key")?;
        let diff_type = row.get::<String>("diff_type")?;
        println!("{diff_type} {schema_key} {row_pk}");
    }
    let checkpoint = lix.create_checkpoint().await?;
    println!("created checkpoint {}", checkpoint.commit_id);

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
        .execute("SELECT COUNT(*) AS count FROM lix_working_diff()", &[])
        .await?;
    let remaining_count = remaining.rows()[0].get::<i64>("count")?;
    println!("working diffs after checkpoint: {remaining_count}");

    lix.close().await?;
    Ok(())
}
