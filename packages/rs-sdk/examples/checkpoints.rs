use lix_sdk::{LixError, OpenLixOptions, Value, open_lix};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    let lix = open_lix(OpenLixOptions::default()).await?;

    // Writes to a tracked SQL surface create ordinary working changes.
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
        &[
            Value::Text("checkpoint-demo".to_string()),
            Value::Text("draft".to_string()),
        ],
    )
    .await?;

    let working_changes = lix
        .execute(
            "SELECT entity_pk, schema_key, change_kind
             FROM lix_working_change
             ORDER BY schema_key, entity_pk",
            &[],
        )
        .await?;

    for row in working_changes.rows() {
        // Row::get<T> performs typed extraction from ExecuteResult.
        let entity_pk = row.get::<serde_json::Value>("entity_pk")?;
        let schema_key = row.get::<String>("schema_key")?;
        let change_kind = row.get::<String>("change_kind")?;
        println!("{change_kind} {schema_key} {entity_pk}");
    }
    assert_eq!(working_changes.len(), 1);

    let checkpoint = lix.create_checkpoint().await?;
    println!("created checkpoint {}", checkpoint.commit_id);

    let checkpoints = lix
        .execute(
            "SELECT commit_id, created_at, lixcol_depth
             FROM lix_checkpoint
             ORDER BY lixcol_depth",
            &[],
        )
        .await?;

    for row in checkpoints.rows() {
        let commit_id = row.get::<String>("commit_id")?;
        let created_at = row.get::<String>("created_at")?;
        let depth = row.get::<i64>("lixcol_depth")?;
        println!("depth {depth}: {commit_id} ({created_at})");
    }
    assert_eq!(
        checkpoints.rows()[0].get::<String>("commit_id")?,
        checkpoint.commit_id
    );

    let remaining = lix
        .execute("SELECT COUNT(*) AS count FROM lix_working_change", &[])
        .await?;
    let remaining_count = remaining.rows()[0].get::<i64>("count")?;
    assert_eq!(remaining_count, 0);
    println!("working changes after checkpoint: {remaining_count}");

    lix.close().await?;
    Ok(())
}
