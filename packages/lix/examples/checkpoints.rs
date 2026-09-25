use lix::{LixError, Value, open_lix};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), LixError> {
    let lix = open_lix().await?;
    let initial_checkpoint = lix
        .execute(
            "SELECT commit_id FROM lix_create_checkpoint($1, $2)",
            &[
                Value::Text("Start checkpoint example".into()),
                Value::Jsonb(serde_json::json!({"_type":"zettel_doc","blocks":[]}).into()),
            ],
        )
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
        .execute("SELECT commit_id FROM lix_create_checkpoint($1, $2)", &[
            Value::Text("Save draft key".into()),
            Value::Jsonb(serde_json::json!({
                "_type":"zettel_doc",
                "blocks":[{"_type":"zettel_block","_key":"context","style":"normal","markDefs":[],"children":[
                    {"_type":"zettel_span","_key":"summary","text":"Added checkpoint-demo as a draft; further edits remain possible.","marks":[]}
                ]}]
            }).into()),
        ])
        .await?
        .rows()[0]
        .get::<String>("commit_id")?;
    println!("created checkpoint {checkpoint}");

    // Checkpoint membership filters the first-parent commit log.
    let checkpoints = lix
        .execute(
            "SELECT l.commit_id, l.position, c.title
             FROM lix_log() AS l
             JOIN lix_conversation AS c ON c.id = l.conversation_id
             WHERE l.is_checkpoint
             ORDER BY position",
            &[],
        )
        .await?;

    for row in checkpoints.rows() {
        let commit_id = row.get::<String>("commit_id")?;
        let depth = row.get::<i64>("position")?;
        let title = row.get::<String>("title")?;
        println!("depth {depth}: {commit_id} {title}");
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
