use crate::common::escape_sql_string;
use crate::{LixBackendTransaction, LixError, Value};

pub(crate) async fn upsert_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[(String, String)],
    update_existing: bool,
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let values_sql = rows
        .iter()
        .map(|(version_id, checkpoint_commit_id)| {
            format!(
                "('{}', '{}')",
                escape_sql_string(version_id),
                escape_sql_string(checkpoint_commit_id)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let on_conflict = if update_existing {
        "DO UPDATE SET checkpoint_commit_id = excluded.checkpoint_commit_id"
    } else {
        "DO NOTHING"
    };
    let sql = format!(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES {values_sql} \
         ON CONFLICT (version_id) {on_conflict}"
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

pub(crate) async fn delete_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_ids: &[String],
) -> Result<(), LixError> {
    if version_ids.is_empty() {
        return Ok(());
    }

    let in_list = version_ids
        .iter()
        .map(|id| format!("'{}'", escape_sql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM lix_internal_last_checkpoint WHERE version_id IN ({in_list})");
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

pub(crate) async fn clear_last_checkpoint_rows_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    transaction
        .execute("DELETE FROM lix_internal_last_checkpoint", &[])
        .await?;
    Ok(())
}

pub(crate) async fn insert_last_checkpoint_for_version_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    version_id: &str,
    checkpoint_commit_id: &str,
) -> Result<(), LixError> {
    upsert_last_checkpoint_rows_in_transaction(
        transaction,
        &[(version_id.to_string(), checkpoint_commit_id.to_string())],
        true,
    )
    .await
}

pub(crate) async fn checkpoint_commit_label_exists_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    entity_id: &str,
    schema_key: &str,
) -> Result<bool, LixError> {
    let exists = transaction
        .execute(
            "SELECT 1 \
             FROM lix_internal_change \
             WHERE entity_id = $1 \
               AND schema_key = $2 \
               AND file_id IS NULL \
               AND plugin_key IS NULL \
             LIMIT 1",
            &[
                Value::Text(entity_id.to_string()),
                Value::Text(schema_key.to_string()),
            ],
        )
        .await?;
    Ok(!exists.rows.is_empty())
}
