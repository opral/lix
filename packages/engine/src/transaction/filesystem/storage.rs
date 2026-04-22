use std::collections::BTreeMap;

use crate::catalog::state_by_version_relation_name;
use crate::{LixBackendTransaction, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FilesystemPayloadChange {
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: Option<String>,
    pub version_id: String,
    pub untracked: bool,
    pub plugin_key: Option<String>,
    pub snapshot_content: Option<String>,
    pub metadata: Option<String>,
    pub origin_key: Option<String>,
}

pub(crate) async fn persist_filesystem_payload_changes_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    changes: &[FilesystemPayloadChange],
) -> Result<(), LixError> {
    let tracked = changes
        .iter()
        .filter(|change| !change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !tracked.is_empty() {
        persist_filesystem_payload_changes_with_untracked_in_transaction(
            transaction,
            &tracked,
            false,
        )
        .await?;
    }

    let untracked = changes
        .iter()
        .filter(|change| change.untracked)
        .cloned()
        .collect::<Vec<_>>();
    if !untracked.is_empty() {
        persist_filesystem_payload_changes_with_untracked_in_transaction(
            transaction,
            &untracked,
            true,
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn persist_filesystem_payload_changes_with_untracked_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    changes: &[FilesystemPayloadChange],
    untracked: bool,
) -> Result<(), LixError> {
    let deduped_changes = dedupe_filesystem_payload_changes(changes);
    if deduped_changes.is_empty() {
        return Ok(());
    }

    let (sql, params) = build_filesystem_payload_changes_insert(&deduped_changes, untracked);
    transaction.execute(&sql, &params).await?;

    Ok(())
}

fn dedupe_filesystem_payload_changes(
    changes: &[FilesystemPayloadChange],
) -> Vec<FilesystemPayloadChange> {
    let mut latest_by_key: BTreeMap<(&str, &str, &str, &str, bool), usize> = BTreeMap::new();
    for (index, change) in changes.iter().enumerate() {
        latest_by_key.insert(
            (
                change.file_id.as_deref().unwrap_or(""),
                &change.version_id,
                &change.schema_key,
                &change.entity_id,
                change.untracked,
            ),
            index,
        );
    }

    let mut ordered_indexes = latest_by_key.into_values().collect::<Vec<_>>();
    ordered_indexes.sort_unstable();
    ordered_indexes
        .into_iter()
        .filter_map(|index| changes.get(index).cloned())
        .collect()
}

fn build_filesystem_payload_changes_insert(
    changes: &[FilesystemPayloadChange],
    untracked: bool,
) -> (String, Vec<Value>) {
    let values_per_row = if untracked { 10 } else { 9 };
    let mut params = Vec::with_capacity(changes.len() * values_per_row);
    let mut rows = Vec::with_capacity(changes.len());

    for (row_index, change) in changes.iter().enumerate() {
        rows.push(values_row_placeholders_sql(row_index, values_per_row));
        params.push(Value::Text(change.entity_id.clone()));
        params.push(Value::Text(change.schema_key.clone()));
        params.push(match &change.file_id {
            Some(file_id) => Value::Text(file_id.clone()),
            None => Value::Null,
        });
        params.push(Value::Text(change.version_id.clone()));
        params.push(match &change.plugin_key {
            Some(plugin_key) => Value::Text(plugin_key.clone()),
            None => Value::Null,
        });
        params.push(match &change.snapshot_content {
            Some(snapshot_content) => Value::Text(snapshot_content.clone()),
            None => Value::Null,
        });
        params.push(Value::Text(change.schema_version.clone()));
        params.push(match &change.metadata {
            Some(metadata) => Value::Text(metadata.clone()),
            None => Value::Null,
        });
        params.push(match &change.origin_key {
            Some(origin_key) => Value::Text(origin_key.clone()),
            None => Value::Null,
        });
        if untracked {
            params.push(Value::Boolean(true));
        }
    }

    let sql = insert_filesystem_payload_changes_sql(&rows.join(", "), untracked);
    (sql, params)
}

fn values_row_placeholders_sql(row_index: usize, values_per_row: usize) -> String {
    let base = row_index * values_per_row;
    let placeholders = (1..=values_per_row)
        .map(|offset| format!("${}", base + offset))
        .collect::<Vec<_>>()
        .join(", ");
    format!("({placeholders})")
}

fn insert_filesystem_payload_changes_sql(row_values: &str, untracked: bool) -> String {
    let state_by_version_table = state_by_version_relation_name();
    if untracked {
        return format!(
            "INSERT INTO {} (\
             entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, origin_key, untracked\
             ) VALUES {row_values}",
            state_by_version_table,
        );
    }

    format!(
        "INSERT INTO {} (\
         entity_id, schema_key, file_id, version_id, plugin_key, snapshot_content, schema_version, metadata, origin_key\
         ) VALUES {row_values}",
        state_by_version_table,
    )
}
