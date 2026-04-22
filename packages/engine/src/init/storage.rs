use crate::backend::QueryExecutor;
use crate::canonical::{build_commit_generation_seed_sql, COMMIT_GRAPH_NODE_TABLE};
use crate::live_state::tracked_relation_name;
use crate::{LixBackend, LixBackendTransaction, LixError, SqlDialect, Value};

pub(crate) async fn prepare_backend_for_init(backend: &dyn LixBackend) -> Result<(), LixError> {
    if backend.dialect() == SqlDialect::Sqlite {
        backend.execute("PRAGMA foreign_keys = ON", &[]).await?;
    }
    Ok(())
}

pub(crate) async fn canonical_commit_exists_without_global_head(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<bool, LixError> {
    Ok(transaction
        .execute(
            "SELECT 1 \
             FROM lix_internal_change c \
             JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = 'lix_commit' \
               AND c.file_id IS NULL \
               AND c.plugin_key IS NULL \
               AND s.content IS NOT NULL \
             LIMIT 1",
            &[],
        )
        .await?
        .rows
        .first()
        .is_some())
}

#[derive(Debug, Clone)]
pub(crate) struct CommitSnapshotRow {
    pub(crate) snapshot_id: String,
    pub(crate) content: String,
}

pub(crate) async fn load_commit_snapshot_rows(
    executor: &mut dyn QueryExecutor,
    commit_id: &str,
) -> Result<Vec<CommitSnapshotRow>, LixError> {
    let result = executor
        .execute(
            "SELECT c.snapshot_id, s.content \
             FROM lix_internal_change c \
             JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.entity_id = $1 \
               AND c.schema_key = 'lix_commit' \
               AND c.file_id IS NULL \
               AND c.plugin_key IS NULL \
               AND s.content IS NOT NULL",
            &[Value::Text(commit_id.to_string())],
        )
        .await?;
    result
        .rows
        .into_iter()
        .map(|row| {
            let snapshot_id = text_cell(row.first(), "commit snapshot_id")?;
            let content = text_cell(row.get(1), "commit snapshot content")?;
            Ok(CommitSnapshotRow {
                snapshot_id,
                content,
            })
        })
        .collect()
}

pub(crate) async fn canonical_change_set_exists(
    transaction: &mut dyn LixBackendTransaction,
    change_set_id: &str,
) -> Result<bool, LixError> {
    exists_with_entity_id(
        transaction,
        "lix_change_set",
        change_set_id,
        "canonical change_set existence",
    )
    .await
}

pub(crate) async fn canonical_commit_exists(
    transaction: &mut dyn LixBackendTransaction,
    commit_id: &str,
) -> Result<bool, LixError> {
    exists_with_entity_id(
        transaction,
        "lix_commit",
        commit_id,
        "canonical commit existence",
    )
    .await
}

async fn exists_with_entity_id(
    transaction: &mut dyn LixBackendTransaction,
    schema_key: &str,
    entity_id: &str,
    _label: &str,
) -> Result<bool, LixError> {
    Ok(transaction
        .execute(
            "SELECT 1 \
             FROM lix_internal_change c \
             JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = $1 \
               AND c.entity_id = $2 \
               AND c.file_id IS NULL \
               AND c.plugin_key IS NULL \
               AND s.content IS NOT NULL \
             LIMIT 1",
            &[
                Value::Text(schema_key.to_string()),
                Value::Text(entity_id.to_string()),
            ],
        )
        .await?
        .rows
        .first()
        .is_some())
}

pub(crate) async fn load_commit_graph_node_count(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<i64, LixError> {
    let result = transaction
        .execute(
            &format!("SELECT COUNT(*) FROM {}", COMMIT_GRAPH_NODE_TABLE),
            &[],
        )
        .await?;
    read_scalar_count(&result, "lix_internal_commit_graph_node count")
}

pub(crate) async fn load_canonical_commit_count(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<i64, LixError> {
    let result = transaction
        .execute(
            "SELECT COUNT(*) \
             FROM lix_internal_change c \
             JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
             WHERE c.schema_key = 'lix_commit' \
               AND c.file_id IS NULL \
               AND c.plugin_key IS NULL \
               AND s.content IS NOT NULL",
            &[],
        )
        .await?;
    read_scalar_count(&result, "lix_commit count")
}

pub(crate) async fn seed_commit_generation_graph_nodes(
    transaction: &mut dyn LixBackendTransaction,
) -> Result<(), LixError> {
    let sql = build_commit_generation_seed_sql(transaction.dialect());
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

pub(crate) async fn load_seeded_system_directory_id(
    transaction: &mut dyn LixBackendTransaction,
    name: &str,
    parent_id: Option<&str>,
) -> Result<Option<String>, LixError> {
    let directory_table = tracked_relation_name("lix_directory_descriptor");
    let result = match parent_id {
        Some(parent_id) => {
            transaction
                .execute(
                    &format!(
                        "SELECT entity_id \
                         FROM {directory_table} \
                         WHERE schema_key = 'lix_directory_descriptor' \
                           AND file_id IS NULL \
                           AND version_id = 'global' \
                           AND untracked = false \
                           AND is_tombstone = 0 \
                           AND name = $1 \
                           AND parent_id = $2 \
                         ORDER BY updated_at DESC, created_at DESC, entity_id DESC \
                         LIMIT 1"
                    ),
                    &[
                        Value::Text(name.to_string()),
                        Value::Text(parent_id.to_string()),
                    ],
                )
                .await?
        }
        None => {
            transaction
                .execute(
                    &format!(
                        "SELECT entity_id \
                         FROM {directory_table} \
                         WHERE schema_key = 'lix_directory_descriptor' \
                           AND file_id IS NULL \
                           AND version_id = 'global' \
                           AND untracked = false \
                           AND is_tombstone = 0 \
                           AND name = $1 \
                           AND parent_id IS NULL \
                         ORDER BY updated_at DESC, created_at DESC, entity_id DESC \
                         LIMIT 1"
                    ),
                    &[Value::Text(name.to_string())],
                )
                .await?
        }
    };
    result
        .rows
        .first()
        .map(|row| text_cell(row.first(), "system directory entity_id"))
        .transpose()
}

fn read_scalar_count(result: &crate::QueryResult, label: &str) -> Result<i64, LixError> {
    let value = result
        .rows
        .first()
        .and_then(|row| row.first())
        .ok_or_else(|| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned no rows"),
            hint: None,
        })?;
    match value {
        Value::Integer(number) => Ok(*number),
        Value::Text(raw) => raw.parse::<i64>().map_err(|error| LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned invalid integer '{raw}': {error}"),
            hint: None,
        }),
        other => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("{label} query returned non-integer value: {other:?}"),
            hint: None,
        }),
    }
}

fn text_cell(value: Option<&Value>, label: &str) -> Result<String, LixError> {
    match value {
        Some(Value::Text(text)) => Ok(text.clone()),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{label} must be text, got {other:?}"),
        )),
        None => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("{label} is missing"),
        )),
    }
}
