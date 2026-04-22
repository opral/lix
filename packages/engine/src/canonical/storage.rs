#![allow(dead_code)]

//! SQL-backed adapter slot for canonical persistence.
//!
//! This module is the intended home for `CanonicalReadStore` and
//! `CanonicalWriteStore` implementations that still rely on raw backend,
//! transaction, executor, or lower `backend/*` helpers during the MVP.

use async_trait::async_trait;
use std::collections::BTreeSet;

use crate::canonical::store::{
    CanonicalBackendRef, CanonicalExecutorRef, CanonicalReadStore, CanonicalTransactionRef,
    CanonicalWriteStore,
};
use crate::common::escape_sql_string;
use crate::common::{storage_scope_key_for_file_id, STORAGE_SCOPE_KEY_COLUMN};
use crate::functions::LixFunctionProvider;
use crate::streams::load_durable_state_commit_low_watermark;
use crate::SqlDialect;
use crate::{LixError, QueryResult, Value};

use crate::canonical::checkpoint_labels::checkpoint_commit_label_entity_id;
use crate::canonical::{
    api, checkpoint_labels, CanonicalAppendSummary, CanonicalChange, CanonicalChangeWrite,
    CanonicalCommit, CanonicalHistoryRequest, CanonicalHistoryRow, CanonicalUntrackedChangeRow,
    CanonicalUntrackedIdentity, CanonicalUntrackedVisibilityWrite, CanonicalVisibleStateRequest,
    CanonicalVisibleStateRow, CommittedCanonicalChangeRow, COMMIT_GRAPH_NODE_TABLE,
    UNTRACKED_CHANGE_VISIBILITY_TABLE,
};

pub(crate) struct SqlCanonicalReadStore<'a> {
    backend: CanonicalBackendRef<'a>,
}

impl<'a> SqlCanonicalReadStore<'a> {
    pub(crate) fn new(backend: CanonicalBackendRef<'a>) -> Self {
        Self { backend }
    }
}

pub(crate) struct SqlCanonicalExecutorReadStore<'a> {
    executor: CanonicalExecutorRef<'a>,
}

impl<'a> SqlCanonicalExecutorReadStore<'a> {
    pub(crate) fn new(executor: CanonicalExecutorRef<'a>) -> Self {
        Self { executor }
    }
}

pub(crate) struct SqlCanonicalWriteStore<'a> {
    transaction: CanonicalTransactionRef<'a>,
}

impl<'a> SqlCanonicalWriteStore<'a> {
    pub(crate) fn new(transaction: CanonicalTransactionRef<'a>) -> Self {
        Self { transaction }
    }
}

async fn execute_query_with_backend(
    backend: CanonicalBackendRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    backend.execute(sql, params).await
}

async fn execute_query_with_executor(
    executor: CanonicalExecutorRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    executor.execute(sql, params).await
}

async fn execute_query_with_transaction(
    transaction: CanonicalTransactionRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    transaction.execute(sql, params).await
}

async fn execute_batch_with_transaction(
    transaction: CanonicalTransactionRef<'_>,
    batch: &crate::canonical::store::CanonicalPreparedBatch,
) -> Result<(), LixError> {
    transaction.execute_batch(batch).await.map(|_| ())
}

async fn execute_ddl_batch_with_backend(
    backend: CanonicalBackendRef<'_>,
    batch_name: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    crate::backend::execute_ddl_batch(backend, batch_name, statements).await
}

async fn add_column_if_missing_with_backend(
    backend: CanonicalBackendRef<'_>,
    table_name: &str,
    column_name: &str,
    column_sql: &str,
) -> Result<(), LixError> {
    crate::backend::add_column_if_missing(backend, table_name, column_name, column_sql).await
}

fn executor_from_transaction(
    transaction: CanonicalTransactionRef<'_>,
) -> impl crate::QueryExecutor + '_ {
    crate::backend::transaction_backend_view(transaction)
}

pub(crate) async fn load_durable_state_commit_low_watermark_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
) -> Result<Option<crate::streams::DurableStateCommitCursor>, LixError> {
    let backend = crate::backend::transaction_backend_view(transaction);
    load_durable_state_commit_low_watermark(&backend).await
}

pub(crate) async fn init_storage(backend: CanonicalBackendRef<'_>) -> Result<(), LixError> {
    const CANONICAL_INDEX_STATEMENTS: &[&str] = &[
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_untracked_change_visibility_change_id \
         ON lix_internal_untracked_change_visibility (change_id)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_lix_internal_untracked_change_visibility_append_seq \
         ON lix_internal_untracked_change_visibility (append_seq)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_untracked_change_visibility_identity \
         ON lix_internal_untracked_change_visibility (version_id, visibility_kind, entity_id, schema_key, file_id, append_seq)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_untracked_change_visibility_schema \
         ON lix_internal_untracked_change_visibility (visibility_kind, version_id, schema_key)",
        "CREATE UNIQUE INDEX IF NOT EXISTS uq_lix_internal_entity_state_timeline_breakpoint_scope_identity \
         ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, schema_key, storage_scope_key, from_depth)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_root_depth \
         ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, from_depth)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_lookup \
         ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, entity_id, storage_scope_key, schema_key, from_depth)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_filters \
         ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, file_id, plugin_key, schema_key, entity_id, from_depth)",
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_entity_state_timeline_breakpoint_scope_filters \
         ON lix_internal_entity_state_timeline_breakpoint (root_commit_id, storage_scope_key, plugin_key, schema_key, entity_id, from_depth)",
    ];

    let create_table_statements = canonical_create_table_statements(backend.dialect());
    let create_table_statement_refs = create_table_statements
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    execute_ddl_batch_with_backend(backend, "canonical.tables", &create_table_statement_refs)
        .await?;
    add_column_if_missing_with_backend(
        backend,
        "lix_internal_entity_state_timeline_breakpoint",
        STORAGE_SCOPE_KEY_COLUMN,
        &format!(
            "TEXT NOT NULL DEFAULT '{}'",
            storage_scope_key_for_file_id(None)
        ),
    )
    .await?;
    backfill_breakpoint_storage_scope_keys(backend).await?;
    execute_ddl_batch_with_backend(backend, "canonical.indexes", CANONICAL_INDEX_STATEMENTS).await
}

pub(crate) async fn backfill_breakpoint_storage_scope_keys(
    backend: CanonicalBackendRef<'_>,
) -> Result<(), LixError> {
    execute_query_with_backend(
        backend,
        &format!(
            "UPDATE lix_internal_entity_state_timeline_breakpoint \
             SET {storage_scope_key} = CASE \
               WHEN file_id IS NULL THEN '{engine_scope}' \
               ELSE 'file:' || file_id \
             END",
            storage_scope_key = crate::common::STORAGE_SCOPE_KEY_COLUMN,
            engine_scope = crate::common::storage_scope_key_for_file_id(None),
        ),
        &[],
    )
    .await
    .map(|_| ())
}

fn canonical_create_table_statements(dialect: SqlDialect) -> Vec<String> {
    let mut statements = vec![
        "CREATE TABLE IF NOT EXISTS lix_internal_snapshot (\
         id TEXT PRIMARY KEY,\
         content TEXT\
         )"
        .to_string(),
        "INSERT INTO lix_internal_snapshot (id, content) \
         SELECT 'no-content', NULL \
         WHERE NOT EXISTS ( \
           SELECT 1 FROM lix_internal_snapshot WHERE id = 'no-content' \
         )"
        .to_string(),
        "CREATE TABLE IF NOT EXISTS lix_internal_change (\
         id TEXT PRIMARY KEY,\
         entity_id TEXT NOT NULL,\
         schema_key TEXT NOT NULL,\
         schema_version TEXT NOT NULL,\
         file_id TEXT,\
         plugin_key TEXT,\
         snapshot_id TEXT NOT NULL,\
         metadata TEXT,\
         created_at TEXT NOT NULL\
         )"
        .to_string(),
    ];

    match dialect {
        SqlDialect::Sqlite => {
            statements.push(
                "CREATE TABLE IF NOT EXISTS lix_internal_untracked_change_visibility (\
                 id TEXT PRIMARY KEY,\
                 change_id TEXT NOT NULL,\
                 version_id TEXT NOT NULL,\
                 visibility_kind TEXT NOT NULL,\
                 entity_id TEXT NOT NULL,\
                 schema_key TEXT NOT NULL,\
                 file_id TEXT,\
                 created_at TEXT NOT NULL,\
                 append_seq INTEGER\
                 )"
                .to_string(),
            );
            statements.push(
                r#"CREATE TRIGGER IF NOT EXISTS trg_lix_internal_untracked_change_visibility_append_seq
AFTER INSERT ON lix_internal_untracked_change_visibility
FOR EACH ROW
WHEN NEW.append_seq IS NULL
BEGIN
  UPDATE lix_internal_untracked_change_visibility
  SET append_seq = NEW.rowid
  WHERE id = NEW.id;
END"#
                    .to_string(),
            );
        }
        SqlDialect::Postgres => {
            statements.push(
                "CREATE TABLE IF NOT EXISTS lix_internal_untracked_change_visibility (\
                 id TEXT PRIMARY KEY,\
                 change_id TEXT NOT NULL,\
                 version_id TEXT NOT NULL,\
                 visibility_kind TEXT NOT NULL,\
                 entity_id TEXT NOT NULL,\
                 schema_key TEXT NOT NULL,\
                 file_id TEXT,\
                 created_at TEXT NOT NULL,\
                 append_seq BIGINT GENERATED BY DEFAULT AS IDENTITY\
                 )"
                .to_string(),
            );
        }
    }

    statements.extend([
        "CREATE TABLE IF NOT EXISTS lix_internal_commit_graph_node (\
         commit_id TEXT PRIMARY KEY,\
         generation BIGINT NOT NULL\
         )"
        .to_string(),
        "CREATE INDEX IF NOT EXISTS idx_lix_internal_commit_graph_node_generation \
         ON lix_internal_commit_graph_node (generation)"
            .to_string(),
        "CREATE TABLE IF NOT EXISTS lix_internal_entity_state_timeline_breakpoint (\
         root_commit_id TEXT NOT NULL,\
         entity_id TEXT NOT NULL,\
         schema_key TEXT NOT NULL,\
         file_id TEXT,\
         storage_scope_key TEXT NOT NULL,\
         from_depth BIGINT NOT NULL,\
         plugin_key TEXT,\
         schema_version TEXT NOT NULL,\
         metadata TEXT,\
         snapshot_id TEXT NOT NULL,\
         change_id TEXT NOT NULL,\
         PRIMARY KEY (root_commit_id, entity_id, schema_key, storage_scope_key, from_depth)\
         )"
        .to_string(),
        "CREATE TABLE IF NOT EXISTS lix_internal_timeline_status (\
         root_commit_id TEXT PRIMARY KEY,\
         built_max_depth BIGINT NOT NULL,\
         built_at TEXT NOT NULL\
         )"
        .to_string(),
    ]);

    statements
}

pub(crate) async fn append_change_batch_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
    batch: &crate::canonical::store::CanonicalPreparedBatch,
) -> Result<(), LixError> {
    execute_batch_with_transaction(transaction, batch).await
}

pub(crate) async fn append_untracked_visibility_batch_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
    batch: &crate::canonical::store::CanonicalPreparedBatch,
) -> Result<(), LixError> {
    execute_batch_with_transaction(transaction, batch).await
}

pub(crate) async fn replace_snapshot_content_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
    snapshot_id: &str,
    snapshot_content: &str,
) -> Result<(), LixError> {
    execute_query_with_transaction(
        transaction,
        "UPDATE lix_internal_snapshot \
         SET content = $1 \
         WHERE id = $2",
        &[
            Value::Text(snapshot_content.to_string()),
            Value::Text(snapshot_id.to_string()),
        ],
    )
    .await
    .map(|_| ())
}

pub(crate) async fn load_history_query_result(
    backend: CanonicalBackendRef<'_>,
    request: &CanonicalHistoryRequest,
) -> Result<QueryResult, LixError> {
    let sql = api::build_canonical_history_query_sql(backend.dialect(), request)?;
    execute_query_with_backend(backend, &sql, &[]).await
}

pub(crate) async fn load_visible_state_query_result(
    executor: CanonicalExecutorRef<'_>,
    request: &CanonicalVisibleStateRequest,
) -> Result<QueryResult, LixError> {
    let sql = api::build_visible_state_query_sql(executor.dialect(), request)?;
    execute_query_with_executor(executor, &sql, &[]).await
}

pub(crate) async fn untracked_visibility_exists(
    executor: CanonicalExecutorRef<'_>,
    change_id: &str,
) -> Result<bool, LixError> {
    let sql = format!(
        "SELECT 1 \
         FROM {} \
         WHERE change_id = {} \
         LIMIT 1",
        UNTRACKED_CHANGE_VISIBILITY_TABLE,
        executor.dialect().placeholder(1),
    );
    let result =
        execute_query_with_executor(executor, &sql, &[Value::Text(change_id.to_string())]).await?;
    Ok(!result.rows.is_empty())
}

pub(crate) async fn load_untracked_change_rows_for_compaction(
    executor: CanonicalExecutorRef<'_>,
    identities: &[CanonicalUntrackedIdentity],
) -> Result<Vec<CanonicalUntrackedChangeRow>, LixError> {
    let mut params = Vec::new();
    let mut predicates = Vec::new();

    if !identities.is_empty() {
        let mut groups = Vec::new();
        for identity in identities {
            let version = executor.dialect().placeholder(params.len() + 1);
            params.push(Value::Text(identity.version_id.clone()));
            let visibility_kind = executor.dialect().placeholder(params.len() + 1);
            params.push(Value::Text(identity.visibility_kind.as_str().to_string()));
            let entity = executor.dialect().placeholder(params.len() + 1);
            params.push(Value::Text(identity.entity_id.clone()));
            let schema = executor.dialect().placeholder(params.len() + 1);
            params.push(Value::Text(identity.schema_key.clone()));
            let file = executor.dialect().placeholder(params.len() + 1);
            params.push(Value::Text(identity.file_id.clone()));
            groups.push(format!(
                "(v.version_id = {version} AND v.visibility_kind = {visibility_kind} AND v.entity_id = {entity} AND v.schema_key = {schema} AND COALESCE(v.file_id, '') = {file})"
            ));
        }
        predicates.push(format!("({})", groups.join(" OR ")));
    }

    let sql = format!(
        "SELECT v.id, v.append_seq, c.id, v.version_id, v.visibility_kind, v.entity_id, v.schema_key, v.file_id, c.snapshot_id, c.created_at, v.created_at \
         FROM lix_internal_untracked_change_visibility v \
         JOIN lix_internal_change c \
           ON c.id = v.change_id \
         {where_sql} \
         ORDER BY v.version_id ASC, v.visibility_kind ASC, v.entity_id ASC, v.schema_key ASC, v.file_id ASC, v.append_seq DESC",
        where_sql = if predicates.is_empty() {
            String::new()
        } else {
            format!("WHERE {}", predicates.join(" AND "))
        }
    );
    let result = execute_query_with_executor(executor, &sql, &params).await?;
    result
        .rows
        .iter()
        .map(|row| {
            Ok(CanonicalUntrackedChangeRow {
                visibility_id: required_text_value(
                    row,
                    0,
                    "lix_internal_untracked_change_visibility.id",
                )?,
                visibility_append_seq: required_integer_value(
                    row,
                    1,
                    "lix_internal_untracked_change_visibility.append_seq",
                )?,
                change_id: required_text_value(row, 2, "lix_internal_change.id")?,
                identity: CanonicalUntrackedIdentity {
                    version_id: required_text_value(
                        row,
                        3,
                        "lix_internal_untracked_change_visibility.version_id",
                    )?,
                    visibility_kind: crate::canonical::CanonicalUntrackedVisibilityKind::parse(
                        &required_text_value(
                            row,
                            4,
                            "lix_internal_untracked_change_visibility.visibility_kind",
                        )?,
                    )?,
                    entity_id: required_text_value(
                        row,
                        5,
                        "lix_internal_untracked_change_visibility.entity_id",
                    )?,
                    schema_key: required_text_value(
                        row,
                        6,
                        "lix_internal_untracked_change_visibility.schema_key",
                    )?,
                    file_id: optional_text_value(
                        row,
                        7,
                        "lix_internal_untracked_change_visibility.file_id",
                    )?
                    .unwrap_or_default(),
                },
                snapshot_id: required_text_value(row, 8, "lix_internal_change.snapshot_id")?,
                change_created_at: required_text_value(row, 9, "lix_internal_change.created_at")?,
                visibility_created_at: required_text_value(
                    row,
                    10,
                    "lix_internal_untracked_change_visibility.created_at",
                )?,
            })
        })
        .collect()
}

pub(crate) async fn delete_visibility_rows_and_orphaned_changes(
    transaction: CanonicalTransactionRef<'_>,
    rows: &[CanonicalUntrackedChangeRow],
) -> Result<(), LixError> {
    const DELETE_CHUNK_SIZE: usize = 256;
    if rows.is_empty() {
        return Ok(());
    }

    let dialect = transaction.dialect();
    let visibility_ids = rows
        .iter()
        .map(|row| row.visibility_id.clone())
        .collect::<Vec<_>>();
    for chunk in visibility_ids.chunks(DELETE_CHUNK_SIZE) {
        let mut params = Vec::with_capacity(chunk.len());
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, visibility_id)| {
                params.push(Value::Text(visibility_id.clone()));
                dialect.placeholder(index + 1)
            })
            .collect::<Vec<_>>();
        execute_query_with_transaction(
            transaction,
            &format!(
                "DELETE FROM lix_internal_untracked_change_visibility \
                 WHERE id IN ({})",
                placeholders.join(", ")
            ),
            &params,
        )
        .await?;
    }

    let change_ids = rows
        .iter()
        .map(|row| row.change_id.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let change_commit_ctes =
        crate::canonical::build_lazy_change_commit_by_change_id_ctes_sql(dialect);
    for chunk in change_ids.chunks(DELETE_CHUNK_SIZE) {
        let mut params = Vec::with_capacity(chunk.len());
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, change_id)| {
                params.push(Value::Text(change_id.clone()));
                dialect.placeholder(index + 1)
            })
            .collect::<Vec<_>>();
        execute_query_with_transaction(
            transaction,
            &format!(
                "WITH {change_commit_ctes} \
                 DELETE FROM lix_internal_change \
                 WHERE id IN ({placeholders}) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM lix_internal_untracked_change_visibility v \
                     WHERE v.change_id = lix_internal_change.id\
                   ) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM change_commit_by_change_id cc \
                     WHERE cc.change_id = lix_internal_change.id\
                   )",
                change_commit_ctes = change_commit_ctes,
                placeholders = placeholders.join(", "),
            ),
            &params,
        )
        .await?;
    }

    let snapshot_ids = rows
        .iter()
        .map(|row| row.snapshot_id.clone())
        .filter(|snapshot_id| snapshot_id != "no-content")
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    for chunk in snapshot_ids.chunks(DELETE_CHUNK_SIZE) {
        let mut params = Vec::with_capacity(chunk.len());
        let placeholders = chunk
            .iter()
            .enumerate()
            .map(|(index, snapshot_id)| {
                params.push(Value::Text(snapshot_id.clone()));
                dialect.placeholder(index + 1)
            })
            .collect::<Vec<_>>();
        execute_query_with_transaction(
            transaction,
            &format!(
                "DELETE FROM lix_internal_snapshot \
                 WHERE id IN ({}) \
                   AND NOT EXISTS (\
                     SELECT 1 FROM lix_internal_change c \
                     WHERE c.snapshot_id = lix_internal_snapshot.id\
                   )",
                placeholders.join(", ")
            ),
            &params,
        )
        .await?;
    }

    Ok(())
}

pub(crate) async fn load_commit_graph_generation(
    executor: CanonicalExecutorRef<'_>,
    commit_id: &str,
) -> Result<Option<i64>, LixError> {
    let p1 = executor.dialect().placeholder(1);
    let sql = format!(
        "SELECT generation FROM {table} WHERE commit_id = {p1}",
        table = COMMIT_GRAPH_NODE_TABLE
    );
    let params = vec![Value::Text(commit_id.to_string())];
    let result = execute_query_with_executor(executor, &sql, &params).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    let Some(value) = row.first() else {
        return Ok(None);
    };
    match value {
        Value::Integer(value) => Ok(Some(*value)),
        Value::Null => Ok(None),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "commit graph generation must be integer".to_string(),
            hint: None,
        }),
    }
}

pub(crate) async fn load_checkpoint_labeled_entity_ids(
    executor: CanonicalExecutorRef<'_>,
    commit_ids: &[String],
) -> Result<BTreeSet<String>, LixError> {
    if commit_ids.is_empty() {
        return Ok(BTreeSet::new());
    }
    let label_entity_ids = commit_ids
        .iter()
        .map(|commit_id| checkpoint_commit_label_entity_id(commit_id))
        .collect::<Vec<_>>();
    let label_in_list = label_entity_ids
        .iter()
        .map(|entity_id| format!("'{}'", escape_sql_string(entity_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let label_sql = format!(
        "SELECT entity_id \
         FROM lix_internal_change \
         WHERE entity_id IN ({label_in_list}) \
           AND schema_key = 'lix_entity_label' \
           AND file_id IS NULL \
           AND plugin_key IS NULL"
    );
    let label_result = execute_query_with_executor(executor, &label_sql, &[]).await?;
    label_result
        .rows
        .iter()
        .map(|row| required_text_value(row, 0, "lix_internal_change.entity_id"))
        .collect()
}

pub(crate) async fn load_best_checkpoint_commit_id(
    executor: CanonicalExecutorRef<'_>,
    commit_ids: &[String],
) -> Result<Option<String>, LixError> {
    if commit_ids.is_empty() {
        return Ok(None);
    }
    let commit_in_list = commit_ids
        .iter()
        .map(|commit_id| format!("'{}'", escape_sql_string(commit_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let order_sql = format!(
        "SELECT commit_id \
         FROM {COMMIT_GRAPH_NODE_TABLE} \
         WHERE commit_id IN ({commit_in_list}) \
         ORDER BY generation DESC, commit_id DESC \
         LIMIT 1"
    );
    let rows = execute_query_with_executor(executor, &order_sql, &[]).await?;
    let Some(first) = rows.rows.first() else {
        return Ok(None);
    };
    Ok(Some(required_text_value(
        first,
        0,
        "lix_internal_commit_graph_node.commit_id",
    )?))
}

pub(crate) async fn load_commit_snapshot_content(
    executor: CanonicalExecutorRef<'_>,
    commit_id: &str,
) -> Result<Option<String>, LixError> {
    let sql = format!(
        "SELECT s.content AS snapshot_content \
         FROM lix_internal_change c \
         LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
         WHERE c.schema_key = 'lix_commit' \
           AND c.entity_id = '{commit_id}' \
           AND c.file_id IS NULL \
           AND c.plugin_key IS NULL \
           AND s.content IS NOT NULL \
         LIMIT 1",
        commit_id = escape_sql_string(commit_id),
    );
    let result = execute_query_with_executor(executor, &sql, &[]).await?;
    Ok(result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(text_from_value))
}

pub(crate) async fn load_change_row_by_id(
    executor: CanonicalExecutorRef<'_>,
    change_id: &str,
) -> Result<Option<CommittedCanonicalChangeRow>, LixError> {
    let sql = "SELECT c.id, c.entity_id, c.schema_key, c.schema_version, c.file_id, c.plugin_key, s.content, c.metadata, c.created_at \
               FROM lix_internal_change c \
               LEFT JOIN lix_internal_snapshot s ON s.id = c.snapshot_id \
               WHERE c.id = $1 \
               LIMIT 1";
    let result =
        execute_query_with_executor(executor, sql, &[Value::Text(change_id.to_string())]).await?;
    let Some(row) = result.rows.first() else {
        return Ok(None);
    };
    Ok(Some(CommittedCanonicalChangeRow {
        id: required_text_value(row, 0, "lix_internal_change.id")?,
        entity_id: required_text_value(row, 1, "lix_internal_change.entity_id")?,
        schema_key: required_text_value(row, 2, "lix_internal_change.schema_key")?,
        schema_version: required_text_value(row, 3, "lix_internal_change.schema_version")?,
        file_id: row.get(4).and_then(text_from_value),
        plugin_key: row.get(5).and_then(text_from_value),
        snapshot_content: row.get(6).and_then(text_from_value),
        metadata: row.get(7).and_then(text_from_value),
        created_at: required_text_value(row, 8, "lix_internal_change.created_at")?,
    }))
}

pub(crate) async fn resolve_last_checkpoint_commit_id_for_tip(
    executor: CanonicalExecutorRef<'_>,
    head_commit_id: &str,
) -> Result<Option<String>, LixError> {
    checkpoint_labels::resolve_last_checkpoint_commit_id_for_tip_with_executor(
        executor,
        head_commit_id,
    )
    .await
}

#[async_trait(?Send)]
impl CanonicalReadStore for SqlCanonicalReadStore<'_> {
    async fn load_commit(&mut self, commit_id: &str) -> Result<Option<CanonicalCommit>, LixError> {
        let _ = commit_id;
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }

    async fn load_change(&mut self, change_id: &str) -> Result<Option<CanonicalChange>, LixError> {
        let _ = change_id;
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }

    async fn load_history(
        &mut self,
        request: &CanonicalHistoryRequest,
    ) -> Result<Vec<CanonicalHistoryRow>, LixError> {
        api::load_history(self.backend, request).await
    }

    async fn load_visible_state(
        &mut self,
        request: &CanonicalVisibleStateRequest,
    ) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
        let _ = request;
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }

    async fn resolve_merge_base(
        &mut self,
        left_head_commit_id: &str,
        right_head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let _ = (left_head_commit_id, right_head_commit_id);
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }
}

#[async_trait(?Send)]
impl CanonicalReadStore for SqlCanonicalExecutorReadStore<'_> {
    async fn load_commit(&mut self, commit_id: &str) -> Result<Option<CanonicalCommit>, LixError> {
        api::load_commit(self.executor, commit_id).await
    }

    async fn load_change(&mut self, change_id: &str) -> Result<Option<CanonicalChange>, LixError> {
        api::load_change(self.executor, change_id).await
    }

    async fn load_history(
        &mut self,
        request: &CanonicalHistoryRequest,
    ) -> Result<Vec<CanonicalHistoryRow>, LixError> {
        let _ = request;
        Err(invalid_canonical_store_access(
            "backend-backed canonical read store",
        ))
    }

    async fn load_visible_state(
        &mut self,
        request: &CanonicalVisibleStateRequest,
    ) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
        api::load_visible_state(self.executor, request).await
    }

    async fn resolve_merge_base(
        &mut self,
        left_head_commit_id: &str,
        right_head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        api::resolve_merge_base(self.executor, left_head_commit_id, right_head_commit_id).await
    }
}

#[async_trait(?Send)]
impl CanonicalWriteStore for SqlCanonicalWriteStore<'_> {
    async fn append_changes(
        &mut self,
        changes: &[CanonicalChangeWrite],
        functions: &mut dyn LixFunctionProvider,
    ) -> Result<CanonicalAppendSummary, LixError> {
        api::append_changes(self.transaction, changes, functions).await
    }

    async fn append_untracked_change_visibility_rows(
        &mut self,
        visibility_rows: &[CanonicalUntrackedVisibilityWrite],
    ) -> Result<(), LixError> {
        api::append_untracked_change_visibility_rows(self.transaction, visibility_rows).await
    }

    async fn replace_snapshot_content(
        &mut self,
        snapshot_id: &str,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        replace_snapshot_content_in_transaction(self.transaction, snapshot_id, snapshot_content)
            .await
    }
}

fn invalid_canonical_store_access(expected: &str) -> LixError {
    LixError::unknown(format!("canonical store access requires a {expected}"))
}

fn text_from_value(value: &Value) -> Option<String> {
    match value {
        Value::Text(value) => Some(value.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Real(value) => Some(value.to_string()),
        _ => None,
    }
}

fn required_text_value(row: &[Value], index: usize, label: &str) -> Result<String, LixError> {
    match row.get(index) {
        Some(Value::Text(value)) if !value.is_empty() => Ok(value.clone()),
        Some(Value::Text(_)) => Err(LixError::unknown(format!("{label} is empty"))),
        Some(Value::Integer(value)) => Ok(value.to_string()),
        Some(other) => Err(LixError::unknown(format!(
            "expected text-like value for {label}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!("missing {label}"))),
    }
}

fn optional_text_value(
    row: &[Value],
    index: usize,
    label: &str,
) -> Result<Option<String>, LixError> {
    match row.get(index) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Text(value)) => Ok(Some(value.clone())),
        Some(Value::Integer(value)) => Ok(Some(value.to_string())),
        Some(other) => Err(LixError::unknown(format!(
            "expected nullable text-like value for {label}, got {other:?}"
        ))),
    }
}

fn required_integer_value(row: &[Value], index: usize, label: &str) -> Result<i64, LixError> {
    match row.get(index) {
        Some(Value::Integer(value)) => Ok(*value),
        Some(Value::Text(value)) => value
            .parse::<i64>()
            .map_err(|error| LixError::unknown(format!("{label} is not a valid integer: {error}"))),
        Some(other) => Err(LixError::unknown(format!(
            "expected integer-like value for {label}, got {other:?}"
        ))),
        None => Err(LixError::unknown(format!("missing {label}"))),
    }
}
