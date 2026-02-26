use std::collections::{BTreeMap, BTreeSet};

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::commit::{generate_commit, DomainChangeInput, GenerateCommitArgs};
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::version::{
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
};
use crate::{LixError, LixTransaction, QueryResult, SqlDialect, Value as EngineValue};

use super::super::ast::lowering::lower_statement;
use super::super::ast::utils::{bind_sql_with_state, PlaceholderState};
use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::postprocess_actions::{VtableDeletePlan, VtableUpdatePlan};
use super::super::contracts::prepared_statement::PreparedStatement;
use super::super::history::commit_runtime::{
    bind_statement_batch_for_dialect, build_statement_batch_from_generate_commit_result,
    load_commit_active_accounts, load_version_info_for_versions, CommitQueryExecutor,
    StatementBatch,
};
use super::super::storage::sql_text::escape_sql_string;

const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const UPDATE_RETURNING_COLUMNS: &[&str] = &[
    "entity_id",
    "file_id",
    "version_id",
    "plugin_key",
    "schema_version",
    "snapshot_content",
    "metadata",
    "writer_key",
    "updated_at",
];

#[async_trait::async_trait(?Send)]
trait SqlExecutor {
    fn dialect(&self) -> SqlDialect;

    async fn execute(&mut self, sql: &str, params: &[EngineValue])
        -> Result<QueryResult, LixError>;
}

struct TransactionExecutor<'a> {
    transaction: &'a mut dyn LixTransaction,
}

#[async_trait::async_trait(?Send)]
impl SqlExecutor for TransactionExecutor<'_> {
    fn dialect(&self) -> SqlDialect {
        self.transaction.dialect()
    }

    async fn execute(
        &mut self,
        sql: &str,
        params: &[EngineValue],
    ) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }
}

struct CommitExecutorAdapter<'a> {
    executor: &'a mut dyn SqlExecutor,
}

#[async_trait::async_trait(?Send)]
impl CommitQueryExecutor for CommitExecutorAdapter<'_> {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[EngineValue],
    ) -> Result<QueryResult, LixError> {
        self.executor.execute(sql, params).await
    }
}

pub(crate) async fn build_update_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableUpdatePlan,
    rows: &[Vec<EngineValue>],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let mut executor = TransactionExecutor { transaction };
    let batch = build_update_followup_statement_batch(
        &mut executor,
        plan,
        rows,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await?;
    bind_statement_batch_for_dialect(batch, executor.dialect())
}

pub(crate) async fn build_delete_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableDeletePlan,
    rows: &[Vec<EngineValue>],
    params: &[EngineValue],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<Vec<PreparedStatement>, LixError> {
    let mut executor = TransactionExecutor { transaction };
    let batch = build_delete_followup_statement_batch(
        &mut executor,
        plan,
        rows,
        params,
        detected_file_domain_changes,
        writer_key,
        functions,
    )
    .await?;
    bind_statement_batch_for_dialect(batch, executor.dialect())
}

async fn build_update_followup_statement_batch(
    executor: &mut dyn SqlExecutor,
    plan: &VtableUpdatePlan,
    rows: &[Vec<EngineValue>],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<StatementBatch, LixError> {
    if rows.is_empty() && detected_file_domain_changes.is_empty() {
        return Ok(StatementBatch {
            statements: Vec::new(),
            params: Vec::new(),
        });
    }

    let timestamp = functions.timestamp();
    let mut domain_changes = Vec::new();
    let mut affected_versions = BTreeSet::new();

    for row in rows {
        if row.len() < UPDATE_RETURNING_COLUMNS.len() {
            return Err(LixError {
                message: "vtable update returning row missing columns".to_string(),
            });
        }

        let entity_id = value_to_string(&row[0], "entity_id")?;
        let file_id = value_to_string(&row[1], "file_id")?;
        let version_id = value_to_string(&row[2], "version_id")?;
        let plugin_key = value_to_string(&row[3], "plugin_key")?;
        let schema_version = value_to_string(&row[4], "schema_version")?;
        let snapshot_content = value_to_optional_text(&row[5], "snapshot_content")?;
        let metadata = value_to_optional_text(&row[6], "metadata")?;
        let row_writer_key = match (
            &plan.explicit_writer_key,
            plan.writer_key_assignment_present,
        ) {
            (Some(explicit), _) => explicit.clone(),
            (None, true) => value_to_optional_text(&row[7], "writer_key")?,
            (None, false) => writer_key.map(ToString::to_string),
        };

        affected_versions.insert(version_id.clone());
        domain_changes.push(DomainChangeInput {
            id: functions.uuid_v7(),
            entity_id,
            schema_key: plan.schema_key.clone(),
            schema_version,
            file_id,
            version_id,
            plugin_key,
            snapshot_content,
            metadata,
            created_at: timestamp.clone(),
            writer_key: row_writer_key,
        });
    }

    for change in detected_file_domain_changes {
        affected_versions.insert(change.version_id.clone());
        let domain_writer_key = change
            .writer_key
            .clone()
            .or_else(|| writer_key.map(ToString::to_string));
        domain_changes.push(DomainChangeInput {
            id: functions.uuid_v7(),
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            version_id: change.version_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: change.snapshot_content.clone(),
            metadata: change.metadata.clone(),
            created_at: timestamp.clone(),
            writer_key: domain_writer_key,
        });
    }

    let mut commit_executor = CommitExecutorAdapter { executor };
    let versions = load_version_info_for_versions(&mut commit_executor, &affected_versions).await?;
    let active_accounts =
        load_commit_active_accounts(&mut commit_executor, &domain_changes).await?;
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp,
            active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )?;
    build_statement_batch_from_generate_commit_result(
        commit_result,
        functions,
        0,
        executor.dialect(),
    )
}

async fn build_delete_followup_statement_batch(
    executor: &mut dyn SqlExecutor,
    plan: &VtableDeletePlan,
    rows: &[Vec<EngineValue>],
    params: &[EngineValue],
    detected_file_domain_changes: &[DetectedFileDomainChange],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<StatementBatch, LixError> {
    let timestamp = functions.timestamp();
    let mut domain_changes = Vec::new();
    let mut affected_versions = BTreeSet::new();
    let mut deleted_directory_scopes: Vec<(String, String)> = Vec::new();
    let mut tombstoned_keys: BTreeSet<(String, String, String)> = BTreeSet::new();

    for row in rows {
        if row.len() < UPDATE_RETURNING_COLUMNS.len() {
            return Err(LixError {
                message: "vtable delete returning row missing columns".to_string(),
            });
        }

        let entity_id = value_to_string(&row[0], "entity_id")?;
        let file_id = value_to_string(&row[1], "file_id")?;
        let version_id = value_to_string(&row[2], "version_id")?;
        let plugin_key = value_to_string(&row[3], "plugin_key")?;
        let schema_version = value_to_string(&row[4], "schema_version")?;
        let _snapshot_content = value_to_optional_text(&row[5], "snapshot_content")?;
        let metadata = value_to_optional_text(&row[6], "metadata")?;
        let row_writer_key = writer_key.map(ToString::to_string);
        tombstoned_keys.insert((entity_id.clone(), file_id.clone(), version_id.clone()));
        if plan.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
            deleted_directory_scopes.push((version_id.clone(), entity_id.clone()));
        }
        affected_versions.insert(version_id.clone());
        domain_changes.push(DomainChangeInput {
            id: functions.uuid_v7(),
            entity_id,
            schema_key: plan.schema_key.clone(),
            schema_version,
            file_id,
            version_id,
            plugin_key,
            snapshot_content: None,
            metadata,
            created_at: timestamp.clone(),
            writer_key: row_writer_key,
        });
    }

    if plan.effective_scope_fallback {
        for fallback_row in load_effective_scope_delete_rows(executor, plan, params).await? {
            let key = (
                fallback_row.entity_id.clone(),
                fallback_row.file_id.clone(),
                fallback_row.version_id.clone(),
            );
            if !tombstoned_keys.insert(key) {
                continue;
            }
            let row_writer_key = writer_key.map(ToString::to_string);
            if plan.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
                deleted_directory_scopes.push((
                    fallback_row.version_id.clone(),
                    fallback_row.entity_id.clone(),
                ));
            }
            affected_versions.insert(fallback_row.version_id.clone());
            domain_changes.push(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id: fallback_row.entity_id,
                schema_key: plan.schema_key.clone(),
                schema_version: fallback_row.schema_version,
                file_id: fallback_row.file_id,
                version_id: fallback_row.version_id,
                plugin_key: fallback_row.plugin_key,
                snapshot_content: None,
                metadata: fallback_row.metadata,
                created_at: timestamp.clone(),
                writer_key: row_writer_key,
            });
        }
    }

    if plan.schema_key == DIRECTORY_DESCRIPTOR_SCHEMA_KEY {
        let cascaded_file_deletes = load_cascaded_file_delete_changes(
            executor,
            &deleted_directory_scopes,
            &timestamp,
            writer_key,
            functions,
        )
        .await?;
        for change in cascaded_file_deletes {
            affected_versions.insert(change.version_id.clone());
            domain_changes.push(change);
        }
    }

    for change in detected_file_domain_changes {
        affected_versions.insert(change.version_id.clone());
        let domain_writer_key = change
            .writer_key
            .clone()
            .or_else(|| writer_key.map(ToString::to_string));
        domain_changes.push(DomainChangeInput {
            id: functions.uuid_v7(),
            entity_id: change.entity_id.clone(),
            schema_key: change.schema_key.clone(),
            schema_version: change.schema_version.clone(),
            file_id: change.file_id.clone(),
            version_id: change.version_id.clone(),
            plugin_key: change.plugin_key.clone(),
            snapshot_content: change.snapshot_content.clone(),
            metadata: change.metadata.clone(),
            created_at: timestamp.clone(),
            writer_key: domain_writer_key,
        });
    }

    if domain_changes.is_empty() {
        return Ok(StatementBatch {
            statements: Vec::new(),
            params: Vec::new(),
        });
    }

    let mut commit_executor = CommitExecutorAdapter { executor };
    let versions = load_version_info_for_versions(&mut commit_executor, &affected_versions).await?;
    let active_accounts =
        load_commit_active_accounts(&mut commit_executor, &domain_changes).await?;
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp,
            active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )?;
    build_statement_batch_from_generate_commit_result(
        commit_result,
        functions,
        0,
        executor.dialect(),
    )
}

struct EffectiveScopeDeleteRow {
    entity_id: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
}

async fn load_effective_scope_delete_rows(
    executor: &mut dyn SqlExecutor,
    plan: &VtableDeletePlan,
    params: &[EngineValue],
) -> Result<Vec<EffectiveScopeDeleteRow>, LixError> {
    let Some(selection_sql) = plan.effective_scope_selection_sql.as_deref() else {
        return Ok(Vec::new());
    };

    let schema_table = quote_ident(&format!("{MATERIALIZED_PREFIX}{}", plan.schema_key));
    let descriptor_table = quote_ident(&format!(
        "{MATERIALIZED_PREFIX}{}",
        version_descriptor_schema_key()
    ));
    let sql = format!(
        "WITH RECURSIVE \
           version_descriptor AS ( \
             SELECT \
               lix_json_extract(snapshot_content, 'id') AS version_id, \
               lix_json_extract(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
             FROM {descriptor_table} \
             WHERE schema_key = '{descriptor_schema_key}' \
               AND file_id = '{descriptor_file_id}' \
               AND version_id = '{descriptor_storage_version_id}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           all_target_versions AS ( \
             SELECT DISTINCT version_id FROM {schema_table} \
             UNION \
             SELECT DISTINCT version_id FROM version_descriptor \
           ), \
           version_chain(target_version_id, ancestor_version_id, depth) AS ( \
             SELECT version_id AS target_version_id, version_id AS ancestor_version_id, 0 AS depth \
             FROM all_target_versions \
             UNION ALL \
             SELECT \
               vc.target_version_id, \
               vd.inherits_from_version_id AS ancestor_version_id, \
               vc.depth + 1 AS depth \
             FROM version_chain vc \
             JOIN version_descriptor vd ON vd.version_id = vc.ancestor_version_id \
             WHERE vd.inherits_from_version_id IS NOT NULL \
               AND vc.depth < 64 \
           ), \
           ranked AS ( \
             SELECT \
               s.entity_id AS entity_id, \
               s.file_id AS file_id, \
               vc.target_version_id AS version_id, \
               s.plugin_key AS plugin_key, \
               s.schema_version AS schema_version, \
               s.metadata AS metadata, \
               s.snapshot_content AS snapshot_content, \
               '{schema_key}' AS schema_key, \
               false AS untracked, \
               CASE \
                 WHEN s.inherited_from_version_id IS NOT NULL THEN s.inherited_from_version_id \
                 WHEN vc.depth = 0 THEN NULL \
                 ELSE s.version_id \
               END AS inherited_from_version_id, \
               ROW_NUMBER() OVER ( \
                 PARTITION BY vc.target_version_id, s.entity_id, s.file_id \
                 ORDER BY vc.depth ASC \
               ) AS rn \
             FROM {schema_table} s \
             JOIN version_chain vc ON vc.ancestor_version_id = s.version_id \
           ) \
         SELECT entity_id, file_id, version_id, plugin_key, schema_version, metadata \
         FROM ranked \
         WHERE rn = 1 \
           AND snapshot_content IS NOT NULL \
           AND ({selection_sql}) \
           AND untracked = false",
        descriptor_table = descriptor_table,
        descriptor_schema_key = escape_sql_string(version_descriptor_schema_key()),
        descriptor_file_id = escape_sql_string(version_descriptor_file_id()),
        descriptor_storage_version_id = escape_sql_string(version_descriptor_storage_version_id()),
        schema_table = schema_table,
        schema_key = escape_sql_string(&plan.schema_key),
    );
    let lowered_sql = lower_single_statement_for_dialect(&sql, executor.dialect())?;
    let bound = bind_sql_with_state(
        &lowered_sql,
        params,
        executor.dialect(),
        PlaceholderState::new(),
    )?;
    let result = executor.execute(&bound.sql, &bound.params).await?;

    let mut resolved = Vec::with_capacity(result.rows.len());
    for row in result.rows {
        if row.len() < 6 {
            return Err(LixError {
                message: "effective scope delete row loader expected six columns".to_string(),
            });
        }
        resolved.push(EffectiveScopeDeleteRow {
            entity_id: value_to_string(&row[0], "entity_id")?,
            file_id: value_to_string(&row[1], "file_id")?,
            version_id: value_to_string(&row[2], "version_id")?,
            plugin_key: value_to_string(&row[3], "plugin_key")?,
            schema_version: value_to_string(&row[4], "schema_version")?,
            metadata: value_to_optional_text(&row[5], "metadata")?,
        });
    }
    Ok(resolved)
}

async fn load_cascaded_file_delete_changes(
    executor: &mut dyn SqlExecutor,
    directory_scopes: &[(String, String)],
    timestamp: &str,
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<Vec<DomainChangeInput>, LixError> {
    if directory_scopes.is_empty() {
        return Ok(Vec::new());
    }

    let mut grouped_directory_ids: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
    for (version_id, directory_id) in directory_scopes {
        grouped_directory_ids
            .entry(version_id.clone())
            .or_default()
            .insert(directory_id.clone());
    }

    let mut changes = Vec::new();
    let mut seen_file_versions: BTreeSet<(String, String)> = BTreeSet::new();
    for (version_id, directory_ids) in grouped_directory_ids {
        if directory_ids.is_empty() {
            continue;
        }
        let materialized_table = format!("{MATERIALIZED_PREFIX}{FILE_DESCRIPTOR_SCHEMA_KEY}");
        let in_list = directory_ids
            .iter()
            .map(|directory_id| format!("'{}'", escape_sql_string(directory_id)))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            "SELECT \
                m.entity_id, \
                m.file_id, \
                m.version_id, \
                m.plugin_key, \
                m.schema_version, \
                m.metadata \
             FROM {materialized_table} m \
             WHERE m.version_id = '{version_id}' \
               AND m.is_tombstone = 0 \
               AND lix_json_extract(m.snapshot_content, 'directory_id') IN ({in_list})",
            materialized_table = materialized_table,
            version_id = escape_sql_string(&version_id),
            in_list = in_list,
        );
        let lowered_sql = lower_single_statement_for_dialect(&sql, executor.dialect())?;
        let result = executor.execute(&lowered_sql, &[]).await?;
        for row in result.rows {
            if row.len() < 6 {
                return Err(LixError {
                    message: "filesystem directory delete cascade expected six file columns"
                        .to_string(),
                });
            }
            let entity_id = value_to_string(&row[0], "entity_id")?;
            let file_id = value_to_string(&row[1], "file_id")?;
            let cascaded_version_id = value_to_string(&row[2], "version_id")?;
            let plugin_key = value_to_string(&row[3], "plugin_key")?;
            let schema_version = value_to_string(&row[4], "schema_version")?;
            let metadata = value_to_optional_text(&row[5], "metadata")?;

            if !seen_file_versions.insert((entity_id.clone(), cascaded_version_id.clone())) {
                continue;
            }

            changes.push(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id,
                schema_key: FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                schema_version,
                file_id,
                version_id: cascaded_version_id,
                plugin_key,
                snapshot_content: None,
                metadata,
                created_at: timestamp.to_string(),
                writer_key: writer_key.map(ToString::to_string),
            });
        }
    }

    Ok(changes)
}

fn lower_single_statement_for_dialect(sql: &str, dialect: SqlDialect) -> Result<String, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single statement".to_string(),
        });
    }
    let statement = statements.remove(0);
    let lowered = lower_statement(statement, dialect)?;
    Ok(lowered.to_string())
}

fn value_to_optional_text(value: &EngineValue, name: &str) -> Result<Option<String>, LixError> {
    match value {
        EngineValue::Null => Ok(None),
        EngineValue::Text(text) => Ok(Some(text.clone())),
        _ => Err(LixError {
            message: format!("vtable update expected text or null for {name}"),
        }),
    }
}

fn value_to_string(value: &EngineValue, name: &str) -> Result<String, LixError> {
    match value {
        EngineValue::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            message: format!("vtable update expected text for {name}"),
        }),
    }
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
