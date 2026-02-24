use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use sqlparser::ast::helpers::attached_token::AttachedToken;
use sqlparser::ast::{
    Assignment, AssignmentTarget, ConflictTarget, DoUpdate, Expr, Ident, ObjectName,
    ObjectNamePart, OnConflict, OnConflictAction, OnInsert, Query, SetExpr, Statement, TableObject,
    Value as SqlValue, Values,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::account::{
    active_account_file_id, active_account_schema_key, active_account_storage_version_id,
    parse_active_account_snapshot,
};
use crate::builtin_schema::types::LixVersionPointer;
use crate::commit::{
    generate_commit, DomainChangeInput, GenerateCommitArgs, GenerateCommitResult,
    MaterializedStateRow, VersionInfo, VersionSnapshot,
};
use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::version::{
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id,
};
use crate::{LixError, LixTransaction, QueryResult, SqlDialect, Value as EngineValue};

use super::sql2::ast::lowering::lower_statement;
use super::sql2::ast::utils::{bind_sql, bind_sql_with_state, PlaceholderState};
use super::sql2::contracts::effects::DetectedFileDomainChange;
use super::sql2::contracts::postprocess_actions::{VtableDeletePlan, VtableUpdatePlan};
use super::sql2::contracts::prepared_statement::PreparedStatement;

const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const SNAPSHOT_TABLE: &str = "lix_internal_snapshot";
const CHANGE_TABLE: &str = "lix_internal_change";
const MATERIALIZED_PREFIX: &str = "lix_internal_state_materialized_v1_";
const VERSION_POINTER_TABLE: &str = "lix_internal_state_materialized_v1_lix_version_pointer";
const VERSION_POINTER_SCHEMA_KEY: &str = "lix_version_pointer";
const CHANGE_AUTHOR_SCHEMA_KEY: &str = "lix_change_author";
const COMMIT_SCHEMA_KEY: &str = "lix_commit";
const COMMIT_EDGE_SCHEMA_KEY: &str = "lix_commit_edge";
const COMMIT_ANCESTRY_TABLE: &str = "lix_internal_commit_ancestry";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const GLOBAL_VERSION: &str = "global";
const SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 32_766;
const POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT: usize = 65_535;
const SNAPSHOT_INSERT_PARAM_COLUMNS: usize = 2;
const CHANGE_INSERT_PARAM_COLUMNS: usize = 9;
const MATERIALIZED_INSERT_PARAM_COLUMNS: usize = 13;
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

    async fn execute(
        &mut self,
        sql: &str,
        params: &[EngineValue],
    ) -> Result<QueryResult, LixError>;
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

fn bind_statement_batch_for_dialect(
    batch: StatementBatch,
    dialect: SqlDialect,
) -> Result<Vec<PreparedStatement>, LixError> {
    let mut prepared = Vec::with_capacity(batch.statements.len());
    for statement in batch.statements {
        let bound = bind_sql(&statement.to_string(), &batch.params, dialect)?;
        prepared.push(PreparedStatement {
            sql: bound.sql,
            params: bound.params,
        });
    }
    Ok(prepared)
}

async fn load_commit_active_accounts(
    executor: &mut dyn SqlExecutor,
    domain_changes: &[DomainChangeInput],
) -> Result<Vec<String>, LixError> {
    if domain_changes.is_empty() {
        return Ok(Vec::new());
    }

    if domain_changes
        .iter()
        .all(|change| change.schema_key == CHANGE_AUTHOR_SCHEMA_KEY)
    {
        return Ok(Vec::new());
    }

    let sql = format!(
        "SELECT snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}' \
           AND snapshot_content IS NOT NULL",
        table_name = UNTRACKED_TABLE,
        schema_key = escape_sql_string(active_account_schema_key()),
        file_id = escape_sql_string(active_account_file_id()),
        version_id = escape_sql_string(active_account_storage_version_id()),
    );
    let result = executor.execute(&sql, &[]).await?;

    let mut deduped = BTreeSet::new();
    for row in result.rows {
        let Some(value) = row.first() else {
            continue;
        };
        let snapshot = match value {
            EngineValue::Text(text) => text,
            EngineValue::Null => continue,
            _ => {
                return Err(LixError {
                    message: "active account snapshot_content must be text".to_string(),
                });
            }
        };
        let account_id = parse_active_account_snapshot(snapshot)?;
        deduped.insert(account_id);
    }

    Ok(deduped.into_iter().collect())
}

async fn load_version_info_for_versions(
    executor: &mut dyn SqlExecutor,
    version_ids: &BTreeSet<String>,
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let mut versions = BTreeMap::new();
    if version_ids.is_empty() {
        return Ok(versions);
    }

    for version_id in version_ids {
        versions.insert(
            version_id.clone(),
            VersionInfo {
                parent_commit_ids: Vec::new(),
                snapshot: VersionSnapshot {
                    id: version_id.clone(),
                    working_commit_id: version_id.clone(),
                },
            },
        );
    }

    let in_list = version_ids
        .iter()
        .map(|version_id| format!("'{}'", escape_sql_string(version_id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT entity_id, snapshot_content \
         FROM {table_name} \
         WHERE schema_key = '{schema_key}' \
           AND version_id = '{global_version}' \
           AND is_tombstone = 0 \
           AND snapshot_content IS NOT NULL \
           AND entity_id IN ({in_list})",
        table_name = VERSION_POINTER_TABLE,
        schema_key = VERSION_POINTER_SCHEMA_KEY,
        global_version = GLOBAL_VERSION,
        in_list = in_list,
    );

    match executor.execute(&sql, &[]).await {
        Ok(result) => {
            for row in result.rows {
                if row.len() < 2 {
                    continue;
                }
                let entity_id = match &row[0] {
                    EngineValue::Text(value) => value.clone(),
                    EngineValue::Null => continue,
                    _ => {
                        return Err(LixError {
                            message: "version tip entity_id must be text".to_string(),
                        });
                    }
                };
                if !version_ids.contains(&entity_id) {
                    continue;
                }
                let Some(parsed) = parse_version_info_from_tip_snapshot(&row[1], &entity_id)?
                else {
                    continue;
                };
                versions.insert(entity_id, parsed);
            }
        }
        Err(err) if is_missing_relation_error(&err) => {}
        Err(err) => return Err(err),
    }

    Ok(versions)
}

fn parse_version_info_from_tip_snapshot(
    value: &EngineValue,
    fallback_version_id: &str,
) -> Result<Option<VersionInfo>, LixError> {
    let raw_snapshot = match value {
        EngineValue::Text(value) => value,
        EngineValue::Null => return Ok(None),
        _ => {
            return Err(LixError {
                message: "version tip snapshot_content must be text".to_string(),
            });
        }
    };

    let snapshot: LixVersionPointer = serde_json::from_str(raw_snapshot).map_err(|error| LixError {
        message: format!("version tip snapshot_content invalid JSON: {error}"),
    })?;
    let version_id = if snapshot.id.is_empty() {
        fallback_version_id.to_string()
    } else {
        snapshot.id
    };
    let working_commit_id = snapshot
        .working_commit_id
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| fallback_version_id.to_string());
    let parent_commit_ids = if snapshot.commit_id.is_empty() || snapshot.commit_id == working_commit_id {
        Vec::new()
    } else {
        vec![snapshot.commit_id]
    };

    Ok(Some(VersionInfo {
        parent_commit_ids,
        snapshot: VersionSnapshot {
            id: version_id,
            working_commit_id,
        },
    }))
}

fn build_snapshot_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![Ident::new("id")])),
        action: OnConflictAction::DoNothing,
    })
}

fn build_materialized_on_conflict() -> OnInsert {
    OnInsert::OnConflict(OnConflict {
        conflict_target: Some(ConflictTarget::Columns(vec![
            Ident::new("entity_id"),
            Ident::new("file_id"),
            Ident::new("version_id"),
        ])),
        action: OnConflictAction::DoUpdate(DoUpdate {
            assignments: vec![
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("snapshot_content")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("snapshot_content"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("schema_version")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("schema_version"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("plugin_key")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("plugin_key"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("change_id")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("change_id"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("metadata")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("metadata"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("writer_key")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("writer_key"),
                    ]),
                },
                Assignment {
                    target: AssignmentTarget::ColumnName(ObjectName(vec![
                        ObjectNamePart::Identifier(Ident::new("updated_at")),
                    ])),
                    value: Expr::CompoundIdentifier(vec![
                        Ident::new("excluded"),
                        Ident::new("updated_at"),
                    ]),
                },
            ],
            selection: None,
        }),
    })
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
        let row_writer_key = match (&plan.explicit_writer_key, plan.writer_key_assignment_present) {
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

    let versions = load_version_info_for_versions(executor, &affected_versions).await?;
    let active_accounts = load_commit_active_accounts(executor, &domain_changes).await?;
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp,
            active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )?;
    build_statements_from_generate_commit_result(commit_result, functions, 0, executor.dialect())
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

    let versions = load_version_info_for_versions(executor, &affected_versions).await?;
    let active_accounts = load_commit_active_accounts(executor, &domain_changes).await?;
    let commit_result = generate_commit(
        GenerateCommitArgs {
            timestamp,
            active_accounts,
            changes: domain_changes,
            versions,
        },
        || functions.uuid_v7(),
    )?;
    build_statements_from_generate_commit_result(commit_result, functions, 0, executor.dialect())
}

struct EffectiveScopeDeleteRow {
    entity_id: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
    schema_version: String,
    metadata: Option<String>,
}

struct StatementBatch {
    statements: Vec<Statement>,
    params: Vec<EngineValue>,
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
               lix_json_text(snapshot_content, 'id') AS version_id, \
               lix_json_text(snapshot_content, 'inherits_from_version_id') AS inherits_from_version_id \
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
               0 AS untracked, \
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
           AND untracked = 0",
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
               AND lix_json_text(m.snapshot_content, 'directory_id') IN ({in_list})",
            materialized_table = format!("{MATERIALIZED_PREFIX}{FILE_DESCRIPTOR_SCHEMA_KEY}"),
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

fn build_statements_from_generate_commit_result(
    commit_result: GenerateCommitResult,
    functions: &mut dyn LixFunctionProvider,
    placeholder_offset: usize,
    dialect: SqlDialect,
) -> Result<StatementBatch, LixError> {
    let mut ensure_no_content = false;
    let mut snapshot_rows = Vec::new();
    let mut statement_params = Vec::new();
    let mut next_placeholder = placeholder_offset + 1;
    let mut change_rows = Vec::new();
    let mut materialized_by_schema: BTreeMap<String, Vec<Vec<Expr>>> = BTreeMap::new();

    for change in &commit_result.changes {
        let snapshot_id = match &change.snapshot_content {
            Some(content) => {
                let id = functions.uuid_v7();
                let id_placeholder = next_placeholder;
                next_placeholder += 1;
                statement_params.push(EngineValue::Text(id.clone()));
                let content_placeholder = next_placeholder;
                next_placeholder += 1;
                statement_params.push(EngineValue::Text(content.clone()));
                snapshot_rows.push(vec![
                    placeholder_expr(id_placeholder),
                    placeholder_expr(content_placeholder),
                ]);
                id
            }
            None => {
                ensure_no_content = true;
                "no-content".to_string()
            }
        };

        change_rows.push(vec![
            text_param_expr(&change.id, &mut next_placeholder, &mut statement_params),
            text_param_expr(&change.entity_id, &mut next_placeholder, &mut statement_params),
            text_param_expr(
                &change.schema_key,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.schema_version,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(&change.file_id, &mut next_placeholder, &mut statement_params),
            text_param_expr(
                &change.plugin_key,
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(&snapshot_id, &mut next_placeholder, &mut statement_params),
            optional_text_param_expr(
                change.metadata.as_deref(),
                &mut next_placeholder,
                &mut statement_params,
            ),
            text_param_expr(
                &change.created_at,
                &mut next_placeholder,
                &mut statement_params,
            ),
        ]);
    }

    for row in &commit_result.materialized_state {
        materialized_by_schema
            .entry(row.schema_key.clone())
            .or_default()
            .push(materialized_row_values_parameterized(
                row,
                &mut next_placeholder,
                &mut statement_params,
            ));
    }

    let mut statements = Vec::new();
    if ensure_no_content {
        statements.push(make_insert_statement(
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            vec![vec![string_expr("no-content"), null_expr()]],
            Some(build_snapshot_on_conflict()),
        ));
    }

    if !snapshot_rows.is_empty() {
        push_chunked_insert_statements(
            &mut statements,
            SNAPSHOT_TABLE,
            vec![Ident::new("id"), Ident::new("content")],
            snapshot_rows,
            Some(build_snapshot_on_conflict()),
            max_rows_per_insert_for_dialect(dialect, SNAPSHOT_INSERT_PARAM_COLUMNS),
        );
    }

    if !change_rows.is_empty() {
        push_chunked_insert_statements(
            &mut statements,
            CHANGE_TABLE,
            vec![
                Ident::new("id"),
                Ident::new("entity_id"),
                Ident::new("schema_key"),
                Ident::new("schema_version"),
                Ident::new("file_id"),
                Ident::new("plugin_key"),
                Ident::new("snapshot_id"),
                Ident::new("metadata"),
                Ident::new("created_at"),
            ],
            change_rows,
            None,
            max_rows_per_insert_for_dialect(dialect, CHANGE_INSERT_PARAM_COLUMNS),
        );
    }

    for (schema_key, rows) in materialized_by_schema {
        let table_name = format!("{}{}", MATERIALIZED_PREFIX, schema_key);
        push_chunked_insert_statements(
            &mut statements,
            &table_name,
            vec![
                Ident::new("entity_id"),
                Ident::new("schema_key"),
                Ident::new("schema_version"),
                Ident::new("file_id"),
                Ident::new("version_id"),
                Ident::new("plugin_key"),
                Ident::new("snapshot_content"),
                Ident::new("change_id"),
                Ident::new("metadata"),
                Ident::new("writer_key"),
                Ident::new("is_tombstone"),
                Ident::new("created_at"),
                Ident::new("updated_at"),
            ],
            rows,
            Some(build_materialized_on_conflict()),
            max_rows_per_insert_for_dialect(dialect, MATERIALIZED_INSERT_PARAM_COLUMNS),
        );
    }

    append_commit_ancestry_statements(
        &mut statements,
        &mut statement_params,
        &mut next_placeholder,
        &commit_result.materialized_state,
    )?;

    Ok(StatementBatch {
        statements,
        params: statement_params,
    })
}

fn max_bind_parameters_for_dialect(dialect: SqlDialect) -> usize {
    match dialect {
        SqlDialect::Sqlite => SQLITE_MAX_BIND_PARAMETERS_PER_STATEMENT,
        SqlDialect::Postgres => POSTGRES_MAX_BIND_PARAMETERS_PER_STATEMENT,
    }
}

fn max_rows_per_insert_for_dialect(dialect: SqlDialect, params_per_row: usize) -> usize {
    (max_bind_parameters_for_dialect(dialect) / params_per_row).max(1)
}

fn push_chunked_insert_statements(
    statements: &mut Vec<Statement>,
    table: &str,
    columns: Vec<Ident>,
    rows: Vec<Vec<Expr>>,
    on: Option<OnInsert>,
    max_rows_per_statement: usize,
) {
    if rows.is_empty() {
        return;
    }

    if rows.len() <= max_rows_per_statement {
        statements.push(make_insert_statement(table, columns, rows, on));
        return;
    }

    let mut chunk = Vec::with_capacity(max_rows_per_statement);
    for row in rows {
        chunk.push(row);
        if chunk.len() == max_rows_per_statement {
            statements.push(make_insert_statement(
                table,
                columns.clone(),
                std::mem::take(&mut chunk),
                on.clone(),
            ));
        }
    }

    if !chunk.is_empty() {
        statements.push(make_insert_statement(table, columns, chunk, on));
    }
}

fn append_commit_ancestry_statements(
    statements: &mut Vec<Statement>,
    params: &mut Vec<EngineValue>,
    next_placeholder: &mut usize,
    materialized_state: &[MaterializedStateRow],
) -> Result<(), LixError> {
    let commit_parents = collect_commit_parent_map_for_ancestry(materialized_state)?;
    for (commit_id, parent_ids) in commit_parents {
        let commit_placeholder = *next_placeholder;
        *next_placeholder += 1;
        params.push(EngineValue::Text(commit_id));

        let self_insert_sql = format!(
            "INSERT INTO {table} (commit_id, ancestor_id, depth) \
             VALUES (?{commit_placeholder}, ?{commit_placeholder}, 0) \
             ON CONFLICT (commit_id, ancestor_id) DO NOTHING",
            table = COMMIT_ANCESTRY_TABLE,
            commit_placeholder = commit_placeholder,
        );
        statements.push(parse_single_statement_from_sql(&self_insert_sql)?);

        for parent_id in parent_ids {
            let parent_placeholder = *next_placeholder;
            *next_placeholder += 1;
            params.push(EngineValue::Text(parent_id));

            let insert_parent_ancestry_sql = format!(
                "INSERT INTO {table} (commit_id, ancestor_id, depth) \
                 SELECT ?{commit_placeholder} AS commit_id, candidate.ancestor_id, MIN(candidate.depth) AS depth \
                 FROM ( \
                   SELECT ?{parent_placeholder} AS ancestor_id, 1 AS depth \
                   UNION ALL \
                   SELECT ancestor_id, depth + 1 AS depth \
                   FROM {table} \
                   WHERE commit_id = ?{parent_placeholder} \
                 ) AS candidate \
                 GROUP BY candidate.ancestor_id \
                 ON CONFLICT (commit_id, ancestor_id) DO UPDATE \
                 SET depth = CASE \
                   WHEN excluded.depth < {table}.depth THEN excluded.depth \
                   ELSE {table}.depth \
                 END",
                table = COMMIT_ANCESTRY_TABLE,
                commit_placeholder = commit_placeholder,
                parent_placeholder = parent_placeholder,
            );
            statements.push(parse_single_statement_from_sql(&insert_parent_ancestry_sql)?);
        }
    }
    Ok(())
}

fn collect_commit_parent_map_for_ancestry(
    materialized_state: &[MaterializedStateRow],
) -> Result<BTreeMap<String, BTreeSet<String>>, LixError> {
    let mut out = BTreeMap::<String, BTreeSet<String>>::new();
    for row in materialized_state {
        if row.schema_key == COMMIT_SCHEMA_KEY && row.lixcol_version_id == GLOBAL_VERSION {
            out.entry(row.entity_id.clone()).or_default();
        }
    }

    for row in materialized_state {
        if row.schema_key != COMMIT_EDGE_SCHEMA_KEY || row.lixcol_version_id != GLOBAL_VERSION {
            continue;
        }
        let Some(raw) = row.snapshot_content.as_deref() else {
            continue;
        };
        let Some((parent_id, child_id)) = parse_commit_edge_snapshot_for_ancestry(raw)? else {
            continue;
        };
        if let Some(parents) = out.get_mut(&child_id) {
            parents.insert(parent_id);
        }
    }

    Ok(out)
}

fn parse_commit_edge_snapshot_for_ancestry(
    raw: &str,
) -> Result<Option<(String, String)>, LixError> {
    let parsed: JsonValue = serde_json::from_str(raw).map_err(|error| LixError {
        message: format!("vtable write commit_edge snapshot invalid JSON: {error}"),
    })?;
    let parent_id = parsed
        .get("parent_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    let child_id = parsed
        .get("child_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string);
    match (parent_id, child_id) {
        (Some(parent_id), Some(child_id)) => Ok(Some((parent_id, child_id))),
        _ => Ok(None),
    }
}

fn parse_single_statement_from_sql(sql: &str) -> Result<Statement, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        message: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            message: "expected a single statement".to_string(),
        });
    }
    Ok(statements.remove(0))
}

fn text_param_expr(
    value: &str,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Expr {
    let index = *next_placeholder;
    *next_placeholder += 1;
    params.push(EngineValue::Text(value.to_string()));
    placeholder_expr(index)
}

fn optional_text_param_expr(
    value: Option<&str>,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Expr {
    match value {
        Some(value) => text_param_expr(value, next_placeholder, params),
        None => null_expr(),
    }
}

fn materialized_row_values_parameterized(
    row: &MaterializedStateRow,
    next_placeholder: &mut usize,
    params: &mut Vec<EngineValue>,
) -> Vec<Expr> {
    vec![
        text_param_expr(&row.entity_id, next_placeholder, params),
        text_param_expr(&row.schema_key, next_placeholder, params),
        text_param_expr(&row.schema_version, next_placeholder, params),
        text_param_expr(&row.file_id, next_placeholder, params),
        text_param_expr(&row.lixcol_version_id, next_placeholder, params),
        text_param_expr(&row.plugin_key, next_placeholder, params),
        optional_text_param_expr(row.snapshot_content.as_deref(), next_placeholder, params),
        text_param_expr(&row.id, next_placeholder, params),
        optional_text_param_expr(row.metadata.as_deref(), next_placeholder, params),
        optional_text_param_expr(row.writer_key.as_deref(), next_placeholder, params),
        number_expr("0"),
        text_param_expr(&row.created_at, next_placeholder, params),
        text_param_expr(&row.created_at, next_placeholder, params),
    ]
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

fn is_missing_relation_error(err: &LixError) -> bool {
    let lower = err.message.to_lowercase();
    lower.contains("no such table")
        || lower.contains("relation")
            && (lower.contains("does not exist")
                || lower.contains("undefined table")
                || lower.contains("unknown"))
}

fn make_insert_statement(
    table: &str,
    columns: Vec<Ident>,
    rows: Vec<Vec<Expr>>,
    on: Option<OnInsert>,
) -> Statement {
    let values = Values {
        explicit_row: false,
        value_keyword: false,
        rows,
    };
    let query = Query {
        with: None,
        body: Box::new(SetExpr::Values(values)),
        order_by: None,
        limit_clause: None,
        fetch: None,
        locks: Vec::new(),
        for_clause: None,
        settings: None,
        format_clause: None,
        pipe_operators: Vec::new(),
    };

    Statement::Insert(sqlparser::ast::Insert {
        insert_token: AttachedToken::empty(),
        or: None,
        ignore: false,
        into: true,
        table: TableObject::TableName(ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            table,
        ))])),
        table_alias: None,
        columns,
        overwrite: false,
        source: Some(Box::new(query)),
        assignments: Vec::new(),
        partitioned: None,
        after_columns: Vec::new(),
        has_table_keyword: false,
        on,
        returning: None,
        replace_into: false,
        priority: None,
        insert_alias: None,
        settings: None,
        format_clause: None,
    })
}

fn string_expr(value: &str) -> Expr {
    Expr::Value(SqlValue::SingleQuotedString(value.to_string()).into())
}

fn placeholder_expr(index_1_based: usize) -> Expr {
    Expr::Value(SqlValue::Placeholder(format!("${index_1_based}")).into())
}

fn number_expr(value: &str) -> Expr {
    Expr::Value(SqlValue::Number(value.to_string(), false).into())
}

fn null_expr() -> Expr {
    Expr::Value(SqlValue::Null.into())
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
