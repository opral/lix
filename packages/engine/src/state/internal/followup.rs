use std::collections::BTreeSet;

use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::deterministic_mode::RuntimeFunctionProvider;
use crate::engine::collect_postprocess_file_cache_targets;
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema::live_layout::{
    logical_live_snapshot_from_row_with_layout, tracked_live_table_name, untracked_live_table_name,
};
use crate::state::commit::{generate_commit, DomainChangeInput, GenerateCommitArgs};
use crate::state::stream::{
    state_commit_stream_changes_from_postprocess_rows, StateCommitStreamChange,
    StateCommitStreamOperation,
};
use crate::version::{
    version_descriptor_file_id, version_descriptor_schema_key,
    version_descriptor_storage_version_id, GLOBAL_VERSION_ID,
};
use crate::{
    CanonicalJson, LixError, LixTransaction, QueryResult, SqlDialect, Value as EngineValue,
};

use crate::sql::ast::lowering::lower_statement;
use crate::sql::ast::utils::{bind_sql_with_state, PlaceholderState};
use crate::sql::execution::contracts::prepared_statement::{PreparedBatch, PreparedStatement};
use crate::sql::execution::execute_prepared::{
    execute_prepared_with_backend, execute_prepared_with_transaction,
};
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::commit::{
    build_prepared_batch_from_generate_commit_result_with_executor, load_commit_active_accounts,
    load_version_info_for_versions, CommitQueryExecutor,
};
use crate::state::internal::write_program::WriteProgram;
use crate::state::internal::{
    InternalStatePlan, PostprocessPlan, VtableDeletePlan, VtableUpdatePlan,
};
use crate::LixBackend;

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

pub(crate) struct PostprocessExecutionOutcome {
    pub(crate) internal_result: QueryResult,
    pub(crate) postprocess_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
}

pub(crate) async fn execute_internal_state_plan_with_backend(
    backend: &dyn LixBackend,
    prepared_statements: &[PreparedStatement],
    internal_state: Option<&InternalStatePlan>,
    should_refresh_file_cache: bool,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<PostprocessExecutionOutcome, LixError> {
    let Some(postprocess_plan) = internal_state.and_then(|plan| plan.postprocess.as_ref()) else {
        return Ok(PostprocessExecutionOutcome {
            internal_result: execute_prepared_with_backend(backend, prepared_statements).await?,
            postprocess_file_cache_targets: BTreeSet::new(),
            state_commit_stream_changes: Vec::new(),
        });
    };

    let mut transaction = backend.begin_transaction().await?;
    let outcome = match execute_postprocess_with_transaction(
        transaction.as_mut(),
        prepared_statements,
        postprocess_plan,
        should_refresh_file_cache,
        functions,
        writer_key,
    )
    .await
    {
        Ok(outcome) => outcome,
        Err(error) => {
            let _ = transaction.rollback().await;
            return Err(error);
        }
    };
    transaction.commit().await?;
    Ok(outcome)
}

pub(crate) async fn execute_internal_state_plan_with_transaction(
    transaction: &mut dyn LixTransaction,
    prepared_statements: &[PreparedStatement],
    internal_state: Option<&InternalStatePlan>,
    should_refresh_file_cache: bool,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<PostprocessExecutionOutcome, LixError> {
    let Some(postprocess_plan) = internal_state.and_then(|plan| plan.postprocess.as_ref()) else {
        return Ok(PostprocessExecutionOutcome {
            internal_result: execute_prepared_with_transaction(transaction, prepared_statements)
                .await?,
            postprocess_file_cache_targets: BTreeSet::new(),
            state_commit_stream_changes: Vec::new(),
        });
    };

    execute_postprocess_with_transaction(
        transaction,
        prepared_statements,
        postprocess_plan,
        should_refresh_file_cache,
        functions,
        writer_key,
    )
    .await
}

pub(crate) async fn execute_postprocess_with_transaction(
    transaction: &mut dyn LixTransaction,
    prepared_statements: &[PreparedStatement],
    postprocess_plan: &PostprocessPlan,
    should_refresh_file_cache: bool,
    functions: &SharedFunctionProvider<RuntimeFunctionProvider>,
    writer_key: Option<&str>,
) -> Result<PostprocessExecutionOutcome, LixError> {
    let internal_result =
        execute_prepared_with_transaction(transaction, prepared_statements).await?;

    let mut postprocess_file_cache_targets = BTreeSet::new();
    if should_refresh_file_cache {
        let schema_key = match postprocess_plan {
            PostprocessPlan::VtableUpdate(update_plan) => &update_plan.schema_key,
            PostprocessPlan::VtableDelete(delete_plan) => &delete_plan.schema_key,
        };
        postprocess_file_cache_targets =
            collect_postprocess_file_cache_targets(&internal_result.rows, schema_key)?;
    }

    let mut state_commit_stream_changes = Vec::new();
    match postprocess_plan {
        PostprocessPlan::VtableUpdate(update_plan) => {
            let changes = state_commit_stream_changes_from_postprocess_rows(
                &internal_result.rows,
                &update_plan.schema_key,
                update_plan.layout.as_ref(),
                StateCommitStreamOperation::Update,
                writer_key,
            )?;
            state_commit_stream_changes.extend(changes);
        }
        PostprocessPlan::VtableDelete(delete_plan) => {
            let changes = state_commit_stream_changes_from_postprocess_rows(
                &internal_result.rows,
                &delete_plan.schema_key,
                delete_plan.layout.as_ref(),
                StateCommitStreamOperation::Delete,
                writer_key,
            )?;
            state_commit_stream_changes.extend(changes);
        }
    }

    let mut followup_functions = functions.clone();
    let followup_params = prepared_statements
        .first()
        .map(|statement| statement.params.as_slice())
        .unwrap_or(&[]);
    let followup_statements = match postprocess_plan {
        PostprocessPlan::VtableUpdate(update_plan) => {
            build_update_followup_statements(
                transaction,
                update_plan,
                &internal_result.rows,
                writer_key,
                &mut followup_functions,
            )
            .await?
        }
        PostprocessPlan::VtableDelete(delete_plan) => {
            build_delete_followup_statements(
                transaction,
                delete_plan,
                &internal_result.rows,
                followup_params,
                writer_key,
                &mut followup_functions,
            )
            .await?
        }
    };
    let mut program = WriteProgram::new();
    program.push_batch(followup_statements);
    execute_write_program_with_transaction(transaction, program).await?;

    Ok(PostprocessExecutionOutcome {
        internal_result,
        postprocess_file_cache_targets,
        state_commit_stream_changes,
    })
}

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
    fn dialect(&self) -> crate::SqlDialect {
        self.executor.dialect()
    }

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
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<PreparedBatch, LixError> {
    let mut executor = TransactionExecutor { transaction };
    build_update_followup_statement_batch(&mut executor, plan, rows, writer_key, functions).await
}

pub(crate) async fn build_delete_followup_statements(
    transaction: &mut dyn LixTransaction,
    plan: &VtableDeletePlan,
    rows: &[Vec<EngineValue>],
    params: &[EngineValue],
    writer_key: Option<&str>,
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<PreparedBatch, LixError> {
    let mut executor = TransactionExecutor { transaction };
    build_delete_followup_statement_batch(&mut executor, plan, rows, params, writer_key, functions)
        .await
}

async fn build_update_followup_statement_batch(
    executor: &mut dyn SqlExecutor,
    plan: &VtableUpdatePlan,
    rows: &[Vec<EngineValue>],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<PreparedBatch, LixError> {
    if rows.is_empty() {
        return Ok(PreparedBatch { steps: Vec::new() });
    }

    let timestamp = functions.timestamp();
    let mut domain_changes = Vec::new();
    let mut affected_versions = BTreeSet::new();

    for row in rows {
        if row.len() < UPDATE_RETURNING_COLUMNS.len() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "vtable update returning row missing columns".to_string(),
            });
        }

        let entity_id = value_to_string(&row[0], "entity_id")?;
        let file_id = value_to_string(&row[1], "file_id")?;
        let version_id = value_to_string(&row[2], "version_id")?;
        let plugin_key = value_to_string(&row[3], "plugin_key")?;
        let schema_version = value_to_string(&row[4], "schema_version")?;
        let snapshot_content = logical_live_snapshot_from_row_with_layout(
            plan.layout.as_ref(),
            &plan.schema_key,
            row,
            5,
            UPDATE_RETURNING_COLUMNS.len(),
        )?
        .map(|snapshot| CanonicalJson::from_text(snapshot.to_string()))
        .transpose()?;
        let metadata =
            canonical_json_from_optional_text(value_to_optional_text(&row[6], "metadata")?)?;
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
            entity_id: require_identity(entity_id, "vtable update entity_id")?,
            schema_key: require_identity(plan.schema_key.clone(), "vtable update schema_key")?,
            schema_version: require_identity(schema_version, "vtable update schema_version")?,
            file_id: require_identity(file_id, "vtable update file_id")?,
            version_id: require_identity(version_id, "vtable update version_id")?,
            plugin_key: require_identity(plugin_key, "vtable update plugin_key")?,
            snapshot_content,
            metadata,
            created_at: timestamp.clone(),
            writer_key: row_writer_key,
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
    build_prepared_batch_from_generate_commit_result_with_executor(
        &mut commit_executor,
        commit_result,
        functions,
    )
    .await
}

async fn build_delete_followup_statement_batch(
    executor: &mut dyn SqlExecutor,
    plan: &VtableDeletePlan,
    rows: &[Vec<EngineValue>],
    params: &[EngineValue],
    writer_key: Option<&str>,
    functions: &mut dyn LixFunctionProvider,
) -> Result<PreparedBatch, LixError> {
    let timestamp = functions.timestamp();
    let mut domain_changes = Vec::new();
    let mut affected_versions = BTreeSet::new();
    let mut tombstoned_keys: BTreeSet<(String, String, String)> = BTreeSet::new();

    for row in rows {
        if row.len() < UPDATE_RETURNING_COLUMNS.len() {
            return Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "vtable delete returning row missing columns".to_string(),
            });
        }

        let entity_id = value_to_string(&row[0], "entity_id")?;
        let file_id = value_to_string(&row[1], "file_id")?;
        let version_id = value_to_string(&row[2], "version_id")?;
        let plugin_key = value_to_string(&row[3], "plugin_key")?;
        let schema_version = value_to_string(&row[4], "schema_version")?;
        let _snapshot_content = logical_live_snapshot_from_row_with_layout(
            plan.layout.as_ref(),
            &plan.schema_key,
            row,
            5,
            UPDATE_RETURNING_COLUMNS.len(),
        )?;
        let metadata =
            canonical_json_from_optional_text(value_to_optional_text(&row[6], "metadata")?)?;
        let row_writer_key = writer_key.map(ToString::to_string);
        tombstoned_keys.insert((entity_id.clone(), file_id.clone(), version_id.clone()));
        affected_versions.insert(version_id.clone());
        domain_changes.push(DomainChangeInput {
            id: functions.uuid_v7(),
            entity_id: require_identity(entity_id, "vtable delete entity_id")?,
            schema_key: require_identity(plan.schema_key.clone(), "vtable delete schema_key")?,
            schema_version: require_identity(schema_version, "vtable delete schema_version")?,
            file_id: require_identity(file_id, "vtable delete file_id")?,
            version_id: require_identity(version_id, "vtable delete version_id")?,
            plugin_key: require_identity(plugin_key, "vtable delete plugin_key")?,
            snapshot_content: None,
            metadata,
            created_at: timestamp.clone(),
            writer_key: row_writer_key,
        });
    }

    if let Some(selection_sql) = plan.effective_scope_untracked_selection_sql.as_deref() {
        delete_effective_scope_untracked_rows(executor, &plan.schema_key, selection_sql, params)
            .await?;
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
            affected_versions.insert(fallback_row.version_id.clone());
            domain_changes.push(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id: require_identity(
                    fallback_row.entity_id,
                    "effective-scope delete fallback entity_id",
                )?,
                schema_key: require_identity(
                    plan.schema_key.clone(),
                    "effective-scope delete fallback schema_key",
                )?,
                schema_version: require_identity(
                    fallback_row.schema_version,
                    "effective-scope delete fallback schema_version",
                )?,
                file_id: require_identity(
                    fallback_row.file_id,
                    "effective-scope delete fallback file_id",
                )?,
                version_id: require_identity(
                    fallback_row.version_id,
                    "effective-scope delete fallback version_id",
                )?,
                plugin_key: require_identity(
                    fallback_row.plugin_key,
                    "effective-scope delete fallback plugin_key",
                )?,
                snapshot_content: None,
                metadata: fallback_row.metadata,
                created_at: timestamp.clone(),
                writer_key: row_writer_key,
            });
        }
    }

    if domain_changes.is_empty() {
        return Ok(PreparedBatch { steps: Vec::new() });
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
    build_prepared_batch_from_generate_commit_result_with_executor(
        &mut commit_executor,
        commit_result,
        functions,
    )
    .await
}

async fn delete_effective_scope_untracked_rows(
    executor: &mut dyn SqlExecutor,
    schema_key: &str,
    selection_sql: &str,
    params: &[EngineValue],
) -> Result<(), LixError> {
    let sql = format!(
        "DELETE FROM {table} WHERE {selection_sql}",
        table = quote_ident(&untracked_live_table_name(schema_key)),
    );
    let bound = bind_sql_with_state(&sql, params, executor.dialect(), PlaceholderState::new())?;
    executor.execute(&bound.sql, &bound.params).await?;
    Ok(())
}

struct EffectiveScopeDeleteRow {
    entity_id: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
    schema_version: String,
    metadata: Option<CanonicalJson>,
}

async fn load_effective_scope_delete_rows(
    executor: &mut dyn SqlExecutor,
    plan: &VtableDeletePlan,
    params: &[EngineValue],
) -> Result<Vec<EffectiveScopeDeleteRow>, LixError> {
    let Some(selection_sql) = plan.effective_scope_selection_sql.as_deref() else {
        return Ok(Vec::new());
    };

    let schema_table = quote_ident(&tracked_live_table_name(&plan.schema_key));
    let descriptor_table = quote_ident(&tracked_live_table_name(version_descriptor_schema_key()));
    let sql = format!(
        "WITH \
           all_real_versions AS ( \
             SELECT lix_json_extract(snapshot_content, 'id') AS version_id \
             FROM {descriptor_table} \
             WHERE schema_key = '{descriptor_schema_key}' \
               AND file_id = '{descriptor_file_id}' \
               AND version_id = '{descriptor_storage_version_id}' \
               AND global = true \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL \
           ), \
           all_target_versions AS ( \
             SELECT DISTINCT version_id \
             FROM {schema_table} \
             WHERE version_id <> '{global_version}' \
             UNION \
             SELECT version_id \
             FROM all_real_versions \
           ), \
           version_chain(target_version_id, ancestor_version_id, depth) AS ( \
             SELECT version_id AS target_version_id, version_id AS ancestor_version_id, 0 AS depth \
             FROM all_target_versions \
             UNION ALL \
             SELECT \
               version_id AS target_version_id, \
               '{global_version}' AS ancestor_version_id, \
               1 AS depth \
             FROM all_target_versions \
             WHERE version_id <> '{global_version}' \
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
               s.global AS global, \
               false AS untracked, \
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
        global_version = escape_sql_string(GLOBAL_VERSION_ID),
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
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "effective scope delete row loader expected six columns".to_string(),
            });
        }
        resolved.push(EffectiveScopeDeleteRow {
            entity_id: value_to_string(&row[0], "entity_id")?,
            file_id: value_to_string(&row[1], "file_id")?,
            version_id: value_to_string(&row[2], "version_id")?,
            plugin_key: value_to_string(&row[3], "plugin_key")?,
            schema_version: value_to_string(&row[4], "schema_version")?,
            metadata: canonical_json_from_optional_text(value_to_optional_text(
                &row[5], "metadata",
            )?)?,
        });
    }
    Ok(resolved)
}

fn canonical_json_from_optional_text(
    value: Option<String>,
) -> Result<Option<CanonicalJson>, LixError> {
    value.map(CanonicalJson::from_text).transpose()
}

fn lower_single_statement_for_dialect(sql: &str, dialect: SqlDialect) -> Result<String, LixError> {
    let mut statements = Parser::parse_sql(&GenericDialect {}, sql).map_err(|error| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.to_string(),
    })?;
    if statements.len() != 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "expected a single statement".to_string(),
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
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("vtable update expected text or null for {name}"),
        }),
    }
}

fn value_to_string(value: &EngineValue, name: &str) -> Result<String, LixError> {
    match value {
        EngineValue::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("vtable update expected text for {name}"),
        }),
    }
}

fn require_identity<T>(value: impl Into<String>, context: &str) -> Result<T, LixError>
where
    T: TryFrom<String, Error = LixError>,
{
    let value = value.into();
    T::try_from(value.clone()).map_err(|_| {
        LixError::unknown(format!(
            "{context} must be a non-empty canonical identity, got '{}'",
            value
        ))
    })
}

fn quote_ident(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}
