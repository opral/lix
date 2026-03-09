use std::collections::BTreeSet;

use crate::commit::{
    append_commit_if_preconditions_hold, AppendCommitArgs, AppendCommitDisposition,
    AppendCommitError, AppendCommitErrorKind, AppendCommitInvariantChecker,
    AppendCommitPreconditions, AppendExpectedTip, AppendWriteLane,
};
use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::sql::storage::sql_text::escape_sql_string;
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::schema_registry::register_schema_sql_statements;
use crate::sql2::runtime::{
    prepare_sql2_read, try_prepare_sql2_write, Sql2PreparedRead, Sql2PreparedWrite,
};
use crate::state_commit_stream::{
    state_commit_stream_changes_from_domain_changes, state_commit_stream_changes_from_planned_rows,
    StateCommitStreamOperation,
};
use crate::validation::{
    validate_inserts, validate_sql2_append_time_write, validate_sql2_batch_local_write,
    validate_updates,
};
use crate::version::{
    active_version_file_id, active_version_schema_key, active_version_storage_version_id,
    parse_active_version_snapshot,
};
use crate::{LixBackend, LixError, LixTransaction, QueryResult, Value};

use super::super::contracts::effects::PlanEffects;
use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::contracts::result_contract::ResultContract;
use super::super::planning::derive_requirements::derive_plan_requirements;
use super::super::planning::plan::build_execution_plan;
use super::intent::{
    authoritative_pending_file_write_targets, collect_execution_intent_with_backend,
    ExecutionIntent, IntentCollectionPolicy,
};
use super::run::SqlExecutionOutcome;
use sqlparser::ast::Statement;

const STORED_SCHEMA_KEY: &str = "lix_stored_schema";
const STORED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_stored_schema_bootstrap";
const UNTRACKED_TABLE: &str = "lix_internal_state_untracked";
const GLOBAL_VERSION_ID: &str = "global";

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct PreparedExecutionContext {
    pub(crate) intent: ExecutionIntent,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) plan: ExecutionPlan,
    pub(crate) sql2_read: Option<Sql2PreparedRead>,
    pub(crate) sql2_write: Option<Sql2PreparedWrite>,
}

pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

struct Sql2AppendInvariantChecker<'a> {
    planned_write: &'a crate::sql2::planner::ir::PlannedWrite,
    schema_cache: crate::validation::SchemaCache,
}

impl<'a> Sql2AppendInvariantChecker<'a> {
    fn new(planned_write: &'a crate::sql2::planner::ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: crate::validation::SchemaCache::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl AppendCommitInvariantChecker for Sql2AppendInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), AppendCommitError> {
        let backend = TransactionBackendAdapter::new(transaction);
        validate_sql2_append_time_write(&backend, &self.schema_cache, self.planned_write)
            .await
            .map_err(|error| AppendCommitError {
                kind: AppendCommitErrorKind::Internal,
                message: error.description,
            })
    }
}

pub(crate) async fn prepare_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    policy: PreparationPolicy,
) -> Result<PreparedExecutionContext, LixError> {
    let (settings, sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(backend)
        .await?;

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::pending_file_writes::ensure_file_insert_ids_for_data_writes(
        &mut statements,
        &functions,
    )?;

    let requirements = derive_plan_requirements(&statements);

    engine
        .maybe_materialize_reads_with_backend_from_statements(
            backend,
            &statements,
            active_version_id,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend read materialization failed: {}",
                error.description
            ),
        })?;

    let sql2_read =
        prepare_sql2_read(backend, &statements, params, active_version_id, writer_key).await;
    let sql2_write =
        try_prepare_sql2_write(backend, &statements, params, active_version_id, writer_key)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend sql2 write preparation failed: {}",
                    error.description
                ),
            })?;
    let plan_statements = sql2_read
        .as_ref()
        .and_then(|prepared| prepared.lowered_read.as_ref())
        .map(|program| program.statements.clone())
        .unwrap_or_else(|| statements.clone());

    let intent = collect_execution_intent_with_backend(
        engine,
        backend,
        &statements,
        params,
        active_version_id,
        writer_key,
        &requirements,
        IntentCollectionPolicy {
            skip_side_effect_collection: policy.skip_side_effect_collection,
        },
    )
    .await
    .map_err(|error| LixError {
        code: error.code,
        description: format!(
            "prepare_execution_with_backend intent collection failed: {}",
            error.description
        ),
    })?;

    let plan = build_execution_plan(
        backend,
        &engine.cel_evaluator,
        plan_statements,
        params,
        Some(active_version_id),
        sql2_read
            .as_ref()
            .and_then(|prepared| prepared.dependency_spec.clone()),
        functions.clone(),
        &intent.detected_file_domain_changes_by_statement,
        &intent.pending_file_delete_targets,
        &authoritative_pending_file_write_targets(&intent.pending_file_writes),
        writer_key,
    )
    .await
    .map_err(LixError::from)
    .map_err(|error| LixError {
        code: error.code,
        description: format!(
            "prepare_execution_with_backend plan building failed: {}",
            error.description
        ),
    })?;

    if !plan.preprocess.mutations.is_empty() {
        validate_inserts(backend, &engine.schema_cache, &plan.preprocess.mutations)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend insert validation failed: {}",
                    error.description
                ),
            })?;
    }
    if !plan.preprocess.update_validations.is_empty() {
        validate_updates(
            backend,
            &engine.schema_cache,
            &plan.preprocess.update_validations,
            params,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend update validation failed: {}",
                error.description
            ),
        })?;
    }
    if let Some(sql2_write) = sql2_write.as_ref() {
        validate_sql2_batch_local_write(backend, &engine.schema_cache, &sql2_write.planned_write)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend sql2 batch-local validation failed: {}",
                    error.description
                ),
            })?;
    }

    Ok(PreparedExecutionContext {
        intent,
        settings,
        sequence_start,
        functions,
        plan,
        sql2_read,
        sql2_write,
    })
}

pub(crate) fn derive_cache_targets(
    plan: &ExecutionPlan,
    active_effects: &PlanEffects,
    effects_are_authoritative: bool,
    postprocess_file_cache_targets: BTreeSet<(String, String)>,
) -> CacheTargets {
    let file_cache_refresh_targets =
        if effects_are_authoritative || plan.requirements.should_refresh_file_cache {
            let mut targets = active_effects.file_cache_refresh_targets.clone();
            targets.extend(postprocess_file_cache_targets.clone());
            targets
        } else {
            BTreeSet::new()
        };

    CacheTargets {
        file_cache_refresh_targets,
    }
}

pub(crate) async fn maybe_execute_sql2_write_with_backend(
    engine: &Engine,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if !sql2_tracked_write_is_live(prepared) && !sql2_untracked_write_is_live(prepared) {
        return Ok(None);
    }

    let mut transaction = engine.backend.begin_transaction().await?;
    let execution = match maybe_execute_sql2_write_with_transaction(
        engine,
        transaction.as_mut(),
        prepared,
        writer_key,
    )
    .await?
    {
        Some(execution) => execution,
        None => return Ok(None),
    };
    transaction.commit().await?;
    Ok(Some(execution))
}

pub(crate) async fn maybe_execute_sql2_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    prepared: &PreparedExecutionContext,
    writer_key: Option<&str>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if sql2_untracked_write_is_live(prepared) {
        return execute_sql2_untracked_write_with_transaction(engine, transaction, prepared).await;
    }

    if !sql2_tracked_write_is_live(prepared) {
        return Ok(None);
    }

    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return Ok(None);
    };
    let Some(domain_change_batch) = sql2_write.domain_change_batch.as_ref() else {
        return Ok(None);
    };
    let Some(commit_preconditions) = sql2_write.planned_write.commit_preconditions.as_ref() else {
        return Ok(None);
    };
    if sql2_commits_filesystem_payload_domain_changes(prepared) {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 tracked filesystem payload persistence failed before append: {}",
                    error.description
                ),
            })?;
    }
    let stream_operation = state_commit_stream_operation(sql2_write);

    for registration in &prepared.plan.preprocess.registrations {
        for statement in
            register_schema_sql_statements(&registration.schema_key, transaction.dialect())
        {
            transaction
                .execute(&statement, &[])
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "sql2 tracked write schema registration failed for '{}': {}",
                        registration.schema_key, error.description
                    ),
                })?;
        }
    }

    if domain_change_batch.changes.is_empty() {
        return Ok(Some(SqlExecutionOutcome {
            public_result: QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            },
            postprocess_file_cache_targets: BTreeSet::new(),
            plugin_changes_committed: false,
            plan_effects_override: Some(PlanEffects::default()),
            state_commit_stream_changes: Vec::new(),
        }));
    }

    let mut append_functions = prepared.functions.clone();
    let timestamp = append_functions.timestamp();
    let mut invariant_checker = Sql2AppendInvariantChecker::new(&sql2_write.planned_write);
    let append_result = append_commit_if_preconditions_hold(
        transaction,
        AppendCommitArgs {
            timestamp,
            changes: domain_change_batch.changes.clone(),
            preconditions: append_preconditions(
                sql2_write,
                domain_change_batch,
                commit_preconditions,
            )?,
        },
        &mut append_functions,
        Some(&mut invariant_checker),
    )
    .await
    .map_err(append_commit_error_to_lix_error)?;

    if let Some(commit_result) = append_result.commit_result.as_ref() {
        mirror_sql2_stored_schema_bootstrap_rows(transaction, commit_result)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 tracked write stored-schema bootstrap mirroring failed: {}",
                    error.description
                ),
            })?;
    }
    if matches!(append_result.disposition, AppendCommitDisposition::Applied) {
        apply_sql2_version_last_checkpoint_side_effects(transaction, sql2_write)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 tracked write version checkpoint side effects failed: {}",
                    error.description
                ),
            })?;
    }

    let plugin_changes_committed =
        matches!(append_result.disposition, AppendCommitDisposition::Applied);
    let plan_effects_override = if plugin_changes_committed {
        semantic_plan_effects_from_domain_changes(&domain_change_batch.changes, stream_operation)?
    } else {
        PlanEffects::default()
    };

    let _ = writer_key;
    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
    }))
}

async fn execute_sql2_untracked_write_with_transaction(
    engine: &Engine,
    transaction: &mut dyn LixTransaction,
    prepared: &PreparedExecutionContext,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return Ok(None);
    };
    let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(None);
    };

    let mut runtime_functions = prepared.functions.clone();
    let timestamp = runtime_functions.timestamp();
    if sql2_persists_filesystem_payload_writes(prepared) {
        engine
            .persist_pending_file_data_updates_in_transaction(
                transaction,
                &prepared.intent.pending_file_writes,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "sql2 untracked filesystem payload persistence failed before state apply: {}",
                    error.description
                ),
            })?;
    }
    apply_sql2_untracked_rows(transaction, &resolved.intended_post_state, &timestamp).await?;
    let plan_effects_override =
        semantic_plan_effects_from_untracked_sql2_write(prepared, sql2_write)?;

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
    }))
}

pub(crate) fn sql2_commits_filesystem_payload_domain_changes(
    prepared: &PreparedExecutionContext,
) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
    ) && sql2_tracked_write_is_live(prepared)
}

fn sql2_persists_filesystem_payload_writes(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
            | crate::sql2::planner::ir::WriteMode::Untracked
    ) && !prepared.intent.pending_file_writes.is_empty()
}

fn sql2_filesystem_write_targets_global_version(sql2_write: &Sql2PreparedWrite) -> bool {
    let target_name = sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        .as_str();
    if !matches!(
        target_name,
        "lix_file" | "lix_file_by_version" | "lix_directory" | "lix_directory_by_version"
    ) {
        return false;
    }

    sql2_write
        .planned_write
        .resolved_write_plan
        .as_ref()
        .is_some_and(|resolved| {
            resolved
                .intended_post_state
                .iter()
                .filter_map(|row| row.version_id.as_deref())
                .any(|version_id| version_id == GLOBAL_VERSION_ID)
        })
}

fn sql2_tracked_write_is_live(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    let target_name = sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        .as_str();
    let filesystem_schema_set_supported =
        prepared
            .intent
            .detected_file_domain_changes
            .iter()
            .all(|change| {
                matches!(
                    change.schema_key.as_str(),
                    DIRECTORY_DESCRIPTOR_SCHEMA_KEY
                        | FILE_DESCRIPTOR_SCHEMA_KEY
                        | BINARY_BLOB_REF_SCHEMA_KEY
                )
            });
    let filesystem_directory_side_effects_only =
        matches!(target_name, "lix_directory" | "lix_directory_by_version")
            && filesystem_schema_set_supported;
    let filesystem_file_side_effects_only =
        matches!(target_name, "lix_file" | "lix_file_by_version")
            && filesystem_schema_set_supported;
    let filesystem_surface = matches!(
        target_name,
        "lix_file" | "lix_file_by_version" | "lix_directory" | "lix_directory_by_version"
    );
    let no_legacy_detected_filesystem_duplicates =
        filesystem_surface || prepared.intent.detected_file_domain_changes.is_empty();
    let no_legacy_untracked_filesystem_duplicates = filesystem_surface
        || prepared
            .intent
            .untracked_filesystem_update_domain_changes
            .is_empty();
    let non_global_filesystem_write = !sql2_filesystem_write_targets_global_version(sql2_write);

    matches!(
        prepared.plan.result_contract,
        ResultContract::DmlNoReturning
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Tracked
    ) && sql2_write.domain_change_batch.is_some()
        && sql2_write.planned_write.commit_preconditions.is_some()
        && live_sql2_operation_supported(sql2_write)
        && (no_legacy_detected_filesystem_duplicates
            || filesystem_directory_side_effects_only
            || filesystem_file_side_effects_only)
        && no_legacy_untracked_filesystem_duplicates
        && non_global_filesystem_write
}

fn sql2_untracked_write_is_live(prepared: &PreparedExecutionContext) -> bool {
    let Some(sql2_write) = prepared.sql2_write.as_ref() else {
        return false;
    };
    let target_name = sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        .as_str();
    let filesystem_surface = matches!(
        target_name,
        "lix_file" | "lix_file_by_version" | "lix_directory" | "lix_directory_by_version"
    );
    let no_legacy_filesystem_duplicates =
        filesystem_surface || prepared.intent.detected_file_domain_changes.is_empty();
    let non_global_filesystem_write = !sql2_filesystem_write_targets_global_version(sql2_write);

    matches!(
        prepared.plan.result_contract,
        ResultContract::DmlNoReturning
    ) && matches!(
        sql2_write.planned_write.command.mode,
        crate::sql2::planner::ir::WriteMode::Untracked
    ) && matches!(
        target_name,
        "lix_active_version"
            | "lix_active_account"
            | "lix_file"
            | "lix_file_by_version"
            | "lix_directory"
            | "lix_directory_by_version"
    ) && no_legacy_filesystem_duplicates
        && non_global_filesystem_write
}

fn live_sql2_operation_supported(sql2_write: &Sql2PreparedWrite) -> bool {
    match sql2_write.planned_write.command.operation_kind {
        crate::sql2::planner::ir::WriteOperationKind::Insert => true,
        crate::sql2::planner::ir::WriteOperationKind::Update
        | crate::sql2::planner::ir::WriteOperationKind::Delete => matches!(
            sql2_write
                .planned_write
                .commit_preconditions
                .as_ref()
                .map(|preconditions| &preconditions.write_lane),
            Some(crate::sql2::planner::ir::WriteLane::SingleVersion(_))
                | Some(crate::sql2::planner::ir::WriteLane::ActiveVersion)
                | Some(crate::sql2::planner::ir::WriteLane::GlobalAdmin)
        ),
    }
}

fn state_commit_stream_operation(sql2_write: &Sql2PreparedWrite) -> StateCommitStreamOperation {
    match sql2_write.planned_write.command.operation_kind {
        crate::sql2::planner::ir::WriteOperationKind::Insert => StateCommitStreamOperation::Insert,
        crate::sql2::planner::ir::WriteOperationKind::Update => StateCommitStreamOperation::Update,
        crate::sql2::planner::ir::WriteOperationKind::Delete => StateCommitStreamOperation::Delete,
    }
}

fn append_preconditions(
    sql2_write: &Sql2PreparedWrite,
    batch: &crate::sql2::planner::semantics::domain_changes::DomainChangeBatch,
    commit_preconditions: &crate::sql2::planner::ir::CommitPreconditions,
) -> Result<AppendCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        crate::sql2::planner::ir::WriteLane::SingleVersion(version_id) => {
            AppendWriteLane::Version(version_id.clone())
        }
        crate::sql2::planner::ir::WriteLane::ActiveVersion => {
            let version_id = batch
                .changes
                .first()
                .map(|change| change.version_id.clone())
                .or_else(|| {
                    sql2_write
                        .planned_write
                        .command
                        .execution_context
                        .requested_version_id
                        .clone()
                })
                .ok_or_else(|| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: "sql2 append execution requires a concrete active version id"
                        .to_string(),
                })?;
            AppendWriteLane::Version(version_id)
        }
        crate::sql2::planner::ir::WriteLane::GlobalAdmin => AppendWriteLane::GlobalAdmin,
    };
    let expected_tip = match &commit_preconditions.expected_tip {
        crate::sql2::planner::ir::ExpectedTip::CommitId(commit_id) => {
            AppendExpectedTip::CommitId(commit_id.clone())
        }
        crate::sql2::planner::ir::ExpectedTip::CreateIfMissing => {
            AppendExpectedTip::CreateIfMissing
        }
    };

    Ok(AppendCommitPreconditions {
        write_lane,
        expected_tip,
        idempotency_key: commit_preconditions.idempotency_key.0.clone(),
    })
}

async fn apply_sql2_untracked_rows(
    transaction: &mut dyn LixTransaction,
    rows: &[crate::sql2::planner::ir::PlannedStateRow],
    timestamp: &str,
) -> Result<(), LixError> {
    for row in rows {
        if row.tombstone {
            apply_sql2_untracked_delete(transaction, row).await?;
        } else {
            apply_sql2_untracked_upsert(transaction, row, timestamp).await?;
        }
    }
    Ok(())
}

async fn apply_sql2_untracked_upsert(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql2::planner::ir::PlannedStateRow,
    timestamp: &str,
) -> Result<(), LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let plugin_key = planned_row_text_value(row, "plugin_key")?;
    let schema_version = planned_row_text_value(row, "schema_version")?;
    let snapshot_content = planned_row_text_value(row, "snapshot_content")?;
    let metadata_sql = planned_row_optional_text_value(row, "metadata")
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let writer_key_sql = planned_row_optional_text_value(row, "writer_key")
        .map(|value| format!("'{}'", escape_sql_string(value)))
        .unwrap_or_else(|| "NULL".to_string());
    let global = row
        .values
        .get("global")
        .and_then(value_as_bool)
        .unwrap_or_else(|| row.version_id.as_deref() == Some(GLOBAL_VERSION_ID));

    let sql = format!(
        "INSERT INTO {table} (\
         entity_id, schema_key, file_id, version_id, global, plugin_key, snapshot_content, metadata, writer_key, schema_version, created_at, updated_at\
         ) VALUES (\
         '{entity_id}', '{schema_key}', '{file_id}', '{version_id}', {global}, '{plugin_key}', '{snapshot_content}', {metadata}, {writer_key}, '{schema_version}', '{timestamp}', '{timestamp}'\
         ) ON CONFLICT (entity_id, schema_key, file_id, version_id) DO UPDATE SET \
         global = excluded.global, \
         plugin_key = excluded.plugin_key, \
         snapshot_content = excluded.snapshot_content, \
         metadata = excluded.metadata, \
         writer_key = excluded.writer_key, \
         schema_version = excluded.schema_version, \
         updated_at = excluded.updated_at",
        table = UNTRACKED_TABLE,
        entity_id = escape_sql_string(&row.entity_id),
        schema_key = escape_sql_string(&row.schema_key),
        file_id = escape_sql_string(file_id),
        version_id = escape_sql_string(row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID)),
        global = if global { "true" } else { "false" },
        plugin_key = escape_sql_string(plugin_key),
        snapshot_content = escape_sql_string(snapshot_content),
        metadata = metadata_sql,
        writer_key = writer_key_sql,
        schema_version = escape_sql_string(schema_version),
        timestamp = escape_sql_string(timestamp),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn apply_sql2_untracked_delete(
    transaction: &mut dyn LixTransaction,
    row: &crate::sql2::planner::ir::PlannedStateRow,
) -> Result<(), LixError> {
    let file_id = planned_row_text_value(row, "file_id")?;
    let sql = format!(
        "DELETE FROM {table} \
         WHERE entity_id = '{entity_id}' \
           AND schema_key = '{schema_key}' \
           AND file_id = '{file_id}' \
           AND version_id = '{version_id}'",
        table = UNTRACKED_TABLE,
        entity_id = escape_sql_string(&row.entity_id),
        schema_key = escape_sql_string(&row.schema_key),
        file_id = escape_sql_string(file_id),
        version_id = escape_sql_string(row.version_id.as_deref().unwrap_or(GLOBAL_VERSION_ID)),
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

fn planned_row_text_value<'a>(
    row: &'a crate::sql2::planner::ir::PlannedStateRow,
    key: &str,
) -> Result<&'a str, LixError> {
    planned_row_optional_text_value(row, key).ok_or_else(|| LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: format!("sql2 untracked execution requires '{key}' in the resolved row"),
    })
}

fn planned_row_optional_text_value<'a>(
    row: &'a crate::sql2::planner::ir::PlannedStateRow,
    key: &str,
) -> Option<&'a str> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(value.as_str()),
        _ => None,
    }
}

fn value_as_bool(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn semantic_plan_effects_from_untracked_sql2_write(
    prepared: &PreparedExecutionContext,
    sql2_write: &Sql2PreparedWrite,
) -> Result<PlanEffects, LixError> {
    let mut effects = PlanEffects::default();
    let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(effects);
    };
    effects.state_commit_stream_changes = state_commit_stream_changes_from_planned_rows(
        &resolved.intended_post_state,
        state_commit_stream_operation(sql2_write),
        true,
        sql2_write
            .planned_write
            .command
            .execution_context
            .writer_key
            .as_deref(),
    )?;
    if matches!(
        sql2_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) {
        effects.file_cache_refresh_targets =
            authoritative_pending_file_write_targets(&prepared.intent.pending_file_writes);
        effects
            .file_cache_refresh_targets
            .extend(prepared.intent.pending_file_delete_targets.iter().cloned());
    }
    if sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_active_version"
    {
        return Ok(effects);
    }
    for row in resolved.intended_post_state.iter().rev() {
        if row.schema_key != active_version_schema_key()
            || planned_row_optional_text_value(row, "file_id") != Some(active_version_file_id())
            || row.version_id.as_deref() != Some(active_version_storage_version_id())
            || row.tombstone
        {
            continue;
        }
        let Some(snapshot_content) = planned_row_optional_text_value(row, "snapshot_content")
        else {
            continue;
        };
        effects.next_active_version_id = Some(parse_active_version_snapshot(snapshot_content)?);
        break;
    }
    Ok(effects)
}

fn append_commit_error_to_lix_error(error: crate::commit::AppendCommitError) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

fn semantic_plan_effects_from_domain_changes(
    changes: &[crate::commit::ProposedDomainChange],
    stream_operation: StateCommitStreamOperation,
) -> Result<PlanEffects, LixError> {
    Ok(PlanEffects {
        state_commit_stream_changes: state_commit_stream_changes_from_domain_changes(
            changes,
            stream_operation,
        )?,
        next_active_version_id: next_active_version_id_from_domain_changes(changes)?,
        file_cache_refresh_targets: file_cache_refresh_targets_from_domain_changes(changes),
    })
}

fn next_active_version_id_from_domain_changes(
    changes: &[crate::commit::ProposedDomainChange],
) -> Result<Option<String>, LixError> {
    for change in changes.iter().rev() {
        if change.schema_key != active_version_schema_key()
            || change.file_id.as_deref() != Some(active_version_file_id())
            || change.version_id != active_version_storage_version_id()
        {
            continue;
        }

        let Some(snapshot_content) = change.snapshot_content.as_deref() else {
            continue;
        };
        return parse_active_version_snapshot(snapshot_content).map(Some);
    }

    Ok(None)
}

fn file_cache_refresh_targets_from_domain_changes(
    changes: &[crate::commit::ProposedDomainChange],
) -> BTreeSet<(String, String)> {
    changes
        .iter()
        .filter(|change| change.file_id.as_deref() != Some("lix"))
        .filter(|change| change.schema_key != FILE_DESCRIPTOR_SCHEMA_KEY)
        .filter(|change| change.schema_key != DIRECTORY_DESCRIPTOR_SCHEMA_KEY)
        .filter_map(|change| {
            change
                .file_id
                .as_ref()
                .map(|file_id| (file_id.clone(), change.version_id.clone()))
        })
        .collect()
}

async fn mirror_sql2_stored_schema_bootstrap_rows(
    transaction: &mut dyn LixTransaction,
    commit_result: &crate::commit::GenerateCommitResult,
) -> Result<(), LixError> {
    for row in &commit_result.materialized_state {
        if row.schema_key != STORED_SCHEMA_KEY || row.lixcol_version_id != GLOBAL_VERSION_ID {
            continue;
        }

        let snapshot_sql = row
            .snapshot_content
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let metadata_sql = row
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let writer_key_sql = row
            .writer_key
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 };

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', {snapshot_content}, '{change_id}', {metadata}, {writer_key}, {is_tombstone}, '{created_at}', '{updated_at}'\
             ) ON CONFLICT (entity_id, file_id, version_id) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             snapshot_content = excluded.snapshot_content, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             writer_key = excluded.writer_key, \
             is_tombstone = excluded.is_tombstone, \
             updated_at = excluded.updated_at",
            table = STORED_SCHEMA_BOOTSTRAP_TABLE,
            entity_id = escape_sql_string(&row.entity_id),
            schema_key = escape_sql_string(&row.schema_key),
            schema_version = escape_sql_string(&row.schema_version),
            file_id = escape_sql_string(&row.file_id),
            version_id = escape_sql_string(&row.lixcol_version_id),
            plugin_key = escape_sql_string(&row.plugin_key),
            snapshot_content = snapshot_sql,
            change_id = escape_sql_string(&row.id),
            metadata = metadata_sql,
            writer_key = writer_key_sql,
            is_tombstone = is_tombstone,
            created_at = escape_sql_string(&row.created_at),
            updated_at = escape_sql_string(&row.created_at),
        );

        transaction.execute(&sql, &[]).await?;
    }

    Ok(())
}

async fn apply_sql2_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixTransaction,
    sql2_write: &Sql2PreparedWrite,
) -> Result<(), LixError> {
    if sql2_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_version"
    {
        return Ok(());
    }

    let Some(resolved) = sql2_write.planned_write.resolved_write_plan.as_ref() else {
        return Ok(());
    };

    match sql2_write.planned_write.command.operation_kind {
        crate::sql2::planner::ir::WriteOperationKind::Insert => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows(&resolved.intended_post_state),
                true,
            )
            .await
        }
        crate::sql2::planner::ir::WriteOperationKind::Update => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows(&resolved.intended_post_state),
                false,
            )
            .await
        }
        crate::sql2::planner::ir::WriteOperationKind::Delete => {
            let version_ids = resolved
                .authoritative_pre_state
                .iter()
                .map(|row| row.entity_id.clone())
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>();
            delete_last_checkpoint_rows(transaction, &version_ids).await
        }
    }
}

fn version_checkpoint_rows(
    rows: &[crate::sql2::planner::ir::PlannedStateRow],
) -> Vec<(String, String)> {
    rows.iter()
        .filter(|row| row.schema_key == crate::version::version_pointer_schema_key())
        .filter_map(|row| {
            row.values
                .get("snapshot_content")
                .and_then(|value| match value {
                    Value::Text(text) => Some(text.as_str()),
                    _ => None,
                })
                .and_then(|snapshot| serde_json::from_str::<serde_json::Value>(snapshot).ok())
                .and_then(|snapshot| {
                    snapshot
                        .get("commit_id")
                        .and_then(serde_json::Value::as_str)
                        .map(|commit_id| (row.entity_id.clone(), commit_id.to_string()))
                })
        })
        .collect()
}

async fn upsert_last_checkpoint_rows(
    transaction: &mut dyn LixTransaction,
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

async fn delete_last_checkpoint_rows(
    transaction: &mut dyn LixTransaction,
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
