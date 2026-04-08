//! Neutral write preparation pipeline.
//!
//! `session/*` owns request orchestration, but the lower-level write-step
//! preparation machinery should not live under the session owner.

use std::borrow::Cow;
use std::ops::ControlFlow;
use std::time::Instant;

use serde_json::Value as JsonValue;
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};

use crate::backend::TransactionBackendAdapter;
use crate::contracts::artifacts::{
    PreparedBatch, PreparedExplainMode, PreparedInsertOnConflictAction,
    PreparedInternalWriteArtifact, PreparedPublicSurfaceRegistryEffect,
    PreparedPublicSurfaceRegistryMutation, PreparedPublicWriteArtifact,
    PreparedPublicWriteContract, PreparedPublicWriteExecutionArtifact,
    PreparedPublicWriteExecutionPartition, PreparedPublicWriteMaterialization,
    PreparedResolvedWritePartition, PreparedResolvedWritePlan, PreparedTrackedWriteExecution,
    PreparedUntrackedWriteExecution, PreparedWriteArtifact, PreparedWriteDiagnosticContext,
    PreparedWriteOperationKind, PreparedWriteStatementKind, PreparedWriteStep,
    UpdateValidationInput, UpdateValidationInputRow,
};
use crate::contracts::functions::{
    clone_boxed_function_provider, LixFunctionProvider, SharedFunctionProvider,
};
use crate::projections::ProjectionRegistry;
use crate::contracts::traits::{
    CompiledSchemaCache, LiveReadShapeContract, LiveStateQueryBackend, PendingView,
    SqlPreparationMetadataReader,
};
use crate::common::errors::classification::normalize_sql_error_with_backend_and_relation_names;
use crate::execution_runtime::ExecutionRuntimeState;
use crate::session::execution_context::ExecutionContext;
use crate::session::SessionWriteSelectorResolver;
use crate::session::collaborators::WriteExecutionCollaborators;
use crate::sql::binder::bind_sql;
use crate::sql::explain::{
    build_public_write_explain_artifacts, prepare_analyzed_explain_template,
    prepare_plain_explain_template, stage_timing, ExplainStage, PublicWriteExplainBuildInput,
};
use crate::sql::prepare::{
    build_public_write_execution, build_public_write_invariant_trace,
    compile_execution_from_template_instance_with_context, finalize_public_write_execution,
    load_sql_compiler_metadata_with_reader, prepare_public_read_artifact,
    public_authoritative_write_error, public_write_preparation_error,
    BoundStatementTemplateInstance, CompiledExecution, PreparationPolicy, UpdateValidationPlan,
};
use crate::sql::semantic_ir::semantics::domain_changes::{
    build_domain_change_batch, derive_commit_preconditions,
};
use crate::write_runtime::{
    ensure_runtime_sequence_initialized_in_transaction, resolve_write_plan_with_functions,
    validate_batch_local_write, validate_inserts, validate_update_inputs, PendingTransactionView,
    PreparedWriteExecutionStep, PreparedWriteRuntimeState,
};
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

const GLOBAL_VERSION_ID: &str = "global";

pub(crate) async fn ensure_execution_runtime_state_for_write_scope(
    collaborators: &dyn WriteExecutionCollaborators,
    transaction: &mut dyn LixBackendTransaction,
    context: &mut ExecutionContext,
) -> Result<(), LixError> {
    if context.execution_runtime_state().is_some() {
        return Ok(());
    }

    let backend = TransactionBackendAdapter::new(transaction);
    let runtime_state = collaborators
        .prepare_execution_runtime_state(&backend)
        .await?;
    context.set_execution_runtime_state(runtime_state);
    Ok(())
}

pub(crate) fn prepared_write_runtime_state_for_execution(
    runtime_state: &ExecutionRuntimeState,
) -> PreparedWriteRuntimeState {
    let functions = clone_boxed_function_provider(runtime_state.provider());
    PreparedWriteRuntimeState::new(runtime_state.settings().enabled, functions)
}

pub(crate) async fn prepare_buffered_write_execution_step(
    collaborators: &dyn WriteExecutionCollaborators,
    mut transaction: &mut dyn LixBackendTransaction,
    pending_transaction_view: Option<&PendingTransactionView>,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    context: &ExecutionContext,
    skip_side_effect_collection: bool,
) -> Result<PreparedWriteExecutionStep, LixError> {
    let statement_kind =
        PreparedWriteStatementKind::for_statement(bound_statement_template.statement());
    let diagnostic_context = PreparedWriteDiagnosticContext::new(collect_statement_relation_names(
        bound_statement_template.statement(),
    ));
    let writer_key = context.options.writer_key.clone();
    let runtime_state = context
        .execution_runtime_state()
        .expect("write execution should install an execution runtime state before preparation");

    if runtime_state.settings().enabled {
        let mut runtime_functions = runtime_state.provider().clone();
        ensure_runtime_sequence_initialized_in_transaction(transaction, &mut runtime_functions)
            .await?;
    }

    let dialect = transaction.dialect();
    let active_history_root_commit_id = {
        let metadata_reader = &mut transaction;
        metadata_reader
            .load_active_history_root_commit_id_for_preparation(context.active_version_id.as_str())
            .await?
    };
    let compiler_metadata = {
        let metadata_reader = &mut transaction;
        load_sql_compiler_metadata_with_reader(metadata_reader, &context.public_surface_registry)
            .await?
    };
    let backend = TransactionBackendAdapter::new(transaction);
    let preparation_context = collaborators
        .sql_preparation_seed(runtime_state.provider(), &context.public_surface_registry)
        .with_compiler_metadata(&compiler_metadata, active_history_root_commit_id.as_deref());

    let mut compiled_execution = match compile_execution_from_template_instance_with_context(
        &preparation_context,
        bound_statement_template,
        context.active_version_id.as_str(),
        &context.active_account_ids,
        writer_key.as_deref(),
        allow_internal_tables,
        PreparationPolicy {
            skip_side_effect_collection,
        },
    )
    .await
    {
        Ok(compiled_execution) => compiled_execution,
        Err(error) => {
            return Err(normalize_sql_error_with_backend_and_relation_names(
                &backend,
                error,
                diagnostic_context.relation_names(),
            )
            .await);
        }
    };

    if let Some(internal) = compiled_execution.internal_execution() {
        if !internal.mutations.is_empty() {
            validate_inserts(
                &backend,
                collaborators.compiled_schema_cache(),
                &internal.mutations,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_buffered_write_execution_step insert validation failed: {}",
                    error.description
                ),
            })?;
        }
        if !internal.update_validations.is_empty() {
            validate_update_plans(
                &backend,
                collaborators.compiled_schema_cache(),
                &internal.update_validations,
                bound_statement_template.params(),
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_buffered_write_execution_step update validation failed: {}",
                    error.description
                ),
            })?;
        }
    }

    if let Some(public_write) = compiled_execution.public_write().cloned() {
        let functions = runtime_state.provider().clone();
        let public_write = match materialize_prepared_public_write(
            &backend,
            collaborators.projection_registry(),
            pending_transaction_view,
            &public_write,
            functions,
        )
        .await
        {
            Ok(public_write) => public_write,
            Err(error) => {
                return Err(normalize_sql_error_with_backend_and_relation_names(
                    &backend,
                    error,
                    diagnostic_context.relation_names(),
                )
                .await);
            }
        };
        compiled_execution.explain = public_write
            .explain
            .request
            .as_ref()
            .map(|_| public_write.explain.clone());
        *compiled_execution
            .public_write_mut()
            .expect("public write preparation must still hold a public write body") = public_write;
    }

    let prepared_step = prepared_write_step_from_compiled_execution(
        dialect,
        statement_kind,
        compiled_execution,
        diagnostic_context,
        writer_key.clone(),
    )?;
    if let Some(public_write) = prepared_step.public_write() {
        validate_batch_local_write(
            &backend,
            collaborators.compiled_schema_cache(),
            public_write,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_buffered_write_execution_step public batch-local validation failed: {}",
                error.description
            ),
        })?;
    }

    let prepared_runtime_state = prepared_write_runtime_state_for_execution(runtime_state);
    PreparedWriteExecutionStep::build(prepared_step, &prepared_runtime_state)
}

pub(crate) fn collect_statement_relation_names(statement: &Statement) -> Vec<String> {
    let mut result = Vec::<String>::new();
    let _ = visit_relations(statement, |relation| {
        if let Some(name) = relation
            .0
            .last()
            .and_then(ObjectNamePart::as_ident)
            .map(|ident| ident.value.clone())
        {
            let exists = result
                .iter()
                .any(|existing| existing.eq_ignore_ascii_case(&name));
            if !exists {
                result.push(name);
            }
        }
        ControlFlow::<()>::Continue(())
    });
    result
}

async fn validate_update_plans(
    backend: &dyn LixBackend,
    cache: &dyn CompiledSchemaCache,
    plans: &[UpdateValidationPlan],
    params: &[Value],
) -> Result<(), LixError> {
    let mut inputs = Vec::with_capacity(plans.len());
    for plan in plans {
        inputs.push(load_update_validation_input(backend, plan, params).await?);
    }
    validate_update_inputs(backend, cache, &inputs).await
}

async fn load_update_validation_input(
    backend: &dyn LixBackend,
    plan: &UpdateValidationPlan,
    params: &[Value],
) -> Result<UpdateValidationInput, LixError> {
    let live_access = backend
        .load_live_read_shape_for_table_name(&plan.table)
        .await?;
    let snapshot_projection = if live_access.is_some() {
        String::new()
    } else {
        ", snapshot_content".to_string()
    };
    let normalized_projection = live_access
        .as_ref()
        .map(|access| access.normalized_projection_sql(None))
        .unwrap_or_default();
    let mut sql = format!(
        "SELECT entity_id, file_id, version_id, schema_key, schema_version{snapshot_projection}{normalized_projection} FROM {}",
        plan.table,
        snapshot_projection = snapshot_projection,
        normalized_projection = normalized_projection,
    );
    if let Some(where_clause) = &plan.where_clause {
        sql.push_str(" WHERE ");
        sql.push_str(&where_clause.to_string());
    }

    let bound = bind_sql(&sql, params, backend.dialect())?;
    let result = backend.execute(&bound.sql, &bound.params).await?;
    let rows = result
        .rows
        .into_iter()
        .map(|row| decode_update_validation_row(live_access.as_deref(), &row))
        .collect::<Result<Vec<_>, _>>()?;

    Ok(UpdateValidationInput {
        plan: plan.clone(),
        rows,
    })
}

fn decode_update_validation_row(
    live_access: Option<&dyn LiveReadShapeContract>,
    row: &[Value],
) -> Result<UpdateValidationInputRow, LixError> {
    let schema_key = value_to_string(&row[3], "schema_key")?;
    Ok(UpdateValidationInputRow {
        entity_id: value_to_string(&row[0], "entity_id")?,
        file_id: value_to_string(&row[1], "file_id")?,
        version_id: value_to_string(&row[2], "version_id")?,
        schema_key: schema_key.clone(),
        schema_version: value_to_string(&row[4], "schema_version")?,
        base_snapshot: required_projected_row_snapshot_json(
            live_access,
            schema_key.as_str(),
            row,
            5,
            5,
        )?,
    })
}

fn required_projected_row_snapshot_json(
    access: Option<&dyn LiveReadShapeContract>,
    schema_key: &str,
    row: &[Value],
    first_projected_column: usize,
    raw_snapshot_index: usize,
) -> Result<JsonValue, LixError> {
    let snapshot = match access {
        Some(access) => access.snapshot_from_projected_row(
            schema_key,
            row,
            first_projected_column,
            raw_snapshot_index,
        )?,
        None => value_snapshot_json(row.get(raw_snapshot_index), schema_key)?,
    };
    snapshot.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "projected row for schema '{}' did not contain a logical snapshot",
                schema_key
            ),
        )
    })
}

fn value_snapshot_json(
    value: Option<&Value>,
    schema_key: &str,
) -> Result<Option<JsonValue>, LixError> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Json(json)) => Ok(Some(json.clone())),
        Some(Value::Text(text)) => serde_json::from_str::<JsonValue>(text)
            .map(Some)
            .map_err(|err| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!(
                        "projected snapshot_content for schema '{}' is not valid JSON: {err}",
                        schema_key
                    ),
                )
            }),
        Some(other) => Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!(
                "projected snapshot_content for schema '{}' must be JSON, text, or null, got {other:?}",
                schema_key
            ),
        )),
    }
}

fn value_to_string(value: &Value, name: &str) -> Result<String, LixError> {
    match value {
        Value::Text(text) => Ok(text.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("expected text value for {name}"),
        }),
    }
}

fn prepared_write_step_from_compiled_execution(
    dialect: crate::SqlDialect,
    statement_kind: PreparedWriteStatementKind,
    compiled: CompiledExecution,
    mut diagnostic_context: PreparedWriteDiagnosticContext,
    writer_key: Option<String>,
) -> Result<PreparedWriteStep, LixError> {
    diagnostic_context.plain_explain_template = compiled
        .plain_explain()
        .map(prepare_plain_explain_template)
        .transpose()?
        .flatten();
    diagnostic_context.analyzed_explain_template = compiled
        .analyzed_explain()
        .map(prepare_analyzed_explain_template)
        .transpose()?
        .flatten();
    diagnostic_context.explain_mode = compiled.explain().and_then(|explain| {
        explain.request().map(|request| {
            if request.requires_execution() {
                PreparedExplainMode::Analyze
            } else {
                PreparedExplainMode::Plain
            }
        })
    });

    let artifact = if let Some(public_read) = compiled.public_read() {
        PreparedWriteArtifact::PublicRead(prepare_public_read_artifact(public_read, dialect)?)
    } else if let Some(public_write) = compiled.public_write() {
        PreparedWriteArtifact::PublicWrite(
            prepared_public_write_artifact_from_prepared_public_write(public_write),
        )
    } else if let Some(internal) = compiled.internal_execution() {
        PreparedWriteArtifact::Internal(PreparedInternalWriteArtifact {
            prepared_batch: PreparedBatch {
                steps: internal.prepared_statements.clone(),
            },
            live_table_requirements: internal.live_table_requirements.clone(),
            mutations: internal.mutations.clone(),
            has_update_validations: !internal.update_validations.is_empty(),
            should_refresh_file_cache: internal.should_refresh_file_cache,
            read_only_query: compiled.read_only_query,
            filesystem_state: compiled.intent.filesystem_state.clone(),
            effects: compiled.effects.clone(),
            writer_key,
        })
    } else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "write preparation compiled an empty execution unexpectedly",
        ));
    };

    let public_surface_registry_effect = prepared_public_surface_registry_effect_for_artifact(
        &artifact,
        diagnostic_context.explain_mode,
    )?;

    Ok(PreparedWriteStep {
        statement_kind,
        result_contract: compiled.result_contract,
        artifact,
        diagnostic_context,
        public_surface_registry_effect,
    })
}

fn prepared_public_surface_registry_effect_for_artifact(
    artifact: &PreparedWriteArtifact,
    explain_mode: Option<PreparedExplainMode>,
) -> Result<PreparedPublicSurfaceRegistryEffect, LixError> {
    if explain_mode.is_some() {
        return Ok(PreparedPublicSurfaceRegistryEffect::None);
    }

    match artifact {
        PreparedWriteArtifact::PublicRead(_) => Ok(PreparedPublicSurfaceRegistryEffect::None),
        PreparedWriteArtifact::PublicWrite(public_write) => {
            let mutations =
                prepared_public_surface_registry_mutations_from_public_write(public_write)?;
            if mutations.is_empty() {
                Ok(PreparedPublicSurfaceRegistryEffect::None)
            } else {
                Ok(PreparedPublicSurfaceRegistryEffect::ApplyMutations(
                    mutations,
                ))
            }
        }
        PreparedWriteArtifact::Internal(internal) => {
            if internal.mutations.iter().any(|row| {
                row.schema_key == "lix_registered_schema"
                    && row.version_id == GLOBAL_VERSION_ID
                    && !row.untracked
            }) {
                Ok(PreparedPublicSurfaceRegistryEffect::ReloadFromStorage)
            } else {
                Ok(PreparedPublicSurfaceRegistryEffect::None)
            }
        }
    }
}

fn prepared_public_surface_registry_mutations_from_public_write(
    public_write: &PreparedPublicWriteArtifact,
) -> Result<Vec<PreparedPublicSurfaceRegistryMutation>, LixError> {
    let Some(resolved) = public_write.contract.resolved_write_plan.as_ref() else {
        return Ok(Vec::new());
    };

    let mut mutations = Vec::new();
    for row in resolved.intended_post_state() {
        if row.schema_key != "lix_registered_schema"
            || row.version_id.as_deref() != Some(GLOBAL_VERSION_ID)
        {
            continue;
        }

        if row.tombstone {
            if let Some((schema_key, _)) = row.entity_id.rsplit_once('~') {
                mutations.push(PreparedPublicSurfaceRegistryMutation::RemoveDynamicSchema {
                    schema_key: schema_key.to_string(),
                });
            }
            continue;
        }

        let Some(snapshot_content) = planned_row_optional_json_text_value(row, "snapshot_content")
        else {
            continue;
        };
        let snapshot = serde_json::from_str(snapshot_content.as_ref()).map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("registered schema snapshot_content invalid JSON: {error}"),
            )
        })?;
        mutations.push(
            PreparedPublicSurfaceRegistryMutation::UpsertRegisteredSchemaSnapshot { snapshot },
        );
    }

    Ok(mutations)
}

fn prepared_public_write_artifact_from_prepared_public_write(
    public_write: &crate::sql::prepare::PreparedPublicWrite,
) -> PreparedPublicWriteArtifact {
    PreparedPublicWriteArtifact {
        contract: PreparedPublicWriteContract {
            operation_kind: prepared_write_operation_kind_from_sql(
                public_write.planned_write.command.operation_kind,
            ),
            target: public_write.planned_write.command.target.clone(),
            on_conflict_action: public_write
                .planned_write
                .command
                .on_conflict
                .as_ref()
                .map(|conflict| prepared_insert_on_conflict_action_from_sql(conflict.action)),
            requested_version_id: public_write
                .planned_write
                .command
                .execution_context
                .requested_version_id
                .clone(),
            active_account_ids: public_write
                .planned_write
                .command
                .execution_context
                .active_account_ids
                .clone(),
            writer_key: public_write
                .planned_write
                .command
                .execution_context
                .writer_key
                .clone(),
            resolved_write_plan: public_write
                .planned_write
                .resolved_write_plan
                .as_ref()
                .map(prepared_resolved_write_plan_from_sql),
        },
        execution: prepared_public_write_execution_artifact_from_sql(&public_write.execution),
    }
}

fn prepared_public_write_execution_artifact_from_sql(
    execution: &crate::sql::physical_plan::PreparedPublicWriteExecution,
) -> PreparedPublicWriteExecutionArtifact {
    match execution {
        crate::sql::physical_plan::PreparedPublicWriteExecution::Noop => {
            PreparedPublicWriteExecutionArtifact::Noop
        }
        crate::sql::physical_plan::PreparedPublicWriteExecution::Materialize(materialization) => {
            PreparedPublicWriteExecutionArtifact::Materialize(PreparedPublicWriteMaterialization {
                partitions: materialization
                    .partitions
                    .iter()
                    .map(prepared_public_write_execution_partition_from_sql)
                    .collect(),
            })
        }
    }
}

fn prepared_public_write_execution_partition_from_sql(
    partition: &crate::sql::physical_plan::PublicWriteExecutionPartition,
) -> PreparedPublicWriteExecutionPartition {
    match partition {
        crate::sql::physical_plan::PublicWriteExecutionPartition::Tracked(tracked) => {
            PreparedPublicWriteExecutionPartition::Tracked(PreparedTrackedWriteExecution {
                schema_live_table_requirements: tracked.schema_live_table_requirements.clone(),
                domain_change_batch: tracked.domain_change_batch.clone(),
                create_preconditions: tracked.create_preconditions.clone(),
                semantic_effects: tracked.semantic_effects.clone(),
            })
        }
        crate::sql::physical_plan::PublicWriteExecutionPartition::Untracked(untracked) => {
            PreparedPublicWriteExecutionPartition::Untracked(PreparedUntrackedWriteExecution {
                intended_post_state: untracked.intended_post_state.clone(),
                semantic_effects: untracked.semantic_effects.clone(),
                persist_filesystem_payloads_before_write: untracked
                    .persist_filesystem_payloads_before_write,
            })
        }
    }
}

fn prepared_resolved_write_plan_from_sql(
    resolved: &crate::sql::logical_plan::public_ir::ResolvedWritePlan,
) -> PreparedResolvedWritePlan {
    PreparedResolvedWritePlan {
        partitions: resolved
            .partitions
            .iter()
            .map(|partition| PreparedResolvedWritePartition {
                execution_mode: partition.execution_mode,
                authoritative_pre_state_rows: partition.authoritative_pre_state_rows.clone(),
                intended_post_state: partition.intended_post_state.clone(),
                workspace_writer_key_updates: partition.workspace_writer_key_updates.clone(),
                filesystem_state: partition.filesystem_state.clone(),
            })
            .collect(),
    }
}

fn prepared_write_operation_kind_from_sql(
    kind: crate::sql::logical_plan::public_ir::WriteOperationKind,
) -> PreparedWriteOperationKind {
    match kind {
        crate::sql::logical_plan::public_ir::WriteOperationKind::Insert => {
            PreparedWriteOperationKind::Insert
        }
        crate::sql::logical_plan::public_ir::WriteOperationKind::Update => {
            PreparedWriteOperationKind::Update
        }
        crate::sql::logical_plan::public_ir::WriteOperationKind::Delete => {
            PreparedWriteOperationKind::Delete
        }
    }
}

fn prepared_insert_on_conflict_action_from_sql(
    action: crate::sql::logical_plan::public_ir::InsertOnConflictAction,
) -> PreparedInsertOnConflictAction {
    match action {
        crate::sql::logical_plan::public_ir::InsertOnConflictAction::DoUpdate => {
            PreparedInsertOnConflictAction::DoUpdate
        }
        crate::sql::logical_plan::public_ir::InsertOnConflictAction::DoNothing => {
            PreparedInsertOnConflictAction::DoNothing
        }
    }
}

fn planned_row_optional_json_text_value<'a>(
    row: &'a crate::contracts::artifacts::PlannedStateRow,
    key: &str,
) -> Option<Cow<'a, str>> {
    match row.values.get(key) {
        Some(Value::Text(value)) => Some(Cow::Borrowed(value.as_str())),
        Some(Value::Json(value)) => Some(Cow::Owned(value.to_string())),
        _ => None,
    }
}

async fn materialize_prepared_public_write<P>(
    backend: &dyn crate::LixBackend,
    projection_registry: &ProjectionRegistry,
    pending_transaction_view: Option<&PendingTransactionView>,
    public_write: &crate::sql::prepare::PreparedPublicWrite,
    functions: SharedFunctionProvider<P>,
) -> Result<crate::sql::prepare::PreparedPublicWrite, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let physical_started = Instant::now();
    let mut public_write = public_write.clone();
    let selector_resolver = SessionWriteSelectorResolver::new(
        backend,
        projection_registry,
        pending_transaction_view.map(|view| view as &dyn PendingView),
    )
    .await?;
    let resolved_write_plan = resolve_write_plan_with_functions(
        backend,
        &public_write.planned_write,
        pending_transaction_view.map(|view| view as &dyn PendingView),
        functions,
        &selector_resolver,
    )
    .await
    .map_err(|error| {
        public_authoritative_write_error(&public_write.canonicalized, error.message.clone())
            .unwrap_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", error.message))
    })?;

    public_write.planned_write.resolved_write_plan = Some(resolved_write_plan);

    let domain_change_batches =
        build_domain_change_batch(&public_write.planned_write).map_err(|error| {
            public_write_preparation_error(&public_write.canonicalized, &error.message)
                .unwrap_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", &error.message))
        })?;
    let commit_preconditions =
        derive_commit_preconditions(&public_write.planned_write).map_err(|error| {
            public_write_preparation_error(&public_write.canonicalized, &error.message)
                .unwrap_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", &error.message))
        })?;
    public_write.planned_write.commit_preconditions = commit_preconditions.clone();

    let mut execution = build_public_write_execution(
        &public_write.planned_write,
        &domain_change_batches,
        &commit_preconditions,
    )?
    .ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public write target must route through explicit public materialization",
        )
    })?;
    let filesystem_state = public_write
        .planned_write
        .resolved_write_plan
        .as_ref()
        .expect("public write materialization requires a resolved write plan")
        .filesystem_state();
    if let crate::sql::physical_plan::PreparedPublicWriteExecution::Materialize(materialization) =
        &mut execution
    {
        finalize_public_write_execution(
            materialization,
            &public_write.planned_write,
            &filesystem_state,
        )?;
    }

    let mut stage_timings = public_write.explain_plan.stage_timings.clone();
    stage_timings.push(stage_timing(
        ExplainStage::PhysicalPlanning,
        physical_started.elapsed(),
    ));

    public_write.domain_change_batches = domain_change_batches.clone();
    public_write.execution = execution.clone();
    public_write.explain = build_public_write_explain_artifacts(PublicWriteExplainBuildInput {
        request: public_write.explain_plan.request.clone(),
        semantics: public_write.explain_plan.semantics.clone(),
        planned_write: public_write.planned_write.clone(),
        execution,
        domain_change_batches,
        invariant_trace: Some(build_public_write_invariant_trace(
            &public_write.planned_write,
        )),
        stage_timings,
    });

    Ok(public_write)
}
