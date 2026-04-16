//! Neutral write preparation pipeline.
//!
//! `session/*` owns request orchestration, but the lower-level write-statement
//! preparation machinery should not live under the session owner.

use std::borrow::Cow;
use std::ops::ControlFlow;
use std::time::Instant;

use serde_json::Value as JsonValue;
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};

use crate::backend::PreparedBatch;
use crate::catalog::CatalogProjectionRegistry;
use crate::catalog::SurfaceRegistry;
use crate::functions::{
    clone_boxed_function_provider, FunctionBindings, LixFunctionProvider, SharedFunctionProvider,
};
use crate::live_state::{LiveRowShapeContract, LiveStateQueryBackend};
use crate::schema::CompiledSchemaCache;
use crate::sql::bind_sql;
use crate::sql::{
    build_change_batches, build_public_write_execution,
    compile_execution_from_bound_statement_with_context, compiled_explain_diagnostics,
    derive_commit_preconditions, finalize_public_write_execution,
    load_sql_compiler_metadata_with_reader_and_pending_overlay,
    normalize_sql_error_with_backend_and_relation_names, prepare_public_read_artifact,
    public_authoritative_write_error, public_write_preparation_error,
    refresh_materialized_public_write_explain, BoundStatementInstance, CompilePolicy,
    CompiledExecution, InsertOnConflictAction, PlannedStateRow, PreparedExplainMode,
    PreparedInsertOnConflictAction, PreparedWriteOperationKind, PreparedWriteStatementKind,
    PublicWriteExecutionPartition, PublicWritePhysicalPlan, PublicWritePlan, ResolvedWritePlan,
    SqlCompilerMetadata, SqlPreparationMetadataReader, SqlPreparationPendingOverlay,
    UpdateValidationPlan, WriteDiagnosticContext, WriteMode, WriteOperationKind,
};
use crate::transaction::ensure_runtime_sequence_initialized_in_transaction;
use crate::transaction::overlay::PendingOverlay;
use crate::transaction::pipeline::resolution::resolve_write_plan_with_functions;
use crate::transaction::pipeline::validation::{
    validate_batch_local_write, validate_inserts, validate_update_inputs,
};
use crate::transaction::{
    PendingWriteOverlay, PreparedDirectWriteArtifact, PreparedPublicSurfaceRegistryEffect,
    PreparedPublicSurfaceRegistryMutation, PreparedPublicWrite, PreparedPublicWriteContract,
    PreparedPublicWriteExecution, PreparedPublicWriteMaterialization,
    PreparedPublicWritePlanArtifact, PreparedResolvedWritePartition, PreparedResolvedWritePlan,
    PreparedWriteArtifact, PreparedWriteFunctionBindings, PreparedWriteStatement,
    SessionCompilerState, TransactionWriteSelectorResolver, UpdateValidationInput,
    UpdateValidationInputRow, WriteCommand, WriteExecutionContext,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct WritePreparationStamp {
    public_surface_registry_generation: u64,
}

impl WritePreparationStamp {
    pub(crate) fn capture(context: &SessionCompilerState) -> Self {
        Self {
            public_surface_registry_generation: context.public_surface_registry_generation(),
        }
    }
}

#[derive(Clone)]
pub(crate) struct WritePreparationContext {
    stamp: WritePreparationStamp,
    public_surface_registry: SurfaceRegistry,
    compiler_metadata: SqlCompilerMetadata,
    active_history_root_commit_id: Option<String>,
    active_version_id: String,
    active_account_ids: Vec<String>,
    origin_key: Option<String>,
}

impl WritePreparationContext {
    pub(crate) fn stamp(&self) -> WritePreparationStamp {
        self.stamp
    }

    fn public_surface_registry(&self) -> &SurfaceRegistry {
        &self.public_surface_registry
    }

    fn compiler_metadata(&self) -> &SqlCompilerMetadata {
        &self.compiler_metadata
    }

    fn active_history_root_commit_id(&self) -> Option<&str> {
        self.active_history_root_commit_id.as_deref()
    }

    fn active_version_id(&self) -> &str {
        &self.active_version_id
    }

    fn active_account_ids(&self) -> &[String] {
        &self.active_account_ids
    }

    fn origin_key(&self) -> Option<&str> {
        self.origin_key.as_deref()
    }
}

struct WriteCommandSeed {
    dialect: crate::SqlDialect,
    statement_kind: PreparedWriteStatementKind,
    diagnostic_context: WriteDiagnosticContext,
    origin_key: Option<String>,
    compiled_execution: CompiledExecution,
    function_bindings: PreparedWriteFunctionBindings,
}

pub(crate) struct CompiledWriteCommand {
    payload: WriteCommandSeed,
}

pub(crate) struct ValidatedWriteCommand {
    payload: WriteCommandSeed,
}

pub(crate) struct MaterializedWriteCommand {
    payload: WriteCommandSeed,
}

impl ValidatedWriteCommand {
    fn relation_names(&self) -> &[String] {
        self.payload.diagnostic_context.relation_names()
    }
}

pub(crate) async fn build_write_preparation_context(
    mut transaction: &mut dyn LixBackendTransaction,
    pending_write_overlay: Option<&PendingWriteOverlay>,
    context: &SessionCompilerState,
) -> Result<WritePreparationContext, LixError> {
    let active_history_root_commit_id = transaction
        .load_active_history_root_commit_id_for_preparation(context.active_version_id.as_str())
        .await?;
    let compiler_metadata = {
        let metadata_reader = &mut transaction;
        load_sql_compiler_metadata_with_reader_and_pending_overlay(
            metadata_reader,
            &context.public_surface_registry,
            pending_write_overlay.map(|view| view as &dyn SqlPreparationPendingOverlay),
        )
        .await?
    };
    Ok(WritePreparationContext {
        stamp: WritePreparationStamp::capture(context),
        public_surface_registry: context.public_surface_registry.clone(),
        compiler_metadata,
        active_history_root_commit_id,
        active_version_id: context.active_version_id.clone(),
        active_account_ids: context.active_account_ids.clone(),
        origin_key: context.origin_key.clone(),
    })
}

pub(crate) async fn ensure_function_bindings_for_write_scope(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    context: &mut SessionCompilerState,
) -> Result<(), LixError> {
    if context.function_bindings().is_some() {
        return Ok(());
    }

    let backend = crate::backend::transaction_backend_view(transaction);
    let function_bindings = execution_context
        .prepare_function_bindings(&backend)
        .await?;
    context.set_function_bindings(function_bindings);
    Ok(())
}

pub(crate) fn prepared_write_function_bindings_for_execution(
    function_bindings: &FunctionBindings,
) -> PreparedWriteFunctionBindings {
    let provider = clone_boxed_function_provider(function_bindings.provider());
    PreparedWriteFunctionBindings::new(function_bindings.deterministic_enabled(), provider)
}

pub(crate) async fn prepare_buffered_write_execution_step(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    pending_write_overlay: Option<&PendingWriteOverlay>,
    bound_statement: &BoundStatementInstance,
    prepared_context: &WritePreparationContext,
    allow_internal_relations: bool,
    context: &SessionCompilerState,
    skip_side_effect_collection: bool,
) -> Result<WriteCommand, LixError> {
    let compiled = compile_write_command(
        execution_context,
        transaction,
        bound_statement,
        prepared_context,
        allow_internal_relations,
        context,
        skip_side_effect_collection,
    )
    .await?;
    let backend = crate::backend::transaction_backend_view(transaction);
    let validated = validate_compiled_write_command(
        &backend,
        execution_context.compiled_schema_cache(),
        bound_statement.params(),
        pending_write_overlay.map(|view| view as &dyn PendingOverlay),
        compiled,
    )
    .await?;
    let relation_names = validated.relation_names().to_vec();
    let materialized = match materialize_validated_write_command(
        &backend,
        execution_context.catalog_projection_registry(),
        pending_write_overlay,
        validated,
    )
    .await
    {
        Ok(materialized) => materialized,
        Err(error) => {
            return Err(normalize_sql_error_with_backend_and_relation_names(
                &backend,
                error,
                &relation_names,
            )
            .await);
        }
    };
    let command = assemble_write_command(materialized)?;
    debug_assert_eq!(
        prepared_context.stamp(),
        WritePreparationStamp::capture(context),
        "prepared write command should use the current prepared-context stamp",
    );
    validate_write_command(
        &backend,
        execution_context.compiled_schema_cache(),
        pending_write_overlay.map(|view| view as &dyn PendingOverlay),
        &command,
    )
    .await?;
    Ok(command)
}

async fn compile_write_command(
    execution_context: &dyn WriteExecutionContext,
    transaction: &mut dyn LixBackendTransaction,
    bound_statement: &BoundStatementInstance,
    prepared_context: &WritePreparationContext,
    allow_internal_relations: bool,
    context: &SessionCompilerState,
    skip_side_effect_collection: bool,
) -> Result<CompiledWriteCommand, LixError> {
    let statement_kind = PreparedWriteStatementKind::for_statement(bound_statement.statement());
    let diagnostic_context = WriteDiagnosticContext::new(collect_statement_relation_names(
        bound_statement.statement(),
    ));
    let origin_key = prepared_context.origin_key.clone();
    let function_bindings = context
        .function_bindings()
        .expect("write execution should install function bindings before preparation");

    if function_bindings.deterministic_enabled() {
        let mut runtime_functions = function_bindings.provider().clone();
        ensure_runtime_sequence_initialized_in_transaction(transaction, &mut runtime_functions)
            .await?;
    }

    let dialect = transaction.dialect();
    let backend = crate::backend::transaction_backend_view(transaction);
    let compiler_context = execution_context
        .sql_compiler_seed(
            function_bindings.provider(),
            prepared_context.public_surface_registry(),
        )
        .with_compiler_metadata(
            prepared_context.compiler_metadata(),
            prepared_context.active_history_root_commit_id(),
        );

    let compiled_execution = match compile_execution_from_bound_statement_with_context(
        &compiler_context,
        bound_statement,
        prepared_context.active_version_id(),
        prepared_context.active_account_ids(),
        prepared_context.origin_key(),
        allow_internal_relations,
        CompilePolicy {
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

    Ok(CompiledWriteCommand {
        payload: WriteCommandSeed {
            dialect,
            statement_kind,
            diagnostic_context,
            origin_key,
            compiled_execution,
            function_bindings: prepared_write_function_bindings_for_execution(function_bindings),
        },
    })
}

async fn validate_compiled_write_command(
    backend: &dyn LixBackend,
    cache: &dyn CompiledSchemaCache,
    params: &[Value],
    pending_overlay: Option<&dyn PendingOverlay>,
    compiled: CompiledWriteCommand,
) -> Result<ValidatedWriteCommand, LixError> {
    if let Some(internal) = compiled.payload.compiled_execution.direct_execution() {
        if !internal.mutations.is_empty() {
            validate_inserts(backend, cache, &internal.mutations, pending_overlay)
                .await
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "prepare_buffered_write_execution_step insert validation failed: {}",
                        error.description
                    ),
                    hint: None,
                })?;
        }
        if !internal.update_validations.is_empty() {
            validate_update_plans(
                backend,
                cache,
                &internal.update_validations,
                params,
                pending_overlay,
            )
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_buffered_write_execution_step update validation failed: {}",
                    error.description
                ),
                hint: None,
            })?;
        }
    }

    Ok(ValidatedWriteCommand {
        payload: compiled.payload,
    })
}

async fn materialize_validated_write_command(
    backend: &dyn crate::LixBackend,
    projection_registry: &CatalogProjectionRegistry,
    pending_write_overlay: Option<&PendingWriteOverlay>,
    mut validated: ValidatedWriteCommand,
) -> Result<MaterializedWriteCommand, LixError> {
    if let Some(public_write) = validated.payload.compiled_execution.public_write().cloned() {
        let public_write = materialize_prepared_public_write(
            backend,
            projection_registry,
            pending_write_overlay,
            &public_write,
            validated.payload.function_bindings.provider().clone(),
        )
        .await?;
        validated.payload.compiled_execution.explain = public_write
            .explain
            .request
            .as_ref()
            .map(|_| public_write.explain.clone());
        *validated
            .payload
            .compiled_execution
            .public_write_mut()
            .expect("public write preparation must still hold a public write body") = public_write;
    }

    Ok(MaterializedWriteCommand {
        payload: validated.payload,
    })
}

fn assemble_write_command(
    materialized: MaterializedWriteCommand,
) -> Result<WriteCommand, LixError> {
    let WriteCommandSeed {
        dialect,
        statement_kind,
        diagnostic_context,
        origin_key,
        compiled_execution,
        function_bindings,
    } = materialized.payload;
    let prepared_statement = prepared_write_statement_from_compiled_execution(
        dialect,
        statement_kind,
        compiled_execution,
        diagnostic_context,
        origin_key,
    )?;
    WriteCommand::build(prepared_statement, &function_bindings)
}

async fn validate_write_command(
    backend: &dyn LixBackend,
    cache: &dyn CompiledSchemaCache,
    pending_overlay: Option<&dyn PendingOverlay>,
    command: &WriteCommand,
) -> Result<(), LixError> {
    if let Some(public_write) = command.prepared().public_write() {
        validate_batch_local_write(backend, cache, public_write, pending_overlay)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_buffered_write_execution_step public batch-local validation failed: {}",
                    error.description
                ),
                hint: None,
            })?;
    }
    Ok(())
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
    pending_overlay: Option<&dyn PendingOverlay>,
) -> Result<(), LixError> {
    let mut inputs = Vec::with_capacity(plans.len());
    for plan in plans {
        inputs.push(load_update_validation_input(backend, plan, params).await?);
    }
    validate_update_inputs(backend, cache, &inputs, pending_overlay).await
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
    live_access: Option<&dyn LiveRowShapeContract>,
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
    access: Option<&dyn LiveRowShapeContract>,
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
            hint: None,
        }),
    }
}

fn prepared_write_statement_from_compiled_execution(
    dialect: crate::SqlDialect,
    statement_kind: PreparedWriteStatementKind,
    compiled: CompiledExecution,
    mut diagnostic_context: WriteDiagnosticContext,
    origin_key: Option<String>,
) -> Result<PreparedWriteStatement, LixError> {
    let explain_diagnostics = compiled_explain_diagnostics(&compiled)?;
    diagnostic_context.plain_explain_template = explain_diagnostics.plain_template;
    diagnostic_context.analyzed_explain_template = explain_diagnostics.analyzed_template;
    diagnostic_context.explain_mode = explain_diagnostics.explain_mode;

    let artifact = if let Some(public_read) = compiled.public_read() {
        PreparedWriteArtifact::PublicRead(prepare_public_read_artifact(public_read, dialect)?)
    } else if let Some(public_write) = compiled.public_write() {
        PreparedWriteArtifact::PublicWrite(
            prepared_public_write_artifact_from_prepared_public_write(public_write),
        )
    } else if let Some(internal) = compiled.direct_execution() {
        PreparedWriteArtifact::Direct(PreparedDirectWriteArtifact {
            prepared_batch: PreparedBatch {
                steps: internal.prepared_statements.clone(),
            },
            live_table_requirements: internal.live_table_requirements.clone(),
            mutations: internal.mutations.clone(),
            has_update_validations: !internal.update_validations.is_empty(),
            should_refresh_file_cache: internal.should_refresh_file_cache,
            read_only_query: compiled.read_only_query,
            filesystem_state: compiled.filesystem_intent.filesystem_state.clone(),
            effects: compiled.effects.clone(),
            origin_key,
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

    Ok(PreparedWriteStatement {
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
            if public_write_mutates_registered_schema(public_write) {
                return Ok(PreparedPublicSurfaceRegistryEffect::ReloadFromStorage);
            }

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
        PreparedWriteArtifact::Direct(internal) => {
            if internal
                .mutations
                .iter()
                .any(|row| row.schema_key == "lix_registered_schema" && !row.untracked)
            {
                Ok(PreparedPublicSurfaceRegistryEffect::ReloadFromStorage)
            } else {
                Ok(PreparedPublicSurfaceRegistryEffect::None)
            }
        }
    }
}

fn public_write_mutates_registered_schema(public_write: &PreparedPublicWrite) -> bool {
    public_write
        .contract
        .resolved_write_plan
        .as_ref()
        .is_some_and(|resolved| {
            resolved
                .intended_post_state()
                .any(|row| row.schema_key == "lix_registered_schema")
        })
}

fn prepared_public_surface_registry_mutations_from_public_write(
    public_write: &PreparedPublicWrite,
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
    public_write: &PublicWritePlan,
) -> PreparedPublicWrite {
    PreparedPublicWrite {
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
                .statement_context
                .requested_version_id
                .clone(),
            active_account_ids: public_write
                .planned_write
                .command
                .statement_context
                .active_account_ids
                .clone(),
            origin_key: public_write
                .planned_write
                .command
                .statement_context
                .origin_key
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
    execution: &PublicWritePhysicalPlan,
) -> PreparedPublicWritePlanArtifact {
    match execution {
        PublicWritePhysicalPlan::Noop => PreparedPublicWritePlanArtifact::Noop,
        PublicWritePhysicalPlan::Materialize(materialization) => {
            PreparedPublicWritePlanArtifact::Materialize(PreparedPublicWriteMaterialization {
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
    partition: &PublicWriteExecutionPartition,
) -> PreparedPublicWriteExecution {
    match partition {
        PublicWriteExecutionPartition::Tracked(tracked) => PreparedPublicWriteExecution {
            execution_mode: WriteMode::Tracked,
            intended_post_state: Vec::new(),
            schema_live_table_requirements: tracked.schema_live_table_requirements.clone(),
            change_batch: tracked.change_batch.clone(),
            create_preconditions: Some(tracked.create_preconditions.clone()),
            semantic_effects: tracked.semantic_effects.clone(),
            persist_filesystem_payloads_before_write: false,
        },
        PublicWriteExecutionPartition::Untracked(untracked) => PreparedPublicWriteExecution {
            execution_mode: WriteMode::Untracked,
            intended_post_state: untracked.intended_post_state.clone(),
            schema_live_table_requirements: Vec::new(),
            change_batch: None,
            create_preconditions: None,
            semantic_effects: untracked.semantic_effects.clone(),
            persist_filesystem_payloads_before_write: untracked
                .persist_filesystem_payloads_before_write,
        },
    }
}

fn prepared_resolved_write_plan_from_sql(
    resolved: &ResolvedWritePlan,
) -> PreparedResolvedWritePlan {
    PreparedResolvedWritePlan {
        partitions: resolved
            .partitions
            .iter()
            .map(|partition| PreparedResolvedWritePartition {
                execution_mode: partition.execution_mode,
                authoritative_pre_state_rows: partition.authoritative_pre_state_rows.clone(),
                intended_post_state: partition.intended_post_state.clone(),
                filesystem_state: partition.filesystem_state.clone(),
            })
            .collect(),
    }
}

fn prepared_write_operation_kind_from_sql(kind: WriteOperationKind) -> PreparedWriteOperationKind {
    match kind {
        WriteOperationKind::Insert => PreparedWriteOperationKind::Insert,
        WriteOperationKind::Update => PreparedWriteOperationKind::Update,
        WriteOperationKind::Delete => PreparedWriteOperationKind::Delete,
    }
}

fn prepared_insert_on_conflict_action_from_sql(
    action: InsertOnConflictAction,
) -> PreparedInsertOnConflictAction {
    match action {
        InsertOnConflictAction::DoUpdate => PreparedInsertOnConflictAction::DoUpdate,
        InsertOnConflictAction::DoNothing => PreparedInsertOnConflictAction::DoNothing,
    }
}

fn planned_row_optional_json_text_value<'a>(
    row: &'a PlannedStateRow,
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
    projection_registry: &CatalogProjectionRegistry,
    pending_write_overlay: Option<&PendingWriteOverlay>,
    public_write: &PublicWritePlan,
    functions: SharedFunctionProvider<P>,
) -> Result<PublicWritePlan, LixError>
where
    P: LixFunctionProvider + Send + 'static,
{
    let physical_started = Instant::now();
    let mut public_write = public_write.clone();
    let selector_functions = clone_boxed_function_provider(&functions);
    let selector_resolver = TransactionWriteSelectorResolver::new(
        backend,
        projection_registry,
        pending_write_overlay.map(|view| view as &dyn PendingOverlay),
        &selector_functions,
    )
    .await?;
    let resolved_write_plan = resolve_write_plan_with_functions(
        backend,
        &public_write.planned_write,
        pending_write_overlay.map(|view| view as &dyn PendingOverlay),
        functions,
        &selector_resolver,
    )
    .await
    .map_err(|error| {
        let hint = error.hint.clone();
        let lix_err =
            public_authoritative_write_error(&public_write.canonicalized, error.message.clone())
                .unwrap_or_else(|| LixError::new("LIX_ERROR_UNKNOWN", error.message));
        match hint {
            Some(hint) => lix_err.with_hint(hint),
            None => lix_err,
        }
    })?;

    public_write.planned_write.resolved_write_plan = Some(resolved_write_plan);

    let change_batches = build_change_batches(&public_write.planned_write).map_err(|error| {
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
        &change_batches,
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
    if let PublicWritePhysicalPlan::Materialize(materialization) = &mut execution {
        finalize_public_write_execution(
            materialization,
            &public_write.planned_write,
            &filesystem_state,
        )?;
    }

    refresh_materialized_public_write_explain(
        &mut public_write,
        execution,
        change_batches,
        physical_started.elapsed(),
    );

    Ok(public_write)
}
