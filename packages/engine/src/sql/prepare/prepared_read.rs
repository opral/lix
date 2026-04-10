use crate::contracts::SqlPreparationMetadataReader;
use crate::contracts::{
    PreparedBatch, PreparedDirectDirectoryHistoryField, PreparedDirectEntityHistoryField,
    PreparedDirectFileHistoryField, PreparedDirectPublicRead, PreparedDirectStateHistoryField,
    PreparedDirectoryHistoryAggregate, PreparedDirectoryHistoryDirectReadPlan,
    PreparedDirectoryHistoryPredicate, PreparedDirectoryHistoryProjection,
    PreparedDirectoryHistorySortKey, PreparedEntityHistoryDirectReadPlan,
    PreparedEntityHistoryPredicate, PreparedEntityHistoryProjection, PreparedEntityHistorySortKey,
    PreparedExplainMode, PreparedFileHistoryAggregate, PreparedFileHistoryDirectReadPlan,
    PreparedFileHistoryPredicate, PreparedFileHistoryProjection, PreparedFileHistorySortKey,
    PreparedInternalReadArtifact, PreparedPublicReadArtifact, PreparedPublicReadExecutionArtifact,
    PreparedReadArtifact, PreparedReadProgram, PreparedReadStep, PreparedStateHistoryAggregate,
    PreparedStateHistoryAggregatePredicate, PreparedStateHistoryDirectReadPlan,
    PreparedStateHistoryPredicate, PreparedStateHistoryProjection,
    PreparedStateHistoryProjectionValue, PreparedStateHistorySortKey,
    PreparedStateHistorySortValue, PreparedStatement, PublicReadResultColumn,
    PublicReadResultColumns, ReadDiagnosticContext,
};
use crate::diagnostics::{
    build_read_diagnostic_catalog_snapshot, normalize_sql_error_with_read_diagnostic_context,
};
use crate::sql::explain::{prepare_analyzed_explain_template, prepare_plain_explain_template};
use crate::sql::logical_plan::direct_reads::{
    DirectDirectoryHistoryField, DirectEntityHistoryField, DirectFileHistoryField,
    DirectPublicReadPlan, DirectStateHistoryField, DirectoryHistoryAggregate,
    DirectoryHistoryDirectReadPlan, DirectoryHistoryPredicate, DirectoryHistoryProjection,
    DirectoryHistorySortKey, EntityHistoryDirectReadPlan, EntityHistoryPredicate,
    EntityHistoryProjection, EntityHistorySortKey, FileHistoryAggregate, FileHistoryDirectReadPlan,
    FileHistoryPredicate, FileHistoryProjection, FileHistorySortKey, StateHistoryAggregate,
    StateHistoryAggregatePredicate, StateHistoryDirectReadPlan, StateHistoryPredicate,
    StateHistoryProjection, StateHistoryProjectionValue, StateHistorySortKey,
    StateHistorySortValue,
};
use crate::sql::physical_plan::{
    LoweredResultColumn, LoweredResultColumns, PreparedPublicReadExecution,
};
use crate::{LixBackend, LixBackendTransaction, LixError, TransactionMode, Value};
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};
use std::ops::ControlFlow;

use super::execution_program::{BoundStatementTemplateInstance, ExecutionProgram};
use super::{
    compile_execution_from_template_instance_with_context, load_sql_compiler_metadata_with_reader,
    CompiledExecution, PreparationPolicy, PreparedPublicRead, SqlPreparationContext,
    SqlPreparationSeed,
};

pub(crate) struct CommittedReadProgramContext<'a> {
    pub(crate) active_version_id: &'a str,
    pub(crate) active_account_ids: &'a [String],
    pub(crate) writer_key: Option<&'a str>,
    pub(crate) preparation_seed: SqlPreparationSeed<'a>,
    pub(crate) base_transaction_mode: TransactionMode,
}

pub(crate) async fn prepare_committed_read_program_with_backend(
    backend: &dyn LixBackend,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    read_context: &CommittedReadProgramContext<'_>,
) -> Result<PreparedReadProgram, LixError> {
    let mut metadata_reader = backend;
    prepare_committed_read_program_from_reader(
        &mut metadata_reader,
        program,
        allow_internal_tables,
        read_context,
    )
    .await
}

pub(crate) async fn prepare_committed_read_program_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    read_context: &CommittedReadProgramContext<'_>,
) -> Result<PreparedReadProgram, LixError> {
    let mut metadata_reader = transaction;
    prepare_committed_read_program_from_reader(
        &mut metadata_reader,
        program,
        allow_internal_tables,
        read_context,
    )
    .await
}

async fn prepare_committed_read_program_from_reader(
    metadata_reader: &mut dyn SqlPreparationMetadataReader,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    read_context: &CommittedReadProgramContext<'_>,
) -> Result<PreparedReadProgram, LixError> {
    let active_history_root_commit_id = metadata_reader
        .load_active_history_root_commit_id_for_preparation(read_context.active_version_id)
        .await?;
    let compiler_metadata = load_sql_compiler_metadata_with_reader(
        metadata_reader,
        read_context.preparation_seed.surface_registry,
    )
    .await?;
    let preparation_context = read_context
        .preparation_seed
        .with_compiler_metadata(&compiler_metadata, active_history_root_commit_id.as_deref());

    compile_committed_read_program(
        &preparation_context,
        program,
        allow_internal_tables,
        read_context,
    )
    .await
}

pub(crate) async fn compile_committed_read_program(
    preparation_context: &dyn SqlPreparationContext,
    program: &ExecutionProgram,
    allow_internal_tables: bool,
    read_context: &CommittedReadProgramContext<'_>,
) -> Result<PreparedReadProgram, LixError> {
    let mut mode = read_context.base_transaction_mode;
    let mut steps = Vec::new();

    for step in program.steps() {
        let prepared_step = compile_committed_read_step(
            preparation_context,
            step,
            allow_internal_tables,
            read_context,
        )
        .await?;
        mode = merge_committed_read_transaction_mode(mode, prepared_step.transaction_mode);
        steps.push(prepared_step);
    }

    Ok(PreparedReadProgram {
        transaction_mode: mode,
        steps,
    })
}

async fn compile_committed_read_step(
    preparation_context: &dyn SqlPreparationContext,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    read_context: &CommittedReadProgramContext<'_>,
) -> Result<PreparedReadStep, LixError> {
    let source_sql = vec![bound_statement_template.statement().to_string()];
    let relation_names = collect_statement_relation_names(bound_statement_template.statement());
    let diagnostic_context =
        base_read_diagnostic_context(preparation_context, source_sql, relation_names);
    let compiled = compile_committed_execution_step(
        &diagnostic_context,
        preparation_context,
        bound_statement_template,
        allow_internal_tables,
        read_context,
    )
    .await?;
    prepared_read_step_from_compiled_execution(
        preparation_context.dialect(),
        compiled,
        diagnostic_context,
    )
}

async fn compile_committed_execution_step(
    diagnostic_context: &ReadDiagnosticContext,
    preparation_context: &dyn SqlPreparationContext,
    bound_statement_template: &BoundStatementTemplateInstance,
    allow_internal_tables: bool,
    read_context: &CommittedReadProgramContext<'_>,
) -> Result<CompiledExecution, LixError> {
    match compile_execution_from_template_instance_with_context(
        preparation_context,
        bound_statement_template,
        read_context.active_version_id,
        read_context.active_account_ids,
        read_context.writer_key,
        allow_internal_tables,
        PreparationPolicy {
            skip_side_effect_collection: false,
        },
    )
    .await
    {
        Ok(compiled) => Ok(compiled),
        Err(error) => Err(normalize_sql_error_with_read_diagnostic_context(
            error,
            diagnostic_context,
        )),
    }
}

fn prepared_read_step_from_compiled_execution(
    dialect: crate::SqlDialect,
    compiled: CompiledExecution,
    mut diagnostic_context: ReadDiagnosticContext,
) -> Result<PreparedReadStep, LixError> {
    let transaction_mode = transaction_mode_for_committed_read_execution(&compiled)?;
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
        PreparedReadArtifact::Public(prepare_public_read_artifact(public_read, dialect)?)
    } else if let Some(internal) = compiled.internal_execution() {
        PreparedReadArtifact::Internal(PreparedInternalReadArtifact {
            prepared_batch: PreparedBatch {
                steps: internal.prepared_statements.clone(),
            },
            result_contract: compiled.result_contract,
        })
    } else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "committed read routing compiled a public write unexpectedly",
        ));
    };

    Ok(PreparedReadStep {
        transaction_mode,
        artifact,
        diagnostic_context,
    })
}

fn base_read_diagnostic_context(
    preparation_context: &dyn SqlPreparationContext,
    source_sql: Vec<String>,
    relation_names: Vec<String>,
) -> ReadDiagnosticContext {
    ReadDiagnosticContext {
        source_sql,
        relation_names: relation_names.clone(),
        catalog_snapshot: build_read_diagnostic_catalog_snapshot(
            preparation_context.surface_registry(),
            &relation_names,
        ),
        explain_mode: None,
        plain_explain_template: None,
        analyzed_explain_template: None,
    }
}

pub(crate) fn prepare_public_read_artifact(
    public_read: &PreparedPublicRead,
    dialect: crate::SqlDialect,
) -> Result<PreparedPublicReadArtifact, LixError> {
    let mut contract = super::public_surface::read::prepared_public_read_contract(public_read);
    if contract.result_columns.is_none() {
        contract.result_columns = result_columns_for_public_read_execution(&public_read.execution);
    }

    let execution = match &public_read.execution {
        PreparedPublicReadExecution::ReadTimeProjection(read) => {
            PreparedPublicReadExecutionArtifact::ReadTimeProjection(read.clone())
        }
        PreparedPublicReadExecution::LoweredSql(lowered) => {
            PreparedPublicReadExecutionArtifact::LoweredSql(prepared_batch_from_lowered_read(
                dialect,
                lowered,
                &public_read.bound_parameters,
                &public_read.runtime_bindings,
            )?)
        }
        PreparedPublicReadExecution::Direct(plan) => {
            PreparedPublicReadExecutionArtifact::Direct(prepared_direct_public_read(plan))
        }
    };

    Ok(PreparedPublicReadArtifact {
        contract,
        freshness_contract: public_read.freshness_contract,
        surface_bindings: public_read.surface_bindings.clone(),
        public_output_columns: public_read.public_output_columns.clone(),
        execution,
    })
}

fn prepared_batch_from_lowered_read(
    dialect: crate::SqlDialect,
    lowered: &crate::sql::physical_plan::LoweredReadProgram,
    params: &[Value],
    runtime_bindings: &crate::sql::binder::RuntimeBindingValues,
) -> Result<PreparedBatch, LixError> {
    let mut batch = PreparedBatch { steps: Vec::new() };
    for statement in &lowered.statements {
        let (sql, params) = statement.bind_and_render_sql(params, runtime_bindings, dialect)?;
        batch.steps.push(PreparedStatement { sql, params });
    }
    Ok(batch)
}

fn result_columns_for_public_read_execution(
    execution: &PreparedPublicReadExecution,
) -> Option<PublicReadResultColumns> {
    match execution {
        PreparedPublicReadExecution::ReadTimeProjection(_) => None,
        PreparedPublicReadExecution::LoweredSql(lowered) => Some(
            public_read_result_columns_from_lowered(&lowered.result_columns),
        ),
        PreparedPublicReadExecution::Direct(plan) => Some(match plan {
            DirectPublicReadPlan::StateHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
            DirectPublicReadPlan::EntityHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
            DirectPublicReadPlan::FileHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
            DirectPublicReadPlan::DirectoryHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
        }),
    }
}

fn public_read_result_columns_from_lowered(
    result_columns: &LoweredResultColumns,
) -> PublicReadResultColumns {
    match result_columns {
        LoweredResultColumns::Static(columns) => PublicReadResultColumns::Static(
            columns
                .iter()
                .copied()
                .map(public_read_result_column_from_lowered)
                .collect(),
        ),
        LoweredResultColumns::ByColumnName(columns_by_name) => {
            PublicReadResultColumns::ByColumnName(
                columns_by_name
                    .iter()
                    .map(|(name, kind)| {
                        (name.clone(), public_read_result_column_from_lowered(*kind))
                    })
                    .collect(),
            )
        }
    }
}

fn public_read_result_column_from_lowered(kind: LoweredResultColumn) -> PublicReadResultColumn {
    match kind {
        LoweredResultColumn::Untyped => PublicReadResultColumn::Untyped,
        LoweredResultColumn::Boolean => PublicReadResultColumn::Boolean,
    }
}

fn prepared_direct_public_read(plan: &DirectPublicReadPlan) -> PreparedDirectPublicRead {
    match plan {
        DirectPublicReadPlan::StateHistory(plan) => {
            PreparedDirectPublicRead::StateHistory(prepared_state_history_direct_read_plan(plan))
        }
        DirectPublicReadPlan::EntityHistory(plan) => {
            PreparedDirectPublicRead::EntityHistory(prepared_entity_history_direct_read_plan(plan))
        }
        DirectPublicReadPlan::FileHistory(plan) => {
            PreparedDirectPublicRead::FileHistory(prepared_file_history_direct_read_plan(plan))
        }
        DirectPublicReadPlan::DirectoryHistory(plan) => PreparedDirectPublicRead::DirectoryHistory(
            prepared_directory_history_direct_read_plan(plan),
        ),
    }
}

fn prepared_state_history_direct_read_plan(
    plan: &StateHistoryDirectReadPlan,
) -> PreparedStateHistoryDirectReadPlan {
    PreparedStateHistoryDirectReadPlan {
        request: plan.request.clone(),
        predicates: plan
            .predicates
            .iter()
            .cloned()
            .map(prepared_state_history_predicate)
            .collect(),
        projections: plan
            .projections
            .iter()
            .cloned()
            .map(prepared_state_history_projection)
            .collect(),
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        group_by_fields: plan
            .group_by_fields
            .iter()
            .cloned()
            .map(prepared_direct_state_history_field)
            .collect(),
        having: plan
            .having
            .clone()
            .map(prepared_state_history_aggregate_predicate),
        sort_keys: plan
            .sort_keys
            .iter()
            .cloned()
            .map(prepared_state_history_sort_key)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
    }
}

fn prepared_entity_history_direct_read_plan(
    plan: &EntityHistoryDirectReadPlan,
) -> PreparedEntityHistoryDirectReadPlan {
    PreparedEntityHistoryDirectReadPlan {
        surface_binding: plan.surface_binding.clone(),
        request: plan.request.clone(),
        predicates: plan
            .predicates
            .iter()
            .cloned()
            .map(prepared_entity_history_predicate)
            .collect(),
        projections: plan
            .projections
            .iter()
            .cloned()
            .map(prepared_entity_history_projection)
            .collect(),
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        sort_keys: plan
            .sort_keys
            .iter()
            .cloned()
            .map(prepared_entity_history_sort_key)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
    }
}

fn prepared_file_history_direct_read_plan(
    plan: &FileHistoryDirectReadPlan,
) -> PreparedFileHistoryDirectReadPlan {
    PreparedFileHistoryDirectReadPlan {
        request: plan.request.clone(),
        predicates: plan
            .predicates
            .iter()
            .cloned()
            .map(prepared_file_history_predicate)
            .collect(),
        projections: plan
            .projections
            .iter()
            .cloned()
            .map(prepared_file_history_projection)
            .collect(),
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        sort_keys: plan
            .sort_keys
            .iter()
            .cloned()
            .map(prepared_file_history_sort_key)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
        aggregate: plan.aggregate.clone().map(prepared_file_history_aggregate),
        aggregate_output_name: plan.aggregate_output_name.clone(),
    }
}

fn prepared_directory_history_direct_read_plan(
    plan: &DirectoryHistoryDirectReadPlan,
) -> PreparedDirectoryHistoryDirectReadPlan {
    PreparedDirectoryHistoryDirectReadPlan {
        request: plan.request.clone(),
        predicates: plan
            .predicates
            .iter()
            .cloned()
            .map(prepared_directory_history_predicate)
            .collect(),
        projections: plan
            .projections
            .iter()
            .cloned()
            .map(prepared_directory_history_projection)
            .collect(),
        wildcard_projection: plan.wildcard_projection,
        wildcard_columns: plan.wildcard_columns.clone(),
        sort_keys: plan
            .sort_keys
            .iter()
            .cloned()
            .map(prepared_directory_history_sort_key)
            .collect(),
        limit: plan.limit,
        offset: plan.offset,
        aggregate: plan
            .aggregate
            .clone()
            .map(prepared_directory_history_aggregate),
        aggregate_output_name: plan.aggregate_output_name.clone(),
    }
}

fn prepared_direct_state_history_field(
    field: DirectStateHistoryField,
) -> PreparedDirectStateHistoryField {
    match field {
        DirectStateHistoryField::EntityId => PreparedDirectStateHistoryField::EntityId,
        DirectStateHistoryField::SchemaKey => PreparedDirectStateHistoryField::SchemaKey,
        DirectStateHistoryField::FileId => PreparedDirectStateHistoryField::FileId,
        DirectStateHistoryField::PluginKey => PreparedDirectStateHistoryField::PluginKey,
        DirectStateHistoryField::SnapshotContent => {
            PreparedDirectStateHistoryField::SnapshotContent
        }
        DirectStateHistoryField::Metadata => PreparedDirectStateHistoryField::Metadata,
        DirectStateHistoryField::SchemaVersion => PreparedDirectStateHistoryField::SchemaVersion,
        DirectStateHistoryField::ChangeId => PreparedDirectStateHistoryField::ChangeId,
        DirectStateHistoryField::CommitId => PreparedDirectStateHistoryField::CommitId,
        DirectStateHistoryField::CommitCreatedAt => {
            PreparedDirectStateHistoryField::CommitCreatedAt
        }
        DirectStateHistoryField::RootCommitId => PreparedDirectStateHistoryField::RootCommitId,
        DirectStateHistoryField::Depth => PreparedDirectStateHistoryField::Depth,
        DirectStateHistoryField::VersionId => PreparedDirectStateHistoryField::VersionId,
    }
}

fn prepared_state_history_aggregate(
    aggregate: StateHistoryAggregate,
) -> PreparedStateHistoryAggregate {
    match aggregate {
        StateHistoryAggregate::Count => PreparedStateHistoryAggregate::Count,
    }
}

fn prepared_state_history_projection_value(
    value: StateHistoryProjectionValue,
) -> PreparedStateHistoryProjectionValue {
    match value {
        StateHistoryProjectionValue::Field(field) => {
            PreparedStateHistoryProjectionValue::Field(prepared_direct_state_history_field(field))
        }
        StateHistoryProjectionValue::Aggregate(aggregate) => {
            PreparedStateHistoryProjectionValue::Aggregate(prepared_state_history_aggregate(
                aggregate,
            ))
        }
    }
}

fn prepared_state_history_projection(
    projection: StateHistoryProjection,
) -> PreparedStateHistoryProjection {
    PreparedStateHistoryProjection {
        output_name: projection.output_name,
        value: prepared_state_history_projection_value(projection.value),
    }
}

fn prepared_state_history_sort_value(
    value: StateHistorySortValue,
) -> PreparedStateHistorySortValue {
    match value {
        StateHistorySortValue::Field(field) => {
            PreparedStateHistorySortValue::Field(prepared_direct_state_history_field(field))
        }
        StateHistorySortValue::Aggregate(aggregate) => {
            PreparedStateHistorySortValue::Aggregate(prepared_state_history_aggregate(aggregate))
        }
    }
}

fn prepared_state_history_sort_key(key: StateHistorySortKey) -> PreparedStateHistorySortKey {
    PreparedStateHistorySortKey {
        output_name: key.output_name,
        value: key.value.map(prepared_state_history_sort_value),
        descending: key.descending,
    }
}

fn prepared_state_history_predicate(
    predicate: StateHistoryPredicate,
) -> PreparedStateHistoryPredicate {
    match predicate {
        StateHistoryPredicate::Eq(field, value) => {
            PreparedStateHistoryPredicate::Eq(prepared_direct_state_history_field(field), value)
        }
        StateHistoryPredicate::NotEq(field, value) => {
            PreparedStateHistoryPredicate::NotEq(prepared_direct_state_history_field(field), value)
        }
        StateHistoryPredicate::Gt(field, value) => {
            PreparedStateHistoryPredicate::Gt(prepared_direct_state_history_field(field), value)
        }
        StateHistoryPredicate::GtEq(field, value) => {
            PreparedStateHistoryPredicate::GtEq(prepared_direct_state_history_field(field), value)
        }
        StateHistoryPredicate::Lt(field, value) => {
            PreparedStateHistoryPredicate::Lt(prepared_direct_state_history_field(field), value)
        }
        StateHistoryPredicate::LtEq(field, value) => {
            PreparedStateHistoryPredicate::LtEq(prepared_direct_state_history_field(field), value)
        }
        StateHistoryPredicate::In(field, values) => {
            PreparedStateHistoryPredicate::In(prepared_direct_state_history_field(field), values)
        }
        StateHistoryPredicate::IsNull(field) => {
            PreparedStateHistoryPredicate::IsNull(prepared_direct_state_history_field(field))
        }
        StateHistoryPredicate::IsNotNull(field) => {
            PreparedStateHistoryPredicate::IsNotNull(prepared_direct_state_history_field(field))
        }
    }
}

fn prepared_state_history_aggregate_predicate(
    predicate: StateHistoryAggregatePredicate,
) -> PreparedStateHistoryAggregatePredicate {
    match predicate {
        StateHistoryAggregatePredicate::Eq(aggregate, value) => {
            PreparedStateHistoryAggregatePredicate::Eq(
                prepared_state_history_aggregate(aggregate),
                value,
            )
        }
        StateHistoryAggregatePredicate::NotEq(aggregate, value) => {
            PreparedStateHistoryAggregatePredicate::NotEq(
                prepared_state_history_aggregate(aggregate),
                value,
            )
        }
        StateHistoryAggregatePredicate::Gt(aggregate, value) => {
            PreparedStateHistoryAggregatePredicate::Gt(
                prepared_state_history_aggregate(aggregate),
                value,
            )
        }
        StateHistoryAggregatePredicate::GtEq(aggregate, value) => {
            PreparedStateHistoryAggregatePredicate::GtEq(
                prepared_state_history_aggregate(aggregate),
                value,
            )
        }
        StateHistoryAggregatePredicate::Lt(aggregate, value) => {
            PreparedStateHistoryAggregatePredicate::Lt(
                prepared_state_history_aggregate(aggregate),
                value,
            )
        }
        StateHistoryAggregatePredicate::LtEq(aggregate, value) => {
            PreparedStateHistoryAggregatePredicate::LtEq(
                prepared_state_history_aggregate(aggregate),
                value,
            )
        }
    }
}

fn prepared_direct_entity_history_field(
    field: DirectEntityHistoryField,
) -> PreparedDirectEntityHistoryField {
    match field {
        DirectEntityHistoryField::Property(property) => {
            PreparedDirectEntityHistoryField::Property(property)
        }
        DirectEntityHistoryField::State(field) => {
            PreparedDirectEntityHistoryField::State(prepared_direct_state_history_field(field))
        }
    }
}

fn prepared_entity_history_projection(
    projection: EntityHistoryProjection,
) -> PreparedEntityHistoryProjection {
    PreparedEntityHistoryProjection {
        output_name: projection.output_name,
        field: prepared_direct_entity_history_field(projection.field),
    }
}

fn prepared_entity_history_sort_key(key: EntityHistorySortKey) -> PreparedEntityHistorySortKey {
    PreparedEntityHistorySortKey {
        output_name: key.output_name,
        field: key.field.map(prepared_direct_entity_history_field),
        descending: key.descending,
    }
}

fn prepared_entity_history_predicate(
    predicate: EntityHistoryPredicate,
) -> PreparedEntityHistoryPredicate {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => {
            PreparedEntityHistoryPredicate::Eq(prepared_direct_entity_history_field(field), value)
        }
        EntityHistoryPredicate::NotEq(field, value) => PreparedEntityHistoryPredicate::NotEq(
            prepared_direct_entity_history_field(field),
            value,
        ),
        EntityHistoryPredicate::Gt(field, value) => {
            PreparedEntityHistoryPredicate::Gt(prepared_direct_entity_history_field(field), value)
        }
        EntityHistoryPredicate::GtEq(field, value) => {
            PreparedEntityHistoryPredicate::GtEq(prepared_direct_entity_history_field(field), value)
        }
        EntityHistoryPredicate::Lt(field, value) => {
            PreparedEntityHistoryPredicate::Lt(prepared_direct_entity_history_field(field), value)
        }
        EntityHistoryPredicate::LtEq(field, value) => {
            PreparedEntityHistoryPredicate::LtEq(prepared_direct_entity_history_field(field), value)
        }
        EntityHistoryPredicate::In(field, values) => {
            PreparedEntityHistoryPredicate::In(prepared_direct_entity_history_field(field), values)
        }
        EntityHistoryPredicate::IsNull(field) => {
            PreparedEntityHistoryPredicate::IsNull(prepared_direct_entity_history_field(field))
        }
        EntityHistoryPredicate::IsNotNull(field) => {
            PreparedEntityHistoryPredicate::IsNotNull(prepared_direct_entity_history_field(field))
        }
    }
}

fn prepared_direct_file_history_field(
    field: DirectFileHistoryField,
) -> PreparedDirectFileHistoryField {
    match field {
        DirectFileHistoryField::Id => PreparedDirectFileHistoryField::Id,
        DirectFileHistoryField::Path => PreparedDirectFileHistoryField::Path,
        DirectFileHistoryField::Data => PreparedDirectFileHistoryField::Data,
        DirectFileHistoryField::Metadata => PreparedDirectFileHistoryField::Metadata,
        DirectFileHistoryField::Hidden => PreparedDirectFileHistoryField::Hidden,
        DirectFileHistoryField::EntityId => PreparedDirectFileHistoryField::EntityId,
        DirectFileHistoryField::SchemaKey => PreparedDirectFileHistoryField::SchemaKey,
        DirectFileHistoryField::FileId => PreparedDirectFileHistoryField::FileId,
        DirectFileHistoryField::VersionId => PreparedDirectFileHistoryField::VersionId,
        DirectFileHistoryField::PluginKey => PreparedDirectFileHistoryField::PluginKey,
        DirectFileHistoryField::SchemaVersion => PreparedDirectFileHistoryField::SchemaVersion,
        DirectFileHistoryField::ChangeId => PreparedDirectFileHistoryField::ChangeId,
        DirectFileHistoryField::LixcolMetadata => PreparedDirectFileHistoryField::LixcolMetadata,
        DirectFileHistoryField::CommitId => PreparedDirectFileHistoryField::CommitId,
        DirectFileHistoryField::CommitCreatedAt => PreparedDirectFileHistoryField::CommitCreatedAt,
        DirectFileHistoryField::RootCommitId => PreparedDirectFileHistoryField::RootCommitId,
        DirectFileHistoryField::Depth => PreparedDirectFileHistoryField::Depth,
    }
}

fn prepared_file_history_projection(
    projection: FileHistoryProjection,
) -> PreparedFileHistoryProjection {
    PreparedFileHistoryProjection {
        output_name: projection.output_name,
        field: prepared_direct_file_history_field(projection.field),
    }
}

fn prepared_file_history_sort_key(key: FileHistorySortKey) -> PreparedFileHistorySortKey {
    PreparedFileHistorySortKey {
        output_name: key.output_name,
        field: key.field.map(prepared_direct_file_history_field),
        descending: key.descending,
    }
}

fn prepared_file_history_predicate(
    predicate: FileHistoryPredicate,
) -> PreparedFileHistoryPredicate {
    match predicate {
        FileHistoryPredicate::Eq(field, value) => {
            PreparedFileHistoryPredicate::Eq(prepared_direct_file_history_field(field), value)
        }
        FileHistoryPredicate::NotEq(field, value) => {
            PreparedFileHistoryPredicate::NotEq(prepared_direct_file_history_field(field), value)
        }
        FileHistoryPredicate::Gt(field, value) => {
            PreparedFileHistoryPredicate::Gt(prepared_direct_file_history_field(field), value)
        }
        FileHistoryPredicate::GtEq(field, value) => {
            PreparedFileHistoryPredicate::GtEq(prepared_direct_file_history_field(field), value)
        }
        FileHistoryPredicate::Lt(field, value) => {
            PreparedFileHistoryPredicate::Lt(prepared_direct_file_history_field(field), value)
        }
        FileHistoryPredicate::LtEq(field, value) => {
            PreparedFileHistoryPredicate::LtEq(prepared_direct_file_history_field(field), value)
        }
        FileHistoryPredicate::In(field, values) => {
            PreparedFileHistoryPredicate::In(prepared_direct_file_history_field(field), values)
        }
        FileHistoryPredicate::IsNull(field) => {
            PreparedFileHistoryPredicate::IsNull(prepared_direct_file_history_field(field))
        }
        FileHistoryPredicate::IsNotNull(field) => {
            PreparedFileHistoryPredicate::IsNotNull(prepared_direct_file_history_field(field))
        }
    }
}

fn prepared_file_history_aggregate(
    aggregate: FileHistoryAggregate,
) -> PreparedFileHistoryAggregate {
    match aggregate {
        FileHistoryAggregate::Count => PreparedFileHistoryAggregate::Count,
    }
}

fn prepared_direct_directory_history_field(
    field: DirectDirectoryHistoryField,
) -> PreparedDirectDirectoryHistoryField {
    match field {
        DirectDirectoryHistoryField::Id => PreparedDirectDirectoryHistoryField::Id,
        DirectDirectoryHistoryField::ParentId => PreparedDirectDirectoryHistoryField::ParentId,
        DirectDirectoryHistoryField::Name => PreparedDirectDirectoryHistoryField::Name,
        DirectDirectoryHistoryField::Path => PreparedDirectDirectoryHistoryField::Path,
        DirectDirectoryHistoryField::Hidden => PreparedDirectDirectoryHistoryField::Hidden,
        DirectDirectoryHistoryField::EntityId => PreparedDirectDirectoryHistoryField::EntityId,
        DirectDirectoryHistoryField::SchemaKey => PreparedDirectDirectoryHistoryField::SchemaKey,
        DirectDirectoryHistoryField::FileId => PreparedDirectDirectoryHistoryField::FileId,
        DirectDirectoryHistoryField::VersionId => PreparedDirectDirectoryHistoryField::VersionId,
        DirectDirectoryHistoryField::PluginKey => PreparedDirectDirectoryHistoryField::PluginKey,
        DirectDirectoryHistoryField::SchemaVersion => {
            PreparedDirectDirectoryHistoryField::SchemaVersion
        }
        DirectDirectoryHistoryField::ChangeId => PreparedDirectDirectoryHistoryField::ChangeId,
        DirectDirectoryHistoryField::LixcolMetadata => {
            PreparedDirectDirectoryHistoryField::LixcolMetadata
        }
        DirectDirectoryHistoryField::CommitId => PreparedDirectDirectoryHistoryField::CommitId,
        DirectDirectoryHistoryField::CommitCreatedAt => {
            PreparedDirectDirectoryHistoryField::CommitCreatedAt
        }
        DirectDirectoryHistoryField::RootCommitId => {
            PreparedDirectDirectoryHistoryField::RootCommitId
        }
        DirectDirectoryHistoryField::Depth => PreparedDirectDirectoryHistoryField::Depth,
    }
}

fn prepared_directory_history_projection(
    projection: DirectoryHistoryProjection,
) -> PreparedDirectoryHistoryProjection {
    PreparedDirectoryHistoryProjection {
        output_name: projection.output_name,
        field: prepared_direct_directory_history_field(projection.field),
    }
}

fn prepared_directory_history_sort_key(
    key: DirectoryHistorySortKey,
) -> PreparedDirectoryHistorySortKey {
    PreparedDirectoryHistorySortKey {
        output_name: key.output_name,
        field: key.field.map(prepared_direct_directory_history_field),
        descending: key.descending,
    }
}

fn prepared_directory_history_predicate(
    predicate: DirectoryHistoryPredicate,
) -> PreparedDirectoryHistoryPredicate {
    match predicate {
        DirectoryHistoryPredicate::Eq(field, value) => PreparedDirectoryHistoryPredicate::Eq(
            prepared_direct_directory_history_field(field),
            value,
        ),
        DirectoryHistoryPredicate::NotEq(field, value) => PreparedDirectoryHistoryPredicate::NotEq(
            prepared_direct_directory_history_field(field),
            value,
        ),
        DirectoryHistoryPredicate::Gt(field, value) => PreparedDirectoryHistoryPredicate::Gt(
            prepared_direct_directory_history_field(field),
            value,
        ),
        DirectoryHistoryPredicate::GtEq(field, value) => PreparedDirectoryHistoryPredicate::GtEq(
            prepared_direct_directory_history_field(field),
            value,
        ),
        DirectoryHistoryPredicate::Lt(field, value) => PreparedDirectoryHistoryPredicate::Lt(
            prepared_direct_directory_history_field(field),
            value,
        ),
        DirectoryHistoryPredicate::LtEq(field, value) => PreparedDirectoryHistoryPredicate::LtEq(
            prepared_direct_directory_history_field(field),
            value,
        ),
        DirectoryHistoryPredicate::In(field, values) => PreparedDirectoryHistoryPredicate::In(
            prepared_direct_directory_history_field(field),
            values,
        ),
        DirectoryHistoryPredicate::IsNull(field) => PreparedDirectoryHistoryPredicate::IsNull(
            prepared_direct_directory_history_field(field),
        ),
        DirectoryHistoryPredicate::IsNotNull(field) => {
            PreparedDirectoryHistoryPredicate::IsNotNull(prepared_direct_directory_history_field(
                field,
            ))
        }
    }
}

fn prepared_directory_history_aggregate(
    aggregate: DirectoryHistoryAggregate,
) -> PreparedDirectoryHistoryAggregate {
    match aggregate {
        DirectoryHistoryAggregate::Count => PreparedDirectoryHistoryAggregate::Count,
    }
}

fn merge_committed_read_transaction_mode(
    current: TransactionMode,
    next: TransactionMode,
) -> TransactionMode {
    match (current, next) {
        (TransactionMode::Write, _) | (_, TransactionMode::Write) => TransactionMode::Write,
        (TransactionMode::Deferred, _) | (_, TransactionMode::Deferred) => {
            TransactionMode::Deferred
        }
        _ => TransactionMode::Read,
    }
}

fn transaction_mode_for_committed_read_execution(
    compiled: &CompiledExecution,
) -> Result<TransactionMode, LixError> {
    if compiled.plain_explain().is_some() {
        return Ok(TransactionMode::Read);
    }
    if let Some(public_read) = compiled.public_read() {
        return Ok(public_read.committed_read_mode().transaction_mode());
    }
    if compiled.internal_execution().is_some() {
        return if compiled.read_only_query {
            Ok(TransactionMode::Read)
        } else {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "committed read routing compiled a non-read internal step unexpectedly",
            ))
        };
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "committed read routing compiled a public write unexpectedly",
    ))
}

fn collect_statement_relation_names(statement: &Statement) -> Vec<String> {
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
