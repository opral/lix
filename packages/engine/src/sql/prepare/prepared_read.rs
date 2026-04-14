use crate::backend::TransactionBeginMode;
use crate::backend::{PreparedBatch, PreparedStatement};
use crate::diagnostics::{
    build_read_diagnostic_catalog_snapshot, normalize_sql_error_with_read_diagnostic_context,
};
use crate::sql::explain::{prepare_analyzed_explain_template, prepare_plain_explain_template};
use crate::sql::logical_plan::direct_reads::{
    DirectDirectoryHistoryField, DirectEntityHistoryField, DirectFileHistoryField,
    DirectStateHistoryField, DirectoryHistoryAggregate, DirectoryHistoryPredicate,
    DirectoryHistoryProjection, DirectoryHistoryReadPlan, DirectoryHistorySortKey,
    EntityHistoryPredicate, EntityHistoryProjection, EntityHistoryReadPlan, EntityHistorySortKey,
    FileHistoryAggregate, FileHistoryPredicate, FileHistoryProjection, FileHistoryReadPlan,
    FileHistorySortKey, HistoryReadPlan, StateHistoryAggregate, StateHistoryAggregatePredicate,
    StateHistoryPredicate, StateHistoryProjection, StateHistoryProjectionValue,
    StateHistoryReadPlan, StateHistorySortKey, StateHistorySortValue,
};
use crate::sql::physical_plan::{
    LoweredResultColumn, LoweredResultColumns, PublicReadPhysicalPlan,
};
use crate::sql::prepare::SqlPreparationMetadataReader;
use crate::sql::{
    PreparedBatchReadArtifact, PreparedDirectReadArtifact, PreparedDirectoryHistoryAggregate,
    PreparedDirectoryHistoryField, PreparedDirectoryHistoryPredicate,
    PreparedDirectoryHistoryProjection, PreparedDirectoryHistoryReadPlan,
    PreparedDirectoryHistorySortKey, PreparedEntityHistoryField, PreparedEntityHistoryPredicate,
    PreparedEntityHistoryProjection, PreparedEntityHistoryReadPlan, PreparedEntityHistorySortKey,
    PreparedExplainMode, PreparedFileHistoryAggregate, PreparedFileHistoryField,
    PreparedFileHistoryPredicate, PreparedFileHistoryProjection, PreparedFileHistoryReadPlan,
    PreparedFileHistorySortKey, PreparedHistoryReadArtifact, PreparedHistoryReadPlan,
    PreparedPublicRead, PreparedPublicReadPlanArtifact, PreparedReadArtifact, PreparedReadBatch,
    PreparedReadStatement, PreparedReadTimeProjectionArtifact, PreparedStateHistoryAggregate,
    PreparedStateHistoryAggregatePredicate, PreparedStateHistoryField,
    PreparedStateHistoryPredicate, PreparedStateHistoryProjection,
    PreparedStateHistoryProjectionValue, PreparedStateHistoryReadPlan, PreparedStateHistorySortKey,
    PreparedStateHistorySortValue, PublicReadResultColumn, PublicReadResultColumns,
    ReadDiagnosticContext,
};
use crate::{LixBackend, LixBackendTransaction, LixError, Value};
use sqlparser::ast::{visit_relations, ObjectNamePart, Statement};
use std::ops::ControlFlow;

use super::execution_program::{BoundStatementInstance, StatementBatch};
use super::{
    compile_execution_from_bound_statement_with_context, load_sql_compiler_metadata_with_reader,
    CompilePolicy, CompiledExecution, PublicReadPlan, SqlCompilerContext, SqlCompilerSeed,
};

pub(crate) struct CommittedReadContext<'a> {
    pub(crate) active_version_id: &'a str,
    pub(crate) active_account_ids: &'a [String],
    pub(crate) writer_key: Option<&'a str>,
    pub(crate) compiler_seed: SqlCompilerSeed<'a>,
    pub(crate) base_transaction_mode: TransactionBeginMode,
}

pub(crate) async fn prepare_committed_read_batch_with_backend(
    backend: &dyn LixBackend,
    statement_batch: &StatementBatch,
    allow_internal_relations: bool,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let mut metadata_reader = backend;
    prepare_committed_read_batch_from_reader(
        &mut metadata_reader,
        statement_batch,
        allow_internal_relations,
        read_context,
    )
    .await
}

pub(crate) async fn prepare_committed_read_batch_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    statement_batch: &StatementBatch,
    allow_internal_relations: bool,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let mut metadata_reader = transaction;
    prepare_committed_read_batch_from_reader(
        &mut metadata_reader,
        statement_batch,
        allow_internal_relations,
        read_context,
    )
    .await
}

async fn prepare_committed_read_batch_from_reader(
    metadata_reader: &mut dyn SqlPreparationMetadataReader,
    statement_batch: &StatementBatch,
    allow_internal_relations: bool,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let active_history_root_commit_id = metadata_reader
        .load_active_history_root_commit_id_for_preparation(read_context.active_version_id)
        .await?;
    let compiler_metadata = load_sql_compiler_metadata_with_reader(
        metadata_reader,
        read_context.compiler_seed.surface_registry,
    )
    .await?;
    let compiler_context = read_context
        .compiler_seed
        .with_compiler_metadata(&compiler_metadata, active_history_root_commit_id.as_deref());

    compile_committed_read_batch(
        &compiler_context,
        statement_batch,
        allow_internal_relations,
        read_context,
    )
    .await
}

pub(crate) async fn compile_committed_read_batch(
    compiler_context: &dyn SqlCompilerContext,
    statement_batch: &StatementBatch,
    allow_internal_relations: bool,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let mut mode = read_context.base_transaction_mode;
    let mut statements = Vec::new();

    for statement in statement_batch.steps() {
        let prepared_statement = compile_committed_read_statement(
            compiler_context,
            statement,
            allow_internal_relations,
            read_context,
        )
        .await?;
        mode = merge_committed_read_transaction_mode(mode, prepared_statement.transaction_mode);
        statements.push(prepared_statement);
    }

    Ok(PreparedReadBatch {
        transaction_mode: mode,
        statements,
    })
}

async fn compile_committed_read_statement(
    compiler_context: &dyn SqlCompilerContext,
    bound_statement: &BoundStatementInstance,
    allow_internal_relations: bool,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadStatement, LixError> {
    let source_sql = vec![bound_statement.statement().to_string()];
    let relation_names = collect_statement_relation_names(bound_statement.statement());
    let diagnostic_context =
        base_read_diagnostic_context(compiler_context, source_sql, relation_names);
    let compiled = compile_committed_execution_step(
        &diagnostic_context,
        compiler_context,
        bound_statement,
        allow_internal_relations,
        read_context,
    )
    .await?;
    prepared_read_statement_from_compiled_execution(
        compiler_context.dialect(),
        compiled,
        diagnostic_context,
    )
}

async fn compile_committed_execution_step(
    diagnostic_context: &ReadDiagnosticContext,
    compiler_context: &dyn SqlCompilerContext,
    bound_statement: &BoundStatementInstance,
    allow_internal_relations: bool,
    read_context: &CommittedReadContext<'_>,
) -> Result<CompiledExecution, LixError> {
    match compile_execution_from_bound_statement_with_context(
        compiler_context,
        bound_statement,
        read_context.active_version_id,
        read_context.active_account_ids,
        read_context.writer_key,
        allow_internal_relations,
        CompilePolicy {
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

fn prepared_read_statement_from_compiled_execution(
    dialect: crate::SqlDialect,
    compiled: CompiledExecution,
    mut diagnostic_context: ReadDiagnosticContext,
) -> Result<PreparedReadStatement, LixError> {
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
    } else if let Some(internal) = compiled.direct_execution() {
        PreparedReadArtifact::Direct(PreparedDirectReadArtifact {
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

    Ok(PreparedReadStatement {
        transaction_mode,
        artifact,
        diagnostic_context,
    })
}

fn base_read_diagnostic_context(
    compiler_context: &dyn SqlCompilerContext,
    source_sql: Vec<String>,
    relation_names: Vec<String>,
) -> ReadDiagnosticContext {
    ReadDiagnosticContext {
        source_sql,
        relation_names: relation_names.clone(),
        catalog_snapshot: build_read_diagnostic_catalog_snapshot(
            compiler_context.surface_registry(),
            &relation_names,
        ),
        explain_mode: None,
        plain_explain_template: None,
        analyzed_explain_template: None,
    }
}

pub(crate) fn prepare_public_read_artifact(
    public_read: &PublicReadPlan,
    dialect: crate::SqlDialect,
) -> Result<PreparedPublicRead, LixError> {
    let mut contract = super::public_surface::read::prepared_public_read_contract(public_read);
    if contract.result_columns.is_none() {
        contract.result_columns = result_columns_for_public_read_execution(&public_read.execution);
    }

    let execution = match &public_read.execution {
        PublicReadPhysicalPlan::ReadTimeProjection(read) => {
            PreparedPublicReadPlanArtifact::ReadTimeProjection(PreparedReadTimeProjectionArtifact {
                read: read.clone(),
            })
        }
        PublicReadPhysicalPlan::LoweredSql(lowered) => {
            PreparedPublicReadPlanArtifact::PreparedBatch(PreparedBatchReadArtifact {
                prepared_batch: prepared_batch_from_lowered_batch(
                    dialect,
                    lowered,
                    &public_read.bound_parameters,
                    &public_read.runtime_bindings,
                )?,
            })
        }
        PublicReadPhysicalPlan::HistoryRead(plan) => {
            PreparedPublicReadPlanArtifact::HistoryRead(PreparedHistoryReadArtifact {
                plan: prepared_history_read_plan(plan),
            })
        }
    };

    Ok(PreparedPublicRead {
        contract,
        freshness_contract: public_read.freshness_contract,
        resolved_relations: public_read.resolved_relations.clone(),
        public_output_columns: public_read.public_output_columns.clone(),
        execution,
    })
}

fn prepared_batch_from_lowered_batch(
    dialect: crate::SqlDialect,
    lowered: &crate::sql::physical_plan::LoweredReadBatch,
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
    execution: &PublicReadPhysicalPlan,
) -> Option<PublicReadResultColumns> {
    match execution {
        PublicReadPhysicalPlan::ReadTimeProjection(_) => None,
        PublicReadPhysicalPlan::LoweredSql(lowered) => Some(
            public_read_result_columns_from_lowered(&lowered.result_columns),
        ),
        PublicReadPhysicalPlan::HistoryRead(plan) => Some(match plan {
            HistoryReadPlan::StateHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
            HistoryReadPlan::EntityHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
            HistoryReadPlan::FileHistory(plan) => {
                public_read_result_columns_from_lowered(&plan.result_columns)
            }
            HistoryReadPlan::DirectoryHistory(plan) => {
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

fn prepared_history_read_plan(plan: &HistoryReadPlan) -> PreparedHistoryReadPlan {
    match plan {
        HistoryReadPlan::StateHistory(plan) => {
            PreparedHistoryReadPlan::StateHistory(prepared_state_history_read_plan(plan))
        }
        HistoryReadPlan::EntityHistory(plan) => {
            PreparedHistoryReadPlan::EntityHistory(prepared_entity_history_read_plan(plan))
        }
        HistoryReadPlan::FileHistory(plan) => {
            PreparedHistoryReadPlan::FileHistory(prepared_file_history_read_plan(plan))
        }
        HistoryReadPlan::DirectoryHistory(plan) => {
            PreparedHistoryReadPlan::DirectoryHistory(prepared_directory_history_read_plan(plan))
        }
    }
}

fn prepared_state_history_read_plan(plan: &StateHistoryReadPlan) -> PreparedStateHistoryReadPlan {
    PreparedStateHistoryReadPlan {
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
            .map(prepared_state_history_field)
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

fn prepared_entity_history_read_plan(
    plan: &EntityHistoryReadPlan,
) -> PreparedEntityHistoryReadPlan {
    PreparedEntityHistoryReadPlan {
        resolved_relation: plan.resolved_relation.clone(),
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

fn prepared_file_history_read_plan(plan: &FileHistoryReadPlan) -> PreparedFileHistoryReadPlan {
    PreparedFileHistoryReadPlan {
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

fn prepared_directory_history_read_plan(
    plan: &DirectoryHistoryReadPlan,
) -> PreparedDirectoryHistoryReadPlan {
    PreparedDirectoryHistoryReadPlan {
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

fn prepared_state_history_field(field: DirectStateHistoryField) -> PreparedStateHistoryField {
    match field {
        DirectStateHistoryField::EntityId => PreparedStateHistoryField::EntityId,
        DirectStateHistoryField::SchemaKey => PreparedStateHistoryField::SchemaKey,
        DirectStateHistoryField::FileId => PreparedStateHistoryField::FileId,
        DirectStateHistoryField::PluginKey => PreparedStateHistoryField::PluginKey,
        DirectStateHistoryField::SnapshotContent => PreparedStateHistoryField::SnapshotContent,
        DirectStateHistoryField::Metadata => PreparedStateHistoryField::Metadata,
        DirectStateHistoryField::SchemaVersion => PreparedStateHistoryField::SchemaVersion,
        DirectStateHistoryField::ChangeId => PreparedStateHistoryField::ChangeId,
        DirectStateHistoryField::CommitId => PreparedStateHistoryField::CommitId,
        DirectStateHistoryField::CommitCreatedAt => PreparedStateHistoryField::CommitCreatedAt,
        DirectStateHistoryField::RootCommitId => PreparedStateHistoryField::RootCommitId,
        DirectStateHistoryField::Depth => PreparedStateHistoryField::Depth,
        DirectStateHistoryField::VersionId => PreparedStateHistoryField::VersionId,
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
            PreparedStateHistoryProjectionValue::Field(prepared_state_history_field(field))
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
            PreparedStateHistorySortValue::Field(prepared_state_history_field(field))
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
            PreparedStateHistoryPredicate::Eq(prepared_state_history_field(field), value)
        }
        StateHistoryPredicate::NotEq(field, value) => {
            PreparedStateHistoryPredicate::NotEq(prepared_state_history_field(field), value)
        }
        StateHistoryPredicate::Gt(field, value) => {
            PreparedStateHistoryPredicate::Gt(prepared_state_history_field(field), value)
        }
        StateHistoryPredicate::GtEq(field, value) => {
            PreparedStateHistoryPredicate::GtEq(prepared_state_history_field(field), value)
        }
        StateHistoryPredicate::Lt(field, value) => {
            PreparedStateHistoryPredicate::Lt(prepared_state_history_field(field), value)
        }
        StateHistoryPredicate::LtEq(field, value) => {
            PreparedStateHistoryPredicate::LtEq(prepared_state_history_field(field), value)
        }
        StateHistoryPredicate::In(field, values) => {
            PreparedStateHistoryPredicate::In(prepared_state_history_field(field), values)
        }
        StateHistoryPredicate::IsNull(field) => {
            PreparedStateHistoryPredicate::IsNull(prepared_state_history_field(field))
        }
        StateHistoryPredicate::IsNotNull(field) => {
            PreparedStateHistoryPredicate::IsNotNull(prepared_state_history_field(field))
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

fn prepared_entity_history_field(field: DirectEntityHistoryField) -> PreparedEntityHistoryField {
    match field {
        DirectEntityHistoryField::Property(property) => {
            PreparedEntityHistoryField::Property(property)
        }
        DirectEntityHistoryField::State(field) => {
            PreparedEntityHistoryField::State(prepared_state_history_field(field))
        }
    }
}

fn prepared_entity_history_projection(
    projection: EntityHistoryProjection,
) -> PreparedEntityHistoryProjection {
    PreparedEntityHistoryProjection {
        output_name: projection.output_name,
        field: prepared_entity_history_field(projection.field),
    }
}

fn prepared_entity_history_sort_key(key: EntityHistorySortKey) -> PreparedEntityHistorySortKey {
    PreparedEntityHistorySortKey {
        output_name: key.output_name,
        field: key.field.map(prepared_entity_history_field),
        descending: key.descending,
    }
}

fn prepared_entity_history_predicate(
    predicate: EntityHistoryPredicate,
) -> PreparedEntityHistoryPredicate {
    match predicate {
        EntityHistoryPredicate::Eq(field, value) => {
            PreparedEntityHistoryPredicate::Eq(prepared_entity_history_field(field), value)
        }
        EntityHistoryPredicate::NotEq(field, value) => {
            PreparedEntityHistoryPredicate::NotEq(prepared_entity_history_field(field), value)
        }
        EntityHistoryPredicate::Gt(field, value) => {
            PreparedEntityHistoryPredicate::Gt(prepared_entity_history_field(field), value)
        }
        EntityHistoryPredicate::GtEq(field, value) => {
            PreparedEntityHistoryPredicate::GtEq(prepared_entity_history_field(field), value)
        }
        EntityHistoryPredicate::Lt(field, value) => {
            PreparedEntityHistoryPredicate::Lt(prepared_entity_history_field(field), value)
        }
        EntityHistoryPredicate::LtEq(field, value) => {
            PreparedEntityHistoryPredicate::LtEq(prepared_entity_history_field(field), value)
        }
        EntityHistoryPredicate::In(field, values) => {
            PreparedEntityHistoryPredicate::In(prepared_entity_history_field(field), values)
        }
        EntityHistoryPredicate::IsNull(field) => {
            PreparedEntityHistoryPredicate::IsNull(prepared_entity_history_field(field))
        }
        EntityHistoryPredicate::IsNotNull(field) => {
            PreparedEntityHistoryPredicate::IsNotNull(prepared_entity_history_field(field))
        }
    }
}

fn prepared_file_history_field(field: DirectFileHistoryField) -> PreparedFileHistoryField {
    match field {
        DirectFileHistoryField::Id => PreparedFileHistoryField::Id,
        DirectFileHistoryField::Path => PreparedFileHistoryField::Path,
        DirectFileHistoryField::Data => PreparedFileHistoryField::Data,
        DirectFileHistoryField::Metadata => PreparedFileHistoryField::Metadata,
        DirectFileHistoryField::Hidden => PreparedFileHistoryField::Hidden,
        DirectFileHistoryField::EntityId => PreparedFileHistoryField::EntityId,
        DirectFileHistoryField::SchemaKey => PreparedFileHistoryField::SchemaKey,
        DirectFileHistoryField::FileId => PreparedFileHistoryField::FileId,
        DirectFileHistoryField::VersionId => PreparedFileHistoryField::VersionId,
        DirectFileHistoryField::PluginKey => PreparedFileHistoryField::PluginKey,
        DirectFileHistoryField::SchemaVersion => PreparedFileHistoryField::SchemaVersion,
        DirectFileHistoryField::ChangeId => PreparedFileHistoryField::ChangeId,
        DirectFileHistoryField::LixcolMetadata => PreparedFileHistoryField::LixcolMetadata,
        DirectFileHistoryField::CommitId => PreparedFileHistoryField::CommitId,
        DirectFileHistoryField::CommitCreatedAt => PreparedFileHistoryField::CommitCreatedAt,
        DirectFileHistoryField::RootCommitId => PreparedFileHistoryField::RootCommitId,
        DirectFileHistoryField::Depth => PreparedFileHistoryField::Depth,
    }
}

fn prepared_file_history_projection(
    projection: FileHistoryProjection,
) -> PreparedFileHistoryProjection {
    PreparedFileHistoryProjection {
        output_name: projection.output_name,
        field: prepared_file_history_field(projection.field),
    }
}

fn prepared_file_history_sort_key(key: FileHistorySortKey) -> PreparedFileHistorySortKey {
    PreparedFileHistorySortKey {
        output_name: key.output_name,
        field: key.field.map(prepared_file_history_field),
        descending: key.descending,
    }
}

fn prepared_file_history_predicate(
    predicate: FileHistoryPredicate,
) -> PreparedFileHistoryPredicate {
    match predicate {
        FileHistoryPredicate::Eq(field, value) => {
            PreparedFileHistoryPredicate::Eq(prepared_file_history_field(field), value)
        }
        FileHistoryPredicate::NotEq(field, value) => {
            PreparedFileHistoryPredicate::NotEq(prepared_file_history_field(field), value)
        }
        FileHistoryPredicate::Gt(field, value) => {
            PreparedFileHistoryPredicate::Gt(prepared_file_history_field(field), value)
        }
        FileHistoryPredicate::GtEq(field, value) => {
            PreparedFileHistoryPredicate::GtEq(prepared_file_history_field(field), value)
        }
        FileHistoryPredicate::Lt(field, value) => {
            PreparedFileHistoryPredicate::Lt(prepared_file_history_field(field), value)
        }
        FileHistoryPredicate::LtEq(field, value) => {
            PreparedFileHistoryPredicate::LtEq(prepared_file_history_field(field), value)
        }
        FileHistoryPredicate::In(field, values) => {
            PreparedFileHistoryPredicate::In(prepared_file_history_field(field), values)
        }
        FileHistoryPredicate::IsNull(field) => {
            PreparedFileHistoryPredicate::IsNull(prepared_file_history_field(field))
        }
        FileHistoryPredicate::IsNotNull(field) => {
            PreparedFileHistoryPredicate::IsNotNull(prepared_file_history_field(field))
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

fn prepared_directory_history_field(
    field: DirectDirectoryHistoryField,
) -> PreparedDirectoryHistoryField {
    match field {
        DirectDirectoryHistoryField::Id => PreparedDirectoryHistoryField::Id,
        DirectDirectoryHistoryField::ParentId => PreparedDirectoryHistoryField::ParentId,
        DirectDirectoryHistoryField::Name => PreparedDirectoryHistoryField::Name,
        DirectDirectoryHistoryField::Path => PreparedDirectoryHistoryField::Path,
        DirectDirectoryHistoryField::Hidden => PreparedDirectoryHistoryField::Hidden,
        DirectDirectoryHistoryField::EntityId => PreparedDirectoryHistoryField::EntityId,
        DirectDirectoryHistoryField::SchemaKey => PreparedDirectoryHistoryField::SchemaKey,
        DirectDirectoryHistoryField::FileId => PreparedDirectoryHistoryField::FileId,
        DirectDirectoryHistoryField::VersionId => PreparedDirectoryHistoryField::VersionId,
        DirectDirectoryHistoryField::PluginKey => PreparedDirectoryHistoryField::PluginKey,
        DirectDirectoryHistoryField::SchemaVersion => PreparedDirectoryHistoryField::SchemaVersion,
        DirectDirectoryHistoryField::ChangeId => PreparedDirectoryHistoryField::ChangeId,
        DirectDirectoryHistoryField::LixcolMetadata => {
            PreparedDirectoryHistoryField::LixcolMetadata
        }
        DirectDirectoryHistoryField::CommitId => PreparedDirectoryHistoryField::CommitId,
        DirectDirectoryHistoryField::CommitCreatedAt => {
            PreparedDirectoryHistoryField::CommitCreatedAt
        }
        DirectDirectoryHistoryField::RootCommitId => PreparedDirectoryHistoryField::RootCommitId,
        DirectDirectoryHistoryField::Depth => PreparedDirectoryHistoryField::Depth,
    }
}

fn prepared_directory_history_projection(
    projection: DirectoryHistoryProjection,
) -> PreparedDirectoryHistoryProjection {
    PreparedDirectoryHistoryProjection {
        output_name: projection.output_name,
        field: prepared_directory_history_field(projection.field),
    }
}

fn prepared_directory_history_sort_key(
    key: DirectoryHistorySortKey,
) -> PreparedDirectoryHistorySortKey {
    PreparedDirectoryHistorySortKey {
        output_name: key.output_name,
        field: key.field.map(prepared_directory_history_field),
        descending: key.descending,
    }
}

fn prepared_directory_history_predicate(
    predicate: DirectoryHistoryPredicate,
) -> PreparedDirectoryHistoryPredicate {
    match predicate {
        DirectoryHistoryPredicate::Eq(field, value) => {
            PreparedDirectoryHistoryPredicate::Eq(prepared_directory_history_field(field), value)
        }
        DirectoryHistoryPredicate::NotEq(field, value) => {
            PreparedDirectoryHistoryPredicate::NotEq(prepared_directory_history_field(field), value)
        }
        DirectoryHistoryPredicate::Gt(field, value) => {
            PreparedDirectoryHistoryPredicate::Gt(prepared_directory_history_field(field), value)
        }
        DirectoryHistoryPredicate::GtEq(field, value) => {
            PreparedDirectoryHistoryPredicate::GtEq(prepared_directory_history_field(field), value)
        }
        DirectoryHistoryPredicate::Lt(field, value) => {
            PreparedDirectoryHistoryPredicate::Lt(prepared_directory_history_field(field), value)
        }
        DirectoryHistoryPredicate::LtEq(field, value) => {
            PreparedDirectoryHistoryPredicate::LtEq(prepared_directory_history_field(field), value)
        }
        DirectoryHistoryPredicate::In(field, values) => {
            PreparedDirectoryHistoryPredicate::In(prepared_directory_history_field(field), values)
        }
        DirectoryHistoryPredicate::IsNull(field) => {
            PreparedDirectoryHistoryPredicate::IsNull(prepared_directory_history_field(field))
        }
        DirectoryHistoryPredicate::IsNotNull(field) => {
            PreparedDirectoryHistoryPredicate::IsNotNull(prepared_directory_history_field(field))
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
    current: TransactionBeginMode,
    next: TransactionBeginMode,
) -> TransactionBeginMode {
    match (current, next) {
        (TransactionBeginMode::Write, _) | (_, TransactionBeginMode::Write) => {
            TransactionBeginMode::Write
        }
        (TransactionBeginMode::Deferred, _) | (_, TransactionBeginMode::Deferred) => {
            TransactionBeginMode::Deferred
        }
        _ => TransactionBeginMode::Read,
    }
}

fn transaction_mode_for_committed_read_execution(
    compiled: &CompiledExecution,
) -> Result<TransactionBeginMode, LixError> {
    if compiled.plain_explain().is_some() {
        return Ok(TransactionBeginMode::Read);
    }
    if let Some(public_read) = compiled.public_read() {
        return Ok(public_read.committed_read_mode().transaction_mode());
    }
    if compiled.direct_execution().is_some() {
        return if compiled.read_only_query {
            Ok(TransactionBeginMode::Read)
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
