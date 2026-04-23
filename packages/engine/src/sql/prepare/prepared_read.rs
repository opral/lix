use crate::backend::PreparedBatch;
use crate::backend::TransactionBeginMode;
use crate::sql::diagnostics::{
    build_read_diagnostic_catalog_snapshot, normalize_sql_error_with_read_diagnostic_context,
};
use crate::sql::explain::{prepare_analyzed_explain_template, prepare_plain_explain_template};
use crate::sql::logical_plan::history_reads::HistoryReadPlan;
use crate::sql::physical_plan::{
    LoweredResultColumn, LoweredResultColumns, PublicReadPhysicalPlan,
};
use crate::sql::prepare::SqlPreparationMetadataReader;
use crate::sql::{
    PreparedBatchReadArtifact, PreparedExplainMode, PreparedPublicRead,
    PreparedPublicReadPlanArtifact, PreparedReadArtifact, PreparedReadBatch, PreparedReadStatement,
    PreparedSql2ReadPlanArtifact, PublicReadResultColumn, PublicReadResultColumns,
    ReadDiagnosticContext,
};
use crate::{LixBackend, LixBackendTransaction, LixError};
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
    pub(crate) origin_key: Option<&'a str>,
    pub(crate) compiler_seed: SqlCompilerSeed<'a>,
    pub(crate) base_transaction_mode: TransactionBeginMode,
}

pub(crate) async fn prepare_committed_read_batch_with_backend(
    backend: &dyn LixBackend,
    statement_batch: &StatementBatch,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let mut metadata_reader = backend;
    prepare_committed_read_batch_from_reader(&mut metadata_reader, statement_batch, read_context)
        .await
}

pub(crate) async fn prepare_committed_read_batch_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    statement_batch: &StatementBatch,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let mut metadata_reader = transaction;
    prepare_committed_read_batch_from_reader(&mut metadata_reader, statement_batch, read_context)
        .await
}

async fn prepare_committed_read_batch_from_reader(
    metadata_reader: &mut dyn SqlPreparationMetadataReader,
    statement_batch: &StatementBatch,
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

    compile_committed_read_batch(&compiler_context, statement_batch, read_context).await
}

pub(crate) async fn compile_committed_read_batch(
    compiler_context: &dyn SqlCompilerContext,
    statement_batch: &StatementBatch,
    read_context: &CommittedReadContext<'_>,
) -> Result<PreparedReadBatch, LixError> {
    let mut mode = read_context.base_transaction_mode;
    let mut statements = Vec::new();

    for statement in statement_batch.steps() {
        let prepared_statement =
            compile_committed_read_statement(compiler_context, statement, read_context).await?;
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
        read_context,
    )
    .await?;
    let is_scalar_read = diagnostic_context.relation_names.is_empty();
    prepared_read_statement_from_compiled_execution(
        compiler_context.surface_registry(),
        compiler_context.dialect(),
        compiled,
        diagnostic_context,
        is_scalar_read,
    )
}

async fn compile_committed_execution_step(
    diagnostic_context: &ReadDiagnosticContext,
    compiler_context: &dyn SqlCompilerContext,
    bound_statement: &BoundStatementInstance,
    read_context: &CommittedReadContext<'_>,
) -> Result<CompiledExecution, LixError> {
    match compile_execution_from_bound_statement_with_context(
        compiler_context,
        bound_statement,
        read_context.active_version_id,
        read_context.active_account_ids,
        read_context.origin_key,
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
    surface_registry: &crate::catalog::SurfaceRegistry,
    dialect: crate::SqlDialect,
    compiled: CompiledExecution,
    mut diagnostic_context: ReadDiagnosticContext,
    is_scalar_read: bool,
) -> Result<PreparedReadStatement, LixError> {
    let transaction_mode =
        transaction_mode_for_committed_read_execution(&compiled, is_scalar_read)?;
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
        PreparedReadArtifact::Public(prepare_public_read_artifact(
            public_read,
            surface_registry,
            dialect,
        )?)
    } else if let Some(scalar) = compiled.scalar_read() {
        PreparedReadArtifact::Scalar(PreparedBatchReadArtifact {
            prepared_batch: PreparedBatch {
                steps: scalar.prepared_statements.clone(),
            },
        })
    } else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "committed read routing compiled an unsupported non-public execution unexpectedly",
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
    surface_registry: &crate::catalog::SurfaceRegistry,
    _dialect: crate::SqlDialect,
) -> Result<PreparedPublicRead, LixError> {
    let mut contract = super::public_surface::read::prepared_public_read_contract(public_read);
    if contract.result_columns.is_none() {
        contract.result_columns = result_columns_for_public_read_execution(&public_read.execution);
    }

    let execution = PreparedPublicReadPlanArtifact::Sql2(PreparedSql2ReadPlanArtifact {
        artifact: prepared_sql2_read_artifact(public_read, surface_registry),
    });

    Ok(PreparedPublicRead {
        contract,
        freshness_contract: public_read.freshness_contract,
        resolved_relations: public_read.resolved_relations.clone(),
        public_output_columns: public_read.public_output_columns.clone(),
        execution,
    })
}

fn prepared_sql2_read_artifact(
    public_read: &PublicReadPlan,
    surface_registry: &crate::catalog::SurfaceRegistry,
) -> crate::sql2::PreparedSql2ReadArtifact {
    crate::sql2::PreparedSql2ReadArtifact {
        sql: public_read.source_statement_sql.clone(),
        bound_parameters: public_read.bound_parameters.clone(),
        active_version_id: public_read.runtime_bindings.active_version_id.clone(),
        surface_names: public_read.resolved_relations.clone(),
        entity_views: crate::sql2::prepared_entity_view_plans_for_registry(
            surface_registry,
            &public_read.resolved_relations,
        ),
        filesystem_views: crate::sql2::prepared_filesystem_view_plans_for_registry(
            surface_registry,
            &public_read.resolved_relations,
        ),
    }
}

fn result_columns_for_public_read_execution(
    execution: &PublicReadPhysicalPlan,
) -> Option<PublicReadResultColumns> {
    match execution {
        PublicReadPhysicalPlan::Sql2 => None,
        PublicReadPhysicalPlan::ReadTimeProjection(_) => None,
        PublicReadPhysicalPlan::LoweredSql(lowered) => Some(
            public_read_result_columns_from_lowered(&lowered.result_columns),
        ),
        PublicReadPhysicalPlan::HistoryRead(plan) => Some(match plan {
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
    is_scalar_read: bool,
) -> Result<TransactionBeginMode, LixError> {
    if compiled.plain_explain().is_some() {
        return Ok(TransactionBeginMode::Read);
    }
    if let Some(public_read) = compiled.public_read() {
        return Ok(public_read.committed_read_mode().transaction_mode());
    }
    if compiled.scalar_read().is_some() {
        if is_scalar_read {
            return Ok(TransactionBeginMode::Read);
        }
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "committed read routing produced a scalar read unexpectedly",
        ));
    }
    Err(LixError::new(
        "LIX_ERROR_UNKNOWN",
        "committed read routing compiled an unsupported non-public execution unexpectedly",
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
