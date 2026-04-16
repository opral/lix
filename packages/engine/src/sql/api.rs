//! Sanctioned high-level root API for the SQL compiler owner.
//!
//! Outside `sql/*`, callers should prefer these use-case entrypoints over
//! stage-oriented modules such as `prepare/*`, `physical_plan/*`, or
//! `semantic_ir/*`.

use std::time::Duration;

use sqlparser::ast::Statement;

use crate::catalog::SurfaceRegistry;
use crate::functions::{DynFunctionProvider, SharedFunctionProvider, SystemFunctionProvider};
use crate::sql::{ChangeBatch, DependencySpec, PreparedExplainMode, PreparedExplainTemplate};
use crate::streams::StateCommitStreamFilter;
use crate::{LixError, SqlDialect, Value};

#[cfg(test)]
pub(crate) use super::analysis::state_resolution::canonical::is_query_only_statements;
pub(crate) use super::analysis::state_resolution::canonical::should_invalidate_installed_plugins_cache_for_statements;
#[cfg(test)]
pub(crate) use super::binder::advance_placeholder_state_for_statement_ast;
pub(crate) use super::binder::RuntimeBindingValues;
pub(crate) use super::explain::{
    prepare_analyzed_explain_template, prepare_plain_explain_template,
};
pub(crate) use super::logical_plan::public_ir::{
    CanonicalStateAssignments, CanonicalStateRowKey, InsertOnConflictAction, MutationPayload,
    PlannedWrite, ResolvedRowRef, ResolvedWritePartition, ResolvedWritePlan, RowLineage,
    SchemaProof, ScopeProof, TargetSetProof, WriteModeRequest, WriteOperationKind,
};
#[cfg(test)]
pub(crate) use super::optimizer::optimize_state_resolution;
pub(crate) use super::parser::{parse_sql_statements, parse_sql_with_timing};
pub(crate) use super::physical_plan::{PublicWriteExecutionPartition, PublicWritePhysicalPlan};
#[cfg(test)]
pub(crate) use super::prepare::execution_program::{StatementTemplate, StatementTemplateCacheKey};
pub(crate) use super::prepare::{
    build_public_write_execution, build_public_write_invariant_trace,
    compile_execution_from_bound_statement_with_context, finalize_public_write_execution,
    load_sql_compiler_metadata, load_sql_compiler_metadata_with_reader,
    load_sql_compiler_metadata_with_reader_and_pending_overlay,
    prepare_committed_read_batch_in_transaction, prepare_committed_read_batch_with_backend,
    prepare_public_read_artifact, public_authoritative_write_error, public_write_preparation_error,
    BoundStatementInstance, CommittedReadContext, CompilePolicy, CompiledExecution, PublicPlan,
    PublicReadPlan, PublicWritePlan, SqlCompilerMetadata, SqlCompilerSeed, StatementBatch,
};
pub(crate) use super::semantic_ir::semantics::changes::{
    build_change_batches, derive_commit_preconditions,
};
pub(crate) use super::semantic_ir::semantics::effective_state_resolver::{
    ExactEffectiveStateRow, ExactEffectiveStateRowRequest,
};
pub(crate) use super::semantic_ir::semantics::filesystem_assignments::{
    DirectoryInsertAssignments, DirectoryUpdateAssignments, FileInsertAssignments,
    FileUpdateAssignments, FilesystemWriteIntent,
};
pub(crate) use super::semantic_ir::semantics::state_assignments::{
    apply_entity_state_assignments, apply_state_assignments, assignments_from_payload,
    build_entity_insert_rows_with_functions, build_state_insert_row,
    ensure_identity_columns_preserved, EntityAssignmentsSemantics, EntityInsertSemantics,
    StateAssignmentsError,
};
pub(crate) use super::semantic_ir::semantics::surface_semantics::{
    overlay_lanes_for_version, public_selector_column_name, public_selector_version_column,
    OverlayLane,
};
pub(crate) use super::support::{
    bind_sql, bind_sql_with_state, parse_sql_script_with_timing, parse_sql_statements_with_timing,
    reject_internal_table_writes, reject_public_create_table, resolve_placeholder_index, BoundSql,
    ParsedSql, PlaceholderState,
};

/// Prepare either a public read or a public write from already-parsed SQL
/// statements using catalog-owned surface semantics and compiler metadata.
#[allow(dead_code)]
pub(crate) async fn prepare_public_plan(
    dialect: SqlDialect,
    functions: DynFunctionProvider,
    registry: &SurfaceRegistry,
    compiler_metadata: &SqlCompilerMetadata,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_history_root_commit_id: Option<&str>,
    active_account_ids: &[String],
    origin_key: Option<&str>,
    allow_internal_relations: bool,
    parse_duration: Option<Duration>,
) -> Result<Option<PublicPlan>, LixError> {
    super::prepare::public_surface::prepare_public_plan_with_registry_context_and_functions(
        dialect,
        functions,
        registry,
        compiler_metadata,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id,
        active_account_ids,
        origin_key,
        allow_internal_relations,
        parse_duration,
    )
    .await
}

/// Prepare a public read from already-parsed SQL statements.
///
/// This is the sanctioned root entrypoint for public-surface read compilation.
#[allow(dead_code)]
pub(crate) async fn prepare_public_read(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    compiler_metadata: &SqlCompilerMetadata,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_history_root_commit_id: Option<&str>,
    origin_key: Option<&str>,
    allow_internal_relations: bool,
    parse_duration: Option<Duration>,
) -> Result<Option<PublicReadPlan>, LixError> {
    super::prepare::try_prepare_public_read_with_registry_and_internal_access(
        dialect,
        registry,
        compiler_metadata,
        parsed_statements,
        params,
        active_version_id,
        active_history_root_commit_id,
        origin_key,
        allow_internal_relations,
        parse_duration,
    )
    .await
}

/// Prepare a public write from already-parsed SQL statements.
///
/// This is the sanctioned root entrypoint for public-surface write compilation.
#[allow(dead_code)]
pub(crate) async fn prepare_public_write(
    dialect: SqlDialect,
    registry: &SurfaceRegistry,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    active_account_ids: &[String],
    origin_key: Option<&str>,
    parse_duration: Option<Duration>,
) -> Result<Option<PublicWritePlan>, LixError> {
    super::prepare::public_surface::try_prepare_public_write_with_registry_and_functions(
        dialect,
        SharedFunctionProvider::new(Box::new(SystemFunctionProvider)),
        registry,
        parsed_statements,
        params,
        active_version_id,
        active_account_ids,
        origin_key,
        parse_duration,
    )
    .await
}

/// Extract the explicit transaction body from a `BEGIN ... COMMIT` script when
/// the caller needs to compile the inner statements as one unit.
pub(crate) fn extract_explicit_transaction_script(
    statements: &[Statement],
    params: &[Value],
) -> Result<Option<Vec<Statement>>, LixError> {
    super::prepare::script::extract_explicit_transaction_script_from_statements(statements, params)
}

/// Derive observe/session dependency metadata from already-parsed SQL
/// statements.
pub(crate) fn derive_dependency_spec(
    statements: &[Statement],
    params: &[Value],
) -> Result<DependencySpec, LixError> {
    super::prepare::dependency_spec::derive_dependency_spec_from_statements(statements, params)
}

/// Convert a compiler dependency specification into a state-commit stream
/// filter for observe/session invalidation workflows.
pub(crate) fn dependency_spec_to_state_commit_stream_filter(
    spec: &DependencySpec,
) -> StateCommitStreamFilter {
    super::prepare::dependency_spec::dependency_spec_to_state_commit_stream_filter(spec)
}

/// Canonicalize state-resolution requirements for a parsed SQL script without
/// exposing analysis-stage internals to outside owners.
#[cfg(test)]
pub(crate) fn canonicalize_state_resolution(
    statements: &[Statement],
) -> super::analysis::state_resolution::canonical::CanonicalStateResolution {
    super::analysis::state_resolution::canonical::canonicalize_state_resolution(statements)
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompiledExplainDiagnostics {
    pub(crate) explain_mode: Option<PreparedExplainMode>,
    pub(crate) plain_template: Option<PreparedExplainTemplate>,
    pub(crate) analyzed_template: Option<PreparedExplainTemplate>,
}

/// Build prepared explain templates and explain mode metadata from a compiled
/// execution without exposing explain-stage internals to callers.
pub(crate) fn compiled_explain_diagnostics(
    compiled: &CompiledExecution,
) -> Result<CompiledExplainDiagnostics, LixError> {
    let plain_template = compiled
        .plain_explain()
        .map(super::explain::prepare_plain_explain_template)
        .transpose()?
        .flatten();
    let analyzed_template = compiled
        .analyzed_explain()
        .map(super::explain::prepare_analyzed_explain_template)
        .transpose()?
        .flatten();
    let explain_mode = compiled.explain().and_then(|explain| {
        explain.request().map(|request| {
            if request.requires_execution() {
                PreparedExplainMode::Analyze
            } else {
                PreparedExplainMode::Plain
            }
        })
    });

    Ok(CompiledExplainDiagnostics {
        explain_mode,
        plain_template,
        analyzed_template,
    })
}

/// Refresh explain artifacts for a materialized public write using root SQL
/// orchestration rather than explain-stage types at the call site.
pub(crate) fn refresh_materialized_public_write_explain(
    public_write: &mut PublicWritePlan,
    execution: PublicWritePhysicalPlan,
    change_batches: Vec<ChangeBatch>,
    physical_planning_duration: Duration,
) {
    let mut stage_timings = public_write.explain_plan.stage_timings.clone();
    stage_timings.push(super::explain::stage_timing(
        super::explain::ExplainStage::PhysicalPlanning,
        physical_planning_duration,
    ));

    public_write.change_batches = change_batches.clone();
    public_write.execution = execution.clone();
    public_write.explain = super::explain::build_public_write_explain_artifacts(
        super::explain::PublicWriteExplainBuildInput {
            request: public_write.explain_plan.request.clone(),
            semantics: public_write.explain_plan.semantics.clone(),
            planned_write: public_write.planned_write.clone(),
            execution,
            change_batches,
            invariant_trace: Some(super::prepare::build_public_write_invariant_trace(
                &public_write.planned_write,
            )),
            stage_timings,
        },
    );
}
