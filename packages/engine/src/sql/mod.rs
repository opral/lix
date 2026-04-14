//! `sql/*` is the engine's SQL compiler subsystem.
//!
//! The long-term ownership model is stage-oriented:
//! parser -> binder -> semantic IR -> logical plan -> routing / optimizer
//! -> physical plan -> prepare -> explain.
//!
//! Post-Plan-20 dependency rules:
//!
//! - compiler-core SQL may depend on owner-owned contracts from
//!   `canonical/read/*`, `session/version_ops/*`,
//!   root-level `live_state`, and `live_state::writer_key::*` where
//!   row-serving writer-key facts are required
//! - compiler-core SQL must not depend on `commit/*`
//! - compiler-core SQL must not depend on `canonical/journal/*` or
//!   `canonical/graph/*` implementation details
//! - SQL should not grow a compiler-owned cross-subsystem capability hub
//! - cross-owner read glue should live in owner-owned contracts or
//!   stage-owned helpers, not in `sql/services/*`
//! - current-state access from compiler-core should use owner-owned logical
//!   `live_state` contracts, not concrete row/scan contracts
//! - direct `filesystem::*` imports inside compiler-core remain explicit
//!   tracked debt during the Plan 9 hardening work
//!
//! The root `sql` module is the sanctioned outside-facing compiler surface.
//! Non-`sql/*` owners should consume high-level entrypoints re-exported from
//! `sql/api.rs` rather than importing stage internals directly.

pub(crate) mod analysis;
mod api;
pub(crate) mod ast;
pub(crate) mod binder;
pub(crate) mod common;
pub(crate) mod dependency;
pub(crate) mod diagnostics;
pub(crate) mod effective_state_request;
pub(crate) mod explain;
pub(crate) mod logical_plan;
pub(crate) mod optimizer;
pub(crate) mod parser;
pub(crate) mod physical_plan;
pub(crate) mod planned_statement;
pub(crate) mod prepare;
pub(crate) mod prepared_artifacts;
mod relation_policy;
pub(crate) mod semantic_ir;
pub(crate) mod support;
pub(crate) mod write_artifacts;

#[cfg(test)]
#[allow(unused_imports)]
pub(crate) use api::{
    advance_placeholder_state_for_statement_ast, canonicalize_state_resolution,
    is_query_only_statements, optimize_state_resolution, StatementTemplate,
    StatementTemplateCacheKey,
};
#[allow(unused_imports)]
pub(crate) use api::{
    apply_entity_state_assignments, apply_state_assignments, assignments_from_payload, bind_sql,
    bind_sql_with_state, build_change_batches, build_entity_insert_rows_with_functions,
    build_public_write_execution, build_public_write_invariant_trace, build_state_insert_row,
    compile_execution_from_bound_statement_with_context, compiled_explain_diagnostics,
    dependency_spec_to_state_commit_stream_filter, derive_commit_preconditions,
    derive_dependency_spec, ensure_identity_columns_preserved, extract_explicit_transaction_script,
    finalize_public_write_execution, load_sql_compiler_metadata,
    load_sql_compiler_metadata_with_reader,
    load_sql_compiler_metadata_with_reader_and_pending_overlay, overlay_lanes_for_version,
    parse_sql, parse_sql_script_with_timing, parse_sql_statements,
    parse_sql_statements_with_timing, parse_sql_with_timing, prepare_analyzed_explain_template,
    prepare_committed_read_batch_in_transaction, prepare_committed_read_batch_with_backend,
    prepare_plain_explain_template, prepare_public_plan, prepare_public_read,
    prepare_public_read_artifact, prepare_public_write, public_authoritative_write_error,
    public_selector_column_name, public_selector_version_column, public_write_preparation_error,
    refresh_materialized_public_write_explain, reject_internal_table_writes,
    reject_public_create_table, resolve_placeholder_index,
    should_invalidate_installed_plugins_cache_for_statements, BoundSql, BoundStatementInstance,
    CanonicalStateAssignments, CanonicalStateRowKey, CommittedReadContext, CompilePolicy,
    CompiledExecution, CompiledExplainDiagnostics, DirectoryInsertAssignments,
    DirectoryUpdateAssignments, EntityAssignmentsSemantics, EntityInsertSemantics,
    ExactEffectiveStateRow, ExactEffectiveStateRowRequest, FileInsertAssignments,
    FileUpdateAssignments, FilesystemWriteIntent, InsertOnConflictAction, MutationPayload,
    OverlayLane, ParsedSql, PlaceholderState, PlannedWrite, PublicPlan, PublicReadPlan,
    PublicWriteExecutionPartition, PublicWritePhysicalPlan, PublicWritePlan, ResolvedRowRef,
    ResolvedWritePartition, ResolvedWritePlan, RowLineage, RuntimeBindingValues, SchemaProof,
    ScopeProof, SqlCompilerMetadata, SqlCompilerSeed, StateAssignmentsError, StatementBatch,
    TargetSetProof, WriteModeRequest, WriteOperationKind,
};
pub(crate) use dependency::{DependencyPrecision, DependencySpec, QueryDependency};
pub(crate) use diagnostics::{
    normalize_sql_error_with_backend_and_relation_names,
    normalize_sql_error_with_read_diagnostic_context,
    sanitize_lowered_public_sql_error_description, transaction_control_statement_denied_error,
};
pub(crate) use effective_state_request::{EffectiveStateRequest, EffectiveStateVersionScope};
pub(crate) use logical_plan::ResultContract;
pub(crate) use planned_statement::{
    coalesce_live_table_requirements, is_untracked_live_table, MutationOperation, MutationRow,
    PlannedStatementSet, SchemaLiveTableRequirement, UpdateValidationPlan,
};
pub(crate) use prepare::SqlPreparationMetadataReader;
pub(crate) use prepare::{
    SqlPreparationPendingOverlay, SqlPreparationPendingRow, SqlPreparationPendingStorage,
};
pub(crate) use prepared_artifacts::*;
pub(crate) use write_artifacts::{
    ChangeBatch, CommitPreconditions, ExpectedHead, IdempotencyKey, OptionalTextPatch, PlanEffects,
    PlannedFilesystemDescriptor, PlannedFilesystemFile, PlannedFilesystemState, PlannedRowIdentity,
    PlannedStateRow, PublicChange, SemanticEffect, SessionStateDelta, WriteLane, WriteMode,
};

// Existing root helpers used outside the compiler stages.
pub(crate) use physical_plan::source_sql::lower_catalog_relation_binding_to_source_sql;
pub(crate) use relation_policy::{
    builtin_relation_inventory, classify_builtin_relation_name, classify_relation_name,
    object_name_is_internal_relation, object_name_is_protected_builtin_ddl_target,
    protected_builtin_public_surface_names, relation_policy_choice_summary, RelationPolicy,
};
