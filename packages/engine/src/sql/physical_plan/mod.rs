//! Physical plan ownership.
//!
//! Backend lowering hangs off this stage root.

mod catalog_relation_sql;
pub(crate) mod lowerer;
pub(crate) mod plan;
mod public_read_artifacts;
pub(crate) mod public_surface_sql_support;
mod rowset_query;
pub(crate) mod source_sql;

pub(crate) use plan::{
    LoweredReadProgram, LoweredResultColumn, LoweredResultColumns, PhysicalPlan,
    PreparedPublicReadExecution, PreparedPublicWriteExecution, PublicWriteExecutionPartition,
    PublicWriteMaterialization, TerminalRelationRenderNode, TrackedWriteExecution,
    UntrackedWriteExecution,
};
pub(crate) use public_read_artifacts::{
    compile_derived_rowset_execution, compile_general_public_read_execution,
    CompilerOwnedPublicReadExecutionSelection,
};
pub(crate) use rowset_query::{compile_public_rowset_query, try_compile_read_time_projection_read};
