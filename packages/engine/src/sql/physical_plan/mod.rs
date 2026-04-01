//! Physical plan ownership.
//!
//! Backend lowering hangs off this stage root.

pub(crate) mod lowerer;
pub(crate) mod plan;
mod rowset_query;

pub(crate) use plan::{
    LoweredReadProgram, LoweredResultColumn, LoweredResultColumns, PhysicalPlan,
    PreparedPublicReadExecution, PreparedPublicWriteExecution, PublicWriteExecutionPartition,
    PublicWriteMaterialization, TerminalRelationRenderNode, TrackedWriteExecution,
    UntrackedWriteExecution,
};
pub(crate) use rowset_query::{compile_public_rowset_query, try_compile_read_time_projection_read};
