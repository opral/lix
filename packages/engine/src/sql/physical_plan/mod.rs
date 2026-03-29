//! Physical plan ownership.
//!
//! Backend lowering hangs off this stage root.

pub(crate) mod lowerer;
pub(crate) mod plan;

pub(crate) use plan::{
    compile_lowered_read_statement, LoweredReadProgram, LoweredResultColumn, LoweredResultColumns,
    PhysicalPlan, PreparedPublicReadExecution, PreparedPublicWriteExecution,
    PublicWriteExecutionPartition, PublicWriteMaterialization, TerminalRelationRenderNode,
    TrackedWriteExecution, UntrackedWriteExecution,
};
