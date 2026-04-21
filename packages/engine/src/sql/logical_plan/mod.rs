//! Stable logical plan ownership.
//!
//! The current public planner IR is re-homed here as the first compiler-owned
//! logical plan module.

pub(crate) mod history_reads;
pub(crate) mod plan;
pub(crate) mod public_ir;
pub(crate) mod result_contract;
pub(crate) mod verify;
pub(crate) use history_reads::{
    DirectoryHistoryAggregate, DirectoryHistoryField, DirectoryHistoryPredicate,
    DirectoryHistoryProjection, DirectoryHistoryReadPlan, DirectoryHistorySortKey,
    EntityHistoryField, EntityHistoryPredicate, EntityHistoryProjection, EntityHistoryReadPlan,
    EntityHistorySortKey, FileHistoryAggregate, FileHistoryField, FileHistoryPredicate,
    FileHistoryProjection, FileHistoryReadPlan, FileHistorySortKey, HistoryReadPlan,
    StateHistoryAggregate, StateHistoryAggregatePredicate, StateHistoryField,
    StateHistoryPredicate, StateHistoryProjection, StateHistoryProjectionValue,
    StateHistoryReadPlan, StateHistorySortKey, StateHistorySortValue,
};
pub(crate) use plan::{
    DirectLogicalPlan, LogicalPlan, NormalizedDirectStatements, PublicReadLogicalPlan,
    PublicWriteLogicalPlan, SurfaceReadPlan,
};
pub(crate) use result_contract::{result_contract_for_statements, ResultContract};
pub(crate) use verify::verify_logical_plan;
