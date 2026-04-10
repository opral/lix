//! Stable logical plan ownership.
//!
//! The current public planner IR is re-homed here as the first compiler-owned
//! logical plan module.

pub(crate) mod dependency_spec;
pub(crate) mod direct_reads;
pub(crate) mod plan;
pub(crate) mod public_ir;
pub(crate) mod result_contract;
pub(crate) mod verify;

pub(crate) use dependency_spec::{DependencyPrecision, DependencySpec};
pub(crate) use direct_reads::{
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
pub(crate) use plan::{
    InternalLogicalPlan, LogicalPlan, PublicReadLogicalPlan, PublicWriteLogicalPlan,
    SurfaceReadPlan,
};
pub(crate) use result_contract::{result_contract_for_statements, ResultContract};
pub(crate) use verify::verify_logical_plan;
