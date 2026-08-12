mod context;
mod control;
mod lifecycle;
mod refs;
mod stage_rows;
mod types;

pub(crate) use context::BranchContext;
pub(crate) use control::BRANCH_HEAD_CONTROL_SPACE;
pub(crate) use control::{
    BranchHeadControl, BranchHeadControlContext, BranchHeadControlObservation,
    BranchHeadControlReader, BranchHeadTrackedReachability, branch_head_control_precondition,
    stage_branch_head_control, stage_delete_branch_head_control,
};
pub(crate) use lifecycle::{BranchLifecycle, BranchOperation, BranchReferenceRole};
pub(crate) use stage_rows::{
    BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY, branch_descriptor_stage_row,
    branch_descriptor_tombstone_row, branch_ref_stage_row, branch_ref_tombstone_row,
};
pub(crate) use types::{BranchHead, BranchRefReader};
