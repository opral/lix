mod context;
mod lifecycle;
mod refs;
mod stage_rows;
mod types;

pub(crate) use context::BranchContext;
pub(crate) use lifecycle::{BranchLifecycle, BranchOperation, BranchReferenceRole};
pub(crate) use stage_rows::{
    branch_descriptor_stage_row, branch_descriptor_tombstone_row, branch_ref_stage_row,
    branch_ref_tombstone_row, BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY,
};
pub(crate) use types::{BranchHead, BranchRefReader};
