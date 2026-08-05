mod context;
mod control;
mod lifecycle;
mod refs;
mod stage_rows;
mod types;

pub(crate) use context::BranchContext;
#[cfg(any(test, feature = "storage-benches"))]
pub(crate) use control::BRANCH_HEAD_CONTROL_SPACE;
pub(crate) use control::{
    BranchHeadControl, BranchHeadControlContext, BranchHeadControlObservation,
    BranchHeadControlReader, GENERATION_MANIFEST_SPACE, GENERATION_RECLAMATION_SPACE,
    GenerationChunkManifest, GenerationReclamation, branch_head_control_precondition,
    decode_reclamation_key, encode_generation_key, generation_scope_digest,
    stage_branch_head_control, stage_delete_branch_head_control, stage_generation_manifest,
    stage_generation_reclamation, untracked_identity_digest,
};
pub(crate) use lifecycle::{BranchLifecycle, BranchOperation, BranchReferenceRole};
pub(crate) use stage_rows::{
    BRANCH_DESCRIPTOR_SCHEMA_KEY, BRANCH_REF_SCHEMA_KEY, branch_descriptor_stage_row,
    branch_descriptor_tombstone_row, branch_ref_stage_row, branch_ref_tombstone_row,
};
pub(crate) use types::{BranchHead, BranchRefReader};
