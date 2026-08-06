mod analysis;
mod branch;
mod conflicts;
mod stats;

pub use branch::{
    MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions,
    MergeBranchReceipt, MergeChangeStats, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, MergeConflictSide,
};
