mod analysis;
mod branch;
mod conflicts;
mod stats;

pub use branch::{
    BranchDiff, BranchDiffChangeKind, BranchDiffEntry, BranchDiffOptions, MergeBranchOptions,
    MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    MergeChangeStats, MergeConflict, MergeConflictChangeKind, MergeConflictKind, MergeConflictSide,
};
pub(crate) use branch::{
    analyze_pinned_branch_pair, branch_diff_from_readers, merge_analysis_in_transaction,
    pinned_branch_diff,
};
