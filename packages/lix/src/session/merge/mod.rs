mod analysis;
mod branch;
mod conflicts;
mod stats;

#[cfg(feature = "storage-benches")]
pub(crate) use analysis::{
    MergeCommits as MergeCommitsForBench, analyze as analyze_merge_for_bench,
};

pub use branch::{
    MergeBranchOptions, MergeBranchOutcome, MergeBranchPreview, MergeBranchPreviewOptions,
    MergeBranchReceipt, MergeChangeStats, MergeConflict, MergeConflictChangeKind,
    MergeConflictKind, MergeConflictSide,
};
