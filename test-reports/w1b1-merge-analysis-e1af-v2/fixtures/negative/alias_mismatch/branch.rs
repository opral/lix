async fn merge_branch_preview(&self) -> Result<MergeResult, Error> {
    let merge_view = transaction.forktree_read_facade();
    let other_view = transaction.forktree_read_facade();
    super::analysis::analyze(&other_view, commits).await
}

