async fn merge_branch_preview(&self) -> Result<MergeResult, Error> {
    let first_view = transaction.forktree_read_facade();
    let second_view = transaction.forktree_read_facade();
    super::analysis::analyze(&first_view, commits).await
}

