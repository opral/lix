async fn merge_branch_preview(&self) -> Result<MergeResult, Error> {
    let merge_view = transaction.forktree_read_facade();
    super::analysis::analyze(&merge_view, commits).await
}

