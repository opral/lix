async fn merge_branch_preview(&self) -> Result<MergeResult, Error> {
    let merge_view = transaction.forktree_read_facade();
    let analysis = super::analysis::analyze(&merge_view, commits).await?;
    Ok(analysis)
}

async fn merge_branch(&self) -> Result<MergeResult, Error> {
    let merge_view = transaction.forktree_read_facade();
    let analysis = super::analysis::analyze(&merge_view, commits).await?;
    Ok(analysis)
}

