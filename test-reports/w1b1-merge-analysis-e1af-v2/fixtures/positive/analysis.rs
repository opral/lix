pub(crate) async fn analyze<R>(
    view: &ForkTreeReadFacade<R>,
    commits: MergeCommits,
) -> Result<MergeResult, Error> {
    let coherent = view.branch("target").await?;
    let rows = coherent.load_authenticated_owner_rows(commits).await?;
    Ok(MergeResult::from_owner_rows(rows))
}

