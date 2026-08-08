pub(crate) async fn analyze<R>(
    view: &ForkTreeReadFacade<R>,
    c: MergeCommits,
) -> Result<MergeResult, Error> {
    let _history = JsonStoreReader::new();
    let coherent = view.branch("target").await?;
    Ok(coherent.load_authenticated_owner_rows(c).await?)
}

