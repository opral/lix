pub(crate) async fn analyze<R>(
    view: &ForkTreeReadFacade<R>,
    c: MergeCommits,
) -> Result<MergeResult, Error> {
    let rows = view.branch("target").await?.load_authenticated_owner_rows(c).await?;
    let _fallback = merge_payload_fallback_cache(rows);
    Ok(MergeResult::default())
}

