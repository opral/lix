struct QuerySource {
    forktree_reader: Reader,
}

async fn load_history_entries(
    query_source: QuerySource,
    commit_graph: CommitGraphReader,
) {
    let local_graph = CommitGraphContext::new().reader(other_read);
    let mut guard = commit_graph.lock().await;
    let _ = local_graph.change_history_from_commit("from").await;
    let _ = query_source
        .forktree_reader
        .scan_state_rows_at_commit("from")
        .await;
}
