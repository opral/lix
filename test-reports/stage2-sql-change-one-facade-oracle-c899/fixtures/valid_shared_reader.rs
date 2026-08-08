struct QuerySource {
    forktree_reader: Reader,
}

fn read_session(read_scope: ReadScope) {
    let query_source = ForkTreeReadFacade::new(read_scope);
    register_lix_change(query_source.clone());
    register_lix_diff(query_source.clone());
    register_history(query_source);
}

async fn load_history_entries(
    query_source: QuerySource,
    commit_graph: CommitGraphReader,
) {
    let chronology = query_source.forktree_reader.clone();
    let mut guard = commit_graph.lock().await;
    let _ = guard.change_history_from_commit("from").await;
    let _ = guard.load_commit_records(&["from"]).await;
    let _ = query_source
        .forktree_reader
        .scan_state_rows_at_commit("from")
        .await;
}
