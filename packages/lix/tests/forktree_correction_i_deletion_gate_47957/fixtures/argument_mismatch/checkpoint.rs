struct CheckpointProvider<S> {
    forktree_reader: crate::forktree::ForkTreeReadFacade<S>,
}

fn make_checkpoint<S>(query_source: HistoryQuerySource<S>) -> CheckpointProvider<S> {
    CheckpointProvider {
        forktree_reader: query_source.forktree_reader.clone(),
    }
}

fn scan_checkpoint<S>(
    provider: &CheckpointProvider<S>,
    other_reader: &crate::forktree::ForkTreeReadFacade<S>,
) {
    let _same_name = &provider.forktree_reader;
    crate::forktree::checkpoint_history(other_reader);
}
