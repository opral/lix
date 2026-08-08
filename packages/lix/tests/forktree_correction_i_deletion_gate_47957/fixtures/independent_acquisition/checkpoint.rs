struct CheckpointProvider<S> {
    forktree_reader: crate::forktree::ForkTreeReadFacade<S>,
}

fn make_checkpoint<S>(query_source: HistoryQuerySource<S>) -> CheckpointProvider<S> {
    CheckpointProvider {
        forktree_reader: crate::forktree::ForkTreeReadFacade::new(
            query_source.store.begin_read(),
        ),
    }
}

fn scan_checkpoint<S>(provider: &CheckpointProvider<S>) {
    crate::forktree::checkpoint_history(&provider.forktree_reader);
}
