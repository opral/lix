struct FilesystemWorkingDiffProvider<S> {
    forktree_reader: crate::forktree::ForkTreeReadFacade<S>,
}

fn make_working_diff<S>(query_source: HistoryQuerySource<S>) -> FilesystemWorkingDiffProvider<S> {
    FilesystemWorkingDiffProvider {
        forktree_reader: query_source.forktree_reader.clone(),
    }
}

fn scan_working_diff<S>(provider: &FilesystemWorkingDiffProvider<S>) {
    crate::forktree::checkpoint_history_fake(&provider.forktree_reader);
}
