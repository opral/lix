struct FilesystemWorkingDiffProvider<S> {
    forktree_reader: crate::forktree::ForkTreeReadFacade<S>,
}

fn make_working_diff<S>(query_source: HistoryQuerySource<S>) -> FilesystemWorkingDiffProvider<S> {
    let independent = query_source.open_another_view();
    FilesystemWorkingDiffProvider {
        forktree_reader: independent.forktree_reader.clone(),
    }
}

fn scan_working_diff<S>(provider: &FilesystemWorkingDiffProvider<S>) {
    crate::forktree::checkpoint_history(&provider.forktree_reader);
}
