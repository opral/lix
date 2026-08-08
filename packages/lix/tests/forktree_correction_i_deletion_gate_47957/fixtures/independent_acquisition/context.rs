struct HistoryQuerySource<R> {
    store: R,
    forktree_reader: ForkTreeReadFacade<R>,
}
