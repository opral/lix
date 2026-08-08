fn changelog_query_source(&self) -> ChangelogQuerySource<Read> {
    let second_reader = ForkTreeReadFacade::new(self.other_store);
    ChangelogQuerySource {
        forktree_reader: ForkTreeReadFacade::new(self.read_store),
    }
}
