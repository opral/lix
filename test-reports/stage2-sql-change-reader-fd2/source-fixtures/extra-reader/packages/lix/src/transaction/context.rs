fn changelog_query_source(&self) -> ChangelogQuerySource<Read> {
    ChangelogQuerySource {
        forktree_reader: ForkTreeReadFacade::new(self.read_store),
    }
}
