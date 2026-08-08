fn scan_changelog_changes<R: ForkTreeReadFacade>(reader: &ForkTreeReadFacade<R>, limit: usize) {}
fn load_exact_change<R: ForkTreeReadFacade>(reader: &ForkTreeReadFacade<R>, id: Id) {}

fn plan(query_source: ChangelogQuerySource<Read>, id: Id) {
    scan_changelog_changes(&query_source.forktree_reader, 10);
    load_exact_change(&query_source.forktree_reader, id);
    hidden_extra_reader();
}
