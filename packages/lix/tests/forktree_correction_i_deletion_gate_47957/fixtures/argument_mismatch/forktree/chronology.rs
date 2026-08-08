pub(crate) fn checkpoint_history<R>(reader: &ForkTreeReadFacade<R>) {
    let marker_commit = reader.marker_commit();
    let implicit_root = reader.root();
    let walked_commit = reader.walk(marker_commit);
    assert_eq!(walked_commit, marker_commit);
    let _ = implicit_root;
}
