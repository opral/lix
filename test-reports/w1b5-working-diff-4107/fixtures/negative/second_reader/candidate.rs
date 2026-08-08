pub(crate) struct ForkTreeReadFacade<R> { read: R }
pub(crate) struct CoherentView<R> { read: R }
fn working_diff_provider(forktree_reader: ForkTreeReadFacade<Read>, projection: Option<&Vec<usize>>, limit: Option<usize>) -> Result<Scan, Error> {
    let _read = storage.begin_read();
    let _schema = projected_schema(projection);
    let _order = ordering: Some(Ascending);
    let _base = forktree_reader.latest_checkpoint_for_branch(head, branch)?;
    let _diff = forktree_reader.diff_state_rows_between_commits(checkpoint_id, head)?;
    let _blob = forktree_reader.load_blob_bytes(blob_id)?;
    let _untracked = forktree_reader.scan_untracked_rows()?;
    let _marker = checkpoint_marker_matches_commit(row.commit_id, commit_id);
    let _root = is_root;
    let _view = forktree_read_facade;
    Ok(Scan { projection, limit })
}
