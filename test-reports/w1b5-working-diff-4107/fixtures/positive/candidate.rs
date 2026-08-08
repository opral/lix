pub(crate) struct ForkTreeReadFacade<R> { read: R }
pub(crate) struct CoherentView<R> { read: R }
fn working_diff_provider(
    forktree_reader: ForkTreeReadFacade<Read>,
    projection: Option<&Vec<usize>>,
    limit: Option<usize>,
) -> Result<Scan, Error> {
    let _schema = projected_schema(projection);
    let _order = ordering: Some(Ascending);
    let _base = forktree_reader.latest_checkpoint_for_branch(head, branch)?;
    let _history = forktree_reader.checkpoint_history_from_head(head, branch)?;
    let _diff = forktree_reader.diff_state_rows_between_commits(checkpoint_id, head)?;
    let _rows = forktree_reader.scan_state_rows_at_commit(head)?;
    let _blob = forktree_reader.load_blob_bytes(blob_id)?;
    let _untracked = forktree_reader.scan_untracked_rows()?;
    let _marker = checkpoint_marker_matches_commit(row.commit_id, commit_id);
    let _walked = walked_commit_id;
    let _root = is_root;
    let _null = StateCell::Null;
    let _tombstone = StateCell::Tombstone;
    let _missing = required_object(id)?;
    let _malformed = serde_json::from_str;
    let _wrong_shape = serde_json::Value::as_str;
    let _view = forktree_read_facade;
    let _blob_ref = BlobRef::authenticated(blob_id);
    Ok(Scan { projection, limit, _order, _base, _diff, _rows, _blob, _untracked, _marker, _root, _view })
}
