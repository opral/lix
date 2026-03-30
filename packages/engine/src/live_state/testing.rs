use crate::live_state::UntrackedWriteRow;

pub(crate) fn local_version_head_write_row(
    version_id: &str,
    commit_id: &str,
    timestamp: &str,
) -> UntrackedWriteRow {
    super::projection::local_version_head_write_row(version_id, commit_id, timestamp)
}
