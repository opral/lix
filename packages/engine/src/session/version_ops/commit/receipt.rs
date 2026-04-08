use crate::canonical::journal::ChangeRow;
use crate::ReplayCursor;

pub(crate) fn latest_replay_cursor_from_change_rows(changes: &[ChangeRow]) -> Option<ReplayCursor> {
    changes
        .iter()
        .map(|change| ReplayCursor::new(change.id.clone(), change.created_at.clone()))
        .max()
}
