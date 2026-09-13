use super::{MigrationOptions, publish::PublicationPlan};
use crate::LixError;
use crate::storage_adapter::{StorageAdapterRead, StorageWriteSet};

pub(super) async fn stage_repair(
    read: &(impl StorageAdapterRead + ?Sized),
    plan: &mut PublicationPlan,
    options: &MigrationOptions,
    entries: &mut usize,
    bytes: &mut usize,
) -> Result<(), LixError> {
    let collected = crate::sync::collect_certified_snapshot_omitted_owners(
        read,
        options.max_changes.saturating_sub(*entries),
        options.max_preflight_bytes.saturating_sub(*bytes),
    )
    .await?;
    *entries = entries.saturating_add(collected.entries);
    *bytes = bytes.saturating_add(collected.bytes);
    let space = crate::tracked_state::TRACKED_STATE_COMMIT_HISTORY_DEFERRED_SPACE;
    let mut staged = StorageWriteSet::new();
    let mut markers = Vec::with_capacity(collected.owners.len());
    for (id, scope) in collected.owners {
        crate::tracked_state::stage_commit_history_omitted(&mut staged, id, scope);
        let key = id.as_uuid().as_bytes();
        let value = staged.staged_value(space, key).ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_MIGRATION_FAILED",
                "omission marker staging produced no value",
            )
        })?;
        *entries = entries.saturating_add(1);
        *bytes = bytes.saturating_add(key.len() + value.len());
        if *entries > options.max_changes || *bytes > options.max_preflight_bytes {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                "snapshot omission marker publication exceeds preflight limits",
            ));
        }
        markers.push((key.to_vec(), value.to_vec()));
    }
    if !markers.is_empty() {
        plan.put_mutable(space, markers)?;
    }
    Ok(())
}
