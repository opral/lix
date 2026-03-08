use std::collections::BTreeSet;

use crate::{Engine, LixError};

use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::planned_statement::MutationRow;

pub(crate) async fn apply_sql_backed_effects(
    engine: &Engine,
    mutations: &[MutationRow],
    pending_file_writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    pending_file_delete_targets: &BTreeSet<(String, String)>,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    untracked_filesystem_update_domain_changes: &[DetectedFileDomainChange],
    plugin_changes_committed: bool,
    filesystem_payload_changes_already_committed: bool,
    writer_key: Option<&str>,
) -> Result<(), LixError> {
    let filesystem_payload_domain_changes = engine
        .collect_live_filesystem_payload_domain_changes(
            pending_file_writes,
            pending_file_delete_targets,
            writer_key,
        )
        .await?;
    let mut tracked_domain_changes = detected_file_domain_changes.to_vec();
    tracked_domain_changes.extend(filesystem_payload_domain_changes.clone());
    tracked_domain_changes =
        crate::engine::dedupe_detected_file_domain_changes(&tracked_domain_changes);
    let tracked_domain_changes_to_persist = if filesystem_payload_changes_already_committed {
        Vec::new()
    } else if plugin_changes_committed {
        crate::engine::dedupe_detected_file_domain_changes(&filesystem_payload_domain_changes)
    } else {
        tracked_domain_changes.clone()
    };
    let should_run_binary_gc =
        crate::engine::should_run_binary_cas_gc(mutations, &tracked_domain_changes);

    if !filesystem_payload_changes_already_committed {
        engine
            .persist_pending_file_data_updates(pending_file_writes)
            .await?;
    }
    if !tracked_domain_changes_to_persist.is_empty() {
        engine
            .persist_detected_file_domain_changes(&tracked_domain_changes_to_persist)
            .await?;
    }
    if !untracked_filesystem_update_domain_changes.is_empty() {
        engine
            .persist_untracked_file_domain_changes(untracked_filesystem_update_domain_changes)
            .await?;
    }
    if should_run_binary_gc {
        engine.garbage_collect_unreachable_binary_cas().await?;
    }
    Ok(())
}
