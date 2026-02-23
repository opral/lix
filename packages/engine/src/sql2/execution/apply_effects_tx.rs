use std::collections::BTreeSet;

use crate::{Engine, LixError};

use super::super::contracts::effects::DetectedFileDomainChange;
use super::super::contracts::planned_statement::MutationRow;
use super::super::type_bridge::to_sql_mutations;

pub(crate) async fn apply_sql_backed_effects(
    engine: &Engine,
    mutations: &[MutationRow],
    pending_file_writes: &[crate::filesystem::pending_file_writes::PendingFileWrite],
    pending_file_delete_targets: &BTreeSet<(String, String)>,
    detected_file_domain_changes: &[DetectedFileDomainChange],
    untracked_filesystem_update_domain_changes: &[DetectedFileDomainChange],
    plugin_changes_committed: bool,
    file_cache_invalidation_targets: &BTreeSet<(String, String)>,
) -> Result<(), LixError> {
    let sql_mutations = to_sql_mutations(mutations);
    let should_run_binary_gc =
        crate::engine::should_run_binary_cas_gc(&sql_mutations, detected_file_domain_changes);

    if !plugin_changes_committed && !detected_file_domain_changes.is_empty() {
        engine
            .persist_detected_file_domain_changes(detected_file_domain_changes)
            .await?;
    }
    if !untracked_filesystem_update_domain_changes.is_empty() {
        engine
            .persist_untracked_file_domain_changes(untracked_filesystem_update_domain_changes)
            .await?;
    }
    engine
        .persist_pending_file_data_updates(pending_file_writes)
        .await?;
    engine
        .persist_pending_file_path_updates(pending_file_writes)
        .await?;
    engine
        .ensure_builtin_binary_blob_store_for_targets(file_cache_invalidation_targets)
        .await?;
    if should_run_binary_gc {
        engine.garbage_collect_unreachable_binary_cas().await?;
    }
    engine
        .invalidate_file_data_cache_entries(file_cache_invalidation_targets)
        .await?;
    engine
        .invalidate_file_path_cache_entries(file_cache_invalidation_targets)
        .await?;

    let _ = pending_file_delete_targets;
    Ok(())
}
