use std::collections::BTreeSet;

use crate::state_commit_stream::StateCommitStreamChange;
use crate::{Engine, LixError};

pub(crate) async fn apply_runtime_post_commit_effects(
    engine: &Engine,
    file_cache_refresh_targets: BTreeSet<(String, String)>,
    should_invalidate_installed_plugins_cache: bool,
    state_commit_stream_changes: Vec<StateCommitStreamChange>,
) -> Result<(), LixError> {
    engine
        .refresh_file_data_for_versions(file_cache_refresh_targets)
        .await?;
    if should_invalidate_installed_plugins_cache {
        engine.invalidate_installed_plugins_cache()?;
    }
    engine.emit_state_commit_stream_changes(state_commit_stream_changes);
    Ok(())
}
