use std::collections::BTreeSet;

use crate::state_commit_stream::StateCommitStreamChange;
use crate::{Engine, LixError};

pub(crate) async fn apply_runtime_post_commit_effects(
    engine: &Engine,
    file_cache_refresh_targets: BTreeSet<(String, String)>,
    should_invalidate_installed_plugins_cache: bool,
    should_emit_observe_tick: bool,
    writer_key: Option<&str>,
    state_commit_stream_changes: Vec<StateCommitStreamChange>,
) -> Result<(), LixError> {
    let _ = file_cache_refresh_targets;
    if should_invalidate_installed_plugins_cache {
        engine.invalidate_installed_plugins_cache()?;
    }
    if should_emit_observe_tick {
        engine.append_observe_tick(writer_key).await?;
    }
    engine.emit_state_commit_stream_changes(state_commit_stream_changes);
    Ok(())
}
