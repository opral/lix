use std::collections::BTreeSet;

use crate::engine::Engine;
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::execute::{self, SqlExecutionOutcome};
use crate::sql::execution::shared_path::PreparedExecutionContext;
use crate::LixError;

pub(crate) async fn apply_owned_execution_post_commit_effects(
    engine: &Engine,
    prepared: &PreparedExecutionContext,
    execution: &SqlExecutionOutcome,
    _writer_key: Option<&str>,
    write_owned_transaction_committed: bool,
    public_surface_registry_dirty: bool,
) -> Result<(), LixError> {
    if !write_owned_transaction_committed {
        execute::persist_runtime_sequence(
            engine,
            prepared.settings,
            prepared.sequence_start,
            &prepared.functions,
        )
        .await?;
    }

    let active_effects = execution
        .plan_effects_override
        .as_ref()
        .unwrap_or(&prepared.plan.effects);
    let effects_are_authoritative = execution.plan_effects_override.is_some();

    if let Some(version_id) = &active_effects.next_active_version_id {
        engine.set_active_version_id(version_id.clone());
    }

    let _file_cache_refresh_targets = derive_cache_targets(
        &prepared.plan,
        active_effects,
        effects_are_authoritative,
        execution.postprocess_file_cache_targets.clone(),
    )
    .file_cache_refresh_targets;

    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    engine.maybe_invalidate_deterministic_settings_cache(
        &prepared.plan.preprocess.mutations,
        &state_commit_stream_changes,
    );

    if !effects_are_authoritative
        && prepared
            .plan
            .requirements
            .should_invalidate_installed_plugins_cache
    {
        engine.invalidate_installed_plugins_cache()?;
    }
    if public_surface_registry_dirty {
        engine.refresh_public_surface_registry().await?;
    }
    engine.emit_state_commit_stream_changes(state_commit_stream_changes);

    Ok(())
}
pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

pub(crate) fn derive_cache_targets(
    plan: &ExecutionPlan,
    active_effects: &PlanEffects,
    effects_are_authoritative: bool,
    postprocess_file_cache_targets: BTreeSet<(String, String)>,
) -> CacheTargets {
    let file_cache_refresh_targets =
        if effects_are_authoritative || plan.requirements.should_refresh_file_cache {
            let mut targets = active_effects.file_cache_refresh_targets.clone();
            targets.extend(postprocess_file_cache_targets.clone());
            targets
        } else {
            BTreeSet::new()
        };

    CacheTargets {
        file_cache_refresh_targets,
    }
}
