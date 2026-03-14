use std::collections::BTreeSet;

use crate::engine::{should_run_binary_cas_gc, Engine};
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::execute::{self, SqlExecutionOutcome};
use crate::sql::execution::shared_path::{
    public_write_filesystem_payload_changes_already_committed, PreparedExecutionContext,
};
use crate::sql::public::runtime::PublicWriteExecutionPartition;
use crate::LixError;

pub(crate) async fn apply_owned_execution_post_commit_effects(
    engine: &Engine,
    prepared: &PreparedExecutionContext,
    execution: &SqlExecutionOutcome,
    writer_key: Option<&str>,
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

    let filesystem_payload_domain_changes = engine
        .collect_live_filesystem_payload_domain_changes(
            &prepared.intent.pending_file_writes,
            &prepared.intent.pending_file_delete_targets,
            writer_key,
        )
        .await?;
    let filesystem_payload_domain_changes =
        crate::engine::dedupe_filesystem_payload_domain_changes(&filesystem_payload_domain_changes);
    let payload_domain_changes_to_persist =
        if public_write_filesystem_payload_changes_already_committed(prepared) {
            Vec::new()
        } else if execution.plugin_changes_committed {
            crate::engine::dedupe_filesystem_payload_domain_changes(
                &filesystem_payload_domain_changes,
            )
        } else {
            filesystem_payload_domain_changes.clone()
        };
    let should_run_binary_gc = should_run_binary_cas_gc(
        &prepared.plan.preprocess.mutations,
        &filesystem_payload_domain_changes,
    );

    if !public_write_filesystem_payload_changes_already_committed(prepared) {
        engine
            .persist_pending_file_data_updates(&prepared.intent.pending_file_writes)
            .await?;
    }
    if !payload_domain_changes_to_persist.is_empty() {
        engine
            .persist_filesystem_payload_domain_changes(&payload_domain_changes_to_persist)
            .await?;
    }
    if should_run_binary_gc {
        engine.garbage_collect_unreachable_binary_cas().await?;
    }

    let mut state_commit_stream_changes = active_effects.state_commit_stream_changes.clone();
    state_commit_stream_changes.extend(execution.state_commit_stream_changes.clone());
    engine.maybe_invalidate_deterministic_settings_cache(
        &prepared.plan.preprocess.mutations,
        &state_commit_stream_changes,
    );
    let should_emit_observe_tick = !state_commit_stream_changes.is_empty();

    if !effects_are_authoritative
        && prepared
            .plan
            .requirements
            .should_invalidate_installed_plugins_cache
    {
        engine.invalidate_installed_plugins_cache()?;
    }
    if should_emit_observe_tick
        && (!write_owned_transaction_committed
            || owned_public_write_requires_post_commit_observe_tick(prepared))
    {
        engine.append_observe_tick(writer_key).await?;
    }
    if public_surface_registry_dirty {
        engine.refresh_public_surface_registry().await?;
    }
    engine.emit_state_commit_stream_changes(state_commit_stream_changes);

    Ok(())
}

fn owned_public_write_requires_post_commit_observe_tick(
    prepared: &PreparedExecutionContext,
) -> bool {
    let Some(public_write) = prepared.public_write.as_ref() else {
        return false;
    };
    let Some(execution) = public_write.execution.as_ref() else {
        return false;
    };

    let saw_untracked = execution
        .partitions
        .iter()
        .any(|partition| matches!(partition, PublicWriteExecutionPartition::Untracked(_)));
    let saw_tracked = execution
        .partitions
        .iter()
        .any(|partition| matches!(partition, PublicWriteExecutionPartition::Tracked(_)));

    saw_untracked && !saw_tracked
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
