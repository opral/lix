use std::collections::BTreeSet;

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::{
    direct_state_file_cache_refresh_targets, file_descriptor_cache_eviction_targets, Engine,
};
use crate::functions::SharedFunctionProvider;
use crate::validation::{validate_inserts, validate_updates};
use crate::{LixBackend, LixError, Value};

use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::planning::derive_requirements::derive_plan_requirements;
use super::super::planning::plan::build_execution_plan;
use super::intent::{
    collect_execution_intent_with_backend, ExecutionIntent, IntentCollectionPolicy,
};
use sqlparser::ast::Statement;

pub(crate) struct PreparationPolicy {
    pub(crate) allow_plugin_cache: bool,
    pub(crate) detect_plugin_file_changes: bool,
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct PreparedExecutionContext {
    pub(crate) intent: ExecutionIntent,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) plan: ExecutionPlan,
}

pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
    pub(crate) file_data_cache_invalidation_targets: BTreeSet<(String, String)>,
    pub(crate) file_path_cache_invalidation_targets: BTreeSet<(String, String)>,
}

pub(crate) async fn prepare_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    policy: PreparationPolicy,
) -> Result<PreparedExecutionContext, LixError> {
    let (settings, sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(backend)
        .await?;

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::pending_file_writes::ensure_file_insert_ids_for_data_writes(
        &mut statements,
        &functions,
    )?;

    let requirements = derive_plan_requirements(&statements);

    engine
        .maybe_materialize_reads_with_backend_from_statements(
            backend,
            &statements,
            active_version_id,
        )
        .await?;

    let intent = collect_execution_intent_with_backend(
        engine,
        backend,
        &statements,
        params,
        active_version_id,
        writer_key,
        &requirements,
        IntentCollectionPolicy {
            allow_plugin_cache: policy.allow_plugin_cache,
            detect_plugin_file_changes: policy.detect_plugin_file_changes,
            skip_side_effect_collection: policy.skip_side_effect_collection,
        },
    )
    .await?;

    let plan = build_execution_plan(
        backend,
        &engine.cel_evaluator,
        statements,
        params,
        functions.clone(),
        &intent.detected_file_domain_changes_by_statement,
        writer_key,
    )
    .await
    .map_err(LixError::from)?;

    if !plan.preprocess.mutations.is_empty() {
        validate_inserts(backend, &engine.schema_cache, &plan.preprocess.mutations).await?;
    }
    if !plan.preprocess.update_validations.is_empty() {
        validate_updates(
            backend,
            &engine.schema_cache,
            &plan.preprocess.update_validations,
            params,
        )
        .await?;
    }

    Ok(PreparedExecutionContext {
        intent,
        settings,
        sequence_start,
        functions,
        plan,
    })
}

pub(crate) fn derive_cache_targets(
    plan: &ExecutionPlan,
    postprocess_file_cache_targets: BTreeSet<(String, String)>,
    intent: &ExecutionIntent,
) -> CacheTargets {
    let file_cache_refresh_targets = if plan.requirements.should_refresh_file_cache {
        let mut targets = direct_state_file_cache_refresh_targets(&plan.preprocess.mutations);
        targets.extend(postprocess_file_cache_targets);
        targets
    } else {
        BTreeSet::new()
    };
    let descriptor_cache_eviction_targets =
        file_descriptor_cache_eviction_targets(&plan.preprocess.mutations);
    let mut file_path_cache_invalidation_targets = file_cache_refresh_targets.clone();
    file_path_cache_invalidation_targets.extend(descriptor_cache_eviction_targets.clone());
    file_path_cache_invalidation_targets.extend(intent.pending_file_delete_targets.iter().cloned());
    let mut file_data_cache_invalidation_targets = file_path_cache_invalidation_targets.clone();
    file_data_cache_invalidation_targets.extend(intent.pending_file_write_targets.iter().cloned());

    CacheTargets {
        file_cache_refresh_targets,
        file_data_cache_invalidation_targets,
        file_path_cache_invalidation_targets,
    }
}
