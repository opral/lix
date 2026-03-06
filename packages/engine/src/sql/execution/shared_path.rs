use std::collections::BTreeSet;

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::Engine;
use crate::functions::SharedFunctionProvider;
use crate::sql2::runtime::{prepare_sql2_read, Sql2PreparedRead};
use crate::validation::{validate_inserts, validate_updates};
use crate::{LixBackend, LixError, Value};

use super::super::contracts::execution_plan::ExecutionPlan;
use super::super::planning::derive_requirements::derive_plan_requirements;
use super::super::planning::plan::build_execution_plan;
use super::intent::{
    authoritative_pending_file_write_targets, collect_execution_intent_with_backend,
    ExecutionIntent, IntentCollectionPolicy,
};
use sqlparser::ast::Statement;

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct PreparedExecutionContext {
    pub(crate) intent: ExecutionIntent,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) plan: ExecutionPlan,
    pub(crate) sql2_read: Option<Sql2PreparedRead>,
}

pub(crate) struct CacheTargets {
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
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

    let sql2_read =
        prepare_sql2_read(backend, &statements, params, active_version_id, writer_key).await;

    let intent = collect_execution_intent_with_backend(
        engine,
        backend,
        &statements,
        params,
        active_version_id,
        writer_key,
        &requirements,
        IntentCollectionPolicy {
            skip_side_effect_collection: policy.skip_side_effect_collection,
        },
    )
    .await?;

    let plan = build_execution_plan(
        backend,
        &engine.cel_evaluator,
        statements,
        params,
        sql2_read
            .as_ref()
            .and_then(|prepared| prepared.dependency_spec.clone()),
        functions.clone(),
        &intent.detected_file_domain_changes_by_statement,
        &intent.pending_file_delete_targets,
        &authoritative_pending_file_write_targets(&intent.pending_file_writes),
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
        sql2_read,
    })
}

pub(crate) fn derive_cache_targets(
    plan: &ExecutionPlan,
    postprocess_file_cache_targets: BTreeSet<(String, String)>,
) -> CacheTargets {
    let file_cache_refresh_targets = if plan.requirements.should_refresh_file_cache {
        let mut targets = plan.effects.file_cache_refresh_targets.clone();
        targets.extend(postprocess_file_cache_targets.clone());
        targets
    } else {
        BTreeSet::new()
    };

    CacheTargets {
        file_cache_refresh_targets,
    }
}
