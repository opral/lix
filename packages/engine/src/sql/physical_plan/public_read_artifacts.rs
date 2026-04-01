use std::collections::{BTreeMap, BTreeSet};
use std::time::Instant;

use serde_json::Value as JsonValue;

use crate::contracts::artifacts::EffectiveStateRequest;
use crate::contracts::surface::SurfaceFamily;
use crate::schema::{SchemaProvider, SqlRegisteredSchemaProvider};
use crate::sql::backend::PushdownDecision;
use crate::sql::explain::{ExplainStage, ExplainTimingCollector};
use crate::sql::logical_plan::public_ir::StructuredPublicRead;
use crate::sql::logical_plan::DependencySpec;
use crate::sql::semantic_ir::semantics::effective_state_resolver::EffectiveStatePlan;
use crate::{LixBackend, LixError};

use super::lowerer::lower_read_for_execution_with_layouts;
use super::plan::PreparedPublicReadExecution;
use super::rowset_query::try_compile_read_time_projection_read;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompilerOwnedPublicReadExecutionSelection {
    pub(crate) execution: PreparedPublicReadExecution,
    pub(crate) pushdown_decision: Option<PushdownDecision>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SpecializedPublicReadArtifactSelection {
    DirectStateHistory,
    DirectEntityHistory,
    DirectDirectoryHistory,
    DirectFileHistory,
    Prepared(CompilerOwnedPublicReadExecutionSelection),
    Declined,
}

pub(crate) async fn select_specialized_public_read_artifact(
    backend: &dyn LixBackend,
    structured_read: &StructuredPublicRead,
    direct_execution: bool,
    dependency_spec: Option<&DependencySpec>,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
    current_version_heads: &BTreeMap<String, String>,
    stage_timings: &mut ExplainTimingCollector,
) -> Result<SpecializedPublicReadArtifactSelection, LixError> {
    if direct_execution {
        return Ok(
            match (
                structured_read.surface_binding.descriptor.surface_family,
                structured_read
                    .surface_binding
                    .descriptor
                    .public_name
                    .as_str(),
            ) {
                (SurfaceFamily::State, "lix_state_history") => {
                    SpecializedPublicReadArtifactSelection::DirectStateHistory
                }
                (SurfaceFamily::Entity, _) => {
                    SpecializedPublicReadArtifactSelection::DirectEntityHistory
                }
                (SurfaceFamily::Filesystem, "lix_directory_history") => {
                    SpecializedPublicReadArtifactSelection::DirectDirectoryHistory
                }
                (SurfaceFamily::Filesystem, _) => {
                    SpecializedPublicReadArtifactSelection::DirectFileHistory
                }
                _ => unreachable!(
                    "direct_execution already restricted to direct-only history surfaces"
                ),
            },
        );
    }

    if let Some(artifact) = try_compile_read_time_projection_read(structured_read) {
        return Ok(SpecializedPublicReadArtifactSelection::Prepared(
            CompilerOwnedPublicReadExecutionSelection {
                execution: PreparedPublicReadExecution::ReadTimeProjection(artifact),
                pushdown_decision: None,
            },
        ));
    }

    let capability_started = Instant::now();
    let known_live_layouts = load_known_live_layouts_for_public_read(
        backend,
        structured_read,
        dependency_spec,
        effective_state_request,
    )
    .await?;
    stage_timings.record(
        ExplainStage::CapabilityResolution,
        capability_started.elapsed(),
    );
    let Some(lowered_read) = lower_read_for_execution_with_layouts(
        backend.dialect(),
        structured_read,
        effective_state_request,
        effective_state_plan,
        &known_live_layouts,
        &current_version_heads,
    )?
    else {
        return Ok(SpecializedPublicReadArtifactSelection::Declined);
    };
    let pushdown_decision = Some(lowered_read.pushdown_decision.clone());

    Ok(SpecializedPublicReadArtifactSelection::Prepared(
        CompilerOwnedPublicReadExecutionSelection {
            execution: PreparedPublicReadExecution::LoweredSql(lowered_read),
            pushdown_decision,
        },
    ))
}

async fn load_known_live_layouts_for_dependency_spec(
    backend: &dyn LixBackend,
    dependency_spec: Option<&DependencySpec>,
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut schemas = BTreeMap::new();
    for schema_key in required_schema_keys_from_dependency_spec(dependency_spec) {
        schemas.insert(
            schema_key.clone(),
            provider.load_latest_schema(&schema_key).await?,
        );
    }
    Ok(schemas)
}

async fn load_known_live_layouts_for_public_read(
    backend: &dyn LixBackend,
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
    effective_state_request: Option<&EffectiveStateRequest>,
) -> Result<BTreeMap<String, JsonValue>, LixError> {
    let mut provider = SqlRegisteredSchemaProvider::new(backend);
    let mut schemas = load_known_live_layouts_for_dependency_spec(backend, dependency_spec).await?;
    if let Some(request) = effective_state_request {
        if let Some(schema_key) = structured_read
            .surface_binding
            .implicit_overrides
            .fixed_schema_key
            .as_ref()
        {
            if !schemas.contains_key(schema_key) {
                schemas.insert(
                    schema_key.clone(),
                    provider.load_latest_schema(schema_key).await?,
                );
            }
        }
        for schema_key in &request.schema_set {
            if schemas.contains_key(schema_key) {
                continue;
            }
            schemas.insert(
                schema_key.clone(),
                provider.load_latest_schema(schema_key).await?,
            );
        }
    }
    Ok(schemas)
}

fn required_schema_keys_from_dependency_spec(
    dependency_spec: Option<&DependencySpec>,
) -> BTreeSet<String> {
    dependency_spec
        .map(|spec| spec.schema_keys.iter().cloned().collect())
        .unwrap_or_default()
}
