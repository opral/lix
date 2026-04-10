use std::collections::BTreeMap;
use std::time::Instant;

use crate::catalog::SurfaceFamily;
use crate::contracts::artifacts::EffectiveStateRequest;
use crate::sql::common::pushdown::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql::explain::{ExplainStage, ExplainTimingCollector};
use crate::sql::logical_plan::public_ir::StructuredPublicRead;
use crate::sql::semantic_ir::semantics::effective_state_resolver::EffectiveStatePlan;
use crate::{LixError, SqlDialect};

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

pub(crate) fn select_specialized_public_read_artifact(
    dialect: SqlDialect,
    structured_read: &StructuredPublicRead,
    direct_execution: bool,
    effective_state_request: Option<&EffectiveStateRequest>,
    effective_state_plan: Option<&EffectiveStatePlan>,
    known_live_schema_definitions: &BTreeMap<String, serde_json::Value>,
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
                pushdown_decision: Some(read_time_projection_pushdown_decision(structured_read)),
            },
        ));
    }

    let capability_started = Instant::now();
    stage_timings.record(
        ExplainStage::CapabilityResolution,
        capability_started.elapsed(),
    );
    let Some(lowered_read) = lower_read_for_execution_with_layouts(
        dialect,
        structured_read,
        effective_state_request,
        effective_state_plan,
        known_live_schema_definitions,
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

fn read_time_projection_pushdown_decision(structured_read: &StructuredPublicRead) -> PushdownDecision {
    let residual_predicates = structured_read.query.selection_predicates.clone();
    let reason = match structured_read.surface_binding.descriptor.surface_family {
        SurfaceFamily::Filesystem => {
            "read-time filesystem execution keeps predicates above the derived source"
        }
        SurfaceFamily::Admin => {
            "read-time admin execution keeps predicates above the derived source"
        }
        SurfaceFamily::State => {
            "read-time state execution keeps predicates above the derived source"
        }
        SurfaceFamily::Entity => {
            "read-time entity execution keeps predicates above the derived source"
        }
        SurfaceFamily::Change => {
            "read-time change execution keeps predicates above the derived source"
        }
    };

    PushdownDecision {
        accepted_predicates: Vec::new(),
        rejected_predicates: residual_predicates
            .iter()
            .map(|predicate| RejectedPredicate {
                predicate: predicate.clone(),
                reason: reason.to_string(),
                support: PushdownSupport::Unsupported,
            })
            .collect(),
        residual_predicates,
    }
}
