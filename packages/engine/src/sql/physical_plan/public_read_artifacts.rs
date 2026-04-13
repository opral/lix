use std::collections::BTreeMap;
use std::time::Instant;

use crate::catalog::SurfaceFamily;
use crate::contracts::ReadTimeProjectionPlan;
use crate::sql::common::pushdown::{PushdownDecision, PushdownSupport, RejectedPredicate};
use crate::sql::explain::{ExplainStage, ExplainTimingCollector};
use crate::sql::logical_plan::public_ir::StructuredPublicRead;
use crate::sql::logical_plan::SurfaceReadPlan;
use crate::{LixError, SqlDialect};

use super::lowerer::lower_read_for_execution_with_layouts;
use super::plan::PublicReadPhysicalPlan;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct CompilerOwnedPublicReadExecutionSelection {
    pub(crate) execution: PublicReadPhysicalPlan,
    pub(crate) pushdown_decision: PushdownDecision,
}

pub(crate) fn compile_read_time_projection_execution(
    surface_read_plan: &SurfaceReadPlan,
    projection_read: ReadTimeProjectionPlan,
) -> CompilerOwnedPublicReadExecutionSelection {
    CompilerOwnedPublicReadExecutionSelection {
        execution: PublicReadPhysicalPlan::ReadTimeProjection(projection_read),
        pushdown_decision: read_time_projection_pushdown_decision(
            surface_read_plan.structured_read(),
        ),
    }
}

pub(crate) fn compile_prepared_batch_public_read_execution(
    dialect: SqlDialect,
    surface_read_plan: &SurfaceReadPlan,
    known_live_schema_definitions: &BTreeMap<String, serde_json::Value>,
    current_version_heads: &BTreeMap<String, String>,
    stage_timings: &mut ExplainTimingCollector,
) -> Result<Option<CompilerOwnedPublicReadExecutionSelection>, LixError> {
    let capability_started = Instant::now();
    stage_timings.record(
        ExplainStage::CapabilityResolution,
        capability_started.elapsed(),
    );
    let Some(lowered_batch) = lower_read_for_execution_with_layouts(
        dialect,
        surface_read_plan.structured_read(),
        surface_read_plan.effective_state_request(),
        surface_read_plan.effective_state_plan(),
        known_live_schema_definitions,
        current_version_heads,
    )?
    else {
        return Ok(None);
    };
    let pushdown_decision = lowered_batch.pushdown_decision.clone();

    Ok(Some(CompilerOwnedPublicReadExecutionSelection {
        execution: PublicReadPhysicalPlan::LoweredSql(lowered_batch),
        pushdown_decision,
    }))
}

fn read_time_projection_pushdown_decision(
    structured_read: &StructuredPublicRead,
) -> PushdownDecision {
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
            .collect::<Vec<_>>(),
        residual_predicates,
    }
}
