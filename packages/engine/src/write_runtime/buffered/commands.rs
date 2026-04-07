use crate::contracts::artifacts::{PreparedPublicSurfaceRegistryEffect, SessionStateDelta};
use crate::write_runtime::TransactionCommitOutcome;
use crate::QueryResult;

use super::PlannedWriteDelta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferedWriteExecutionRoute {
    Internal,
    PublicReadCommitted,
    Other,
}

#[derive(Clone)]
pub(crate) struct BufferedWriteCommandMetadata {
    pub(crate) route: BufferedWriteExecutionRoute,
    pub(crate) has_materialization_plan: bool,
    pub(crate) planned_write_delta: Option<PlannedWriteDelta>,
    pub(crate) registry_mutated_during_planning: bool,
}

#[derive(Clone)]
pub(crate) struct BufferedWriteSessionEffects {
    pub(crate) session_delta: SessionStateDelta,
    pub(crate) public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect,
}

impl Default for BufferedWriteSessionEffects {
    fn default() -> Self {
        Self {
            session_delta: SessionStateDelta::default(),
            public_surface_registry_effect: PreparedPublicSurfaceRegistryEffect::None,
        }
    }
}

pub(crate) struct BufferedWriteExecutionResult {
    pub(crate) public_result: QueryResult,
    pub(crate) clear_pending_public_commit_session: bool,
    pub(crate) session_effects: BufferedWriteSessionEffects,
    pub(crate) commit_outcome: TransactionCommitOutcome,
}
