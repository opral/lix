use crate::session::SessionStateDelta;
use crate::transaction::{PreparedPublicSurfaceRegistryEffect, TransactionCommitOutcome};
use crate::QueryResult;

use super::TransactionWriteDelta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferedWriteFlushClass {
    DirectWrite,
    CommittedRead,
    NoPreFlush,
}

#[derive(Clone)]
pub(crate) struct BufferedWriteCommandMetadata {
    pub(crate) flush_class: BufferedWriteFlushClass,
    pub(crate) has_materialization_plan: bool,
    pub(crate) transaction_write_delta: Option<TransactionWriteDelta>,
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
    pub(crate) clear_pending_commit_state: bool,
    pub(crate) session_effects: BufferedWriteSessionEffects,
    pub(crate) commit_outcome: TransactionCommitOutcome,
}
