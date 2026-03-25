use async_trait::async_trait;

use crate::engine::{DeferredTransactionSideEffects, Engine};
use crate::{LixBackendTransaction, LixError, QueryResult};

use super::contracts::TransactionCommitOutcome;
use super::write_plan::PlannedWriteDelta;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum BufferedWriteExecutionRoute {
    Internal,
    PublicReadMaterializedState,
    Other,
}

#[derive(Clone)]
pub(crate) struct BufferedWriteCommandMetadata {
    pub(crate) route: BufferedWriteExecutionRoute,
    pub(crate) has_materialization_plan: bool,
    pub(crate) planned_write_delta: Option<PlannedWriteDelta>,
    pub(crate) registry_mutated_during_planning: bool,
}

pub(crate) struct BufferedWriteExecutionResult {
    pub(crate) public_result: QueryResult,
    pub(crate) clear_pending_public_commit_session: bool,
    pub(crate) commit_outcome: TransactionCommitOutcome,
}

#[async_trait(?Send)]
pub(crate) trait BufferedWriteScope<A>
where
    A: BufferedWriteAdapter + ?Sized,
{
    fn backend_transaction_mut(&mut self) -> Result<&mut dyn LixBackendTransaction, LixError>;

    fn buffered_write_journal_is_empty(&self) -> bool;

    fn buffered_write_pending_transaction_view(
        &self,
    ) -> Result<Option<A::PendingTransactionView>, LixError>;

    fn can_stage_planned_write_delta(&self, delta: &PlannedWriteDelta) -> Result<bool, LixError>;

    fn stage_planned_write_delta(&mut self, delta: PlannedWriteDelta) -> Result<(), LixError>;

    fn clear_pending_public_commit_session(&mut self);

    fn take_pending_public_commit_session(&mut self) -> Option<A::PendingPublicCommitSession>;

    fn restore_pending_public_commit_session(
        &mut self,
        session: Option<A::PendingPublicCommitSession>,
    );

    fn buffered_write_commit_outcome_mut(&mut self) -> &mut TransactionCommitOutcome;

    async fn flush_buffered_write_journal(
        &mut self,
        engine: &Engine,
        context: &mut A::Context,
    ) -> Result<(), LixError>;
}

#[async_trait(?Send)]
pub(crate) trait BufferedWriteAdapter {
    type Input;
    type Command;
    type Context;
    type PendingTransactionView;
    type PendingPublicCommitSession;

    async fn compile_command(
        &self,
        engine: &Engine,
        transaction: &mut dyn LixBackendTransaction,
        pending_transaction_view: Option<&Self::PendingTransactionView>,
        input: &Self::Input,
        allow_internal_tables: bool,
        context: &Self::Context,
        skip_side_effect_collection: bool,
    ) -> Result<Self::Command, LixError>;

    fn command_metadata(
        &self,
        command: &Self::Command,
    ) -> Result<BufferedWriteCommandMetadata, LixError>;

    fn apply_planning_effects(
        &self,
        command: &Self::Command,
        context: &mut Self::Context,
    ) -> Result<(), LixError>;

    async fn refresh_public_surface_registry_from_pending_transaction_view(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        pending_transaction_view: Option<&Self::PendingTransactionView>,
        context: &mut Self::Context,
    ) -> Result<(), LixError>;

    async fn execute_command(
        &self,
        engine: &Engine,
        transaction: &mut dyn LixBackendTransaction,
        pending_transaction_view: Option<&Self::PendingTransactionView>,
        pending_public_commit_session: &mut Option<Self::PendingPublicCommitSession>,
        command: &Self::Command,
        context: &mut Self::Context,
        deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
        skip_side_effect_collection: bool,
    ) -> Result<BufferedWriteExecutionResult, LixError>;
}
