use std::collections::{BTreeMap, BTreeSet};

use crate::live_state::effective::{resolve_effective_rows, EffectiveRowsRequest};
use crate::live_state::shared::query::entity_id_in_constraint;
use crate::LixError;

use super::contracts::{CommitOutcome, TransactionDelta, TransactionJournal};
use super::coordinator::TransactionCoordinator;
use super::read_context::ReadContext;
use super::write_plan::WritePlan;
use super::write_runner::apply_write_plan;

pub(crate) struct LiveStateWriteState<'a> {
    read_context: ReadContext<'a>,
    journal: TransactionJournal,
    outcome: CommitOutcome,
    executed: bool,
}

impl<'a> LiveStateWriteState<'a> {
    pub(crate) fn new(read_context: ReadContext<'a>) -> Self {
        Self {
            read_context,
            journal: TransactionJournal::default(),
            outcome: CommitOutcome::default(),
            executed: false,
        }
    }

    pub(crate) fn journal(&self) -> &TransactionJournal {
        &self.journal
    }

    pub(crate) fn is_executed(&self) -> bool {
        self.executed
    }

    pub(crate) fn outcome(&self) -> CommitOutcome {
        self.outcome.clone()
    }

    pub(crate) fn stage(&mut self, delta: TransactionDelta) -> Result<(), LixError> {
        if self.executed {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "cannot stage new transaction work after execute()",
            ));
        }
        self.journal.stage(delta)
    }

    pub(crate) async fn execute(
        &mut self,
        coordinator: &mut TransactionCoordinator<'_>,
    ) -> Result<(), LixError> {
        if self.executed {
            return Ok(());
        }

        coordinator.register_staged_schemas().await?;
        let plan = prepare_materialization_plan(&self.read_context, &self.journal).await?;
        let transaction = coordinator.backend_transaction_mut()?;
        self.outcome
            .merge(apply_write_plan(transaction, &plan).await?);
        self.executed = true;
        Ok(())
    }
}

pub(crate) async fn prepare_materialization_plan(
    read_context: &ReadContext<'_>,
    journal: &TransactionJournal,
) -> Result<WritePlan, LixError> {
    let Some(pending) = journal.write_journal().pending_write_overlay() else {
        return Ok(WritePlan::default());
    };
    let Some(plan) = journal.write_journal().materialization_plan() else {
        return Ok(WritePlan::default());
    };

    let pending_context = read_context.with_pending(pending);
    let effective_context = pending_context.effective_state_context();

    for ((schema_key, version_id), entity_ids) in grouped_entities(&journal.aggregated_delta()) {
        let constraints = if entity_ids.is_empty() {
            Vec::new()
        } else {
            vec![entity_id_in_constraint(
                entity_ids.into_iter().collect::<Vec<_>>(),
            )]
        };
        let _ = resolve_effective_rows(
            &EffectiveRowsRequest {
                schema_key,
                version_id,
                constraints,
                required_columns: Vec::new(),
                include_global: true,
                include_untracked: true,
                include_tombstones: true,
            },
            &effective_context,
        )
        .await?;
    }

    Ok(plan.clone())
}

fn grouped_entities(delta: &TransactionDelta) -> BTreeMap<(String, String), BTreeSet<String>> {
    let mut grouped = BTreeMap::<(String, String), BTreeSet<String>>::new();
    for row in &delta.tracked_writes {
        grouped
            .entry((row.schema_key.clone(), row.version_id.clone()))
            .or_default()
            .insert(row.entity_id.clone());
    }
    for row in &delta.untracked_writes {
        grouped
            .entry((row.schema_key.clone(), row.version_id.clone()))
            .or_default()
            .insert(row.entity_id.clone());
    }
    grouped
}
