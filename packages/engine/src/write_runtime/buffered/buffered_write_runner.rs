use crate::engine::{DeferredTransactionSideEffects, Engine};
use crate::{LixError, QueryResult};

use super::commands::{
    BufferedWriteAdapter, BufferedWriteCommandMetadata, BufferedWriteExecutionRoute,
    BufferedWriteScope,
};

pub(crate) async fn execute_buffered_write_input<A, S>(
    engine: &Engine,
    write_transaction: &mut S,
    adapter: &A,
    input: &A::Input,
    allow_internal_tables: bool,
    context: &mut A::Context,
    deferred_side_effects: Option<&mut DeferredTransactionSideEffects>,
    skip_side_effect_collection: bool,
) -> Result<QueryResult, LixError>
where
    A: BufferedWriteAdapter + ?Sized,
    S: BufferedWriteScope<A> + ?Sized,
{
    loop {
        let pending_transaction_view =
            write_transaction.buffered_write_pending_transaction_view()?;
        let command = {
            let transaction = write_transaction.backend_transaction_mut()?;
            adapter
                .compile_command(
                    engine,
                    transaction,
                    pending_transaction_view.as_ref(),
                    input,
                    allow_internal_tables,
                    context,
                    skip_side_effect_collection,
                )
                .await
        };
        let command = match command {
            Ok(command) => command,
            Err(error) if !write_transaction.buffered_write_journal_is_empty() => {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await?;
                let _ = error;
                continue;
            }
            Err(error) => return Err(error),
        };

        let metadata = adapter.command_metadata(&command)?;
        if let Some(statement_delta) = metadata.planned_write_delta.clone() {
            let continuation_safe =
                write_transaction.can_stage_planned_write_delta(&statement_delta)?;
            if !write_transaction.buffered_write_journal_is_empty() && !continuation_safe {
                write_transaction
                    .flush_buffered_write_journal(engine, context)
                    .await?;
                continue;
            }

            write_transaction.stage_planned_write_delta(statement_delta)?;
            if continuation_safe {
                adapter.apply_planning_effects(&command, context)?;
            }
            if metadata.registry_mutated_during_planning {
                write_transaction
                    .buffered_write_commit_outcome_mut()
                    .refresh_public_surface_registry = true;
            }
            if metadata.registry_mutated_during_planning {
                let pending_transaction_view =
                    write_transaction.buffered_write_pending_transaction_view()?;
                let transaction = write_transaction.backend_transaction_mut()?;
                adapter
                    .refresh_public_surface_registry_from_pending_transaction_view(
                        transaction,
                        pending_transaction_view.as_ref(),
                        context,
                    )
                    .await?;
            }
            return Ok(QueryResult {
                rows: Vec::new(),
                columns: Vec::new(),
            });
        }

        if should_flush_before_command(&metadata, write_transaction) {
            write_transaction
                .flush_buffered_write_journal(engine, context)
                .await?;
            continue;
        }

        let mut pending_public_commit_session =
            write_transaction.take_pending_public_commit_session();
        let execution = {
            let transaction = write_transaction.backend_transaction_mut()?;
            adapter
                .execute_command(
                    engine,
                    transaction,
                    pending_transaction_view.as_ref(),
                    &mut pending_public_commit_session,
                    &command,
                    context,
                    deferred_side_effects,
                    skip_side_effect_collection,
                )
                .await?
        };
        write_transaction.restore_pending_public_commit_session(pending_public_commit_session);

        if execution.clear_pending_public_commit_session {
            write_transaction.clear_pending_public_commit_session();
        }
        write_transaction
            .buffered_write_commit_outcome_mut()
            .merge(execution.commit_outcome);
        return Ok(execution.public_result);
    }
}

fn should_flush_before_command<A, S>(
    metadata: &BufferedWriteCommandMetadata,
    write_transaction: &S,
) -> bool
where
    A: BufferedWriteAdapter + ?Sized,
    S: BufferedWriteScope<A> + ?Sized,
{
    match metadata.route {
        BufferedWriteExecutionRoute::Internal => {
            !write_transaction.buffered_write_journal_is_empty()
                && !metadata.has_materialization_plan
        }
        BufferedWriteExecutionRoute::PublicReadCommitted => {
            !write_transaction.buffered_write_journal_is_empty()
        }
        BufferedWriteExecutionRoute::Other => false,
    }
}
