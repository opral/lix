use std::collections::BTreeSet;

use crate::checkpoint::apply_public_version_last_checkpoint_side_effects;
use crate::contracts::artifacts::{
    CommitPreconditions, DomainChangeBatch, ExpectedHead, PlanEffects, PublicDomainChange,
    WriteLane,
};
use crate::runtime::functions::LixFunctionProvider;
use crate::runtime::{SchemaCache, TransactionBackendAdapter};
use crate::sql::prepare::{
    semantic_plan_effects_from_domain_changes, state_commit_stream_operation, PreparedPublicWrite,
};
use crate::write_runtime::commit::ProposedDomainChange;
use crate::write_runtime::commit::{
    append_tracked_with_pending_public_session, BufferedTrackedAppendArgs, CreateCommitDisposition,
    CreateCommitError, CreateCommitErrorKind, CreateCommitExpectedHead, CreateCommitIdempotencyKey,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitWriteLane,
    PendingPublicCommitSession,
};
use crate::write_runtime::validate_commit_time_write;
use crate::{LixBackendTransaction, LixError, QueryResult};

use super::effects::mirror_public_registered_schema_bootstrap_rows;
use super::planned_write::TrackedTxnUnit;
use super::runtime::{empty_public_write_execution_outcome, SqlExecutionOutcome};

struct PublicCommitInvariantChecker<'a> {
    planned_write: &'a crate::sql::logical_plan::public_ir::PlannedWrite,
    schema_cache: SchemaCache,
}

impl<'a> PublicCommitInvariantChecker<'a> {
    fn new(planned_write: &'a crate::sql::logical_plan::public_ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: SchemaCache::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl CreateCommitInvariantChecker for PublicCommitInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixBackendTransaction,
    ) -> Result<(), CreateCommitError> {
        let backend = TransactionBackendAdapter::new(transaction);
        validate_commit_time_write(&backend, &self.schema_cache, self.planned_write)
            .await
            .map_err(|error| CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: error.description,
            })
    }
}

pub(super) async fn run_public_tracked_append_txn_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    unit: &TrackedTxnUnit,
    mut pending_commit_session: Option<&mut Option<PendingPublicCommitSession>>,
) -> Result<Option<SqlExecutionOutcome>, LixError> {
    if unit
        .execution
        .domain_change_batch
        .as_ref()
        .is_some_and(|batch| batch.changes.is_empty())
        && !unit.has_compiler_only_filesystem_changes()
    {
        return Ok(Some(empty_public_write_execution_outcome()));
    }

    let mut create_commit_functions = unit.runtime_state.provider().clone();
    let canonical_preconditions = canonical_create_commit_preconditions_for_tracked_unit(unit)?;
    if pending_commit_session
        .as_ref()
        .is_some_and(|slot| slot.as_ref().is_some())
        && !unit.has_compiler_only_filesystem_changes()
    {
        crate::write_runtime::ensure_runtime_sequence_initialized_in_transaction(
            transaction,
            &create_commit_functions,
        )
        .await?;
    }

    let mut invariant_checker = PublicCommitInvariantChecker::new(&unit.public_write.planned_write);
    let invariant_checker = if unit.is_merged_transaction_plan() {
        None
    } else {
        Some(&mut invariant_checker as &mut dyn CreateCommitInvariantChecker)
    };
    let append_outcome = append_tracked_with_pending_public_session(
        transaction,
        BufferedTrackedAppendArgs {
            timestamp: None,
            changes: unit
                .execution
                .domain_change_batch
                .as_ref()
                .map(|batch| public_domain_changes_to_proposed(&batch.changes))
                .transpose()?
                .unwrap_or_default(),
            filesystem_state: unit.filesystem_state.clone(),
            preconditions: canonical_preconditions.clone(),
            active_account_ids: unit
                .public_write
                .planned_write
                .command
                .execution_context
                .active_account_ids
                .clone(),
            writer_key: unit.writer_key.clone(),
            should_emit_observe_tick: unit.should_emit_observe_tick(),
        },
        &mut create_commit_functions,
        invariant_checker,
        pending_commit_session.as_deref_mut(),
        !unit.has_compiler_only_filesystem_changes(),
    )
    .await?;

    if append_outcome.merged_into_pending_session
        && create_commit_functions
            .deterministic_sequence_persist_highest_seen()
            .is_some()
    {
        crate::write_runtime::persist_runtime_sequence_in_transaction(
            transaction,
            unit.runtime_state.settings(),
            &create_commit_functions,
        )
        .await?;
    }

    if let Some(applied_output) = append_outcome.applied_output.as_ref() {
        mirror_public_registered_schema_bootstrap_rows(transaction, applied_output)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "public tracked write registered-schema bootstrap mirroring failed: {}",
                    error.description
                ),
            })?;
    }

    let applied_domain_change_batch =
        if matches!(append_outcome.disposition, CreateCommitDisposition::Applied) {
            Some(DomainChangeBatch {
                changes: public_domain_changes_from_proposed(
                    &append_outcome.applied_domain_changes,
                ),
                write_lane: unit
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .map(|batch| batch.write_lane.clone())
                    .unwrap_or_else(|| match &unit.execution.create_preconditions.write_lane {
                        crate::contracts::artifacts::WriteLane::SingleVersion(version_id) => {
                            WriteLane::SingleVersion(version_id.clone())
                        }
                        crate::contracts::artifacts::WriteLane::ActiveVersion => {
                            WriteLane::ActiveVersion
                        }
                        crate::contracts::artifacts::WriteLane::GlobalAdmin => {
                            WriteLane::GlobalAdmin
                        }
                    }),
                writer_key: unit
                    .execution
                    .domain_change_batch
                    .as_ref()
                    .and_then(|batch| batch.writer_key.clone())
                    .or_else(|| {
                        unit.public_write
                            .planned_write
                            .command
                            .execution_context
                            .writer_key
                            .clone()
                    }),
                semantic_effects: Vec::new(),
            })
        } else {
            None
        };
    if let Some(applied_domain_change_batch) = applied_domain_change_batch.as_ref() {
        apply_public_version_last_checkpoint_side_effects(
            transaction,
            &unit.public_write,
            applied_domain_change_batch,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "public tracked write version checkpoint side effects failed: {}",
                error.description
            ),
        })?;
    }

    let plugin_changes_committed =
        matches!(append_outcome.disposition, CreateCommitDisposition::Applied);

    let plan_effects_override = if plugin_changes_committed {
        if unit.has_compiler_only_filesystem_changes() {
            semantic_plan_effects_from_domain_changes(
                &append_outcome.applied_domain_changes,
                state_commit_stream_operation(
                    unit.public_write.planned_write.command.operation_kind,
                ),
                unit.writer_key.as_deref(),
            )?
        } else {
            unit.execution.semantic_effects.clone()
        }
    } else {
        PlanEffects::default()
    };

    Ok(Some(SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        internal_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed,
        canonical_commit_receipt: append_outcome.receipt,
        plan_effects_override: Some(plan_effects_override),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: plugin_changes_committed && unit.should_emit_observe_tick(),
    }))
}

fn canonical_create_commit_preconditions_for_tracked_unit(
    unit: &TrackedTxnUnit,
) -> Result<CreateCommitPreconditions, LixError> {
    canonical_create_commit_preconditions_from_public_write(
        &unit.execution.create_preconditions,
        unit.execution.domain_change_batch.as_ref(),
        &unit.public_write,
    )
}

fn canonical_create_commit_preconditions_from_public_write(
    commit_preconditions: &CommitPreconditions,
    batch: Option<&DomainChangeBatch>,
    public_write: &PreparedPublicWrite,
) -> Result<CreateCommitPreconditions, LixError> {
    let write_lane = match &commit_preconditions.write_lane {
        crate::contracts::artifacts::WriteLane::SingleVersion(version_id) => {
            CreateCommitWriteLane::Version(version_id.clone())
        }
        crate::contracts::artifacts::WriteLane::ActiveVersion => {
            let version_id = batch
                .into_iter()
                .flat_map(|batch| batch.changes.first())
                .map(|change| change.version_id.clone())
                .next()
                .or_else(|| {
                    public_write
                        .planned_write
                        .command
                        .execution_context
                        .requested_version_id
                        .clone()
                })
                .ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "public commit execution requires a concrete active version id",
                    )
                })?;
            CreateCommitWriteLane::Version(version_id)
        }
        crate::contracts::artifacts::WriteLane::GlobalAdmin => CreateCommitWriteLane::GlobalAdmin,
    };
    let expected_head = match &commit_preconditions.expected_head {
        ExpectedHead::CurrentHead => CreateCommitExpectedHead::CurrentHead,
    };

    Ok(CreateCommitPreconditions {
        write_lane,
        expected_head,
        idempotency_key: match &commit_preconditions.expected_head {
            ExpectedHead::CurrentHead => CreateCommitIdempotencyKey::CurrentHeadFingerprint(
                commit_preconditions.idempotency_key.0.clone(),
            ),
        },
    })
}

fn public_domain_changes_to_proposed(
    changes: &[PublicDomainChange],
) -> Result<Vec<ProposedDomainChange>, LixError> {
    changes
        .iter()
        .map(public_domain_change_to_proposed)
        .collect()
}

fn public_domain_change_to_proposed(
    change: &PublicDomainChange,
) -> Result<ProposedDomainChange, LixError> {
    Ok(ProposedDomainChange {
        entity_id: crate::EntityId::new(change.entity_id.clone())?,
        schema_key: crate::CanonicalSchemaKey::new(change.schema_key.clone())?,
        schema_version: change
            .schema_version
            .clone()
            .map(crate::CanonicalSchemaVersion::new)
            .transpose()?,
        file_id: change.file_id.clone().map(crate::FileId::new).transpose()?,
        plugin_key: change
            .plugin_key
            .clone()
            .map(crate::CanonicalPluginKey::new)
            .transpose()?,
        snapshot_content: change.snapshot_content.clone(),
        metadata: change.metadata.clone(),
        version_id: crate::VersionId::new(change.version_id.clone())?,
        writer_key: change.writer_key.clone(),
    })
}

fn public_domain_changes_from_proposed(
    changes: &[ProposedDomainChange],
) -> Vec<PublicDomainChange> {
    changes
        .iter()
        .map(|change| PublicDomainChange {
            entity_id: change.entity_id.to_string(),
            schema_key: change.schema_key.to_string(),
            schema_version: change.schema_version.as_ref().map(ToString::to_string),
            file_id: change.file_id.as_ref().map(ToString::to_string),
            plugin_key: change.plugin_key.as_ref().map(ToString::to_string),
            snapshot_content: change.snapshot_content.clone(),
            metadata: change.metadata.clone(),
            version_id: change.version_id.to_string(),
            writer_key: change.writer_key.clone(),
        })
        .collect()
}
