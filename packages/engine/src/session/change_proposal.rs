//! Repository-global, immutable-snapshot change proposals.
//!
//! A proposal is tracked on Lix's global control branch, never on either
//! participating branch. Its record pins the exact source, target, and
//! merge-base commits seen by the reviewer. A branch advance therefore makes
//! acceptance stale instead of changing the reviewed content under a human's
//! feet.

use bytes::Bytes;
use serde_json::json;

use crate::LixError;
use crate::branch::{
    BranchHeadControlObservation, BranchLifecycle, BranchOperation, BranchReferenceRole,
    branch_head_control_precondition,
};
use crate::changelog::CommitId;
use crate::proposal::{
    CHANGE_PROPOSAL_SCHEMA_KEY, ChangeProposalMutation, ChangeProposalRecord,
    ChangeProposalStateRecord, change_proposal_state_label,
};
use crate::storage_adapter::Storage;
use crate::transaction::types::{
    TransactionJson, TransactionWrite, TransactionWriteMode, TransactionWriteRow,
};
use crate::{GLOBAL_BRANCH_ID, entity_pk::EntityPk};

use super::context::SessionContext;
use super::merge::{
    BranchDiff, MergeBranchReceipt, analyze_pinned_branch_pair, merge_analysis_in_transaction,
    pinned_branch_diff,
};

/// Inputs for creating an immutable change proposal from `source_branch_id`
/// into `target_branch_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateChangeProposalOptions {
    /// Optional caller-provided proposal id. If omitted, Lix generates a UUID
    /// v7 through the transaction's configured function provider.
    pub id: Option<String>,
    pub source_branch_id: String,
    pub target_branch_id: String,
}

/// Durable lifecycle state. Resolved proposals remain queryable for audit;
/// rejecting never deletes the source branch or its work.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChangeProposalState {
    Open,
    Accepted,
    Rejected,
}

/// Public projection of a repository-global proposal record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeProposal {
    pub id: String,
    pub source_branch_id: String,
    pub target_branch_id: String,
    pub base_commit_id: String,
    pub source_head_commit_id: String,
    pub target_head_commit_id: String,
    pub state: ChangeProposalState,
    pub accepted_target_head_commit_id: Option<String>,
}

/// The frozen review diff plus whether its pinned heads are still current.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeProposalDiff {
    pub proposal: ChangeProposal,
    /// The source branch has not advanced beyond the submitted snapshot.
    pub source_head_is_current: bool,
    /// The target branch has not advanced beyond the reviewed snapshot.
    pub target_head_is_current: bool,
    /// `true` only when this is an open, current, conflict-free snapshot.
    /// Acceptance still performs final compare-and-swap guards at commit.
    pub is_accept_ready: bool,
    /// The authored source contribution from `merge_base` to source, with
    /// merge/conflict information against the pinned target.
    pub review: BranchDiff,
}

/// Result of accepting a proposal. `merge` records the exact target branch
/// publication, while `proposal` exposes the terminal durable lifecycle row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AcceptChangeProposalReceipt {
    pub proposal: ChangeProposal,
    pub merge: MergeBranchReceipt,
}

impl<StorageImpl> SessionContext<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Creates a global proposal after pinning source and target heads in the
    /// same atomic write that publishes the record. A concurrent branch move
    /// therefore cannot create a supposedly coherent, immediately stale
    /// proposal.
    pub async fn create_change_proposal(
        &self,
        options: CreateChangeProposalOptions,
    ) -> Result<ChangeProposal, LixError> {
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                validate_proposal_branch_pair(
                    &options.source_branch_id,
                    &options.target_branch_id,
                    BranchOperation::CreateChangeProposal,
                )?;
                if let Some(existing_id) = transaction
                    .load_open_change_proposal_for_branch_pair(
                        &options.source_branch_id,
                        &options.target_branch_id,
                    )
                    .await?
                {
                    return Err(change_proposal_already_open(
                        &options.source_branch_id,
                        &options.target_branch_id,
                        &existing_id,
                    ));
                }

                let controls = transaction
                    .observe_branch_head_controls(&[
                        options.source_branch_id.clone(),
                        options.target_branch_id.clone(),
                    ])
                    .await?;
                let source = require_existing_branch_control(
                    controls.first(),
                    &options.source_branch_id,
                    BranchOperation::CreateChangeProposal,
                    BranchReferenceRole::Source,
                )?;
                let target = require_existing_branch_control(
                    controls.get(1),
                    &options.target_branch_id,
                    BranchOperation::CreateChangeProposal,
                    BranchReferenceRole::Target,
                )?;
                let base_commit_id = {
                    let mut graph = transaction.commit_graph_reader().await;
                    graph
                        .merge_base(&target.commit_id, &source.commit_id)
                        .await?
                        .commit_id
                };
                let record = ChangeProposalRecord {
                    id: options
                        .id
                        .unwrap_or_else(|| transaction.functions().call_uuid_v7().to_string()),
                    source_branch_id: options.source_branch_id,
                    target_branch_id: options.target_branch_id,
                    base_commit_id,
                    source_head_commit_id: source.commit_id,
                    target_head_commit_id: target.commit_id,
                    state: ChangeProposalStateRecord::Open,
                    accepted_target_head_commit_id: None,
                };
                transaction.stage_storage_precondition(branch_head_control_precondition(
                    &record.source_branch_id,
                    Some(source.raw_token),
                )?);
                transaction.stage_storage_precondition(branch_head_control_precondition(
                    &record.target_branch_id,
                    Some(target.raw_token),
                )?);
                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Insert,
                        rows: vec![change_proposal_stage_row(&record)],
                    })
                    .await?;
                transaction.stage_change_proposal_mutation(ChangeProposalMutation::Create {
                    proposal_id: record.id.clone(),
                    source_branch_id: record.source_branch_id.clone(),
                    target_branch_id: record.target_branch_id.clone(),
                });
                Ok(change_proposal_from_record(&record))
            })
        })
        .await
    }

    /// Reads one global proposal. `None` means its id was never published.
    pub async fn get_change_proposal(
        &self,
        proposal_id: &str,
    ) -> Result<Option<ChangeProposal>, LixError> {
        let proposal_id = proposal_id.to_string();
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                Ok(transaction
                    .load_change_proposal(&proposal_id)
                    .await?
                    .map(|record| change_proposal_from_record(&record)))
            })
        })
        .await
    }

    /// Lists every global proposal in stable id order.
    pub async fn list_change_proposals(&self) -> Result<Vec<ChangeProposal>, LixError> {
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                Ok(transaction
                    .scan_change_proposals()
                    .await?
                    .iter()
                    .map(change_proposal_from_record)
                    .collect())
            })
        })
        .await
    }

    /// Returns the frozen review diff for one proposal. This never silently
    /// retargets to branch heads that may have moved since review began.
    pub async fn change_proposal_diff(
        &self,
        proposal_id: &str,
    ) -> Result<ChangeProposalDiff, LixError> {
        let proposal_id = proposal_id.to_string();
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let record = require_change_proposal(
                    transaction.load_change_proposal(&proposal_id).await?,
                    &proposal_id,
                )?;
                let controls = transaction
                    .observe_branch_head_controls(&[
                        record.source_branch_id.clone(),
                        record.target_branch_id.clone(),
                    ])
                    .await?;
                let source_head_is_current = controls.first().is_some_and(|observation| {
                    observation.control.is_some_and(|control| {
                        control.head_commit_id == record.source_head_commit_id
                    })
                });
                let target_head_is_current = controls.get(1).is_some_and(|observation| {
                    observation.control.is_some_and(|control| {
                        control.head_commit_id == record.target_head_commit_id
                    })
                });
                let review = pinned_branch_diff(
                    transaction,
                    &record.source_branch_id,
                    &record.target_branch_id,
                    record.base_commit_id,
                    record.source_head_commit_id,
                    record.target_head_commit_id,
                )
                .await?;
                let is_accept_ready = record.state == ChangeProposalStateRecord::Open
                    && source_head_is_current
                    && target_head_is_current
                    && review.conflicts.is_empty();
                Ok(ChangeProposalDiff {
                    proposal: change_proposal_from_record(&record),
                    source_head_is_current,
                    target_head_is_current,
                    is_accept_ready,
                    review,
                })
            })
        })
        .await
    }

    /// Accepts the exact snapshot of an open proposal.
    ///
    /// Engine sessions are branch-scoped, so this method must run on a session
    /// pinned to the proposal's target branch. The Rust SDK offers a global
    /// wrapper that opens such a child session automatically without changing
    /// the caller's workspace selection.
    pub async fn accept_change_proposal(
        &self,
        proposal_id: &str,
    ) -> Result<AcceptChangeProposalReceipt, LixError> {
        let proposal_id = proposal_id.to_string();
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let mut record = require_change_proposal(
                    transaction.load_change_proposal(&proposal_id).await?,
                    &proposal_id,
                )?;
                ensure_open(&record)?;
                if transaction.active_branch_id() != record.target_branch_id {
                    return Err(change_proposal_target_mismatch(
                        &record.id,
                        transaction.active_branch_id(),
                        &record.target_branch_id,
                    ));
                }

                let controls = transaction
                    .observe_branch_head_controls(&[
                        record.source_branch_id.clone(),
                        record.target_branch_id.clone(),
                    ])
                    .await?;
                let source_raw_token = require_current_proposal_control(
                    controls.first(),
                    &record,
                    &record.source_branch_id,
                    record.source_head_commit_id,
                    BranchReferenceRole::Source,
                )?;
                let target_raw_token = require_current_proposal_control(
                    controls.get(1),
                    &record,
                    &record.target_branch_id,
                    record.target_head_commit_id,
                    BranchReferenceRole::Target,
                )?;
                transaction.stage_storage_precondition(branch_head_control_precondition(
                    &record.source_branch_id,
                    Some(source_raw_token),
                )?);
                transaction.stage_storage_precondition(branch_head_control_precondition(
                    &record.target_branch_id,
                    Some(target_raw_token),
                )?);

                let analysis = analyze_pinned_branch_pair(
                    transaction,
                    record.base_commit_id,
                    record.source_head_commit_id,
                    record.target_head_commit_id,
                )
                .await?;
                let merge =
                    merge_analysis_in_transaction(transaction, &record.source_branch_id, analysis)
                        .await?;

                record.state = ChangeProposalStateRecord::Accepted;
                record.accepted_target_head_commit_id = Some(CommitId::parse_lix(
                    &merge.target_head_after_commit_id,
                    "accepted change proposal target head",
                )?);
                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: vec![change_proposal_stage_row(&record)],
                    })
                    .await?;
                transaction.stage_change_proposal_mutation(ChangeProposalMutation::Resolve {
                    proposal_id: record.id.clone(),
                    source_branch_id: record.source_branch_id.clone(),
                    target_branch_id: record.target_branch_id.clone(),
                });
                Ok(AcceptChangeProposalReceipt {
                    proposal: change_proposal_from_record(&record),
                    merge,
                })
            })
        })
        .await
    }

    /// Marks an open proposal rejected while retaining the proposal record,
    /// pinned review metadata, and source branch unchanged.
    pub async fn reject_change_proposal(
        &self,
        proposal_id: &str,
    ) -> Result<ChangeProposal, LixError> {
        let proposal_id = proposal_id.to_string();
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                let mut record = require_change_proposal(
                    transaction.load_change_proposal(&proposal_id).await?,
                    &proposal_id,
                )?;
                ensure_open(&record)?;
                record.state = ChangeProposalStateRecord::Rejected;
                record.accepted_target_head_commit_id = None;
                transaction
                    .stage_write(TransactionWrite::Rows {
                        mode: TransactionWriteMode::Replace,
                        rows: vec![change_proposal_stage_row(&record)],
                    })
                    .await?;
                transaction.stage_change_proposal_mutation(ChangeProposalMutation::Resolve {
                    proposal_id: record.id.clone(),
                    source_branch_id: record.source_branch_id.clone(),
                    target_branch_id: record.target_branch_id.clone(),
                });
                Ok(change_proposal_from_record(&record))
            })
        })
        .await
    }
}

#[derive(Debug, Clone)]
struct ExistingBranchControl {
    commit_id: CommitId,
    raw_token: Bytes,
}

fn validate_proposal_branch_pair(
    source_branch_id: &str,
    target_branch_id: &str,
    operation: BranchOperation,
) -> Result<(), LixError> {
    BranchLifecycle::require_non_empty_id(
        source_branch_id,
        operation,
        BranchReferenceRole::Source,
    )?;
    BranchLifecycle::require_non_empty_id(
        target_branch_id,
        operation,
        BranchReferenceRole::Target,
    )?;
    if source_branch_id == target_branch_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_MERGE,
            "a change proposal must target a different branch",
        )
        .with_details(json!({
            "operation": operation.label(),
            "source_branch_id": source_branch_id,
            "target_branch_id": target_branch_id,
        })));
    }
    if source_branch_id == GLOBAL_BRANCH_ID || target_branch_id == GLOBAL_BRANCH_ID {
        return Err(LixError::new(
            LixError::CODE_INVALID_MERGE,
            "a change proposal cannot source from or target Lix's global control branch",
        )
        .with_details(json!({
            "operation": operation.label(),
            "source_branch_id": source_branch_id,
            "target_branch_id": target_branch_id,
            "global_branch_id": GLOBAL_BRANCH_ID,
        })));
    }
    Ok(())
}

fn require_existing_branch_control(
    observation: Option<&BranchHeadControlObservation>,
    branch_id: &str,
    operation: BranchOperation,
    role: BranchReferenceRole,
) -> Result<ExistingBranchControl, LixError> {
    let observation = observation.ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "branch-head control point batch omitted a requested branch",
        )
    })?;
    let control = observation
        .control
        .ok_or_else(|| LixError::branch_not_found(branch_id, operation.label(), role.label()))?;
    let raw_token = observation.raw_token.clone().ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "existing branch-head control omitted its compare-and-swap token",
        )
    })?;
    Ok(ExistingBranchControl {
        commit_id: control.head_commit_id,
        raw_token,
    })
}

fn require_current_proposal_control(
    observation: Option<&BranchHeadControlObservation>,
    record: &ChangeProposalRecord,
    branch_id: &str,
    expected_commit_id: CommitId,
    role: BranchReferenceRole,
) -> Result<Bytes, LixError> {
    let existing = require_existing_branch_control(
        observation,
        branch_id,
        BranchOperation::AcceptChangeProposal,
        role,
    )?;
    if existing.commit_id != expected_commit_id {
        return Err(change_proposal_stale(
            record,
            branch_id,
            expected_commit_id,
            Some(existing.commit_id),
        ));
    }
    Ok(existing.raw_token)
}

fn require_change_proposal(
    record: Option<ChangeProposalRecord>,
    proposal_id: &str,
) -> Result<ChangeProposalRecord, LixError> {
    record.ok_or_else(|| {
        LixError::new(
            LixError::CODE_CHANGE_PROPOSAL_NOT_FOUND,
            format!("change proposal '{proposal_id}' was not found"),
        )
        .with_details(json!({ "proposal_id": proposal_id }))
    })
}

fn ensure_open(record: &ChangeProposalRecord) -> Result<(), LixError> {
    if record.state == ChangeProposalStateRecord::Open {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_CHANGE_PROPOSAL_NOT_OPEN,
        format!("change proposal '{}' is no longer open", record.id),
    )
    .with_details(json!({
        "proposal_id": record.id,
        "state": change_proposal_state_label(record.state),
    })))
}

fn change_proposal_already_open(
    source_branch_id: &str,
    target_branch_id: &str,
    proposal_id: &str,
) -> LixError {
    LixError::new(
        LixError::CODE_CHANGE_PROPOSAL_ALREADY_OPEN,
        format!(
            "an open change proposal already exists from branch '{source_branch_id}' to '{target_branch_id}'"
        ),
    )
    .with_details(json!({
        "source_branch_id": source_branch_id,
        "target_branch_id": target_branch_id,
        "proposal_id": proposal_id,
    }))
}

fn change_proposal_stale(
    record: &ChangeProposalRecord,
    branch_id: &str,
    expected_head_commit_id: CommitId,
    actual_head_commit_id: Option<CommitId>,
) -> LixError {
    LixError::new(
        LixError::CODE_CHANGE_PROPOSAL_STALE,
        format!(
            "change proposal '{}' is stale because branch '{branch_id}' moved after review began",
            record.id
        ),
    )
    .with_hint(
        "Create a replacement proposal from the current branch heads, then review that snapshot.",
    )
    .with_details(json!({
        "proposal_id": record.id,
        "branch_id": branch_id,
        "expected_head_commit_id": expected_head_commit_id.to_string(),
        "actual_head_commit_id": actual_head_commit_id.map(|commit_id| commit_id.to_string()),
    }))
}

fn change_proposal_target_mismatch(
    proposal_id: &str,
    active_branch_id: &str,
    target_branch_id: &str,
) -> LixError {
    LixError::new(
        LixError::CODE_CHANGE_PROPOSAL_TARGET_MISMATCH,
        format!(
            "change proposal '{proposal_id}' targets branch '{target_branch_id}', but this session is on '{active_branch_id}'"
        ),
    )
    .with_hint("Open a session pinned to the proposal target branch before accepting it.")
    .with_details(json!({
        "proposal_id": proposal_id,
        "active_branch_id": active_branch_id,
        "target_branch_id": target_branch_id,
    }))
}

fn change_proposal_from_record(record: &ChangeProposalRecord) -> ChangeProposal {
    ChangeProposal {
        id: record.id.clone(),
        source_branch_id: record.source_branch_id.clone(),
        target_branch_id: record.target_branch_id.clone(),
        base_commit_id: record.base_commit_id.to_string(),
        source_head_commit_id: record.source_head_commit_id.to_string(),
        target_head_commit_id: record.target_head_commit_id.to_string(),
        state: match record.state {
            ChangeProposalStateRecord::Open => ChangeProposalState::Open,
            ChangeProposalStateRecord::Accepted => ChangeProposalState::Accepted,
            ChangeProposalStateRecord::Rejected => ChangeProposalState::Rejected,
        },
        accepted_target_head_commit_id: record
            .accepted_target_head_commit_id
            .map(|commit_id| commit_id.to_string()),
    }
}

/// The SQL-visible proposal entity is a normal tracked, global row. Its
/// private controls supply lifecycle/pair CAS guards, while this row remains
/// the sole durable source of proposal data.
fn change_proposal_stage_row(record: &ChangeProposalRecord) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_pk: Some(EntityPk::single(&record.id)),
        schema_key: CHANGE_PROPOSAL_SCHEMA_KEY.to_string(),
        file_id: None,
        snapshot: Some(TransactionJson::from_value_unchecked(
            record.snapshot_json(),
        )),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: true,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: GLOBAL_BRANCH_ID.to_string(),
    }
}
