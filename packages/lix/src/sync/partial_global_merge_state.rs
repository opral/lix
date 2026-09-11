//! Durable runtime descriptor-only GLOBAL reconciliation.
//! Native body progress and terminal outcomes never replace the captured upload.
use crate::sync::{NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest};
use crate::sync::{NativeGlobalRestartReceipt, NativeGlobalRestartRequest};
use crate::{GLOBAL_BRANCH_ID, LixError};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PartialGlobalBodyFrontier {
    pub accepted: String,
    pub prepared: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(super) struct PartialGlobalMergeState {
    pub version: u32,
    pub epoch_id: String,
    pub original_upload: super::partial_push_state::PreparedPartialUpload,
    pub upload_settled: bool,
    pub request: NativeGlobalMigrationRequest,
    pub frontiers: BTreeMap<String, PartialGlobalBodyFrontier>,
    pub acknowledged_roots: BTreeSet<String>,
    pub restart_intent: Option<NativeGlobalRestartRequest>,
    pub restart_receipt: Option<NativeGlobalRestartReceipt>,
    pub previous_abort: Option<NativeGlobalRestartReceipt>,
    pub receipt: Option<NativeGlobalMigrationReceipt>,
    pub cleanup_complete: bool,
}
fn invalid() -> LixError {
    LixError::new(
        "LIX_PARTIAL_GLOBAL_MERGE_STATE_INVALID",
        "global merge transition disagrees with captured upload or exact prepared request",
    )
}
impl PartialGlobalMergeState {
    pub(super) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        if self.version != 1
            || crate::storage_codec::id_string::uuid_bytes_from_canonical(&self.epoch_id).is_none()
            || self.frontiers.len() != self.request.new_branches.len() + 1
        {
            return Err(invalid());
        }
        let upload = &self.original_upload;
        if crate::storage_codec::id_string::uuid_bytes_from_canonical(&upload.attempt_id).is_none()
            || upload.created_refs.len() > 32
            || upload.expected.head != self.request.base_commit_id
            || upload.target.head != self.request.captured_local_head_commit_id
            || upload.expected.checkpoint != self.request.checkpoint_commit_id
            || upload.target.checkpoint != self.request.checkpoint_commit_id
            || upload.created_refs.len() != self.request.new_branches.len()
            || upload
                .created_refs
                .iter()
                .zip(&self.request.new_branches)
                .any(|(a, b)| {
                    a.branch_id != b.branch_id
                        || a.head_commit_id != b.head_commit_id
                        || a.checkpoint_commit_id != b.checkpoint_commit_id
                })
            || (self.upload_settled && self.receipt.is_none())
            || (self.cleanup_complete && !self.upload_settled)
        {
            return Err(invalid());
        }
        for (branch, frontier) in &self.frontiers {
            if branch != GLOBAL_BRANCH_ID
                && !self
                    .request
                    .new_branches
                    .iter()
                    .any(|b| &b.branch_id == branch)
            {
                return Err(invalid());
            }
            for key in std::iter::once(&frontier.accepted).chain(frontier.prepared.iter()) {
                crate::changelog::CommitId::parse_lix(key, "global conversion frontier")?;
            }
        }
        if !self.frontiers.contains_key(GLOBAL_BRANCH_ID) {
            return Err(invalid());
        }
        if let Some(previous) = &self.previous_abort {
            let NativeGlobalRestartReceipt::Restarted { intent } = previous else {
                return Err(invalid());
            };
            intent.validate()?;
            if intent.next_attempt_id != self.request.attempt_id
                || intent.request.base_commit_id != self.request.base_commit_id
                || intent.request.captured_local_head_commit_id
                    != self.request.captured_local_head_commit_id
                || intent.request.checkpoint_commit_id != self.request.checkpoint_commit_id
                || intent.request.new_branches != self.request.new_branches
            {
                return Err(invalid());
            }
        }
        if self
            .acknowledged_roots
            .iter()
            .any(|branch| !self.frontiers.contains_key(branch))
        {
            return Err(invalid());
        }
        if let Some(intent) = &self.restart_intent {
            intent.validate()?;
            if intent.request != self.request || self.receipt.is_some() {
                return Err(invalid());
            }
            if let Some(outcome) = &self.restart_receipt {
                outcome.validate_for(intent)?;
            }
        } else if self.restart_receipt.is_some() {
            return Err(invalid());
        }
        if let Some(receipt) = &self.receipt {
            receipt.validate()?;
            if receipt.request != self.request
                || self.acknowledged_roots.len() != self.frontiers.len()
                || self.frontiers.values().any(|f| f.prepared.is_some())
                || self.frontiers[GLOBAL_BRANCH_ID].accepted
                    != self.request.captured_local_head_commit_id
            {
                return Err(invalid());
            }
            for branch in &self.request.new_branches {
                if self.frontiers[&branch.branch_id].accepted != branch.head_commit_id {
                    return Err(invalid());
                }
            }
        } else if self.cleanup_complete {
            return Err(invalid());
        }
        Ok(())
    }
    pub(super) fn prepare_wave(
        &self,
        branch: &str,
        previous: &str,
        target: &str,
    ) -> Result<Self, LixError> {
        self.validate()?;
        if self.upload_settled || self.restart_intent.is_some() || self.receipt.is_some() {
            return Err(invalid());
        }
        crate::changelog::CommitId::parse_lix(target, "global prepared target")?;
        let mut next = self.clone();
        let frontier = next.frontiers.get_mut(branch).ok_or_else(invalid)?;
        if frontier.accepted != previous
            || frontier.prepared.as_ref().is_some_and(|old| old != target)
        {
            return Err(invalid());
        }
        frontier.prepared = Some(target.to_owned());
        next.validate()?;
        Ok(next)
    }
    pub(super) fn acknowledge_wave(
        &self,
        branch: &str,
        previous: &str,
        target: &str,
    ) -> Result<Self, LixError> {
        self.validate()?;
        let mut next = self.clone();
        let frontier = next.frontiers.get_mut(branch).ok_or_else(invalid)?;
        if self.restart_intent.is_some()
            || self.receipt.is_some()
            || frontier.accepted != previous
            || frontier.prepared.as_deref() != Some(target)
        {
            return Err(invalid());
        }
        frontier.accepted = target.to_owned();
        frontier.prepared = None;
        next.acknowledged_roots.insert(branch.to_owned());
        next.validate()?;
        Ok(next)
    }
    pub(super) fn acknowledge_merge(
        &self,
        receipt: NativeGlobalMigrationReceipt,
    ) -> Result<Self, LixError> {
        self.validate()?;
        if self.receipt.as_ref().is_some_and(|old| old != &receipt) {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.restart_intent = None;
        next.restart_receipt = None;
        next.receipt = Some(receipt);
        next.validate()?;
        Ok(next)
    }
    pub(super) fn prepare_restart(&self, next_attempt_id: String) -> Result<Self, LixError> {
        self.validate()?;
        if self.receipt.is_some() {
            return Err(invalid());
        }
        let intent = NativeGlobalRestartRequest {
            request: self.request.clone(),
            next_attempt_id,
        };
        intent.validate()?;
        if self
            .restart_intent
            .as_ref()
            .is_some_and(|old| old != &intent)
        {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.restart_intent = Some(intent);
        next.validate()?;
        Ok(next)
    }
    pub(super) fn acknowledge_restart(
        &self,
        outcome: NativeGlobalRestartReceipt,
    ) -> Result<Self, LixError> {
        let intent = self.restart_intent.as_ref().ok_or_else(invalid)?;
        outcome.validate_for(intent)?;
        if self
            .restart_receipt
            .as_ref()
            .is_some_and(|old| old != &outcome)
        {
            return Err(invalid());
        }
        match &outcome {
            NativeGlobalRestartReceipt::Committed { receipt } => {
                self.acknowledge_merge(receipt.clone())
            }
            NativeGlobalRestartReceipt::Restarted { .. } => {
                let mut next = self.clone();
                next.restart_receipt = Some(outcome);
                next.validate()?;
                Ok(next)
            }
        }
    }
    pub(super) fn capture_successor(
        &self,
        request: NativeGlobalMigrationRequest,
        boundaries: BTreeMap<String, String>,
    ) -> Result<Self, LixError> {
        self.validate()?;
        request.validate()?;
        let Some(NativeGlobalRestartReceipt::Restarted { intent }) = &self.restart_receipt else {
            return Err(invalid());
        };
        if request.attempt_id != intent.next_attempt_id
            || request.base_commit_id != self.request.base_commit_id
            || request.captured_local_head_commit_id != self.request.captured_local_head_commit_id
            || request.checkpoint_commit_id != self.request.checkpoint_commit_id
            || request.new_branches != self.request.new_branches
        {
            return Err(invalid());
        }
        let mut next = self.clone();
        next.previous_abort = self.restart_receipt.clone();
        next.request = request;
        next.restart_intent = None;
        next.restart_receipt = None;
        next.acknowledged_roots.clear();
        next.frontiers = boundaries
            .into_iter()
            .map(|(branch, accepted)| {
                (
                    branch,
                    PartialGlobalBodyFrontier {
                        accepted,
                        prepared: None,
                    },
                )
            })
            .collect();
        next.validate()?;
        Ok(next)
    }
}

use super::partial_state::{
    PARTIAL_REPLICA_STATE_SPACE, PartialReplicaState, load_partial_replica_state,
    partial_replica_state_key,
};
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageWriteSet, ValueSemantics,
};
use bytes::Bytes;
pub(crate) const PARTIAL_GLOBAL_MERGE_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0021),
    "sync.partial_global_merge.v1",
    ValueSemantics::Mutable,
);
const MAX_RECORD_BYTES: usize = 512 * 1024;
fn key() -> StorageKey {
    StorageKey(Bytes::from_static(b"current"))
}
fn record_guard(raw: Option<Bytes>) -> StoragePrecondition {
    match raw {
        Some(expected) => StoragePrecondition::KeyValueEquals {
            space: PARTIAL_GLOBAL_MERGE_SPACE,
            key: key(),
            expected,
        },
        None => StoragePrecondition::KeyAbsent {
            space: PARTIAL_GLOBAL_MERGE_SPACE,
            key: key(),
        },
    }
}
pub(super) async fn load_partial_global_merge_state(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
) -> Result<
    (
        Option<PartialGlobalMergeState>,
        Option<Bytes>,
        Vec<StoragePrecondition>,
    ),
    LixError,
> {
    let (actual, receipt) = load_partial_replica_state(read)
        .await?
        .ok_or_else(invalid)?;
    if &actual != state {
        return Err(invalid());
    }
    let values = PointReadPlan::new(PARTIAL_GLOBAL_MERGE_SPACE, &[key()])
        .materialize(read, Default::default())
        .await?
        .value;
    let raw = match values.into_iter().next().flatten() {
        None => None,
        Some(StorageProjectedValue::FullValue(bytes)) => Some(bytes),
        _ => return Err(invalid()),
    };
    let record = raw
        .as_ref()
        .map(|bytes| {
            if bytes.len() > MAX_RECORD_BYTES {
                return Err(invalid());
            }
            let record: PartialGlobalMergeState =
                serde_json::from_slice(bytes).map_err(|_| invalid())?;
            record.validate()?;
            if record.epoch_id != state.epoch_id() {
                return Err(invalid());
            }
            Ok(record)
        })
        .transpose()?;
    Ok((
        record,
        raw.clone(),
        vec![
            record_guard(raw),
            StoragePrecondition::KeyValueEquals {
                space: PARTIAL_REPLICA_STATE_SPACE,
                key: partial_replica_state_key(),
                expected: receipt,
            },
        ],
    ))
}
#[must_use = "publish every returned guard with the complete outbox write"]
pub(super) fn stage_global_record(
    writes: &mut StorageWriteSet,
    record: &PartialGlobalMergeState,
    previous: Option<Bytes>,
) -> Result<StoragePrecondition, LixError> {
    record.validate()?;
    let bytes = serde_json::to_vec(record).map_err(|_| invalid())?;
    if bytes.len() > MAX_RECORD_BYTES {
        return Err(invalid());
    }
    writes.put(PARTIAL_GLOBAL_MERGE_SPACE, key(), bytes);
    Ok(record_guard(previous))
}

#[must_use = "persist captured GLOBAL attempt with every guard before network I/O"]
pub(super) async fn stage_capture_partial_global_merge(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    state: &PartialReplicaState,
    request: NativeGlobalMigrationRequest,
) -> Result<Vec<StoragePrecondition>, LixError> {
    let (existing, raw, mut guards) = load_partial_global_merge_state(read, state).await?;
    if existing.is_some() {
        return Err(invalid());
    }
    let selected = &state.descriptor().selected_branch.branch_id;
    if selected != GLOBAL_BRANCH_ID {
        guards.extend(super::partial_merge_state::ordinary_upload_merge_guards(read, state).await?);
        let (selected_push, selected_raw, selected_epoch) =
            super::partial_push_state::load_partial_push_state(read, state, selected).await?;
        let selected_observation =
            crate::branch::observe_branch_control_coordinate(read, selected).await?;
        let selected_control = selected_observation.control.ok_or_else(invalid)?;
        if selected_control
            .working_diff_checkpoint_commit_id
            .map(|id| id.to_string())
            .as_ref()
            != Some(&selected_push.confirmed.checkpoint)
        {
            return Err(LixError::new(
                "LIX_PARTIAL_GLOBAL_SELECTED_RECONCILIATION_REQUIRED",
                "selected checkpoint changes require reconciliation before GLOBAL capture; pending data remains retained",
            ));
        }
        for child in &request.new_branches {
            let child_id = crate::changelog::CommitId::parse_lix(
                &child.head_commit_id,
                "created child source",
            )?;
            let header = crate::tracked_state::load_published_commit_state_topology(read, child_id)
                .await?
                .ok_or_else(invalid)?;
            let child_record = super::partial_merge_analysis::record(read, child_id, true).await?;
            if header.global_scope()
                || !super::partial_merge_analysis::bounded_ancestor(
                    read,
                    &child_record,
                    crate::changelog::CommitId::parse_lix(
                        &selected_push.confirmed.head,
                        "confirmed selected source",
                    )?,
                    &mut Default::default(),
                    1024,
                )
                .await?
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED",
                    "new branch source is outside the confirmed selected ancestry; native local roots remain retained",
                ));
            }
        }
        guards.push(selected_epoch);
        guards.push(StoragePrecondition::KeyValueEquals {
            space: super::partial_push_state::PARTIAL_BRANCH_PUSH_SPACE,
            key: StorageKey(Bytes::copy_from_slice(
                &crate::storage_codec::id_string::uuid_bytes_from_canonical(selected)
                    .ok_or_else(invalid)?,
            )),
            expected: selected_raw,
        });
        guards.push(crate::branch::branch_head_control_precondition(
            selected,
            selected_observation.raw_token,
        )?);
    }
    let (push, push_raw, receipt_guard) =
        super::partial_push_state::load_partial_push_state(read, state, GLOBAL_BRANCH_ID).await?;
    let upload = push.prepared.ok_or_else(invalid)?;
    if push.confirmed != upload.expected {
        return Err(invalid());
    }
    let observed = crate::branch::observe_branch_control_coordinate(read, GLOBAL_BRANCH_ID).await?;
    let control = observed.control.ok_or_else(invalid)?;
    if control
        .working_diff_checkpoint_commit_id
        .map(|id| id.to_string())
        .as_ref()
        != Some(&upload.target.checkpoint)
    {
        return Err(invalid());
    }
    let target =
        crate::changelog::CommitId::parse_lix(&upload.target.head, "GLOBAL captured prefix")?;
    let mut cursor = control.head_commit_id;
    let mut seen = BTreeSet::new();
    while cursor != target {
        if seen.len() >= 1024 || !seen.insert(cursor) {
            return Err(invalid());
        }
        let node = super::partial_merge_analysis::record(read, cursor, true).await?;
        if node.is_checkpoint
            || node.parent_commit_ids.len() != 1
            || node.base_commit_id.is_some()
            || node.account_id != state.active_account_id()
        {
            return Err(invalid());
        }
        cursor = node.parent_commit_ids[0];
    }
    let mut frontiers = BTreeMap::from([(
        GLOBAL_BRANCH_ID.into(),
        PartialGlobalBodyFrontier {
            accepted: request.base_commit_id.clone(),
            prepared: None,
        },
    )]);
    for child in &request.new_branches {
        // Created-ref capture has already proved these are confirmed source roots.
        // The zero-body wave still obtains authority retention before any merge.
        frontiers.insert(
            child.branch_id.clone(),
            PartialGlobalBodyFrontier {
                accepted: child.head_commit_id.clone(),
                prepared: None,
            },
        );
    }
    let record = PartialGlobalMergeState {
        version: 1,
        epoch_id: state.epoch_id().into(),
        original_upload: upload,
        upload_settled: false,
        request,
        frontiers,
        acknowledged_roots: BTreeSet::new(),
        restart_intent: None,
        restart_receipt: None,
        previous_abort: None,
        receipt: None,
        cleanup_complete: false,
    };
    record.validate()?;
    super::migration_global_descriptor_proof::prove_local_descriptor_global_source(
        read,
        &record.request,
        state.active_account_id(),
    )
    .await?;
    guards.push(receipt_guard);
    guards.push(StoragePrecondition::KeyValueEquals {
        space: super::partial_push_state::PARTIAL_BRANCH_PUSH_SPACE,
        key: StorageKey(Bytes::copy_from_slice(
            &crate::storage_codec::id_string::uuid_bytes_from_canonical(GLOBAL_BRANCH_ID)
                .ok_or_else(invalid)?,
        )),
        expected: push_raw,
    });
    guards.push(crate::branch::branch_head_control_precondition(
        GLOBAL_BRANCH_ID,
        observed.raw_token,
    )?);
    guards.push(stage_global_record(writes, &record, raw)?);
    Ok(guards)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: u128) -> String {
        uuid::Uuid::from_u128(n).to_string()
    }
    fn journal() -> PartialGlobalMergeState {
        let request = NativeGlobalMigrationRequest {
            attempt_id: id(1),
            base_commit_id: id(2),
            expected_authority_head_commit_id: id(3),
            captured_local_head_commit_id: id(4),
            checkpoint_commit_id: id(5),
            new_branches: vec![crate::sync::NativeNewBranchCoordinate {
                branch_id: id(6),
                head_commit_id: id(7),
                checkpoint_commit_id: id(8),
            }],
        };
        PartialGlobalMergeState {
            version: 1,
            acknowledged_roots: Default::default(),
            restart_intent: None,
            restart_receipt: None,
            previous_abort: None,
            epoch_id: id(9),
            upload_settled: false,
            original_upload: super::super::partial_push_state::PreparedPartialUpload {
                attempt_id: id(20),
                expected: super::super::partial_push_state::PartialPushCoordinate {
                    head: id(2),
                    checkpoint: id(5),
                },
                target: super::super::partial_push_state::PartialPushCoordinate {
                    head: id(4),
                    checkpoint: id(5),
                },
                created_refs: vec![super::super::partial_push_state::CreatedPartialRef {
                    branch_id: id(6),
                    head_commit_id: id(7),
                    checkpoint_commit_id: id(8),
                }],
            },
            request,
            frontiers: BTreeMap::from([
                (
                    GLOBAL_BRANCH_ID.into(),
                    PartialGlobalBodyFrontier {
                        accepted: id(2),
                        prepared: None,
                    },
                ),
                (
                    id(6),
                    PartialGlobalBodyFrontier {
                        accepted: id(8),
                        prepared: None,
                    },
                ),
            ]),
            receipt: None,
            cleanup_complete: false,
        }
    }
    #[test]
    fn lost_wave_response_preserves_identical_prepared_request_after_reload() {
        let original = journal();
        let prepared = original
            .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
            .unwrap();
        let bytes = serde_json::to_vec(&prepared).unwrap();
        let reopened: PartialGlobalMergeState = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(
            reopened
                .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
                .unwrap(),
            prepared
        );
        assert!(
            reopened
                .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(11))
                .is_err()
        );
        assert_eq!(reopened.original_upload, original.original_upload);
        assert!(
            reopened
                .acknowledge_wave(GLOBAL_BRANCH_ID, &id(3), &id(4))
                .is_err()
        );
    }
    #[test]
    fn global_receipt_requires_every_exact_new_head_acknowledged() {
        let j = journal();
        let receipt = NativeGlobalMigrationReceipt {
            request: j.request.clone(),
            merge_commit_id: id(10),
        };
        assert!(j.acknowledge_merge(receipt.clone()).is_err());
        let j = j
            .prepare_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
            .unwrap()
            .acknowledge_wave(GLOBAL_BRANCH_ID, &id(2), &id(4))
            .unwrap();
        assert!(j.acknowledge_merge(receipt.clone()).is_err());
        let j = j
            .prepare_wave(&id(6), &id(8), &id(7))
            .unwrap()
            .acknowledge_wave(&id(6), &id(8), &id(7))
            .unwrap()
            .acknowledge_merge(receipt.clone())
            .unwrap();
        assert_eq!(j.acknowledge_merge(receipt).unwrap(), j);
        let mut changed = j.receipt.clone().unwrap();
        changed.request.new_branches[0].checkpoint_commit_id = id(12);
        assert!(j.acknowledge_merge(changed).is_err());
    }
}

/// A durable exact M receipt makes the captured GLOBAL suffix valid authority
/// dependencies without prematurely changing ordinary GLOBAL confirmation.
pub(super) async fn confirmed_global_merge_bases(
    read: &(impl StorageAdapterRead + ?Sized),
    state: &PartialReplicaState,
) -> Result<Vec<crate::changelog::CommitId>, LixError> {
    let (outbox, _, _) = load_partial_global_merge_state(read, state).await?;
    let Some(outbox) = outbox.filter(|r| r.receipt.is_some()) else {
        return Ok(Vec::new());
    };
    let base = crate::changelog::CommitId::parse_lix(
        &outbox.request.base_commit_id,
        "GLOBAL receipt base",
    )?;
    let mut cursor = crate::changelog::CommitId::parse_lix(
        &outbox.request.captured_local_head_commit_id,
        "GLOBAL captured base",
    )?;
    let mut result = Vec::new();
    let mut seen = BTreeSet::new();
    while cursor != base {
        if result.len() >= 32 || !seen.insert(cursor) {
            return Err(invalid());
        }
        let record = super::partial_merge_analysis::record(read, cursor, true).await?;
        if record.is_checkpoint
            || record.parent_commit_ids.len() != 1
            || record.base_commit_id.is_some()
            || record.account_id != state.active_account_id()
        {
            return Err(invalid());
        }
        result.push(cursor);
        cursor = record.parent_commit_ids[0];
    }
    Ok(result)
}
