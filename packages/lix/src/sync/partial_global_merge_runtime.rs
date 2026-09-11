//! Network phase for one durably captured descriptor-only GLOBAL attempt.
//! Capture and transition publication are supplied by the local storage owner.
use super::partial_global_merge_state::PartialGlobalMergeState;
use super::*;
use crate::changelog::CommitId;
use crate::storage_adapter::{Storage, StorageAdapter, StorageAdapterRead};
use std::collections::BTreeSet;
pub(super) trait PartialGlobalMergeOwner {
    fn current(&self) -> &PartialGlobalMergeState;
    fn publish(&mut self, next: PartialGlobalMergeState) -> SyncTransportFuture<'_, ()>;
}
fn unresolved(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_GLOBAL_MERGE_PENDING", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "partial global merge coordinate")
}
async fn prepare_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    journal: &PartialGlobalMergeState,
    branch: &str,
) -> Result<NativeGlobalBodyWaveRequest, LixError> {
    let frontier = journal
        .frontiers
        .get(branch)
        .ok_or_else(|| unresolved("global journal omits branch frontier"))?;
    let final_head = if branch == crate::GLOBAL_BRANCH_ID {
        &journal.request.captured_local_head_commit_id
    } else {
        &journal
            .request
            .new_branches
            .iter()
            .find(|b| b.branch_id == branch)
            .ok_or_else(|| unresolved("unknown global journal branch"))?
            .head_commit_id
    };
    let target = match &frontier.prepared {
        Some(target) => id(target)?,
        None => {
            partial_upload::wave_target(read, id(final_head)?, id(&frontier.accepted)?, 32).await?
        }
    };
    let mut cursor = target;
    let mut reverse = Vec::new();
    let mut seen = BTreeSet::new();
    while cursor != id(&frontier.accepted)? {
        if reverse.len() == 32 || !seen.insert(cursor) {
            return Err(unresolved("global prepared wave exceeded exact bound"));
        }
        let commit = commit::load_sync_commit(read, cursor)
            .await?
            .ok_or_else(|| unresolved("frozen migration body is absent"))?;
        if commit.parent_commit_ids.len() != 1 {
            return Err(unresolved("global migration wave is not linear"));
        }
        cursor = id(&commit.parent_commit_ids[0])?;
        reverse.push(commit);
    }
    reverse.reverse();
    let wave = NativeGlobalBodyWaveRequest {
        request: journal.request.clone(),
        branch_id: branch.into(),
        previous_commit_id: frontier.accepted.clone(),
        bodies: SyncPushRequest {
            commits: reverse,
            ref_updates: vec![],
            inline_blobs: vec![],
        },
    };
    wave.validate()?;
    Ok(wave)
}
pub(super) async fn upload_and_merge_partial_global_attempt<S, C>(
    storage: &StorageAdapter<S>,
    transport: &HttpSyncTransport<C>,
    journal: &mut impl PartialGlobalMergeOwner,
) -> Result<NativeGlobalMigrationReceipt, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    journal.current().validate()?;
    if journal.current().upload_settled || journal.current().restart_intent.is_some() {
        return Err(unresolved(
            "resume the durable GLOBAL terminal transition before sending bodies",
        ));
    }

    // Global bodies establish original global bases before selected bodies use them.
    let mut branches = vec![crate::GLOBAL_BRANCH_ID.to_owned()];
    branches.extend(
        journal
            .current()
            .request
            .new_branches
            .iter()
            .map(|b| b.branch_id.clone()),
    );
    if journal.current().receipt.is_none() {
        for branch in branches {
            loop {
                let current = journal.current().clone();
                let final_head = if branch == crate::GLOBAL_BRANCH_ID {
                    current.request.captured_local_head_commit_id.clone()
                } else {
                    current
                        .request
                        .new_branches
                        .iter()
                        .find(|b| b.branch_id == branch)
                        .unwrap()
                        .head_commit_id
                        .clone()
                };
                let frontier = &current.frontiers[&branch];
                // A zero-commit new branch still needs its native pin. A separate pin
                // acknowledged bit is required; equality alone cannot infer server pinning.
                if frontier.accepted == final_head
                    && frontier.prepared.is_none()
                    && current.acknowledged_roots.contains(&branch)
                {
                    break;
                }
                let read = storage.begin_read(Default::default()).await?;
                let wave = prepare_wave(&read, &current, &branch).await?;
                drop(read);
                let target = wave
                    .bodies
                    .commits
                    .last()
                    .map(|c| c.commit_id.clone())
                    .unwrap_or_else(|| wave.previous_commit_id.clone());
                journal
                    .publish(current.prepare_wave(&branch, &wave.previous_commit_id, &target)?)
                    .await?;
                native_migration_pin_upload::prepare_native_migration_blobs(
                    storage,
                    transport.active_account_id(),
                    transport,
                    &wave.bodies,
                )
                .await?;
                transport.push_native_global_migration_wave(&wave).await?;
                let next = journal.current().acknowledge_wave(
                    &branch,
                    &wave.previous_commit_id,
                    &target,
                )?;
                journal.publish(next).await?;
            }
        }
        let receipt = transport
            .merge_native_global_migration(&journal.current().request)
            .await?;
        journal
            .publish(journal.current().acknowledge_merge(receipt)?)
            .await?;
    }
    let receipt = journal
        .current()
        .receipt
        .clone()
        .ok_or_else(|| unresolved("global outcome missing after publication"))?;
    Ok(receipt)
}

use super::http::{HttpSyncTransport, RawHttpClient, TimedLeasedPartialDescriptor};
use super::partial_global_merge_state::{
    load_partial_global_merge_state, stage_capture_partial_global_merge, stage_global_record,
};
use super::partial_reconcile::PreparedDescriptor;
use super::partial_state::PartialReplicaState;
use crate::engine::Engine;
use std::sync::Arc;

struct RuntimeGlobalOwner<S: Storage + Clone + Send + Sync + 'static> {
    storage: StorageAdapter<S>,
    state: Arc<PartialReplicaState>,
    record: PartialGlobalMergeState,
}
impl<S: Storage + Clone + Send + Sync + 'static> PartialGlobalMergeOwner for RuntimeGlobalOwner<S> {
    fn current(&self) -> &PartialGlobalMergeState {
        &self.record
    }
    fn publish(&mut self, next: PartialGlobalMergeState) -> SyncTransportFuture<'_, ()> {
        Box::pin(async move {
            next.validate()?;
            if next.epoch_id != self.record.epoch_id
                || next.original_upload != self.record.original_upload
            {
                return Err(unresolved(
                    "GLOBAL transition changed captured native upload",
                ));
            }
            if next.request != self.record.request {
                let Some(NativeGlobalRestartReceipt::Restarted { intent }) =
                    &self.record.restart_receipt
                else {
                    return Err(unresolved(
                        "GLOBAL successor has no durable terminal restart proof",
                    ));
                };
                if intent.request != self.record.request
                    || intent.next_attempt_id != next.request.attempt_id
                    || next.previous_abort != self.record.restart_receipt
                {
                    return Err(unresolved(
                        "GLOBAL successor differs from exact restart proof",
                    ));
                }
            }
            let read = self.storage.begin_read(Default::default()).await?;
            let (actual, raw, mut guards) =
                load_partial_global_merge_state(&read, &self.state).await?;
            if actual.as_ref() != Some(&self.record) {
                return Err(unresolved("GLOBAL transition raced another owner"));
            }
            let mut writes = self.storage.new_write_set();
            guards.push(stage_global_record(&mut writes, &next, raw)?);
            drop(read);
            persist(&self.storage, writes, guards).await?;
            self.record = next;
            Ok(())
        })
    }
}
async fn persist<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    writes: crate::storage_adapter::StorageWriteSet,
    preconditions: Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<(), LixError> {
    storage
        .commit_partial_replica_write_set(
            partial_replica_write_capability(),
            writes,
            crate::storage_adapter::StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}

async fn resume_restart<S, C>(
    owner: &mut RuntimeGlobalOwner<S>,
    transport: &HttpSyncTransport<C>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    if owner.record.receipt.is_some() {
        return Ok(());
    }
    if owner.record.restart_intent.is_none() {
        owner
            .publish(
                owner
                    .record
                    .prepare_restart(uuid::Uuid::now_v7().to_string())?,
            )
            .await?;
    }
    if owner.record.restart_receipt.is_none() {
        let outcome = transport
            .restart_native_global_migration(owner.record.restart_intent.as_ref().unwrap())
            .await?;
        owner
            .publish(owner.record.acknowledge_restart(outcome)?)
            .await?;
    }
    if owner.record.receipt.is_some() {
        return Ok(());
    }
    let wrapper = transport
        .partial_replica_descriptor(Some(&owner.state.descriptor().selected_branch.branch_id))
        .await?;
    wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
    if wrapper.wire.descriptor.global_branch.checkpoint.commit_id
        != owner.record.request.checkpoint_commit_id
    {
        return Err(LixError::new(
            "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED",
            "GLOBAL checkpoint changed; the native local upload remains retained",
        ));
    }
    let mut request = owner.record.request.clone();
    request.attempt_id = owner
        .record
        .restart_intent
        .as_ref()
        .unwrap()
        .next_attempt_id
        .clone();
    request.expected_authority_head_commit_id =
        wrapper.wire.descriptor.global_branch.head.commit_id.clone();
    let boundaries = std::iter::once((
        crate::GLOBAL_BRANCH_ID.into(),
        request.base_commit_id.clone(),
    ))
    .chain(
        request
            .new_branches
            .iter()
            .map(|b| (b.branch_id.clone(), b.head_commit_id.clone())),
    )
    .collect();
    owner
        .publish(owner.record.capture_successor(request, boundaries)?)
        .await
}

pub(super) async fn cleanup_adopted_global_attempt<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    let (record, _, _) = load_partial_global_merge_state(&read, state).await?;
    drop(read);
    let Some(record) = record.filter(|r| r.upload_settled) else {
        return Ok(false);
    };
    transport
        .cleanup_native_global_migration(&record.request)
        .await?;
    let read = storage.begin_read(Default::default()).await?;
    let (actual, _, guards) = load_partial_global_merge_state(&read, state).await?;
    if actual.as_ref() != Some(&record) {
        return Err(unresolved("GLOBAL cleanup raced local state"));
    }
    let mut writes = storage.new_write_set();
    writes.delete(
        PARTIAL_GLOBAL_MERGE_SPACE,
        crate::storage_adapter::StorageKey(bytes::Bytes::from_static(b"current")),
    );
    drop(read);
    persist(storage, writes, guards).await?;
    Ok(true)
}

async fn ensure_global_upload<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
) -> Result<(), LixError> {
    let read = storage.begin_read(Default::default()).await?;
    let prepared = partial_upload::prepare_partial_ordinary_upload(
        &read,
        state,
        crate::GLOBAL_BRANCH_ID,
        uuid::Uuid::now_v7().to_string(),
        32,
        1024 * 1024,
    )
    .await?;
    let Some(prepared) = prepared else {
        return Ok(());
    };
    let mut writes = storage.new_write_set();
    let mut guards = partial_push_state::stage_prepare_partial_upload(
        &read,
        &mut writes,
        state,
        crate::GLOBAL_BRANCH_ID,
        &prepared.upload,
    )
    .await?;
    guards.extend(prepared.control_guard);
    drop(read);
    if !writes.is_empty() {
        persist(storage, writes, guards).await?;
    }
    Ok(())
}

pub(super) fn prepare_descriptor_with_global_merge<'a, S, C>(
    engine: Arc<Engine<S>>,
    previous: Arc<PartialReplicaState>,
    transport: &'a HttpSyncTransport<C>,
    wrapper: TimedLeasedPartialDescriptor,
    recovery: partial_publication::PartialRecoveryPolicy,
) -> SyncTransportFuture<'a, PreparedDescriptor>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    Box::pin(async move {
        let storage = engine.storage();
        let read = storage.begin_read(Default::default()).await?;
        let (existing, _, _) = load_partial_global_merge_state(&read, &previous).await?;
        let (push, _, _) =
            partial_push_state::load_partial_push_state(&read, &previous, crate::GLOBAL_BRANCH_ID)
                .await?;
        let local =
            crate::branch::observe_branch_control_coordinate(&read, crate::GLOBAL_BRANCH_ID)
                .await?
                .control
                .ok_or_else(|| unresolved("GLOBAL control disappeared"))?;
        let remote = &wrapper.wire.descriptor.global_branch;
        let needs_merge = existing.is_some()
            || (local.head_commit_id != push.confirmed.head
                && remote.head.commit_id != push.confirmed.head);
        drop(read);
        if !needs_merge {
            return partial_merge_runtime::prepare_descriptor_with_merge(
                engine, previous, transport, wrapper, recovery,
            )
            .await;
        }
        if existing.as_ref().is_some_and(|r| r.upload_settled) {
            cleanup_adopted_global_attempt(&storage, &previous, transport).await?;
            return Ok(PreparedDescriptor::LocalProgress);
        }
        if existing.is_none()
            && recover_included_global_upload(&storage, &previous, transport, &wrapper).await?
        {
            return partial_merge_runtime::prepare_descriptor_with_merge(
                engine, previous, transport, wrapper, recovery,
            )
            .await;
        }
        if existing.is_none() {
            ensure_global_upload(&storage, &previous).await.map_err(|error| {
                if error.code=="LIX_PARTIAL_CREATED_REF_SOURCE_PENDING" {
                    LixError::new("LIX_PARTIAL_GLOBAL_SELECTED_RECONCILIATION_REQUIRED",
                        "created branch source requires selected publication before GLOBAL reconciliation; pending data remains retained")
                } else {error}
            })?;
            let read = storage.begin_read(Default::default()).await?;
            let (push, _, _) = partial_push_state::load_partial_push_state(
                &read,
                &previous,
                crate::GLOBAL_BRANCH_ID,
            )
            .await?;
            let upload = push
                .prepared
                .ok_or_else(|| unresolved("GLOBAL divergence has no captured ordinary upload"))?;
            if upload.created_refs.is_empty() {
                return Err(LixError::new(
                    "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED",
                    "GLOBAL divergence is outside disjoint native branch creation; pending edits remain local",
                ));
            }
            let request = NativeGlobalMigrationRequest {
                attempt_id: uuid::Uuid::now_v7().to_string(),
                base_commit_id: upload.expected.head.clone(),
                expected_authority_head_commit_id: remote.head.commit_id.clone(),
                captured_local_head_commit_id: upload.target.head.clone(),
                checkpoint_commit_id: upload.expected.checkpoint.clone(),
                new_branches: upload
                    .created_refs
                    .iter()
                    .map(|b| NativeNewBranchCoordinate {
                        branch_id: b.branch_id.clone(),
                        head_commit_id: b.head_commit_id.clone(),
                        checkpoint_commit_id: b.checkpoint_commit_id.clone(),
                    })
                    .collect(),
            };
            let mut writes = storage.new_write_set();
            let guards =
                stage_capture_partial_global_merge(&read, &mut writes, &previous, request).await?;
            drop(read);
            persist(&storage, writes, guards).await?;
        }
        let read = storage.begin_read(Default::default()).await?;
        let record = load_partial_global_merge_state(&read, &previous)
            .await?
            .0
            .ok_or_else(|| unresolved("GLOBAL capture disappeared"))?;
        drop(read);
        let mut owner = RuntimeGlobalOwner {
            storage: storage.clone(),
            state: previous.clone(),
            record,
        };
        if owner.record.restart_intent.is_some() {
            resume_restart(&mut owner, transport).await?;
        }
        for attempt in 0..8 {
            match upload_and_merge_partial_global_attempt(&storage, transport, &mut owner).await {
                Ok(_) => break,
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED"
                            | "LIX_MIGRATION_GLOBAL_BODY_INVALID"
                            | "LIX_MIGRATION_GLOBAL_ATTEMPT_RESTARTED"
                    ) =>
                {
                    let fresh = transport
                        .partial_replica_descriptor(Some(
                            &previous.descriptor().selected_branch.branch_id,
                        ))
                        .await?;
                    let changed = fresh.wire.descriptor.global_branch.head.commit_id
                        != owner.record.request.expected_authority_head_commit_id;
                    if owner.record.restart_intent.is_none() {
                        owner
                            .publish(
                                owner
                                    .record
                                    .prepare_restart(uuid::Uuid::now_v7().to_string())?,
                            )
                            .await?;
                    }
                    let outcome = transport
                        .restart_native_global_migration(
                            owner.record.restart_intent.as_ref().unwrap(),
                        )
                        .await?;
                    owner
                        .publish(owner.record.acknowledge_restart(outcome)?)
                        .await?;
                    if owner.record.receipt.is_some() {
                        break;
                    }
                    if !changed {
                        return Err(LixError::new(
                            "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED",
                            error.message,
                        ));
                    }
                    if attempt == 7 {
                        return Err(unresolved(
                            "authority changed repeatedly; exact aborted GLOBAL attempt remains recoverable",
                        ));
                    }
                    resume_restart(&mut owner, transport).await?;
                }
                Err(error) => return Err(error),
            }
        }
        let read = storage.begin_read(Default::default()).await?;
        let current_global =
            crate::branch::observe_branch_control_coordinate(&read, crate::GLOBAL_BRANCH_ID)
                .await?
                .control
                .ok_or_else(|| unresolved("GLOBAL control disappeared after authority receipt"))?;
        let newer_global =
            current_global.head_commit_id != owner.record.request.captured_local_head_commit_id;
        drop(read);
        if newer_global {
            let basis = transport
                .partial_replica_descriptor(Some(&previous.descriptor().selected_branch.branch_id))
                .await?;
            let leased = transport.fork_native_baseline_lease(&basis.wire.lease)?;
            let mut seen = BTreeSet::new();
            loop {
                basis.deadline.check(&basis.wire.lease.lease_id)?;
                let result = async {
                    let read = storage.begin_read(Default::default()).await?;
                    let verified =
                        partial_global_merge_settlement::verify_partial_global_prefix_settlement(
                            &read, &previous,
                        )
                        .await?;
                    let mut writes = storage.new_write_set();
                    let guards =
                        partial_push_state::stage_settle_partial_global_merge_confirmation(
                            &read,
                            &mut writes,
                            &previous,
                            verified,
                        )
                        .await?;
                    drop(read);
                    persist(&storage, writes, guards).await
                }
                .await;
                match result {
                    Ok(()) => break,
                    Err(error) => {
                        hydrate_error(&storage, &previous, &leased, error, &mut seen).await?
                    }
                }
            }
            cleanup_adopted_global_attempt(&storage, &previous, transport).await?;
            // The next ordinary wave captures L→L2; native serving state was never
            // rewritten by this prefix confirmation.
            return Ok(PreparedDescriptor::LocalProgress);
        }
        // M retained original GLOBAL L, so existing selected commits keep their native
        // historical base. Drain only normal selected uploads; never reinterpret a
        // conflicting selected history as a GLOBAL success.
        let selected = &previous.descriptor().selected_branch.branch_id;
        if selected != crate::GLOBAL_BRANCH_ID {
            for _ in 0..68 {
                let storage_ref = &storage;
                let state_ref = previous.as_ref();
                let result = partial_upload_cycle::upload_partial_once(
                    &storage,
                    &previous,
                    selected,
                    uuid::Uuid::now_v7().to_string(),
                    32,
                    1024 * 1024,
                    |request| async move {
                        partial_blob_upload::push_partial_with_blobs(
                            storage_ref,
                            state_ref,
                            transport,
                            &request,
                        )
                        .await
                    },
                )
                .await;
                match result {
                    Ok(false) => break,
                    Ok(true) => {}
                    Err(error) if error.code == LixError::CODE_TRANSACTION_CONFLICT => {
                        return Err(LixError::new(
                            "LIX_PARTIAL_GLOBAL_SELECTED_RECONCILIATION_REQUIRED",
                            "selected divergence requires reconciliation; GLOBAL outcome and all pending edits remain retained",
                        ));
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        let wrapper = transport.partial_replica_descriptor(Some(selected)).await?;
        partial_reconcile::prepare_clean_descriptor(
            engine,
            previous,
            transport,
            wrapper,
            partial_publication::PartialRecoveryPolicy::NativeGlobalMerge,
        )
        .await
    })
}

async fn hydrate_error<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
    error: LixError,
    seen: &mut BTreeSet<String>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    let Some(demand) = runtime::native_sync_demand_request_for_error(&error)? else {
        return Err(error);
    };
    if seen.len() >= 4096 || !seen.insert(format!("{demand:?}")) {
        return Err(unresolved("GLOBAL proof repeated a hydrated native input"));
    }
    partial_runtime::hydrate_demand(storage, state, transport, demand).await
}

/// Recover a published ordinary creation group even if its ACK was lost and a
/// later authority GLOBAL commit has already advanced beyond captured L.
async fn recover_included_global_upload<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
    wrapper: &TimedLeasedPartialDescriptor,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    let (push, _, _) =
        partial_push_state::load_partial_push_state(&read, state, crate::GLOBAL_BRANCH_ID).await?;
    let Some(upload) = push.prepared else {
        return Ok(false);
    };
    drop(read);
    if upload.created_refs.is_empty()
        || upload.target.checkpoint != wrapper.wire.descriptor.global_branch.checkpoint.commit_id
    {
        return Ok(false);
    }
    let leased = transport.fork_native_baseline_lease(&wrapper.wire.lease)?;
    let mut seen = BTreeSet::new();
    loop {
        wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
        let result = async {
            let read = storage.begin_read(Default::default()).await?;
            let local =
                partial_merge_analysis::record(&read, id(&upload.target.head)?, true).await?;
            partial_merge_analysis::bounded_ancestor(
                &read,
                &local,
                id(&wrapper.wire.descriptor.global_branch.head.commit_id)?,
                &mut Default::default(),
                1024,
            )
            .await
        }
        .await;
        match result {
            Ok(false) => return Ok(false),
            Ok(true) => break,
            Err(error) => hydrate_error(storage, state, &leased, error, &mut seen).await?,
        }
    }
    for child in &upload.created_refs {
        let child_wrapper = transport
            .partial_replica_descriptor(Some(&child.branch_id))
            .await?;
        if child_wrapper
            .wire
            .descriptor
            .selected_branch
            .checkpoint
            .commit_id
            != child.checkpoint_commit_id
        {
            return Err(LixError::new(
                "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED",
                "created child checkpoint changed before inclusion recovery; local upload remains retained",
            ));
        }
        let child_transport = transport.fork_native_baseline_lease(&child_wrapper.wire.lease)?;
        let mut seen = BTreeSet::new();
        loop {
            child_wrapper
                .deadline
                .check(&child_wrapper.wire.lease.lease_id)?;
            let result = async {
                let read = storage.begin_read(Default::default()).await?;
                let captured =
                    partial_merge_analysis::record(&read, id(&child.head_commit_id)?, true).await?;
                partial_merge_analysis::bounded_ancestor(
                    &read,
                    &captured,
                    id(&child_wrapper.wire.descriptor.selected_branch.head.commit_id)?,
                    &mut Default::default(),
                    1024,
                )
                .await
            }
            .await;
            match result {
                Ok(true) => break,
                Ok(false) => {
                    return Err(LixError::new(
                        "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED",
                        "created child no longer contains captured head; local upload remains retained",
                    ));
                }
                Err(error) => {
                    hydrate_error(storage, state, &child_transport, error, &mut seen).await?
                }
            }
        }
    }
    let read = storage.begin_read(Default::default()).await?;
    let mut writes = storage.new_write_set();
    let guards = partial_push_state::stage_acknowledge_partial_upload(
        &read,
        &mut writes,
        state,
        crate::GLOBAL_BRANCH_ID,
        &upload,
        true,
    )
    .await?;
    drop(read);
    persist(storage, writes, guards).await?;
    Ok(true)
}

pub(super) fn waits_for_state_change(error: &LixError) -> bool {
    matches!(
        error.code.as_str(),
        "LIX_PARTIAL_GLOBAL_SCOPE_UNSUPPORTED"
            | "LIX_PARTIAL_GLOBAL_MERGE_PENDING"
            | "LIX_PARTIAL_GLOBAL_SELECTED_RECONCILIATION_REQUIRED"
            | "LIX_PARTIAL_GLOBAL_NEWER_LOCAL_RECONCILIATION_REQUIRED"
            | "LIX_PARTIAL_GLOBAL_MERGE_STATE_INVALID"
            | "LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED"
    )
}
