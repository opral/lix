//! Durable, cancellable background reconciliation. No SQL replay or foreground I/O.
use super::http::{HttpSyncTransport, RawHttpClient, TimedLeasedPartialDescriptor};
use super::partial_merge_protocol::{PartialMergeRequest, RetainedBodyWaveRequest};
use super::partial_merge_state::{
    PreparedMergeBodyWave, load_partial_merge_state, stage_acknowledge_partial_merge_body_wave,
    stage_capture_partial_merge, stage_prepare_partial_merge_body_wave,
    stage_record_partial_merge_receipt, stage_rollover_partial_merge,
};
use super::partial_reconcile::PreparedDescriptor;
use super::partial_state::PartialReplicaState;
use crate::storage_adapter::{Storage, StorageAdapter, StorageAdapterRead, StorageWriteOptions};
use crate::{LixError, changelog::CommitId, engine::Engine};
use std::{collections::BTreeSet, sync::Arc};
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_STATE_INVALID", message)
}
fn id(value: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(value, "merge runtime coordinate")
}
async fn persist<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    writes: crate::storage_adapter::StorageWriteSet,
    preconditions: Vec<crate::storage_adapter::StoragePrecondition>,
) -> Result<(), LixError> {
    storage
        .commit_partial_replica_write_set(
            super::partial_replica_write_capability(),
            writes,
            StorageWriteOptions {
                preconditions,
                await_durable: true,
                ..Default::default()
            },
        )
        .await?;
    Ok(())
}
async fn hydrate<S, C>(
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
    let Some(demand) = super::runtime::native_sync_demand_request_for_error(&error)? else {
        return Err(error);
    };
    if seen.len() >= 4096 || !seen.insert(format!("{demand:?}")) {
        return Err(invalid("merge preparation repeated a hydrated input"));
    }
    super::partial_runtime::hydrate_demand(storage, state, transport, demand).await
}

/// A local catalog publication may depend on selected work preceding it. Drain
/// only that selected prefix; later commits become eligible after GLOBAL ACK.
async fn captured_ordinary_prefix(
    read: &(impl StorageAdapterRead + ?Sized),
    head: CommitId,
    boundary: CommitId,
    catalog: CommitId,
) -> Result<CommitId, LixError> {
    let target = super::partial_upload::wave_target(read, head, boundary, 256).await?;
    let mut current = target;
    let mut reverse = Vec::new();
    while current != boundary {
        let record = super::partial_upload::local_record(read, current).await?;
        if record.parent_commit_ids.len() != 1 || record.is_checkpoint {
            // Nonlinear native closures keep their existing bounded analysis.
            return Ok(target);
        }
        if reverse.len() == 256 {
            return Err(invalid("captured ordinary prefix exceeds its native bound"));
        }
        current = record.parent_commit_ids[0];
        reverse.push(record);
    }
    let mut eligible = boundary;
    let mut known = BTreeSet::from([catalog]);
    for record in reverse.into_iter().rev() {
        let base = record
            .base_commit_id
            .ok_or_else(|| invalid("selected prefix has no catalog"))?;
        if !known.contains(&base) {
            if !super::partial_merge_analysis::catalog_contains(read, base, catalog, 1024).await? {
                break;
            }
            known.insert(base);
        }
        eligible = record.commit_id;
    }
    if eligible == boundary && target != boundary {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_MERGE_PENDING",
            "selected suffix awaits its local catalog publication",
        ));
    }
    Ok(eligible)
}

pub(super) async fn captured_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    request: &PartialMergeRequest,
    accepted: &str,
    prepared: Option<&PreparedMergeBodyWave>,
) -> Result<RetainedBodyWaveRequest, LixError> {
    let previous = prepared.map_or(accepted, |wave| wave.previous.as_str());
    if previous != accepted {
        return Err(invalid(
            "prepared merge wave changed its durable predecessor",
        ));
    }
    let mut known = [
        &request.base_commit_id,
        &request.expected_authority_head_commit_id,
        &request.checkpoint_commit_id,
        &request.expected_authority_checkpoint_commit_id,
        &request.global_head_commit_id,
        &request.global_checkpoint_commit_id,
    ]
    .into_iter()
    .map(|value| id(value))
    .collect::<Result<BTreeSet<_>, _>>()?;
    // Even an already included head needs a complete retained body for the
    // immutable attempt before its acknowledgment can be committed.
    known.remove(&id(&request.captured_local_head_commit_id)?);
    // Replay the deterministic bounded local closure from immutable coordinates.
    // The accepted cursor names a prefix, not a first-parent ancestry claim.
    let closure = super::partial_checkpoint_upload::load_local_dependency_closure(
        read,
        &request.branch_id,
        &super::partial_upload::local_record(read, id(&request.captured_local_head_commit_id)?)
            .await?
            .account_id,
        &[
            id(&request.captured_local_head_commit_id)?,
            id(&request.captured_local_checkpoint_commit_id)?,
        ],
        known,
        id(&request.global_head_commit_id)?,
        1024,
        64 * 1024 * 1024,
    )
    .await?;
    let start = if previous == request.base_commit_id {
        0
    } else {
        closure
            .iter()
            .position(|commit| commit.commit_id == previous)
            .ok_or_else(|| invalid("accepted body cursor is outside captured dependency closure"))?
            + 1
    };
    let end = if let Some(wave) = prepared {
        closure
            .iter()
            .position(|commit| commit.commit_id == wave.target)
            .ok_or_else(|| invalid("prepared body cursor is outside captured dependency closure"))?
            + 1
    } else {
        (start + 32).min(closure.len())
    };
    if end <= start || end - start > 32 {
        return Err(invalid(
            "retained body cursor does not name a bounded forward page",
        ));
    }
    let commits = closure[start..end].to_vec();
    let wave = RetainedBodyWaveRequest {
        request: request.clone(),
        expected_previous_commit_id: previous.into(),
        bodies: super::SyncPushRequest {
            commits,
            ref_updates: vec![],
            inline_blobs: vec![],
        },
    };
    wave.validate()?;
    if serde_json::to_vec(&wave)
        .map_err(|_| invalid("merge wave encoding failed"))?
        .len()
        > 64 * 1024 * 1024
    {
        return Err(invalid("captured merge wave exceeds upload byte bound"));
    }
    Ok(wave)
}

/// Complete the durable restart lane before permitting any old/new body RPC.
async fn recover_expired_attempt<S, C>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
    branch: &str,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    use super::partial_merge_state::{
        stage_capture_restarted_partial_merge, stage_prepare_partial_merge_restart,
        stage_record_partial_merge_restart_outcome,
    };
    let read = storage.begin_read(Default::default()).await?;
    let (record, _, _) = load_partial_merge_state(&read, state, branch).await?;
    let record = record.ok_or_else(|| invalid("restart lost its captured attempt"))?;
    if record.authority_receipt.is_some() {
        return Ok(());
    }
    let intent = record
        .restart
        .as_ref()
        .map(|restart| restart.request.clone())
        .unwrap_or_else(|| super::PartialAttemptRestartRequest {
            old: record.request.clone(),
            next_attempt_id: uuid::Uuid::now_v7().to_string(),
        });
    let has_receipt = record
        .restart
        .as_ref()
        .is_some_and(|restart| restart.receipt.is_some());
    let mut writes = storage.new_write_set();
    let guards = stage_prepare_partial_merge_restart(&read, &mut writes, state, &intent).await?;
    drop(read);
    persist(storage, writes, guards).await?;
    if !has_receipt {
        let outcome = transport.restart_partial_attempt(&intent).await?;
        let read = storage.begin_read(Default::default()).await?;
        let mut writes = storage.new_write_set();
        let guards = stage_record_partial_merge_restart_outcome(
            &read,
            &mut writes,
            state,
            &intent,
            &outcome,
        )
        .await?;
        drop(read);
        persist(storage, writes, guards).await?;
        if matches!(
            outcome,
            super::PartialAttemptRestartOutcome::Committed { .. }
        ) {
            return Ok(());
        }
    }
    // The old R is no longer a publishable promise. Lease fresh coordinates,
    // then prove and persist B/current-L2/new-R before any successor body upload.
    let wrapper = transport.partial_replica_descriptor(Some(branch)).await?;
    let candidate = transport.fork_native_baseline_lease(&wrapper.wire.lease)?;
    let mut seen = BTreeSet::new();
    loop {
        wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
        let result = async {
            let read = storage.begin_read(Default::default()).await?;
            let descriptor = &wrapper.wire.descriptor;
            let (global, _, _) = super::partial_push_state::load_partial_push_state(
                &read,
                state,
                crate::GLOBAL_BRANCH_ID,
            )
            .await?;
            let request = PartialMergeRequest {
                attempt_id: intent.next_attempt_id.clone(),
                branch_id: branch.into(),
                base_commit_id: intent.old.base_commit_id.clone(),
                expected_authority_head_commit_id: descriptor
                    .selected_branch
                    .head
                    .commit_id
                    .clone(),
                captured_local_head_commit_id: intent.old.captured_local_head_commit_id.clone(),
                checkpoint_commit_id: intent.old.checkpoint_commit_id.clone(),
                expected_authority_checkpoint_commit_id: descriptor
                    .selected_branch
                    .checkpoint
                    .commit_id
                    .clone(),
                captured_local_checkpoint_commit_id: intent
                    .old
                    .captured_local_checkpoint_commit_id
                    .clone(),
                global_head_commit_id: global.confirmed.head,
                global_checkpoint_commit_id: global.confirmed.checkpoint,
            };
            let mut writes = storage.new_write_set();
            let guards =
                stage_capture_restarted_partial_merge(&read, &mut writes, state, &request).await?;
            drop(read);
            persist(storage, writes, guards).await
        }
        .await;
        match result {
            Ok(()) => return Ok(()),
            Err(error) => hydrate(storage, state, &candidate, error, &mut seen).await?,
        }
    }
}

// Erase this child operation before composing the worker select loop.
pub(super) fn prepare_descriptor_with_merge<'a, S, C>(
    engine: Arc<Engine<S>>,
    previous: Arc<PartialReplicaState>,
    transport: &'a HttpSyncTransport<C>,
    wrapper: TimedLeasedPartialDescriptor,
    recovery: super::partial_publication::PartialRecoveryPolicy,
) -> super::SyncTransportFuture<'a, PreparedDescriptor>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    Box::pin(async move {
        let storage = engine.storage();
        let branch = &previous.descriptor().selected_branch.branch_id;
        let mut seen = BTreeSet::new();
        let candidate = transport.fork_native_baseline_lease(&wrapper.wire.lease)?;
        // A later authority commit can hide an accepted ordinary upload from
        // exact-head equality. Settle the exact frozen tuple by native proof;
        // never resend it as a fresh LWW write.
        loop {
            wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
            let inclusion = async {
                let read = storage.begin_read(Default::default()).await?;
                let mut writes = storage.new_write_set();
                let guards = super::partial_push_state::stage_acknowledge_included_partial_upload(
                    &read,
                    &mut writes,
                    &previous,
                    &wrapper.wire.descriptor,
                )
                .await?;
                drop(read);
                if let Some(guards) = guards {
                    persist(&storage, writes, guards).await?;
                }
                Ok::<(), LixError>(())
            }
            .await;
            match inclusion {
                Ok(()) => break,
                Err(error) => hydrate(&storage, &previous, &candidate, error, &mut seen).await?,
            }
        }
        // Read-only inputs may hydrate before durable capture; authoring L stays
        // protected by local control CAS and the partial epoch receipt.
        loop {
            wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
            let result = async {
                let read = storage.begin_read(Default::default()).await?;
                let (outbox, _, _) = load_partial_merge_state(&read, &previous, branch).await?;
                let (push, _, _) =
                    super::partial_push_state::load_partial_push_state(&read, &previous, branch)
                        .await?;
                let control = crate::branch::BranchHeadControlContext::new()
                    .reader(&read)
                    .load(branch)
                    .await?
                    .ok_or_else(|| invalid("merge selected control disappeared"))?;
                if outbox.is_none()
                    && (control.head_commit_id == push.confirmed.head
                        || wrapper.wire.descriptor.selected_branch.head.commit_id
                            == push.confirmed.head)
                {
                    return Ok(false);
                }
                if let Some(outbox) = &outbox {
                    if outbox.authority_receipt.is_none()
                        || control.head_commit_id == outbox.request.captured_local_head_commit_id
                    {
                        return Ok(true);
                    }
                }
                let base = outbox
                    .as_ref()
                    .map_or(push.confirmed.head.clone(), |record| {
                        record.request.captured_local_head_commit_id.clone()
                    });
                let (global, _, _) = super::partial_push_state::load_partial_push_state(
                    &read,
                    &previous,
                    crate::GLOBAL_BRANCH_ID,
                )
                .await?;
                let checkpoint = outbox.as_ref().map_or_else(
                    || push.confirmed.checkpoint.clone(),
                    |record| record.request.captured_local_checkpoint_commit_id.clone(),
                );
                let local_checkpoint = control
                    .working_diff_checkpoint_commit_id
                    .ok_or_else(|| invalid("captured local checkpoint disappeared"))?;
                let (captured_head, captured_checkpoint) = if local_checkpoint == checkpoint {
                    (
                        captured_ordinary_prefix(
                            &read,
                            control.head_commit_id,
                            id(&base)?,
                            id(&global.confirmed.head)?,
                        )
                        .await?,
                        local_checkpoint,
                    )
                } else {
                    let target = super::partial_checkpoint_upload::next_checkpoint_target(
                        &read,
                        branch,
                        control.head_commit_id,
                        local_checkpoint,
                        id(&checkpoint)?,
                    )
                    .await?;
                    let target_checkpoint = id(&target.checkpoint)?;
                    let (source_branch, source) =
                        super::commit::load_sync_checkpoint_source(&read, target_checkpoint)
                            .await?
                            .ok_or_else(|| invalid("pending checkpoint has no retained source"))?;
                    if source_branch != *branch {
                        return Err(invalid(
                            "pending checkpoint source belongs to another branch",
                        ));
                    }
                    let prefix = if source == id(&base)? {
                        source
                    } else {
                        captured_ordinary_prefix(
                            &read,
                            source,
                            id(&base)?,
                            id(&global.confirmed.head)?,
                        )
                        .await?
                    };
                    if prefix == source {
                        let mut checkpoint_eligible = true;
                        for target in [id(&target.head)?, target_checkpoint] {
                            let record = super::partial_upload::local_record(&read, target).await?;
                            let catalog = record
                                .base_commit_id
                                .ok_or_else(|| invalid("selected checkpoint has no catalog"))?;
                            checkpoint_eligible &= super::partial_merge_analysis::catalog_contains(
                                &read,
                                catalog,
                                id(&global.confirmed.head)?,
                                1024,
                            )
                            .await?;
                        }
                        if checkpoint_eligible {
                            (id(&target.head)?, target_checkpoint)
                        } else if source != id(&base)? {
                            (source, id(&checkpoint)?)
                        } else {
                            return Err(LixError::new(
                                "LIX_PARTIAL_REPLICA_MERGE_PENDING",
                                "selected checkpoint awaits its local catalog publication",
                            ));
                        }
                    } else {
                        (prefix, id(&checkpoint)?)
                    }
                };
                let request = PartialMergeRequest {
                    attempt_id: uuid::Uuid::now_v7().to_string(),
                    branch_id: branch.clone(),
                    base_commit_id: base,
                    expected_authority_head_commit_id: wrapper
                        .wire
                        .descriptor
                        .selected_branch
                        .head
                        .commit_id
                        .clone(),
                    captured_local_head_commit_id: captured_head.to_string(),
                    checkpoint_commit_id: checkpoint,
                    expected_authority_checkpoint_commit_id: wrapper
                        .wire
                        .descriptor
                        .selected_branch
                        .checkpoint
                        .commit_id
                        .clone(),
                    captured_local_checkpoint_commit_id: captured_checkpoint.to_string(),
                    global_head_commit_id: global.confirmed.head,
                    global_checkpoint_commit_id: global.confirmed.checkpoint,
                };
                super::partial_merge_analysis::analyze_native_divergence(
                    &read,
                    id(&request.base_commit_id)?,
                    id(&request.expected_authority_head_commit_id)?,
                    id(&request.captured_local_head_commit_id)?,
                    previous.active_account_id(),
                    &request.branch_id,
                    &[
                        id(&request.checkpoint_commit_id)?,
                        id(&request.expected_authority_checkpoint_commit_id)?,
                    ],
                    id(&request.global_head_commit_id)?,
                    super::PartialMergeBudget {
                        max_local_commits: 1024,
                        max_local_members: 65536,
                        max_local_payload_bytes: 64 * 1024 * 1024,
                        max_remote_graph_records: 1024,
                    },
                )
                .await?;
                let mut writes = storage.new_write_set();
                let guards = if outbox.is_some() {
                    stage_rollover_partial_merge(&read, &mut writes, &previous, &request).await?
                } else {
                    stage_capture_partial_merge(&read, &mut writes, &previous, &request).await?
                };
                drop(read);
                persist(&storage, writes, guards).await?;
                Ok(true)
            }
            .await;
            match result {
                Ok(false) => {
                    return super::partial_reconcile::prepare_clean_descriptor(
                        engine, previous, transport, wrapper, recovery,
                    )
                    .await;
                }
                Ok(true) => break,
                Err(error) => hydrate(&storage, &previous, &candidate, error, &mut seen).await?,
            }
        }
        for _ in 0..68 {
            engine.sync_mode().ensure_partial_admission_healthy()?;
            let read = storage.begin_read(Default::default()).await?;
            let (record, _, _) = load_partial_merge_state(&read, &previous, branch).await?;
            let record = record.ok_or_else(|| invalid("captured merge disappeared"))?;
            if record.authority_receipt.is_some() {
                drop(read);
                break;
            }
            if record.restart.is_some() {
                drop(read);
                recover_expired_attempt(&storage, &previous, transport, branch).await?;
                continue;
            }
            if record.accepted_body_tip == record.request.captured_local_head_commit_id {
                let request = record.request.clone();
                drop(read);
                let receipt = match transport.merge_partial_replica(&request).await {
                    Ok(receipt) => receipt,
                    Err(error)
                        if error.code == "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED"
                            || error.code == "LIX_PARTIAL_ATTEMPT_RESTARTED" =>
                    {
                        recover_expired_attempt(&storage, &previous, transport, branch).await?;
                        continue;
                    }
                    Err(error) => return Err(error),
                };
                let read = storage.begin_read(Default::default()).await?;
                let mut writes = storage.new_write_set();
                let guards =
                    stage_record_partial_merge_receipt(&read, &mut writes, &previous, &receipt)
                        .await?;
                drop(read);
                persist(&storage, writes, guards).await?;
                break;
            }
            let wave = captured_wave(
                &read,
                &record.request,
                &record.accepted_body_tip,
                record.prepared_body_wave.as_ref(),
            )
            .await?;
            let exact = PreparedMergeBodyWave {
                previous: wave.expected_previous_commit_id.clone(),
                target: wave
                    .bodies
                    .commits
                    .last()
                    .expect("validated wave")
                    .commit_id
                    .clone(),
            };
            let mut writes = storage.new_write_set();
            let guards = stage_prepare_partial_merge_body_wave(
                &read,
                &mut writes,
                &previous,
                &record.request,
                &exact.previous,
                &exact.target,
            )
            .await?;
            drop(read);
            persist(&storage, writes, guards).await?;
            super::partial_blob_upload::prepare_partial_upload_blobs(
                &storage,
                &previous,
                transport,
                &wave.bodies,
            )
            .await?;
            if let Err(error) = transport.retained_body_wave(&wave).await {
                if error.code == "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED"
                    || error.code == "LIX_PARTIAL_ATTEMPT_RESTARTED"
                {
                    recover_expired_attempt(&storage, &previous, transport, branch).await?;
                    continue;
                }
                return Err(error);
            }

            let read = storage.begin_read(Default::default()).await?;
            let mut writes = storage.new_write_set();
            let guards = stage_acknowledge_partial_merge_body_wave(
                &read,
                &mut writes,
                &previous,
                &record.request,
                &exact.previous,
                &exact.target,
            )
            .await?;
            drop(read);
            persist(&storage, writes, guards).await?;
        }
        let wrapper = transport.partial_replica_descriptor(Some(branch)).await?;
        super::partial_reconcile::prepare_clean_descriptor(
            engine,
            previous,
            transport,
            wrapper,
            super::partial_publication::PartialRecoveryPolicy::NativeMerge,
        )
        .await
    })
}
