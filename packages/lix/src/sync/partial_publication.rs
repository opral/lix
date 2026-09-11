//! Atomic adoption of a fully prepared authority working set. Hydration is
//! deliberately outside this module: preparation returns native missing-input
//! errors before any control or receipt is published.
use std::sync::Arc;

use super::http::CandidateBaselineDeadline;
use super::partial_push_state::{
    PartialPushCoordinate, load_partial_push_state, stage_remote_partial_confirmation,
};
use super::partial_state::{
    PartialReplicaState, load_partial_replica_state, stage_partial_replica_state,
};
use crate::LixError;
use crate::engine::Engine;
use crate::storage_adapter::{Storage, StoragePrecondition, StorageWriteOptions, StorageWriteSet};

/// Owned, single-use publication proof. Its private fields prevent callers
/// replacing the evaluated write set or omitting a captured CAS dependency.
pub(super) struct PreparedPartialPublication {
    // Exact owning engine, not a serializable receipt that another backing
    // store could copy. Preparation and publication use this same write gate.
    origin_write_gate: Arc<tokio::sync::Mutex<()>>,
    branch_switch_completion: Option<super::partial_branch_switch::PartialBranchSwitchCompletion>,
    previous: Arc<PartialReplicaState>,
    next: Arc<PartialReplicaState>,
    interests_revision: u64,
    deadline: CandidateBaselineDeadline,
    writes: StorageWriteSet,
    preconditions: Vec<StoragePrecondition>,
}
fn conflict(message: &str) -> LixError {
    LixError::new(LixError::CODE_TRANSACTION_CONFLICT, message)
}

/// A matching acknowledged authority ref does not change the serving basis.
/// In particular, this never reloads native inputs after our own upload ACK.
/// Pending local commits require the separate native rebase path.
pub(super) async fn prepare_clean_partial_publication<S>(
    engine: &Engine<S>,
    next: Arc<PartialReplicaState>,
    deadline: CandidateBaselineDeadline,
) -> Result<Option<PreparedPartialPublication>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    prepare_partial_publication(engine, next, deadline, PartialRecoveryPolicy::Normal).await
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum PartialRecoveryPolicy {
    Normal,
    ExpiredBaseline,
    NativeMerge,
    NativeGlobalMerge,
}

pub(super) async fn prepare_partial_publication<S>(
    engine: &Engine<S>,
    next: Arc<PartialReplicaState>,
    deadline: CandidateBaselineDeadline,
    policy: PartialRecoveryPolicy,
) -> Result<Option<PreparedPartialPublication>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    deadline.check(&next.baseline_lease().lease_id)?;
    let mode = engine.sync_mode();
    mode.ensure_partial_admission_healthy()?;
    let previous = mode
        .partial_admission()
        .ok_or_else(|| conflict("partial publication has no admission"))?;
    if next.repository_id() != previous.repository_id()
        || next.remote_id() != previous.remote_id()
        || next.active_account_id() != previous.active_account_id()
        || next.epoch_id() != previous.epoch_id()
        || next.descriptor().selected_branch.branch_id
            != previous.descriptor().selected_branch.branch_id
        || next.descriptor().global_branch.branch_id
            != previous.descriptor().global_branch.branch_id
    {
        return Err(conflict("candidate belongs to another partial admission"));
    }
    if next.descriptor().cursor < previous.descriptor().cursor {
        return Err(conflict(
            "candidate descriptor regressed behind the admitted cursor",
        ));
    }
    if next.descriptor().cursor == previous.descriptor().cursor
        && next.descriptor() != previous.descriptor()
    {
        return Err(LixError::new(
            super::SYNC_PROTOCOL_MISMATCH_CODE,
            "one authority cursor cannot describe different repository coordinates",
        ));
    }
    let registry = mode
        .read_interests()
        .ok_or_else(|| conflict("partial publication has no retained interests"))?;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await?;
    let (actual, receipt) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(|| conflict("partial receipt disappeared"))?;
    if &actual != previous.as_ref() {
        return Err(conflict("partial publication admission changed"));
    }
    let mut preconditions = super::partial_interest_journal::restore_candidate_read_interests(
        &read, &previous, &registry,
    )
    .await?;
    let mut writes = storage.new_write_set();
    let mut changed = false;
    for (index, branch) in [
        &next.descriptor().selected_branch,
        &next.descriptor().global_branch,
    ]
    .into_iter()
    .enumerate()
    {
        if index == 1 && branch.branch_id == next.descriptor().selected_branch.branch_id {
            continue;
        }
        if branch.branch_id == crate::GLOBAL_BRANCH_ID
            && policy == PartialRecoveryPolicy::NativeGlobalMerge
        {
            let verified =
                super::partial_global_merge_settlement::verify_partial_global_merge_settlement(
                    &read, &previous, &next,
                )
                .await?;
            preconditions.extend(
                super::partial_push_state::stage_settle_partial_global_merge_confirmation(
                    &read,
                    &mut writes,
                    &previous,
                    verified,
                )
                .await?,
            );
            changed = true;
            continue;
        }
        if index == 0 && policy == PartialRecoveryPolicy::NativeMerge {
            let verified = super::partial_merge_settlement::verify_partial_merge_settlement(
                &read, &previous, &next,
            )
            .await?;
            preconditions.extend(
                super::partial_push_state::stage_settle_partial_merge_confirmation(
                    &read,
                    &mut writes,
                    &previous,
                    verified,
                )
                .await?,
            );
            changed = true;
            continue;
        }
        let (push, _, _) = load_partial_push_state(&read, &previous, &branch.branch_id).await?;
        let observed = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(std::slice::from_ref(&branch.branch_id))
            .await?
            .pop()
            .ok_or_else(|| conflict("partial branch observation is absent"))?;
        let control = observed
            .control
            .ok_or_else(|| conflict("partial branch disappeared"))?;
        if push.prepared.is_some()
            || control.head_commit_id != push.confirmed.head
            || control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_ref()
                != Some(&push.confirmed.checkpoint)
        {
            return Err(LixError::new(
                if policy == PartialRecoveryPolicy::ExpiredBaseline {
                    "LIX_PARTIAL_REPLICA_BASELINE_RECOVERY_PENDING"
                } else {
                    "LIX_PARTIAL_REPLICA_REBASE_REQUIRED"
                },
                "remote baseline adoption preserves pending edits until native rebase is prepared",
            ));
        }
        let target = PartialPushCoordinate {
            head: branch.head.commit_id.clone(),
            checkpoint: branch.checkpoint.commit_id.clone(),
        };
        changed |= target != push.confirmed;
        preconditions.extend(
            stage_remote_partial_confirmation(
                &read,
                &mut writes,
                &previous,
                &branch.branch_id,
                &push.confirmed,
                target,
            )
            .await?,
        );
        preconditions.push(crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observed.raw_token,
        )?);
    }
    if !changed && policy == PartialRecoveryPolicy::Normal {
        return Ok(None);
    }
    for branch in [
        &next.descriptor().selected_branch,
        &next.descriptor().global_branch,
    ] {
        if next.serving_generation(&branch.branch_id)?
            == previous.serving_generation(&branch.branch_id)?
        {
            return Err(conflict(
                "candidate must use a fresh local serving generation",
            ));
        }
    }
    let interests = registry.snapshot()?;
    preconditions.push(stage_partial_replica_state(
        &mut writes,
        &next,
        Some(receipt),
    )?);
    crate::catalog::stage_catalog_revision(&mut writes);
    // File path indexes are cached independently of SQL catalog/generation.
    // Rotate in the same atomic publication so warm negative paths and counts
    // cannot keep serving the previous baseline after admission advances.
    crate::filesystem::stage_path_index_revision(&mut writes);
    if next.descriptor().global_branch.head != previous.descriptor().global_branch.head
        || next.descriptor().global_branch.checkpoint
            != previous.descriptor().global_branch.checkpoint
    {
        // The account proof token identifies the visible global account view.
        // Retaining it across a new global baseline could reuse an active proof
        // for a remotely disabled account.
        crate::account::stage_account_revision(&mut writes);
    }
    // This bridge owns and drops the coherent read. Only the exact evaluated
    // staged values and their source/fresh-generation guards can escape.
    let prepared = engine
        .prepare_partial_candidate(read, &next, &interests)
        .await?;
    preconditions.extend(prepared.source_control_guards);
    let candidate_writes = Arc::try_unwrap(prepared.writes)
        .map_err(|_| conflict("candidate evaluator retained a publication write capability"))?;
    writes.extend(candidate_writes);
    Ok(Some(PreparedPartialPublication {
        origin_write_gate: engine.collaboration_write_gate(),
        branch_switch_completion: None,
        previous,
        next,
        interests_revision: interests.revision,
        deadline,
        writes,
        preconditions,
    }))
}

/// Once started, an owned task retains both operation gates through durability
/// and admission publication, even when its caller is cancelled. No network
/// request is made while holding these gates.
pub(super) async fn publish_prepared_partial<S>(
    engine: Arc<Engine<S>>,
    prepared: PreparedPartialPublication,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if !Arc::ptr_eq(
        &engine.collaboration_write_gate(),
        &prepared.origin_write_gate,
    ) {
        return Err(conflict(
            "candidate was prepared by another engine/storage owner",
        ));
    }
    let owner = engine.partial_owner();
    let owner_guard = if owner.is_installed() {
        Some(owner.retain_for_owned_work()?)
    } else {
        None
    };
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let task = super::platform::spawn_sync_task(async move {
        let _owner_guard = owner_guard;
        let result = async {
            let mode = engine.sync_mode();
    mode.ensure_partial_admission_healthy()?;
            let registry = mode
                .read_interests()
                .ok_or_else(|| conflict("partial publication lost interest registry"))?;
            // A caller may hold an explicit transaction longer than the
            // server's candidate pin. Bound both gates by the original HTTP
            // request deadline, including after caller cancellation.
            let gates = async {
                let interest = registry.begin_publication(prepared.interests_revision).await?;
                let write = engine.collaboration_write_gate().lock_owned().await;
                Ok::<_, LixError>((interest, write))
            };
            let expiry = super::platform::sleep(prepared.deadline.remaining()?);
            futures_util::pin_mut!(gates, expiry);
            let (_interest_guard, _write_guard) = match futures_util::future::select(gates, expiry).await {
                futures_util::future::Either::Left((result, _)) => result?,
                futures_util::future::Either::Right(_) => return Err(LixError::new(
                    "LIX_PARTIAL_CANDIDATE_EXPIRED",
                    "candidate expired while waiting for local operations; prepare a fresh candidate",
                )),
            };
            prepared.deadline.check(&prepared.next.baseline_lease().lease_id)?;
            mode.ensure_partial_admission_healthy()?;
            if mode.partial_admission().as_deref() != Some(prepared.previous.as_ref()) {
                return Err(conflict("another candidate changed the partial admission"));
            }
            let storage = engine.storage();
            let result = storage
                .commit_partial_replica_write_set(
                    super::partial_replica_write_capability(),
                    prepared.writes,
                    StorageWriteOptions {
                        await_durable: true,
                        preconditions: prepared.preconditions,
                        ..Default::default()
                    },
                )
                .await;
            match result {
                Ok(_) => {
                    if let Some(completion) = &prepared.branch_switch_completion {
                        if let Err(error) = completion.complete() {
                            mode.fail_partial_admission(error.clone());
                            engine.fail_observers(error.clone());
                            return Err(error);
                        }
                        mode.admit_partial_replica(prepared.next.clone(), super::partial_replica_write_capability());
                    }
                    // Durability may have stalled past the server pin. The
                    // committed receipt cannot be rolled back, but it must not
                    // become an active admission without renewed retention.
                    if let Err(error) = prepared.deadline.check(&prepared.next.baseline_lease().lease_id) {
                        mode.fail_partial_admission(error.clone());
                        engine.fail_observers(error.clone());
                        return Err(error);
                    }
                    mode.admit_partial_replica(
                        prepared.next,
                        super::partial_replica_write_capability(),
                    );
                    engine.notify_observers();
                    Ok(())
                }
                Err(error) => {
                    // A backend may report an ambiguous commit. Fail observers
                    // closed; the exact durable receipt fences further writes.
                    let error: LixError = error.into();
                    if error.code == LixError::CODE_STORAGE_COMMIT_OUTCOME_UNKNOWN {
                        mode.fail_partial_admission(error.clone());
                        engine.fail_observers(error.clone());
                    }
                    Err(error)
                }
            }
        }
        .await;
        let _ = sender.send(result);
    })?;
    let result = receiver
        .await
        .map_err(|_| conflict("partial publication task stopped before reporting its outcome"))?;
    task.join().await?;
    result
}

/// Same serving roots, new authority pin: publish only the receipt. The exact
/// source/gate/deadline contracts are the same as root adoption.
pub(super) async fn prepare_clean_lease_reacquisition<S>(
    engine: &Engine<S>,
    wire: &super::LeasedPartialReplicaDescriptor,
    deadline: CandidateBaselineDeadline,
) -> Result<PreparedPartialPublication, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let mode = engine.sync_mode();
    mode.ensure_partial_admission_healthy()?;
    let previous = mode
        .partial_admission()
        .ok_or_else(|| conflict("lease recovery has no admission"))?;
    wire.validate(
        previous.repository_id(),
        previous.active_account_id(),
        Some(&previous.descriptor().selected_branch.branch_id),
    )?;
    deadline.check(&wire.lease.lease_id)?;
    if wire.descriptor.cursor < previous.descriptor().cursor {
        return Err(conflict("lease recovery cursor regressed"));
    }
    if !same_serving_basis(previous.descriptor(), &wire.descriptor) {
        return Err(conflict("lease-only recovery cannot replace serving roots"));
    }
    let next = Arc::new(previous.with_reacquired_baseline_lease(wire.lease.clone())?);
    let registry = mode
        .read_interests()
        .ok_or_else(|| conflict("lease recovery has no interest registry"))?;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await?;
    let (actual, receipt) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(|| conflict("lease recovery receipt absent"))?;
    if &actual != previous.as_ref() {
        return Err(conflict("lease recovery source changed"));
    }
    let mut writes = storage.new_write_set();
    let mut guards = Vec::new();
    for (index, branch) in [
        &previous.descriptor().selected_branch,
        &previous.descriptor().global_branch,
    ]
    .into_iter()
    .enumerate()
    {
        if index == 1 && branch.branch_id == previous.descriptor().selected_branch.branch_id {
            continue;
        }
        let (push, _, _) = load_partial_push_state(&read, &previous, &branch.branch_id).await?;
        let observed = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load_observed(std::slice::from_ref(&branch.branch_id))
            .await?
            .pop()
            .ok_or_else(|| conflict("lease recovery control absent"))?;
        let control = observed
            .control
            .ok_or_else(|| conflict("lease recovery control absent"))?;
        if push.prepared.is_some()
            || control.head_commit_id != push.confirmed.head
            || control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_ref()
                != Some(&push.confirmed.checkpoint)
        {
            return Err(LixError::new(
                "LIX_PARTIAL_REPLICA_BASELINE_RECOVERY_PENDING",
                "expired baseline recovery preserves the pending local suffix until native rebase",
            ));
        }
        guards.push(crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observed.raw_token,
        )?);
        // Reuse the push owner's guarded observation; discard its identical
        // staged bytes so lease-only publication writes no push state.
        let mut unchanged_push = storage.new_write_set();
        guards.extend(
            stage_remote_partial_confirmation(
                &read,
                &mut unchanged_push,
                &previous,
                &branch.branch_id,
                &push.confirmed,
                push.confirmed.clone(),
            )
            .await?,
        );
    }
    guards.push(stage_partial_replica_state(
        &mut writes,
        &next,
        Some(receipt),
    )?);
    let interests_revision = registry.snapshot()?.revision;
    Ok(PreparedPartialPublication {
        origin_write_gate: engine.collaboration_write_gate(),
        branch_switch_completion: None,
        previous,
        next,
        interests_revision,
        deadline,
        writes,
        preconditions: guards,
    })
}

pub(super) fn same_serving_basis(
    left: &super::PartialReplicaDescriptor,
    right: &super::PartialReplicaDescriptor,
) -> bool {
    left.lix_id == right.lix_id
        && left.selected_branch.branch_id == right.selected_branch.branch_id
        && left.global_branch.branch_id == right.global_branch.branch_id
        && left.selected_branch.head == right.selected_branch.head
        && left.selected_branch.checkpoint == right.selected_branch.checkpoint
        && left.global_branch.head == right.global_branch.head
        && left.global_branch.checkpoint == right.global_branch.checkpoint
}

// Append within partial_publication. Existing same-branch publisher checks
// remain unchanged; this distinct proof owns explicit selected-scope admission.
pub(super) async fn require_clean_switch_source<S>(
    engine: &Engine<S>,
    previous: &PartialReplicaState,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await?;
    super::partial_push_state::clean_branch_source_guards(&read, previous).await?;
    Ok(())
}

pub(super) async fn prepare_branch_switch_publication<S>(
    engine: &Engine<S>,
    next: Arc<PartialReplicaState>,
    deadline: CandidateBaselineDeadline,
) -> Result<PreparedPartialPublication, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    deadline.check(&next.baseline_lease().lease_id)?;
    let mode = engine.sync_mode();
    mode.ensure_partial_admission_healthy()?;
    let previous = mode
        .partial_admission()
        .ok_or_else(|| conflict("branch switch has no current admission"))?;
    if next.repository_id() != previous.repository_id()
        || next.remote_id() != previous.remote_id()
        || next.active_account_id() != previous.active_account_id()
        || next.epoch_id() != previous.epoch_id()
        || next.descriptor().global_branch.branch_id
            != previous.descriptor().global_branch.branch_id
        || next.descriptor().cursor < previous.descriptor().cursor
    {
        return Err(conflict(
            "branch switch candidate identity or cursor differs",
        ));
    }
    let registry = mode
        .read_interests()
        .ok_or_else(|| conflict("branch switch has no interest registry"))?;
    let storage = engine.storage();
    let read = storage.begin_read(Default::default()).await?;
    let (actual, receipt) = load_partial_replica_state(&read)
        .await?
        .ok_or_else(|| conflict("branch switch receipt disappeared"))?;
    if &actual != previous.as_ref() {
        return Err(conflict("branch switch admission changed"));
    }
    let mut preconditions =
        super::partial_push_state::clean_branch_source_guards(&read, &previous).await?;
    preconditions.extend(
        super::partial_interest_journal::restore_candidate_read_interests(
            &read, &previous, &registry,
        )
        .await?,
    );
    let mut writes = storage.new_write_set();
    let mut visited = std::collections::BTreeSet::new();
    for branch in [
        &next.descriptor().selected_branch,
        &next.descriptor().global_branch,
    ] {
        if !visited.insert(branch.branch_id.clone()) {
            continue;
        }
        let observation =
            crate::branch::observe_branch_control_coordinate(&read, &branch.branch_id).await?;
        preconditions.extend(
            super::partial_push_state::stage_admitted_branch_coordinate(
                &read,
                &mut writes,
                &previous,
                branch,
                &observation,
            )
            .await?,
        );
        preconditions.push(crate::branch::branch_head_control_precondition(
            &branch.branch_id,
            observation.raw_token,
        )?);
    }
    let interests = registry.snapshot()?;
    preconditions.push(stage_partial_replica_state(
        &mut writes,
        &next,
        Some(receipt),
    )?);
    crate::catalog::stage_catalog_revision(&mut writes);
    crate::filesystem::stage_path_index_revision(&mut writes);
    if next.descriptor().global_branch != previous.descriptor().global_branch {
        crate::account::stage_account_revision(&mut writes);
    }
    // Only this owner allows an authenticated target with no physical control.
    // Existing target local work was checked against its archived confirmation.
    let prepared = engine
        .prepare_partial_branch_candidate(read, &next, &interests)
        .await?;
    preconditions.extend(prepared.source_control_guards);
    writes.extend(
        Arc::try_unwrap(prepared.writes)
            .map_err(|_| conflict("branch candidate retained its staged write capability"))?,
    );
    Ok(PreparedPartialPublication {
        origin_write_gate: engine.collaboration_write_gate(),
        previous,
        next,
        interests_revision: interests.revision,
        deadline,
        writes,
        preconditions,
        branch_switch_completion: None,
    })
}

impl PreparedPartialPublication {
    pub(super) fn with_branch_switch_completion(
        mut self,
        completion: super::partial_branch_switch::PartialBranchSwitchCompletion,
    ) -> Self {
        self.branch_switch_completion = Some(completion);
        self
    }
}
