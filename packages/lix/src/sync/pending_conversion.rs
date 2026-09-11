//! Explicit migration reconciliation; original full source is read-only.
use super::*;
use crate::migration::PendingConversionJournal;
use crate::storage_adapter::{Storage, StorageAdapter};
pub(crate) trait ConversionJournalOwner {
    fn current(&self) -> &PendingConversionJournal;
    fn publish(&mut self, next: PendingConversionJournal) -> SyncTransportFuture<'_, ()>;
    fn publish_global_successor(
        &mut self,
        _next: PendingConversionJournal,
        _proof: MigrationGlobalSuccessor,
    ) -> SyncTransportFuture<'_, ()> {
        Box::pin(async {
            Err(unresolved(
                "migration journal owner does not admit a changed global successor",
            ))
        })
    }
}
/// Created only after exact terminal restart plus authenticated native ancestry.
/// Authority native merge admission still validates the full candidate catalog,
/// original selections, uniqueness, foreign keys and plugins under new G.
pub(crate) struct MigrationGlobalSuccessor {
    old: PartialMergeRequest,
    next: PartialMergeRequest,
    receipt: PartialAttemptRestartReceipt,
}
impl MigrationGlobalSuccessor {
    pub(crate) fn validate_transition(
        &self,
        previous: &PendingConversionJournal,
        next: &PendingConversionJournal,
        repository: &str,
        account: &str,
    ) -> Result<(), LixError> {
        let intent = previous
            .restart
            .as_ref()
            .ok_or_else(|| unresolved("successor lacks durable restart intent"))?;
        self.receipt.validate_for(repository, account, intent)?;
        if previous.request != self.old
            || next.request != self.next
            || previous.restart_receipt.as_ref() != Some(&self.receipt)
            || previous.native_source_pin.is_none()
            || previous.native_source_pin != next.native_source_pin
            || next.request.attempt_id != intent.next_attempt_id
        {
            return Err(unresolved(
                "global successor proof differs from exact journal transition",
            ));
        }
        let mut frozen = self.next.clone();
        frozen.attempt_id = self.old.attempt_id.clone();
        frozen.expected_authority_head_commit_id =
            self.old.expected_authority_head_commit_id.clone();
        frozen.global_head_commit_id = self.old.global_head_commit_id.clone();
        if frozen != self.old {
            return Err(unresolved(
                "global successor changed original source coordinates",
            ));
        }
        Ok(())
    }
}
pub(crate) struct ReconciledPendingConversion {
    manifest_digest: [u8; 32],
    source_branch: String,
    source_head: String,
    state: PartialReplicaState,
    deadline: super::http::CandidateBaselineDeadline,
}
impl ReconciledPendingConversion {
    pub(crate) fn source_coordinate(&self) -> (&str, &str) {
        (&self.source_branch, &self.source_head)
    }
    pub(crate) fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub(crate) fn state(&self) -> &PartialReplicaState {
        &self.state
    }
    pub(crate) fn check_deadline(&self) -> Result<(), LixError> {
        self.deadline.check(&self.state.baseline_lease().lease_id)
    }
}
fn unresolved(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_CONVERSION_UNRESOLVED", message)
        .with_details(serde_json::json!({"sourcePreserved":true,"retrySameConversion":true}))
}
fn id(value: &str) -> Result<crate::changelog::CommitId, LixError> {
    crate::changelog::CommitId::parse_lix(value, "conversion native coordinate")
}
pub(super) async fn graph<C: super::http::RawHttpClient + Clone + 'static>(
    transport: &super::http::HttpSyncTransport<C>,
    commit: &str,
) -> Result<crate::changelog::CommitRecord, LixError> {
    let response = transport
        .native_metadata(&super::native_metadata::NativeMetadataRequest {
            epoch_id: uuid::Uuid::now_v7().to_string(),
            objects: vec![crate::tracked_state::NativeMetadataRef::CommitGraphRecord(
                commit.into(),
            )],
        })
        .await?;
    let object = response
        .objects
        .first()
        .ok_or_else(|| unresolved("conversion native graph response is empty"))?;
    crate::commit_graph::validate_native_commit_graph_record(id(commit)?, &object.bytes)?;
    crate::storage_codec::decode("conversion native graph", &object.bytes)
}
pub(super) async fn includes<C: super::http::RawHttpClient + Clone + 'static>(
    transport: &super::http::HttpSyncTransport<C>,
    ancestor: &str,
    head: &str,
) -> Result<bool, LixError> {
    let ancestor_record = graph(transport, ancestor).await?;
    let mut pending = vec![head.to_owned()];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(commit) = pending.pop() {
        if commit == ancestor {
            return Ok(true);
        }
        if !seen.insert(commit.clone()) {
            continue;
        }
        if seen.len() > 4096 {
            return Err(unresolved(
                "conversion ancestry exceeds bounded proof budget",
            ));
        }
        let record = graph(transport, &commit).await?;
        if record.generation <= ancestor_record.generation {
            continue;
        }
        if record.parent_commit_ids.len() == 1
            && record.first_parent_jump_span > 0
            && record
                .generation
                .checked_sub(record.first_parent_jump_span)
                .is_some_and(|generation| generation >= ancestor_record.generation)
        {
            // Native Myers jumps cover only a certified linear corridor; merge
            // nodes reset the lane, so no causal side parent is skipped.
            let jump = graph(transport, &record.first_parent_jump_commit_id.to_string()).await?;
            if record.generation.checked_sub(record.first_parent_jump_span) != Some(jump.generation)
            {
                return Err(unresolved(
                    "conversion native jump generation is inconsistent",
                ));
            }
            pending.push(jump.commit_id.to_string());
        } else {
            pending.extend(
                record
                    .parent_commit_ids
                    .into_iter()
                    .map(|id| id.to_string()),
            );
        }
    }
    Ok(false)
}
fn validate_descriptor(
    request: &PartialMergeRequest,
    descriptor: &PartialReplicaDescriptor,
) -> Result<(), LixError> {
    if descriptor.selected_branch.branch_id != request.branch_id
        || descriptor.selected_branch.checkpoint.commit_id != request.checkpoint_commit_id
        || descriptor.global_branch.head.commit_id != request.global_head_commit_id
        || descriptor.global_branch.checkpoint.commit_id != request.global_checkpoint_commit_id
    {
        return Err(unresolved(
            "catalog or checkpoint changed; original source and exact native outcomes remain retained",
        ));
    }
    Ok(())
}
async fn restart<C: super::http::RawHttpClient + Clone + 'static>(
    journal: &mut impl ConversionJournalOwner,
    transport: &super::http::HttpSyncTransport<C>,
) -> Result<(), LixError> {
    let current = journal.current().clone();
    let intent = current
        .restart
        .clone()
        .unwrap_or_else(|| PartialAttemptRestartRequest {
            old: current.request.clone(),
            next_attempt_id: uuid::Uuid::now_v7().to_string(),
        });
    if current.restart.is_none() {
        let mut next = current.clone();
        next.restart = Some(intent.clone());
        journal.publish(next).await?;
    }
    if journal.current().restart_receipt.is_none() {
        let outcome = transport.restart_partial_attempt(&intent).await?;
        let mut next = journal.current().clone();
        match outcome {
            PartialAttemptRestartOutcome::Committed { receipt, .. } => {
                next.receipt = Some(receipt);
                next.accepted_tip = next.request.captured_local_head_commit_id.clone();
                next.prepared_tip = None;
                next.restart = None;
                next.restart_receipt = None;
                journal.publish(next).await?;
                return Ok(());
            }
            PartialAttemptRestartOutcome::Restarted { receipt } => {
                next.restart_receipt = Some(receipt);
                journal.publish(next).await?;
            }
        }
    }
    let descriptor = transport
        .partial_replica_descriptor(Some(&intent.old.branch_id))
        .await?;
    let changed_global =
        descriptor.wire.descriptor.global_branch.head.commit_id != intent.old.global_head_commit_id;
    if changed_global {
        let view = &descriptor.wire.descriptor;
        if journal.current().native_source_pin.is_none()
            || view.selected_branch.branch_id != intent.old.branch_id
            || view.selected_branch.checkpoint.commit_id != intent.old.checkpoint_commit_id
            || view.global_branch.checkpoint.commit_id != intent.old.global_checkpoint_commit_id
        {
            return Err(unresolved(
                "global successor requires native source pin and unchanged branch/checkpoints",
            ));
        }
        descriptor.deadline.check(&descriptor.wire.lease.lease_id)?;
        let leased = transport.fork_native_baseline_lease(&descriptor.wire.lease)?;
        if !includes(
            &leased,
            &intent.old.global_head_commit_id,
            &view.global_branch.head.commit_id,
        )
        .await?
        {
            return Err(unresolved(
                "new global basis is not a descendant of the terminally fenced attempt",
            ));
        }
        descriptor.deadline.check(&descriptor.wire.lease.lease_id)?;
    } else {
        validate_descriptor(&intent.old, &descriptor.wire.descriptor)?;
    }
    let previous = journal.current().clone();
    let mut next = journal.current().clone();
    next.request.attempt_id = intent.next_attempt_id.clone();
    if changed_global {
        next.request.global_head_commit_id = descriptor
            .wire
            .descriptor
            .global_branch
            .head
            .commit_id
            .clone();
    }
    next.request.expected_authority_head_commit_id =
        descriptor.wire.descriptor.selected_branch.head.commit_id;
    if next.native_source_pin.is_none() {
        next.accepted_tip = next.request.base_commit_id.clone();
    }
    next.prepared_tip = None;
    next.restart = None;
    next.restart_receipt = None;
    if changed_global {
        let proof = MigrationGlobalSuccessor {
            old: intent.old,
            next: next.request.clone(),
            receipt: previous
                .restart_receipt
                .ok_or_else(|| unresolved("global successor lacks durable terminal receipt"))?,
        };
        journal.publish_global_successor(next, proof).await
    } else {
        journal.publish(next).await
    }
}
/// All waves are immutable source exports; every prepared target and accepted
/// outcome becomes durable before proceeding. Cancellation leaves resumable
/// source-bank keyed state, including exact restart UUIDs.
pub(crate) async fn reconcile_pending_conversion<S, C>(
    source: &StorageAdapter<S>,
    journal: &mut impl ConversionJournalOwner,
    transport: &super::http::HttpSyncTransport<C>,
    remote: &str,
    epoch: &str,
) -> Result<ReconciledPendingConversion, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::http::RawHttpClient + Clone + 'static,
{
    loop {
        let current = journal.current().clone();
        if current.restart.is_some() {
            restart(journal, transport).await?;
            continue;
        }
        // An ordinary ACK may have been lost before conversion began. An exact
        // authenticated native L head is already a valid inclusion proof, not M=[L,L].
        if current.receipt.is_some()
            || current.request.expected_authority_head_commit_id
                == current.request.captured_local_head_commit_id
        {
            break;
        }
        if current.accepted_tip == current.request.captured_local_head_commit_id {
            let result = if let Some(pin) = &current.native_source_pin {
                transport
                    .merge_native_migration(&super::NativeMigrationMergeRequest {
                        request: current.request.clone(),
                        source_branch_id: pin.clone(),
                    })
                    .await
            } else {
                transport.merge_partial_replica(&current.request).await
            };
            match result {
                Ok(receipt) => {
                    let mut next = current;
                    next.receipt = Some(receipt);
                    journal.publish(next).await?;
                    break;
                }
                Err(error)
                    if matches!(
                        error.code.as_str(),
                        "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED"
                            | "LIX_PARTIAL_ATTEMPT_RESTARTED"
                            | "LIX_PARTIAL_MERGE_AUTHORITY_CHANGED"
                    ) =>
                {
                    restart(journal, transport).await?;
                    continue;
                }
                Err(error) => return Err(error),
            }
        }
        if let Some(pin) = &current.native_source_pin {
            let read = source.begin_read(Default::default()).await?;
            let wave = super::native_migration_pin_upload::native_migration_pin_wave(
                &read,
                &current.request,
                pin,
                &current.accepted_tip,
                current.prepared_tip.as_deref(),
                current.accepted_tip != current.request.base_commit_id,
            )
            .await?;
            drop(read);
            let target = wave.ref_updates[0]
                .head_commit_id
                .clone()
                .ok_or_else(|| unresolved("migration wave omitted source pin target"))?;
            let mut next = current.clone();
            next.prepared_tip = Some(target.clone());
            journal.publish(next).await?;
            super::native_migration_pin_upload::push_native_migration_with_blobs(
                source,
                transport.active_account_id(),
                transport,
                &wave,
            )
            .await?;
            let mut next = journal.current().clone();
            next.accepted_tip = target;
            next.prepared_tip = None;
            journal.publish(next).await?;
            continue;
        }
        let prepared = current.prepared_tip.as_ref().map(|target| {
            super::partial_merge_state::PreparedMergeBodyWave {
                previous: current.accepted_tip.clone(),
                target: target.clone(),
            }
        });
        let read = source.begin_read(Default::default()).await?;
        let wave = super::partial_merge_runtime::captured_wave(
            &read,
            &current.request,
            &current.accepted_tip,
            prepared.as_ref(),
        )
        .await?;
        drop(read);
        let target = wave
            .bodies
            .commits
            .last()
            .ok_or_else(|| unresolved("empty conversion native wave"))?
            .commit_id
            .clone();
        let mut next = current.clone();
        next.prepared_tip = Some(target.clone());
        journal.publish(next).await?;
        match transport.retained_body_wave(&wave).await {
            Ok(response) => {
                if response.accepted_tip != target {
                    return Err(unresolved(
                        "authority changed conversion wave acknowledgment",
                    ));
                }
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "LIX_NATIVE_UPLOAD_ATTEMPT_EXPIRED" | "LIX_PARTIAL_ATTEMPT_RESTARTED"
                ) =>
            {
                restart(journal, transport).await?;
                continue;
            }
            Err(error) => return Err(error),
        }
        let mut next = journal.current().clone();
        next.accepted_tip = target;
        next.prepared_tip = None;
        journal.publish(next).await?;
    }
    let current = journal.current();
    let wrapper = transport
        .partial_replica_descriptor(Some(&current.request.branch_id))
        .await?;
    // A durable exact M is validated below against its original global base.
    // Later authority catalog advances do not invalidate that native outcome;
    // they require a separate authenticated ancestry proof, never a new request.
    if current.receipt.is_some() {
        let descriptor = &wrapper.wire.descriptor;
        if descriptor.selected_branch.branch_id != current.request.branch_id
            || descriptor.selected_branch.checkpoint.commit_id
                != current.request.checkpoint_commit_id
            || descriptor.global_branch.checkpoint.commit_id
                != current.request.global_checkpoint_commit_id
        {
            return Err(unresolved("recovered outcome checkpoint or branch changed"));
        }
    } else {
        validate_descriptor(&current.request, &wrapper.wire.descriptor)?;
    }
    wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
    let leased = transport.fork_native_baseline_lease(&wrapper.wire.lease)?;
    if current.receipt.is_some()
        && current.request.global_head_commit_id
            != wrapper.wire.descriptor.global_branch.head.commit_id
    {
        if !includes(
            &leased,
            &current.request.global_head_commit_id,
            &wrapper.wire.descriptor.global_branch.head.commit_id,
        )
        .await?
        {
            return Err(unresolved(
                "current global state no longer retains the recovered merge basis",
            ));
        }
    }
    let included = if let Some(receipt) = &current.receipt {
        receipt.validate_for(&current.request)?;
        let native = graph(&leased, &receipt.merge_commit_id).await?;
        if native.is_checkpoint
            || native.parent_commit_ids
                != vec![
                    id(&current.request.expected_authority_head_commit_id)?,
                    id(&current.request.captured_local_head_commit_id)?,
                ]
            || native.account_id != transport.active_account_id()
            || native.base_commit_id != Some(id(&current.request.global_head_commit_id)?)
        {
            return Err(unresolved(
                "authority native M does not include original source L",
            ));
        }
        receipt.merge_commit_id.as_str()
    } else {
        current.request.captured_local_head_commit_id.as_str()
    };
    if !includes(
        &leased,
        included,
        &wrapper.wire.descriptor.selected_branch.head.commit_id,
    )
    .await?
    {
        return Err(unresolved(
            "authority no longer descends from the exact recovered native outcome",
        ));
    }
    wrapper.deadline.check(&wrapper.wire.lease.lease_id)?;
    let state = PartialReplicaState::from_leased(
        remote.into(),
        transport.active_account_id().into(),
        epoch.into(),
        wrapper.wire,
    )?;
    Ok(ReconciledPendingConversion {
        manifest_digest: current.manifest_digest,
        source_branch: current.request.branch_id.clone(),
        source_head: current.request.captured_local_head_commit_id.clone(),
        state,
        deadline: wrapper.deadline,
    })
}

pub(crate) async fn reconcile_pending_conversion_authenticated<
    S: Storage + Clone + Send + Sync + 'static,
>(
    source: &StorageAdapter<S>,
    journal: &mut impl ConversionJournalOwner,
    authenticated: &AuthenticatedPartialConversion,
) -> Result<ReconciledPendingConversion, LixError> {
    use futures_util::FutureExt as _;
    let server = authenticated.server();
    let transport = super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        if transport.lix_id() != authenticated.state().repository_id()
            || transport.active_account_id() != authenticated.state().active_account_id()
        {
            return Err(unresolved(
                "migration connection changed authenticated repository/account",
            ));
        }
        reconcile_pending_conversion(
            source,
            journal,
            &transport,
            &server.url,
            authenticated.state().epoch_id(),
        )
        .await
    }
    .await;
    let close = transport.close_session().fuse();
    let timeout = super::platform::sleep(std::time::Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, timeout);
    futures_util::select_biased! {_=close=>{},_=timeout=>{}};
    result
}

/// Cleanup is best effort after durable epoch publication; cancellation keeps
/// the original journal coordinates available for an exact retry.
pub(crate) async fn cleanup_pending_conversion_authenticated(
    authenticated: &AuthenticatedPartialConversion,
    journal: &crate::migration::PendingConversionJournal,
) -> Result<(), LixError> {
    let pin = journal
        .native_source_pin
        .as_ref()
        .ok_or_else(|| unresolved("cleanup journal has no native source pin"))?;
    if journal.receipt.is_none() {
        return Err(unresolved("cleanup lacks a durable native merge outcome"));
    }
    let server = authenticated.server();
    let transport = super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        if transport.lix_id() != authenticated.state().repository_id()
            || transport.active_account_id() != authenticated.state().active_account_id()
        {
            return Err(unresolved("cleanup authority identity changed"));
        }
        transport
            .cleanup_native_migration(&super::NativeMigrationCleanupRequest {
                migration: super::NativeMigrationMergeRequest {
                    request: journal.request.clone(),
                    source_branch_id: pin.clone(),
                },
            })
            .await?;
        Ok(())
    }
    .await;
    let _ = transport.close_session().await;
    result
}
pub(crate) async fn finish_conversion_cleanup_bounded<
    F: std::future::Future<Output = Result<(), LixError>>,
>(
    cleanup: F,
) {
    use futures_util::FutureExt as _;
    let cleanup = cleanup.fuse();
    let timeout = super::platform::sleep(std::time::Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(cleanup, timeout);
    futures_util::select_biased! {_=cleanup=>{},_=timeout=>{}};
}
