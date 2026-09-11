//! Explicit migration network work; source owner retains its closed-storage claim.
use super::*;
use crate::changelog::CommitId;
use crate::migration::GlobalConversionJournal;
use crate::storage_adapter::{Storage, StorageAdapter, StorageAdapterRead};
use std::collections::{BTreeMap, BTreeSet};
pub(crate) trait GlobalConversionJournalOwner {
    fn current(&self) -> &GlobalConversionJournal;
    fn publish(&mut self, next: GlobalConversionJournal) -> SyncTransportFuture<'_, ()>;
}
fn unresolved(s: &str) -> LixError {
    LixError::new("LIX_PARTIAL_CONVERSION_UNRESOLVED", s)
        .with_details(serde_json::json!({"sourcePreserved":true,"retrySameConversion":true}))
}
fn id(s: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(s, "global conversion coordinate")
}
/// Choose an actual complete published authority boundary from the frozen
/// manifest. Original C is a checkpoint to preserve, not a invented remote ref.
pub(crate) async fn global_new_branch_upload_boundaries(
    read: &(impl StorageAdapterRead + ?Sized),
    manifest: &FullConversionManifest,
    request: &NativeGlobalMigrationRequest,
) -> Result<BTreeMap<String, String>, LixError> {
    let boundaries = manifest
        .branches
        .iter()
        .filter(|b| b.branch_id != crate::GLOBAL_BRANCH_ID)
        .filter_map(|b| b.confirmed.as_ref())
        .flat_map(|c| [&c.head, &c.checkpoint])
        .map(|s| id(s))
        .collect::<Result<BTreeSet<_>, _>>()?;
    let mut result = BTreeMap::from([(
        crate::GLOBAL_BRANCH_ID.into(),
        request.base_commit_id.clone(),
    )]);
    for branch in &request.new_branches {
        let mut cursor = id(&branch.head_commit_id)?;
        let mut seen = BTreeSet::new();
        while !boundaries.contains(&cursor) {
            if seen.len() >= 65536 || !seen.insert(cursor) {
                return Err(unresolved(
                    "new branch has no ordinary published authority boundary within migration limit",
                ));
            }
            let node = super::partial_merge_analysis::record(read, cursor, true).await?;
            if node.is_checkpoint
                || node.parent_commit_ids.len() != 1
                || node.account_id != manifest.account_id
                || node.base_commit_id.is_none()
            {
                return Err(unresolved(
                    "new branch recovery requires history outside ordinary account-stable migration",
                ));
            }
            cursor = node.parent_commit_ids[0];
        }
        result.insert(branch.branch_id.clone(), cursor.to_string());
    }
    Ok(result)
}
async fn prepare_wave(
    read: &(impl StorageAdapterRead + ?Sized),
    journal: &GlobalConversionJournal,
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
            super::partial_upload::wave_target(read, id(final_head)?, id(&frontier.accepted)?, 32)
                .await?
        }
    };
    let mut cursor = target;
    let mut reverse = Vec::new();
    let mut seen = BTreeSet::new();
    while cursor != id(&frontier.accepted)? {
        if reverse.len() == 32 || !seen.insert(cursor) {
            return Err(unresolved("global prepared wave exceeded exact bound"));
        }
        let commit = super::commit::load_sync_commit(read, cursor)
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
async fn reconcile_native_global_attempt<S, C>(
    storage: &StorageAdapter<S>,
    transport: &super::http::HttpSyncTransport<C>,
    journal: &mut impl GlobalConversionJournalOwner,
) -> Result<NativeGlobalMigrationReceipt, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::http::RawHttpClient + Clone + 'static,
{
    journal.current().validate()?;
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
                super::native_migration_pin_upload::prepare_native_migration_blobs(
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
    let baseline = transport
        .partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
        .await?;
    baseline.deadline.check(&baseline.wire.lease.lease_id)?;
    let leased = transport.fork_native_baseline_lease(&baseline.wire.lease)?;
    let node = super::pending_conversion::graph(&leased, &receipt.merge_commit_id).await?;
    baseline.deadline.check(&baseline.wire.lease.lease_id)?;
    if node.base_commit_id.is_some()
        || node.account_id != transport.active_account_id()
        || node.parent_commit_ids
            != vec![
                id(&receipt.request.expected_authority_head_commit_id)?,
                id(&receipt.request.captured_local_head_commit_id)?,
            ]
    {
        return Err(unresolved(
            "global native outcome does not preserve captured original parents",
        ));
    }
    Ok(receipt)
}
async fn recover_global_restart<S, C>(
    storage: &StorageAdapter<S>,
    transport: &super::http::HttpSyncTransport<C>,
    manifest: &FullConversionManifest,
    journal: &mut impl GlobalConversionJournalOwner,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::http::RawHttpClient + Clone + 'static,
{
    if journal.current().restart_intent.is_none() {
        journal
            .publish(
                journal
                    .current()
                    .prepare_restart(uuid::Uuid::now_v7().to_string())?,
            )
            .await?;
    }
    if journal.current().restart_receipt.is_none() {
        let outcome = transport
            .restart_native_global_migration(journal.current().restart_intent.as_ref().unwrap())
            .await?;
        journal
            .publish(journal.current().acknowledge_restart(outcome)?)
            .await?;
    }
    if journal.current().receipt.is_some() {
        return Ok(());
    }
    let current = journal.current().clone();
    let Some(NativeGlobalRestartReceipt::Restarted { intent }) = &current.restart_receipt else {
        return Err(unresolved("global restart outcome missing"));
    };
    let fresh = transport
        .partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
        .await?;
    if fresh.wire.descriptor.lix_id != manifest.repository_id
        || fresh.wire.descriptor.global_branch.checkpoint.commit_id
            != current.request.checkpoint_commit_id
    {
        return Err(unresolved(
            "authority identity/checkpoint changed across terminal global restart",
        ));
    }
    let mut request = current.request.clone();
    request.attempt_id = intent.next_attempt_id.clone();
    request.expected_authority_head_commit_id = fresh.wire.descriptor.global_branch.head.commit_id;
    let read = storage.begin_read(Default::default()).await?;
    let boundaries = global_new_branch_upload_boundaries(&read, manifest, &request).await?;
    drop(read);
    journal
        .publish(current.capture_successor(request, boundaries)?)
        .await
}

/// Resume durable intent before any old-attempt request. Transport uncertainty
/// keeps the same exact attempt; only a terminal authority restart receipt can
/// authorize changing R or resetting acknowledged native body frontiers.
pub(crate) async fn reconcile_native_global_conversion<S, C>(
    storage: &StorageAdapter<S>,
    transport: &super::http::HttpSyncTransport<C>,
    manifest: &FullConversionManifest,
    journal: &mut impl GlobalConversionJournalOwner,
) -> Result<NativeGlobalMigrationReceipt, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: super::http::RawHttpClient + Clone + 'static,
{
    for _ in 0..8 {
        if journal.current().restart_intent.is_some() {
            recover_global_restart(storage, transport, manifest, journal).await?;
        }
        match reconcile_native_global_attempt(storage, transport, journal).await {
            Ok(receipt) => return Ok(receipt),
            Err(error) => {
                // These are definitive native refusals, not an unknown network outcome.
                // Fetching the current coordinate decides whether retrying at a new R is
                // useful. The terminal abort itself remains independent of current R.
                if !matches!(
                    error.code.as_str(),
                    "LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED"
                        | "LIX_MIGRATION_GLOBAL_BODY_INVALID"
                        | "LIX_MIGRATION_GLOBAL_ATTEMPT_RESTARTED"
                ) {
                    return Err(error);
                }
                let fresh = transport
                    .partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
                    .await?;
                let changed = fresh.wire.descriptor.global_branch.head.commit_id
                    != journal.current().request.expected_authority_head_commit_id;
                if journal.current().restart_intent.is_none() {
                    journal
                        .publish(
                            journal
                                .current()
                                .prepare_restart(uuid::Uuid::now_v7().to_string())?,
                        )
                        .await?;
                }
                let outcome = transport
                    .restart_native_global_migration(
                        journal.current().restart_intent.as_ref().unwrap(),
                    )
                    .await?;
                journal
                    .publish(journal.current().acknowledge_restart(outcome)?)
                    .await?;
                if journal.current().receipt.is_some() {
                    continue;
                }
                // Durable abort releases any imported roots and fences delayed requests.
                // Preserve that outcome on a permanent validation refusal so explicit
                // retry can recover without leaking the prior attempt's retention pin.
                if !changed {
                    return Err(error);
                }
            }
        }
    }
    Err(unresolved(
        "authority changed repeatedly during explicit global migration; retry preserves the frozen source and exact journal",
    ))
}

// sync::native_global_conversion_driver additions.
/// Non-serializable proof: an authenticated native M contains captured global L
/// and atomically published every exact new branch coordinate in this request.
pub(crate) struct ReconciledGlobalConversion {
    manifest_digest: [u8; 32],
    repository: String,
    account: String,
    receipt: NativeGlobalMigrationReceipt,
}
impl ReconciledGlobalConversion {
    pub(crate) fn manifest_digest(&self) -> [u8; 32] {
        self.manifest_digest
    }
    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn receipt(&self) -> &NativeGlobalMigrationReceipt {
        &self.receipt
    }
}
pub(crate) async fn reconcile_global_conversion_authenticated<
    S: Storage + Clone + Send + Sync + 'static,
>(
    source: &StorageAdapter<S>,
    manifest: &FullConversionManifest,
    journal: &mut impl GlobalConversionJournalOwner,
    authenticated: &AuthenticatedPartialConversion,
) -> Result<ReconciledGlobalConversion, LixError> {
    use futures_util::FutureExt as _;
    let server = authenticated.server();
    let transport = super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        if transport.lix_id() != authenticated.state().repository_id()
            || transport.active_account_id() != authenticated.state().active_account_id()
        {
            return Err(unresolved("global migration authority identity changed"));
        }
        let receipt =
            reconcile_native_global_conversion(source, &transport, manifest, journal).await?;
        Ok(ReconciledGlobalConversion {
            manifest_digest: journal.current().manifest_digest,
            repository: transport.lix_id().into(),
            account: transport.active_account_id().into(),
            receipt,
        })
    }
    .await;
    let close = transport.close_session().fuse();
    let timeout = super::platform::sleep(std::time::Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, timeout);
    futures_util::select_biased! {_=close=>{},_=timeout=>{}};
    result
}

/// Prove the final coherent leased baseline still retains the native global M.
/// This does not renew the lease; the epoch owner checks its original monotonic
/// deadline again immediately before and after pointer publication.
pub(crate) async fn verify_global_conversion_baseline_authenticated(
    authenticated: &AuthenticatedPartialConversion,
    proof: &ReconciledGlobalConversion,
    state: &PartialReplicaState,
) -> Result<(), LixError> {
    use futures_util::FutureExt as _;
    if proof.repository() != state.repository_id()
        || proof.account() != state.active_account_id()
        || state.descriptor().global_branch.checkpoint.commit_id
            != proof.receipt().request.checkpoint_commit_id
    {
        return Err(unresolved(
            "final global migration baseline changed repository, account or checkpoint",
        ));
    }
    let server = authenticated.server();
    let transport = super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        if transport.lix_id() != proof.repository()
            || transport.active_account_id() != proof.account()
        {
            return Err(unresolved("global proof authority identity changed"));
        }
        let baseline = transport
            .partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
            .await?;
        baseline.deadline.check(&baseline.wire.lease.lease_id)?;
        let leased = transport.fork_native_baseline_lease(&baseline.wire.lease)?;
        if !super::pending_conversion::includes(
            &leased,
            &proof.receipt().merge_commit_id,
            &state.descriptor().global_branch.head.commit_id,
        )
        .await?
        {
            return Err(unresolved(
                "final baseline does not retain the exact original global merge",
            ));
        }
        baseline.deadline.check(&baseline.wire.lease.lease_id)?;
        Ok(())
    }
    .await;
    let close = transport.close_session().fuse();
    let timeout = super::platform::sleep(std::time::Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, timeout);
    futures_util::select_biased! {_=close=>{},_=timeout=>{}};
    result
}
pub(crate) async fn cleanup_global_conversion_authenticated(
    authenticated: &AuthenticatedPartialConversion,
    request: &NativeGlobalMigrationRequest,
) -> Result<(), LixError> {
    use futures_util::FutureExt as _;
    let server = authenticated.server();
    let transport = super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        if transport.lix_id() != authenticated.state().repository_id()
            || transport.active_account_id() != authenticated.state().active_account_id()
        {
            return Err(unresolved("global cleanup authority identity changed"));
        }
        transport.cleanup_native_global_migration(request).await?;
        Ok(())
    }
    .await;
    let close = transport.close_session().fuse();
    let timeout = super::platform::sleep(std::time::Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, timeout);
    futures_util::select_biased! {_=close=>{},_=timeout=>{}};
    result
}

/// A resumed branch attempt retains its exact adopted authority basis. That
/// basis is not reconstructed from a later descriptor or written into source.
pub(crate) async fn verify_resumed_global_basis_authenticated(
    authenticated: &AuthenticatedPartialConversion,
    proof: &ReconciledGlobalConversion,
    request: &PartialMergeRequest,
) -> Result<(), LixError> {
    use futures_util::FutureExt as _;
    if request.global_checkpoint_commit_id != proof.receipt().request.checkpoint_commit_id {
        return Err(unresolved("resumed branch global checkpoint changed"));
    }
    let server = authenticated.server();
    let transport = super::http::HttpSyncTransport::connect(&server.url, &server.headers).await?;
    let result = async {
        if transport.lix_id() != proof.repository()
            || transport.active_account_id() != proof.account()
        {
            return Err(unresolved("resumed global proof identity changed"));
        }
        let baseline = transport
            .partial_replica_descriptor(Some(crate::GLOBAL_BRANCH_ID))
            .await?;
        baseline.deadline.check(&baseline.wire.lease.lease_id)?;
        let leased = transport.fork_native_baseline_lease(&baseline.wire.lease)?;
        if !super::pending_conversion::includes(
            &leased,
            &proof.receipt().merge_commit_id,
            &request.global_head_commit_id,
        )
        .await?
        {
            return Err(unresolved(
                "durable branch attempt predates original global publication",
            ));
        }
        baseline.deadline.check(&baseline.wire.lease.lease_id)?;
        Ok(())
    }
    .await;
    let close = transport.close_session().fuse();
    let timeout = super::platform::sleep(std::time::Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, timeout);
    futures_util::select_biased! {_=close=>{},_=timeout=>{}};
    result
}
