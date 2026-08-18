use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use futures_util::{FutureExt, select_biased};

use crate::storage_adapter::Storage;
use crate::{CreateBranchOptions, GLOBAL_BRANCH_ID, Lix, LixError, SwitchBranchOptions, Value};

use crate::branch::{branch_descriptor_tombstone_row, branch_ref_tombstone_row};
use crate::transaction_types::RawWriteBatch;

use super::platform::{HttpSyncTransport, SyncTask, sleep, spawn_sync_task};
use super::{SyncBranch, SyncTransport, reconcile_sync_branches};

const SYNC_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const SYNC_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);
const SYNC_LOCAL_CHANGE_CODE: &str = "LIX_SYNC_LOCAL_CHANGE";
// Branch catalog mutations may be ordinary server writes with no canonical
// event. Refresh frequently enough that a branch admitted on an inactive
// control lane becomes visible on the active replica before the SQL
// readiness deadline, while keeping the pass bounded and independent of the
// event-stream long poll.
const SYNC_TOPOLOGY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct SyncRuntime {
    shutdown: Arc<AtomicBool>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    task: SyncTask,
}
impl SyncRuntime {
    pub(crate) fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
        self.shutdown_tx.send_replace(true);
    }

    pub(crate) async fn stop_and_join(&self) -> Result<(), LixError> {
        self.stop();
        self.task.join().await
    }
}

impl Drop for SyncRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) async fn activate_sync_mode<StorageImpl>(
    lix: &Lix<StorageImpl>,
    server: &crate::ServerOptions,
) -> Result<Arc<SyncRuntime>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let remote_id = server.url.trim_end_matches('/').to_owned();
    let headers = server.headers.clone();
    // Keep a reopened replica on the branch it last used. New replicas have
    // no durable mapping yet and therefore let the server choose its default
    // branch during the first handshake.
    let remembered_branch = lix.load_sync_replica_branch(&remote_id).await?;
    // A fresh local engine necessarily has a synthetic seed commit. Capture
    // its identity before branch selection so first-contact bootstrap can
    // retire only that projection; reopened replicas never run this cleanup.
    let fresh_local_seed_commit_id = if remembered_branch.is_none() {
        let result = lix
            .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
            .await?;
        let row = result.rows().first().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "fresh sync bootstrap is missing its local seed commit",
            )
        })?;
        Some(row.get::<String>("commit_id")?)
    } else {
        None
    };
    let local_default_branch = if remembered_branch.is_none() {
        // The handle starts on the repository's local default branch. Capture
        // that concrete branch ID before selecting the server branch; reading
        // the default key through a branch-scoped session can legitimately
        // return no row on a pristine replica.
        Some(lix.active_branch_id().await?)
    } else {
        None
    };
    // An initialized replica already has a durable branch selector, cursor,
    // and cached scopes. Reopening it must stay on the local hot path: do not
    // make the application wait for a handshake (or a five-second transport
    // timeout) before cached reads become available. The worker reconnects in
    // the background and performs the normal pull/admission loop there.
    // Select the remembered local branch before checking its manifest: the
    // helper is branch-scoped and a freshly opened Lix handle normally starts
    // on its own default branch.
    if let Some(branch_id) = remembered_branch.as_deref() {
        select_server_branch(lix, branch_id, None).await?;
    }
    let has_durable_replica =
        remembered_branch.is_some() && lix.has_initialized_sync_replica(&remote_id).await?;
    let initial_transport = if has_durable_replica {
        lix.restore_sync_scope_readiness(&remote_id).await?;
        None
    } else {
        match HttpSyncTransport::connect(&remote_id, &headers, remembered_branch.as_deref()).await {
            Ok(transport) => {
                // The handshake identifies the branch; the catalog supplies
                // its authoritative user-facing name/hidden bit. A fresh
                // local store otherwise creates a synthetic `sync` row and
                // permanently leaks that implementation detail through the
                // ordinary branch SQL surface.
                let server_branch = transport
                    .list_branches()
                    .await?
                    .into_iter()
                    .find(|branch| branch.id == transport.branch_id());
                let fresh_bootstrap = remembered_branch.is_none()
                    && local_default_branch.as_deref() != Some(transport.branch_id());
                if fresh_bootstrap {
                    // A normal create_branch from the local seed would copy
                    // that seed's synthetic descriptor into the new branch.
                    // Retire it first, then create the replica branch from
                    // the canonical global root so its initial scope has no
                    // branch-local control residue.
                    let local_default_branch_id =
                        local_default_branch.as_deref().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "fresh sync bootstrap is missing local default branch",
                            )
                        })?;
                    retire_sync_bootstrap_placeholder(lix, local_default_branch_id).await?;
                    let temporary_name = format!("__lix_sync_target_{}", transport.branch_id());
                    create_sync_branch_from_global(lix, transport.branch_id(), &temporary_name)
                        .await?;
                    if let Some(metadata) = server_branch.as_ref() {
                        rename_sync_branch(lix, &metadata.id, &metadata.name).await?;
                    }
                } else {
                    select_server_branch(
                        lix,
                        transport.branch_id(),
                        server_branch.as_ref().map(|branch| branch.name.as_str()),
                    )
                    .await?;
                }
                if remembered_branch.is_none()
                    && fresh_bootstrap
                    && let Some(metadata) = server_branch.as_ref()
                {
                    // Rebind the pristine local default before the first
                    // canonical control event is applied. The placeholder
                    // descriptor was retired on the global control lane
                    // before selecting the authoritative branch, so this
                    // operation only installs the durable default selector.
                    reconcile_initial_server_branch(lix, metadata, None).await?;
                }
                if fresh_local_seed_commit_id.is_some() {
                    // Freeze the local bootstrap/control commits before any
                    // canonical event is applied. Their physical records
                    // remain usable for local jump metadata, but public
                    // derived commit/change surfaces must not expose them.
                    lix.mark_sync_bootstrap_commits_hidden().await?;
                }
                let mut client = lix.sync_lifecycle(transport.clone()).await?;
                // Every first-contact replica has a temporary local seed root
                // even when its generated branch ID already matches the
                // server's default. Arm one cleanup pass so canonical history
                // can replace that root without exposing an extra commit.
                if let Some(seed_commit_id) = fresh_local_seed_commit_id {
                    client.mark_fresh_bootstrap_cleanup(seed_commit_id);
                }
                // A first online open returns only after the local replica has
                // caught up to the head visible during the handshake.
                // The first online open is a finite catch-up operation. Once
                // the background worker starts, lifecycle flushes use the
                // mandatory event long-poll; opening a handle must not wait
                // for an idle heartbeat before returning.
                client.flush_without_wait().await?;
                // The selected branch identity and its initial head are
                // already established by the handshake/head probe. Treat
                // that local control snapshot as readable immediately; the
                // worker continues reconciling the complete branch catalog
                // asynchronously. This keeps metadata APIs such as
                // `lix_active_branch_commit_id()` off the network hot path
                // during first open while preserving monotonic local reads.
                lix.sync_mode_state().mark_scope_hydrated_for_branch(
                    transport.branch_id(),
                    super::CONTROL_SYNC_SCOPE,
                    lix.sync_mode_state().scope_generation(),
                );
                lix.persist_sync_replica_config(&remote_id, transport.branch_id())
                    .await?;
                Some(transport)
            }
            Err(error) => {
                let Some(branch_id) = lix.load_sync_replica_branch(&remote_id).await? else {
                    return Err(error);
                };
                lix.switch_branch(SwitchBranchOptions { branch_id }).await?;
                if !lix.has_initialized_sync_replica(&remote_id).await? {
                    return Err(error);
                }
                lix.restore_sync_scope_readiness(&remote_id).await?;
                None
            }
        }
    };
    lix.set_sync_role(crate::sync::SyncRole::Replica {
        remote_id: remote_id.clone(),
    })?;
    // A process/session restart loses the in-memory file-view map, while the
    // late byte projections themselves are durable metadata. Restore them
    // before exposing the replica to application reads.
    lix.restore_sync_file_projections(&remote_id).await?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let worker_lix = lix.clone();
    let worker_shutdown = Arc::clone(&shutdown);
    let task = spawn_sync_task(async move {
        run_sync_worker(
            worker_lix,
            remote_id,
            headers,
            initial_transport,
            worker_shutdown,
            shutdown_rx,
        )
        .await;
    })?;

    Ok(Arc::new(SyncRuntime {
        shutdown,
        shutdown_tx,
        task,
    }))
}

/// Shared synchronization policy. Platform-specific runtimes only decide how
/// this future is spawned and how its timer/fetch futures are implemented.
async fn run_sync_worker<StorageImpl>(
    lix: Lix<StorageImpl>,
    remote_id: String,
    headers: Vec<(String, String)>,
    initial_transport: Option<HttpSyncTransport>,
    shutdown: Arc<AtomicBool>,
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut transport = initial_transport;
    let mut retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
    let mut topology_bootstrap_pending = true;
    let mut change_watcher = lix.sync_mode_state().change_watcher();

    while !shutdown.load(Ordering::Acquire) {
        let result = sync_iteration(
            &lix,
            &remote_id,
            &headers,
            &mut transport,
            &mut topology_bootstrap_pending,
            &mut change_watcher,
            &mut shutdown_rx,
        )
        .await;

        let delay = match result {
            Ok(()) => {
                retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
                Duration::ZERO
            }
            Err(error) => {
                let local_wake = error.code == SYNC_LOCAL_CHANGE_CODE;
                if !local_wake && error.code != LixError::CODE_CLOSED {
                    tracing::warn!(error = ?error, "sync runtime iteration failed");
                    transport = None;
                }
                if shutdown.load(Ordering::Acquire) || error.code == LixError::CODE_CLOSED {
                    break;
                }
                let delay = if local_wake {
                    Duration::ZERO
                } else if error.code == LixError::CODE_INVALID_PARAM {
                    retry_backoff.max(Duration::from_secs(5))
                } else {
                    retry_backoff
                };
                if !local_wake {
                    retry_backoff = retry_backoff
                        .checked_mul(2)
                        .unwrap_or(SYNC_MAX_RETRY_BACKOFF)
                        .min(SYNC_MAX_RETRY_BACKOFF);
                }
                delay
            }
        };

        if delay > Duration::ZERO
            && wait_for_delay_or_shutdown(delay, &mut shutdown_rx)
                .await
                .is_err()
        {
            break;
        }
    }
}

async fn sync_iteration<StorageImpl>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    headers: &[(String, String)],
    transport: &mut Option<HttpSyncTransport>,
    topology_bootstrap_pending: &mut bool,
    change_watcher: &mut tokio::sync::watch::Receiver<u64>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if *shutdown_rx.borrow() {
        return Err(stopped_error());
    }
    let active_branch_id = lix.active_branch_id_for_sync_worker()?;
    if transport
        .as_ref()
        .is_some_and(|current| current.branch_id() != active_branch_id)
    {
        *transport = None;
    }
    let current = match transport.clone() {
        Some(current) => current,
        None => {
            let current =
                HttpSyncTransport::connect(remote_id, headers, Some(&active_branch_id)).await?;
            if current.branch_id() != active_branch_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync server branch changed while the replica was offline",
                ));
            }
            lix.persist_sync_replica_config(remote_id, current.branch_id())
                .await?;
            *transport = Some(current.clone());
            *topology_bootstrap_pending = true;
            current
        }
    };

    let active_branch_for_worker = active_branch_id.clone();
    let worker_session = lix
        .open_internal_session_suppressed(active_branch_id, lix.active_account_id().to_owned())
        .await?;
    let result = async {
        let local_branch_ids = worker_session
            .execute(
                "SELECT id FROM lix_branch WHERE id != $1",
                &[Value::Text(GLOBAL_BRANCH_ID.to_owned())],
            )
            .await?
            .rows()
            .iter()
            .filter_map(|row| row.get::<String>("id").ok())
            .collect::<Vec<_>>();
        let mut pending_inactive_branches = Vec::new();
        for branch_id in local_branch_ids {
            if branch_id != active_branch_for_worker
                && lix.sync_branch_has_pending(remote_id, &branch_id).await?
            {
                pending_inactive_branches.push(branch_id);
            }
        }

        let topology_transport = current.clone();
        let control_scope = [super::CONTROL_SYNC_SCOPE.to_owned()];
        let control_scope_demanded = !lix
            .sync_mode_state()
            .scopes_are_hydrated_for_branch(&control_scope, &active_branch_for_worker);
        if *topology_bootstrap_pending || control_scope_demanded {
            match reconcile_sync_branches(&worker_session, &topology_transport, false).await {
                Ok(()) => {
                    let pending = lix
                        .sync_branch_has_pending(remote_id, &active_branch_for_worker)
                        .await?;
                    *topology_bootstrap_pending = pending;
                    if !pending {
                        mark_control_scope_hydrated(lix, &active_branch_for_worker);
                    }
                }
                Err(error) => {
                    tracing::warn!(
                        error = ?error,
                        "initial sync topology reconciliation failed"
                    );
                }
            }
        }

        let mut client = worker_session.sync_lifecycle(current).await?;
        let flush_result = flush_interruptibly(
            lix,
            remote_id,
            &active_branch_for_worker,
            &topology_transport,
            &mut client,
            change_watcher,
            shutdown_rx,
        )
        .await;
        if flush_result.is_ok() {
            lix.restore_sync_file_projections(remote_id).await?;
            if let Err(error) =
                reconcile_sync_branches(&worker_session, &topology_transport, true).await
            {
                tracing::warn!(error = ?error, "sync post-topology reconciliation failed");
            } else if !lix
                .sync_branch_has_pending(remote_id, &active_branch_for_worker)
                .await?
            {
                mark_control_scope_hydrated(lix, &active_branch_for_worker);
            }

            let readiness = lix
                .sync_mode_state()
                .hydrated_scopes_snapshot_for_branch(&active_branch_for_worker);
            let had_pending_inactive_branches = !pending_inactive_branches.is_empty();
            for branch_id in pending_inactive_branches {
                if let Err(error) = flush_inactive_branch(lix, remote_id, headers, branch_id).await
                {
                    tracing::warn!(
                        error = ?error,
                        "inactive sync branch queue was not flushed"
                    );
                }
            }
            if had_pending_inactive_branches
                && let Err(error) =
                    reconcile_sync_branches(&worker_session, &topology_transport, true).await
            {
                tracing::warn!(
                    error = ?error,
                    "sync post-inactive-branch topology reconciliation failed"
                );
            }
            lix.sync_mode_state()
                .restore_hydrated_scopes_for_branch(&active_branch_for_worker, readiness);
        }
        flush_result.map(|_| ())
    }
    .await;
    let close = worker_session.close().await;
    match (result, close) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

async fn flush_interruptibly<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    active_branch_id: &str,
    topology_transport: &Transport,
    client: &mut super::SyncClient<StorageImpl, Transport>,
    change_watcher: &mut tokio::sync::watch::Receiver<u64>,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<super::SyncFlushReceipt, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport + Clone,
{
    let _ = change_watcher.borrow_and_update();
    let local_changed = change_watcher.changed().fuse();
    let shutdown_changed = shutdown_rx.changed().fuse();
    let flush = client.flush().fuse();
    futures_util::pin_mut!(local_changed, shutdown_changed, flush);

    loop {
        let topology_timer = sleep(SYNC_TOPOLOGY_INTERVAL).fuse();
        futures_util::pin_mut!(topology_timer);
        select_biased! {
            changed = shutdown_changed => {
                let _ = changed;
                return Err(stopped_error());
            }
            changed = local_changed => {
                let _ = changed;
                return Err(LixError::new(
                    SYNC_LOCAL_CHANGE_CODE,
                    "local sync state changed while waiting for remote events",
                ));
            }
            result = flush => return result,
            timer = topology_timer => {
                timer?;
                reconcile_topology_during_pull(
                    lix,
                    remote_id,
                    active_branch_id,
                    topology_transport,
                ).await?;
            }
        }
    }
}

async fn reconcile_topology_during_pull<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    active_branch_id: &str,
    transport: &Transport,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport + Clone,
{
    let session = lix
        .open_internal_session_suppressed(
            active_branch_id.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let result = reconcile_sync_branches(&session, transport, true).await;
    let close = session.close().await;
    match (result, close) {
        (Err(error), _) => {
            tracing::warn!(error = ?error, "background sync topology reconciliation failed");
            Ok(())
        }
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => {
            if !lix
                .sync_branch_has_pending(remote_id, active_branch_id)
                .await?
            {
                mark_control_scope_hydrated(lix, active_branch_id);
            }
            Ok(())
        }
    }
}

async fn flush_inactive_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    headers: &[(String, String)],
    branch_id: String,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let transport = HttpSyncTransport::connect(remote_id, headers, Some(&branch_id)).await?;
    if transport.branch_id() != branch_id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync server returned another branch for an inactive queue",
        ));
    }
    let session = lix
        .open_internal_session_suppressed(branch_id, lix.active_account_id().to_owned())
        .await?;
    let mut client = session.sync_lifecycle(transport).await?;
    let result = client.flush().await;
    let close = session.close().await;
    match (result, close) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(_), Ok(())) => Ok(()),
    }
}

fn mark_control_scope_hydrated<StorageImpl>(lix: &Lix<StorageImpl>, branch_id: &str)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    lix.sync_mode_state().mark_scope_hydrated_for_branch(
        branch_id,
        super::CONTROL_SYNC_SCOPE,
        lix.sync_mode_state().scope_generation(),
    );
}

async fn wait_for_delay_or_shutdown(
    duration: Duration,
    shutdown_rx: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<(), LixError> {
    if *shutdown_rx.borrow() {
        return Err(stopped_error());
    }
    let timer = sleep(duration).fuse();
    let changed = shutdown_rx.changed().fuse();
    futures_util::pin_mut!(timer, changed);
    select_biased! {
        _ = changed => Err(stopped_error()),
        result = timer => result,
    }
}

fn stopped_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "sync worker is stopping")
}

async fn select_server_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    server_branch_id: &str,
    authoritative_name: Option<&str>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    if lix.active_branch_id().await? == server_branch_id {
        return Ok(());
    }
    let existing = lix
        .execute(
            "SELECT id FROM lix_branch WHERE id = $1",
            &[Value::Text(server_branch_id.to_owned())],
        )
        .await?;
    if existing.rows().is_empty() {
        lix.create_branch(CreateBranchOptions {
            id: Some(server_branch_id.to_owned()),
            name: authoritative_name.unwrap_or("sync").to_owned(),
            from_commit_id: None,
        })
        .await?;
    }
    lix.switch_branch(SwitchBranchOptions {
        branch_id: server_branch_id.to_owned(),
    })
    .await?;
    Ok(())
}

async fn retire_sync_bootstrap_placeholder<StorageImpl>(
    lix: &Lix<StorageImpl>,
    local_branch_id: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let global = lix
        .open_internal_session_suppressed(
            GLOBAL_BRANCH_ID.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let mut transaction = global.begin_transaction().await?;
    let mut rows = RawWriteBatch::with_capacity(2);
    rows.push(branch_descriptor_tombstone_row(local_branch_id));
    rows.push(branch_ref_tombstone_row(local_branch_id));
    transaction.stage_sync_rows(rows).await?;
    transaction.commit().await?;
    global.close().await
}

async fn rename_sync_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    branch_id: &str,
    name: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let global = lix
        .open_internal_session_suppressed(
            GLOBAL_BRANCH_ID.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let result = global
        .execute(
            "UPDATE lix_branch SET name = $1 WHERE id = $2",
            &[
                Value::Text(name.to_owned()),
                Value::Text(branch_id.to_owned()),
            ],
        )
        .await;
    let close = global.close().await;
    match (result, close) {
        (Err(error), _) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Ok(_), Ok(())) => Ok(()),
    }
}

async fn create_sync_branch_from_global<StorageImpl>(
    lix: &Lix<StorageImpl>,
    branch_id: &str,
    name: &str,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let global = lix
        .open_internal_session_suppressed(
            GLOBAL_BRANCH_ID.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let head = global
        .execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await?
        .rows()
        .first()
        .and_then(|row| row.get::<String>("commit_id").ok())
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync global branch has no bootstrap head",
            )
        })?;
    global
        .create_branch(CreateBranchOptions {
            id: Some(branch_id.to_owned()),
            name: name.to_owned(),
            from_commit_id: Some(head),
        })
        .await?;
    global.close().await?;
    lix.switch_branch(SwitchBranchOptions {
        branch_id: branch_id.to_owned(),
    })
    .await?;
    Ok(())
}

/// Rebinds a pristine local repository's synthetic branch to the authoritative
/// server branch after the first canonical pull. This is an internal control
/// operation: it changes neither the public branch API nor the sync protocol,
/// and the suppressed session prevents the metadata from becoming a client
/// proposal. The old local default is removed only on first contact, before a
/// replica can have a durable pending queue.
async fn reconcile_initial_server_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    server_branch: &SyncBranch,
    local_default_branch_id: Option<&str>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let branch_id = lix.active_branch_id().await?;
    if branch_id != server_branch.id {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync bootstrap branch changed before metadata reconciliation",
        ));
    }
    let default_value = serde_json::to_string(&server_branch.id).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("encode sync default branch value: {error}"),
        )
    })?;
    let global_session = lix
        .open_internal_session_suppressed(
            GLOBAL_BRANCH_ID.to_owned(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    global_session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global) \
             VALUES ($2, CAST($1 AS JSONB), true) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            &[
                Value::Text(default_value),
                Value::Text(crate::init::DEFAULT_BRANCH_KEY.to_owned()),
            ],
        )
        .await?;
    global_session.close().await?;
    if let Some(local_default_branch_id) = local_default_branch_id
        && local_default_branch_id != server_branch.id
        && local_default_branch_id != GLOBAL_BRANCH_ID
    {
        // Branch descriptors and refs are global control facts. Deleting the
        // synthetic local placeholder through the selected server branch
        // would create a local control commit and move that branch's head
        // after canonical bootstrap. Keep cleanup on the global lane.
        let cleanup = lix
            .open_internal_session_suppressed(
                GLOBAL_BRANCH_ID.to_owned(),
                lix.active_account_id().to_owned(),
            )
            .await?;
        cleanup
            .execute(
                "DELETE FROM lix_branch WHERE id = $1",
                &[Value::Text(local_default_branch_id.to_owned())],
            )
            .await?;
        return cleanup.close().await;
    }
    // The selected branch's canonical replay already owns its local head.
    // Metadata is reconciled by the normal branch-catalog pass once the
    // global control lane has materialized the corresponding commit.
    Ok(())
}
