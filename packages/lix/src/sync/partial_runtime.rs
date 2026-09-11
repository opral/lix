//! Demand-only worker for an admitted partial replica. It never performs the
//! full replica's snapshot, history inventory, upload or certified pull loop.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures_util::{FutureExt, select_biased};

use crate::storage_adapter::{Storage, StorageAdapter, StorageWriteOptions};
use crate::{LixError, tracked_state::NativeMetadataRef};

use super::SyncTransport;
use super::http::{HttpSyncTransport, RawHttpClient};
use super::native_metadata::{
    NativeMetadataRequest, native_metadata_is_resident, stage_native_metadata,
};
use super::partial_hydration::{hydrate_native_object, native_object_is_resident};
use super::partial_state::{PartialReplicaState, load_partial_replica_state};
use super::platform::{sleep, spawn_sync_task};
use super::runtime::{SyncDemand, SyncDemandRequest, SyncRuntime, SyncShutdown, stopped_error};

pub(crate) async fn start_partial_runtime_with_engine<S>(
    storage: StorageAdapter<S>,
    state: Arc<PartialReplicaState>,
    server: Option<crate::ServerOptions>,
    transport: Option<super::platform::HttpSyncTransport>,
    changes: Option<tokio::sync::watch::Receiver<u64>>,
    engine: Option<Arc<crate::engine::Engine<S>>>,
) -> Result<Arc<SyncRuntime>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if let Some(transport) = &transport {
        validate_admission(&storage, &state, transport).await?;
    }
    {
        let read = storage.begin_read(Default::default()).await?;
        if load_partial_replica_state(&read)
            .await?
            .as_ref()
            .map(|(state, _)| state)
            != Some(state.as_ref())
        {
            return Err(LixError::new(
                "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                "partial worker durable admission changed",
            ));
        }
        if let Some(server) = &server {
            if super::http::normalize_sync_locator(&server.url)?.protocol_url
                != super::http::normalize_sync_locator(state.remote_id())?.protocol_url
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "partial worker remote differs from durable admission",
                ));
            }
        }
    }
    storage.admit_partial_replica_writer(super::partial_replica_write_capability());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(64);
    let owner_guard = engine
        .as_ref()
        .filter(|engine| engine.partial_owner().is_installed())
        .map(|engine| engine.partial_owner().retain_for_owned_work())
        .transpose()?;
    let task = spawn_sync_task(async move {
        let _owner_guard = owner_guard;
        let result = run_platform_partial_worker(
            storage,
            state,
            transport,
            server,
            shutdown_rx,
            demand_rx,
            changes,
            engine,
        )
        .await;
        let _ = completion_tx.send(result);
    })?;
    Ok(Arc::new(SyncRuntime {
        shutdown_tx,
        demand_tx,
        completion_rx: Mutex::new(Some(completion_rx)),
        task,
    }))
}

/// Erase the platform worker at its owner boundary so downstream storage
/// adapters do not instantiate its entire nested async layout in their callers.
fn run_platform_partial_worker<S>(
    storage: StorageAdapter<S>,
    state: Arc<PartialReplicaState>,
    transport: Option<super::platform::HttpSyncTransport>,
    server: Option<crate::ServerOptions>,
    shutdown_rx: tokio::sync::watch::Receiver<SyncShutdown>,
    demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
    changes: Option<tokio::sync::watch::Receiver<u64>>,
    engine: Option<Arc<crate::engine::Engine<S>>>,
) -> super::SyncTransportFuture<'static, ()>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(async move {
        let connect =
            move || -> super::SyncTransportFuture<'static, super::platform::HttpSyncTransport> {
                let server = server.clone();
                Box::pin(async move {
                    let server = server.ok_or_else(|| {
                        LixError::new(
                            "LIX_PARTIAL_REPLICA_OFFLINE",
                            "missing input requires a configured authority",
                        )
                    })?;
                    super::platform::HttpSyncTransport::connect(&server.url, &server.headers).await
                })
            };
        run_partial_worker_with_engine(
            storage,
            state,
            transport,
            connect,
            shutdown_rx,
            demand_rx,
            changes,
            engine,
        )
        .await
    })
}

async fn validate_admission<S: Storage + Clone + Send + Sync + 'static, C: RawHttpClient>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
) -> Result<(), LixError> {
    let read = storage.begin_read(Default::default()).await?;
    if transport.protocol_url()
        != super::http::normalize_sync_locator(state.remote_id())?.protocol_url
        || transport.lix_id() != state.repository_id()
        || transport.active_account_id() != state.active_account_id()
        || load_partial_replica_state(&read)
            .await?
            .as_ref()
            .map(|(state, _)| state)
            != Some(state)
    {
        return Err(LixError::new(
            "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
            "partial demand worker authority or durable admission changed",
        ));
    }
    transport.bind_native_baseline_lease(state.baseline_lease())?;
    Ok(())
}

async fn hydrate_metadata<S: Storage + Clone + Send + Sync + 'static, C: RawHttpClient>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<C>,
    address: NativeMetadataRef,
) -> Result<(), LixError> {
    {
        let read = storage.begin_read(Default::default()).await?;
        if native_metadata_is_resident(&read, state, &address).await? {
            return Ok(());
        }
    }
    let request = NativeMetadataRequest {
        epoch_id: state.epoch_id().to_owned(),
        objects: vec![address],
    };
    // No local read or transaction remains open across network I/O.
    let response = transport.native_metadata(&request).await?;
    for attempt in 0..4 {
        let read = storage.begin_read(Default::default()).await?;
        let mut writes = storage.new_write_set();
        let preconditions =
            stage_native_metadata(&read, &mut writes, state, &request, &response).await?;
        drop(read);
        match storage
            .commit_partial_replica_write_set(
                super::partial_replica_write_capability(),
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
        {
            Err(crate::storage_adapter::StorageWriteSetError::Storage(
                crate::storage_adapter::StorageError::PreconditionFailed(_),
            )) if attempt < 3 => {
                continue;
            }
            result => return result.map(|_| ()).map_err(Into::into),
        }
    }
    unreachable!("bounded metadata CAS loop returns")
}

/// Resolve stale queued demands from durable local data before connecting.
/// This check also prevents corrupt resident objects triggering a handshake.
async fn demand_is_resident<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    request: &SyncDemandRequest,
) -> Result<bool, LixError> {
    match request {
        SyncDemandRequest::NativeObject(address, _) => {
            native_object_is_resident(storage, state, *address).await
        }
        SyncDemandRequest::NativeObjects(addresses, _) => {
            let mut complete = true;
            for address in addresses {
                complete &= native_object_is_resident(storage, state, *address).await?;
            }
            Ok(complete)
        }
        SyncDemandRequest::NativeMetadata(address, _) => {
            let read = storage.begin_read(Default::default()).await?;
            let resident = native_metadata_is_resident(&read, state, address).await?;
            drop(read);
            if !resident {
                return Ok(false);
            }
            if matches!(address, NativeMetadataRef::CommitGraphRecord(_)) {
                return Ok(
                    super::partial_write_frontier::next_missing_baseline_write_frontier(
                        storage, state,
                    )
                    .await?
                    .is_none(),
                );
            }
            Ok(true)
        }
        SyncDemandRequest::BlobManifest(address, _) => {
            super::partial_blob::manifest_is_resident(storage, state, *address).await
        }
        SyncDemandRequest::Chunks(ids) => {
            let mut resident = true;
            for id in ids {
                let hash = crate::binary_cas::ChunkHash::from_hex(id)?;
                resident &= super::partial_blob::chunk_is_resident(storage, state, hash).await?;
            }
            Ok(resident)
        }
        SyncDemandRequest::History(_) => Err(LixError::new(
            "LIX_PARTIAL_REPLICA_DEMAND_UNSUPPORTED",
            "partial replica requires a typed native address for history dependencies",
        )),
        #[cfg(test)]
        SyncDemandRequest::PublicationBarrier => Err(LixError::new(
            "LIX_PARTIAL_REPLICA_DEMAND_UNSUPPORTED",
            "partial demand worker does not publish writes",
        )),
    }
}

// Erase this child operation before composing the worker select loop.
pub(super) fn hydrate_demand<'a, S: Storage + Clone + Send + Sync + 'static, C: RawHttpClient>(
    storage: &'a StorageAdapter<S>,
    state: &'a PartialReplicaState,
    transport: &'a HttpSyncTransport<C>,
    request: SyncDemandRequest,
) -> super::SyncTransportFuture<'a, ()> {
    Box::pin(async move {
        match request {
            SyncDemandRequest::BlobManifest(address, _) => {
                if super::partial_blob::manifest_is_resident(storage, state, address).await? {
                    return Ok(());
                }
                let ids = [address.to_hex()];
                let manifests = transport.get_blobs(&ids).await?;
                if manifests.len() != 1 {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "blob manifest response must contain exactly the requested blob",
                    ));
                }
                super::partial_blob::install_manifest(storage, state, address, &manifests[0])
                    .await?;
                Ok(())
            }
            SyncDemandRequest::Chunks(ids) => {
                for id in ids {
                    let hash = crate::binary_cas::ChunkHash::from_hex(&id)?;
                    if super::partial_blob::chunk_is_resident(storage, state, hash).await? {
                        continue;
                    }
                    let bytes = transport.get_chunk(&id).await?.ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            "authority lacks demanded blob chunk",
                        )
                    })?;
                    super::partial_blob::install_chunk(storage, state, hash, &bytes).await?;
                }
                Ok(())
            }
            SyncDemandRequest::NativeObjects(addresses, _) => {
                super::partial_hydration::hydrate_native_objects(
                    storage,
                    state,
                    &addresses,
                    64 * 1024 * 1024,
                    |requested| async move { transport.native_objects(&requested).await },
                    |range| async move { transport.native_object_range(&range).await },
                )
                .await
                .map(|_| ())
            }
            SyncDemandRequest::NativeObject(address, _) => {
                // Scoped native nodes can exceed one transport page; enforce an
                // assembly budget independently from the 1 MiB response pages.
                hydrate_native_object(
                    storage,
                    state,
                    address,
                    64 * 1024 * 1024,
                    |range| async move { transport.native_object_range(&range).await },
                )
                .await
                .map(|_| ())
            }
            SyncDemandRequest::NativeMetadata(address, _) => {
                let graph = matches!(address, NativeMetadataRef::CommitGraphRecord(_));
                hydrate_metadata(storage, state, transport, address).await?;
                if graph {
                    super::partial_write_frontier::prepare_baseline_write_frontier(
                        storage,
                        state,
                        |address| hydrate_metadata(storage, state, transport, address),
                    )
                    .await?;
                }
                Ok(())
            }
            SyncDemandRequest::History(_) => Err(LixError::new(
                "LIX_PARTIAL_REPLICA_DEMAND_UNSUPPORTED",
                "partial replica requires a typed native address for this dependency",
            )),
            #[cfg(test)]
            SyncDemandRequest::PublicationBarrier => Err(LixError::new(
                "LIX_PARTIAL_REPLICA_DEMAND_UNSUPPORTED",
                "partial demand worker does not publish writes",
            )),
        }
    })
}

async fn run_partial_worker<
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
>(
    storage: StorageAdapter<S>,
    state: Arc<PartialReplicaState>,
    transport: HttpSyncTransport<C>,
    shutdown_rx: tokio::sync::watch::Receiver<SyncShutdown>,
    demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
) -> Result<(), LixError> {
    run_partial_worker_connecting(
        storage,
        state,
        Some(transport),
        || {
            Box::pin(async {
                Err(LixError::new(
                    "LIX_PARTIAL_REPLICA_OFFLINE",
                    "authority unavailable",
                ))
            })
        },
        shutdown_rx,
        demand_rx,
    )
    .await
}

/// Observe only admitted branch controls and tiny push records before deciding
/// to connect. Clean offline reopen performs no authority handshake.
async fn upload_pending_once<S, C, Connect>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    transport: &mut Option<HttpSyncTransport<C>>,
    connect: &mut Connect,
) -> Result<bool, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
    Connect: FnMut() -> super::SyncTransportFuture<'static, HttpSyncTransport<C>>,
{
    let mut progress = false;
    let read = storage.begin_read(Default::default()).await?;
    let cleanup_pending =
        super::partial_global_merge_state::load_partial_global_merge_state(&read, state)
            .await?
            .0
            .is_some_and(|record| record.upload_settled);
    drop(read);
    if cleanup_pending {
        if transport.is_none() {
            let connected = connect().await?;
            validate_admission(storage, state, &connected).await?;
            *transport = Some(connected);
        }
        progress |= super::partial_global_merge_runtime::cleanup_adopted_global_attempt(
            storage,
            state,
            transport.as_ref().expect("connected"),
        )
        .await?;
    }
    for (index, branch) in [
        &state.descriptor().global_branch,
        &state.descriptor().selected_branch,
    ]
    .into_iter()
    .enumerate()
    {
        if index == 1 && branch.branch_id == state.descriptor().global_branch.branch_id {
            continue;
        }
        let read = storage.begin_read(Default::default()).await?;
        let (push, _, _) =
            super::partial_push_state::load_partial_push_state(&read, state, &branch.branch_id)
                .await?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .load(&branch.branch_id)
            .await?
            .ok_or_else(|| LixError::unknown("partial upload branch disappeared"))?;
        let clean = push.prepared.is_none()
            && control.head_commit_id == push.confirmed.head
            && control
                .working_diff_checkpoint_commit_id
                .map(|id| id.to_string())
                .as_ref()
                == Some(&push.confirmed.checkpoint);
        drop(read);
        if clean {
            continue;
        }
        if transport.is_none() {
            let connected = connect().await?;
            validate_admission(storage, state, &connected).await?;
            *transport = Some(connected);
        }
        let connected = transport.as_ref().expect("connected above");
        let upload = super::partial_upload_cycle::upload_partial_once(
            storage,
            state,
            &branch.branch_id,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
            |request| async move {
                super::partial_blob_upload::push_partial_with_blobs(
                    storage, state, connected, &request,
                )
                .await
            },
        )
        .await;
        match upload {
            Ok(changed) => progress |= changed,
            Err(error) if error.code == "LIX_PARTIAL_CREATED_REF_SOURCE_PENDING" => {}
            Err(error) => return Err(error),
        }
    }
    Ok(progress)
}

async fn run_partial_worker_connecting<S, C, Connect>(
    storage: StorageAdapter<S>,
    state: Arc<PartialReplicaState>,
    transport: Option<HttpSyncTransport<C>>,
    connect: Connect,
    shutdown_rx: tokio::sync::watch::Receiver<SyncShutdown>,
    demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
    Connect: FnMut() -> super::SyncTransportFuture<'static, HttpSyncTransport<C>>,
{
    run_partial_worker_with_changes(
        storage,
        state,
        transport,
        connect,
        shutdown_rx,
        demand_rx,
        None,
    )
    .await
}

fn lease_renewal_delay(expires_at_ms: u64) -> Duration {
    let now = chrono::Utc::now().timestamp_millis().max(0) as u64;
    Duration::from_millis(
        expires_at_ms
            .saturating_sub(now)
            .min(crate::gc::NATIVE_BASELINE_LEASE_TTL_MS)
            / 2,
    )
}

pub(super) async fn run_partial_worker_with_changes<S, C, Connect>(
    storage: StorageAdapter<S>,
    state: Arc<PartialReplicaState>,
    transport: Option<HttpSyncTransport<C>>,
    connect: Connect,
    shutdown_rx: tokio::sync::watch::Receiver<SyncShutdown>,
    demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
    changes: Option<tokio::sync::watch::Receiver<u64>>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
    Connect: FnMut() -> super::SyncTransportFuture<'static, HttpSyncTransport<C>>,
{
    run_partial_worker_with_engine(
        storage,
        state,
        transport,
        connect,
        shutdown_rx,
        demand_rx,
        changes,
        None,
    )
    .await
}

pub(super) async fn run_partial_worker_with_engine<S, C, Connect>(
    storage: StorageAdapter<S>,
    mut state: Arc<PartialReplicaState>,
    mut transport: Option<HttpSyncTransport<C>>,
    mut connect: Connect,
    mut shutdown_rx: tokio::sync::watch::Receiver<SyncShutdown>,
    mut demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
    mut changes: Option<tokio::sync::watch::Receiver<u64>>,
    engine: Option<Arc<crate::engine::Engine<S>>>,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
    Connect: FnMut() -> super::SyncTransportFuture<'static, HttpSyncTransport<C>>,
{
    let mut publication: Option<super::SyncTransportFuture<'static, ()>> = None;
    let mut watch_cursor = state.descriptor().cursor;
    let mut blocked_global_cursor: Option<u64> = None;
    let mut force_descriptor_refresh = false;
    let mut watch_after = web_time::Instant::now();
    let mut upload_due = changes.is_some();
    let mut retry_upload = false;
    let mut retry_delay = Duration::from_millis(100);
    let mut retry_deadline = web_time::Instant::now();
    let mut queued_demand = None;
    // A deadline, rather than restarting a relative sleep on each query, keeps
    // an active working set pinned even under continuous local activity.
    let mut renewal_deadline =
        web_time::Instant::now() + lease_renewal_delay(state.baseline_lease().expires_at_ms);
    let mut baseline_expired: Option<LixError> = None;
    let mut terminal_error = None;
    'worker: while *shutdown_rx.borrow() == SyncShutdown::Running {
        if let Some(engine) = &engine {
            engine.sync_mode().ensure_partial_admission_healthy()?;
            let current = engine.sync_mode().partial_admission().ok_or_else(|| {
                LixError::new(
                    "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                    "worker lost admission",
                )
            })?;
            if current.as_ref() != state.as_ref() {
                let lease_changed =
                    current.baseline_lease().lease_id != state.baseline_lease().lease_id;
                transport = transport
                    .as_ref()
                    .map(|value| value.fork_native_baseline_lease(current.baseline_lease()))
                    .transpose()?;
                state = current;
                watch_cursor = state.descriptor().cursor;
                watch_after = web_time::Instant::now();
                renewal_deadline = web_time::Instant::now()
                    + lease_renewal_delay(state.baseline_lease().expires_at_ms);
                if lease_changed {
                    baseline_expired = None;
                }
            }
        }
        // Foreground missing inputs preempt background upload. Cancellation can
        // have an ambiguous server outcome; the durable captured tuple survives.
        if queued_demand.is_none() {
            queued_demand = demand_rx.try_recv().ok();
        }
        if queued_demand.is_none() {
            if let Some(mut completion) = publication.take() {
                {
                    let shutdown = shutdown_rx.changed().fuse();
                    let demand = demand_rx.recv().fuse();
                    let renewal = async {
                        if changes.is_some() && baseline_expired.is_none() {
                            sleep(
                                renewal_deadline
                                    .saturating_duration_since(web_time::Instant::now()),
                            )
                            .await;
                        } else {
                            futures_util::future::pending::<()>().await;
                        }
                    }
                    .fuse();
                    let done = completion.as_mut().fuse();
                    futures_util::pin_mut!(shutdown, demand, renewal, done);
                    select_biased! {
                        _ = shutdown => break,
                        next = demand => {
                            let Some(next) = next else { break };
                            queued_demand = Some(next);
                        },
                        _ = renewal => {},
                        result = done => {
                            if let Err(error) = result {
                                tracing::warn!(code=%error.code, "partial publication did not complete");
                            }
                            watch_after = web_time::Instant::now() + Duration::from_millis(100);
                            continue;
                        }
                    }
                }
                publication = Some(completion);
            }
        }
        if queued_demand.is_none()
            && changes.is_some()
            && baseline_expired.is_none()
            && web_time::Instant::now() >= renewal_deadline
        {
            let renew = async {
                if transport.is_none() {
                    let connected = connect().await?;
                    validate_admission(&storage, &state, &connected).await?;
                    transport = Some(connected);
                }
                transport
                    .as_ref()
                    .expect("connected")
                    .renew_native_baseline_lease()
                    .await
            }
            .fuse();
            let shutdown = shutdown_rx.changed().fuse();
            let demand = demand_rx.recv().fuse();
            futures_util::pin_mut!(renew, shutdown, demand);
            select_biased! {
                _ = shutdown => break,
                next = demand => {
                    let Some(next) = next else { break };
                    queued_demand = Some(next);
                },
                result = renew => match result {
                    // A successful renewal proves a fresh server TTL. Use a
                    // monotonic interval so client/server clock skew cannot
                    // trigger a zero-delay renewal loop.
                    Ok(_) => renewal_deadline = web_time::Instant::now() + Duration::from_millis(crate::gc::NATIVE_BASELINE_LEASE_TTL_MS / 2),
                    Err(error) => {
                        if is_terminal_partial_transport_error(&error) {
                            terminal_error = Some(error);
                            break 'worker;
                        }
                        tracing::warn!(code = %error.code, message = %error.message, "partial replica baseline renewal failed");
                        if error.code == "LIX_PARTIAL_BASELINE_EXPIRED" {
                            baseline_expired = Some(error);
                        } else {
                            renewal_deadline = web_time::Instant::now() + Duration::from_secs(5);
                        }
                    }
                }
            }
            if queued_demand.is_none() {
                continue;
            }
        }
        if retry_upload && web_time::Instant::now() >= retry_deadline {
            upload_due = true;
        }
        if queued_demand.is_none() && upload_due {
            upload_due = false;
            retry_upload = false;
            let upload = upload_pending_once(&storage, &state, &mut transport, &mut connect).fuse();
            let shutdown = shutdown_rx.changed().fuse();
            let demand = demand_rx.recv().fuse();
            futures_util::pin_mut!(upload, shutdown, demand);
            select_biased! {
                _ = shutdown => break,
                next = demand => {
                    let Some(next) = next else { break };
                    queued_demand = Some(next);
                    upload_due = true;
                },
                result = upload => match result {
                    Ok(progress) => { if progress {force_descriptor_refresh=true;} upload_due = progress; retry_upload = false; retry_delay = Duration::from_millis(100); },
                    Err(error) => {
                        if is_terminal_partial_transport_error(&error) {
                            terminal_error = Some(error);
                            break 'worker;
                        }
                        force_descriptor_refresh=true;
                        tracing::warn!(code = %error.code, message = %error.message, "partial replica upload retained for retry");
                        retry_upload = true;
                        retry_delay = (retry_delay * 2).min(Duration::from_secs(5));
                        retry_deadline = web_time::Instant::now() + retry_delay;
                    }
                }
            }
            if queued_demand.is_none() {
                continue;
            }
        }
        if queued_demand.is_none()
            && publication.is_none()
            && engine.is_some()
            && web_time::Instant::now() >= watch_after
        {
            let engine = engine.as_ref().expect("checked").clone();
            let after_cursor = watch_cursor;
            let blocked_cursor = blocked_global_cursor;
            let request_fresh = force_descriptor_refresh;
            let recovery = if baseline_expired.is_some() {
                super::partial_publication::PartialRecoveryPolicy::ExpiredBaseline
            } else {
                super::partial_publication::PartialRecoveryPolicy::Normal
            };
            let watch = async {
                if transport.is_none() {
                    let connected = connect().await?;
                    validate_admission(&storage, &state, &connected).await?;
                    transport = Some(connected);
                }
                let connected = transport.as_ref().expect("connected");
                let wrapper = if request_fresh {
                    connected
                        .partial_replica_descriptor(Some(
                            &state.descriptor().selected_branch.branch_id,
                        ))
                        .await?
                } else {
                    connected
                        .wait_partial_replica_descriptor(
                            &state.descriptor().selected_branch.branch_id,
                            after_cursor,
                        )
                        .await?
                };
                let cursor = wrapper.wire.descriptor.cursor;
                if !request_fresh && blocked_cursor.is_some_and(|blocked| cursor <= blocked) {
                    return Ok((
                        cursor,
                        super::partial_reconcile::PreparedDescriptor::NoChange,
                    ));
                }

                let prepared =
                    super::partial_global_merge_runtime::prepare_descriptor_with_global_merge(
                        engine.clone(),
                        state.clone(),
                        connected,
                        wrapper,
                        recovery,
                    )
                    .await.map_err(|error| {
                        if super::partial_global_merge_runtime::waits_for_state_change(&error) {
                            error.with_details(serde_json::json!({"authorityCursor":cursor,"pendingPreserved":true,"waitForChange":true}))
                        } else {error}
                    })?;
                Ok::<_, LixError>((cursor, prepared))
            }
            .fuse();
            let shutdown = shutdown_rx.changed().fuse();
            let demand = demand_rx.recv().fuse();
            let renew_while_watching = changes.is_some() && baseline_expired.is_none();
            let changed = async {
                match changes.as_mut() {
                    Some(receiver) => receiver.changed().await.is_ok(),
                    None => futures_util::future::pending::<bool>().await,
                }
            }
            .fuse();
            let renewal = async {
                if renew_while_watching {
                    sleep(renewal_deadline.saturating_duration_since(web_time::Instant::now()))
                        .await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            }
            .fuse();
            let retry_enabled = retry_upload;
            let retry_at = retry_deadline;
            let retry = async move {
                if retry_enabled {
                    sleep(retry_at.saturating_duration_since(web_time::Instant::now())).await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            }
            .fuse();
            futures_util::pin_mut!(watch, shutdown, demand, changed, renewal, retry);
            select_biased! {
                _ = shutdown => break,
                next = demand => { let Some(next) = next else { break }; queued_demand = Some(next); },
                live = changed => { if !live { break; } upload_due = true; blocked_global_cursor=None; force_descriptor_refresh=true; },
                _ = renewal => {},
                _ = retry => { upload_due = true; },
                result = watch => match result {
                    Ok((cursor, super::partial_reconcile::PreparedDescriptor::LocalProgress)) => {
                        watch_cursor=watch_cursor.max(cursor);blocked_global_cursor=None;
                        force_descriptor_refresh=true;upload_due=true;retry_upload=false;
                        watch_after=web_time::Instant::now();
                    },
                    Ok((cursor, super::partial_reconcile::PreparedDescriptor::NoChange)) => {
                        force_descriptor_refresh=false;
                        if blocked_global_cursor.is_some_and(|blocked|cursor>blocked){blocked_global_cursor=None;}
                        watch_cursor = watch_cursor.max(cursor);
                        watch_after = web_time::Instant::now() + Duration::from_millis(100);
                    },
                    Ok((_, super::partial_reconcile::PreparedDescriptor::Ready(prepared))) => {
                        force_descriptor_refresh=false; blocked_global_cursor=None;
                        publication = Some(Box::pin(super::partial_publication::publish_prepared_partial(engine.clone(), prepared)));
                    },
                    Err(error) => {
                        if is_terminal_partial_transport_error(&error) {
                            terminal_error = Some(error);
                            break 'worker;
                        }
                        if super::partial_global_merge_runtime::waits_for_state_change(&error) {
                            let cursor=error.details.as_ref().and_then(|v|v.get("authorityCursor")).and_then(serde_json::Value::as_u64).unwrap_or(watch_cursor);
                            blocked_global_cursor=Some(cursor);watch_cursor=watch_cursor.max(cursor);
                            force_descriptor_refresh=false;retry_upload=false;upload_due=false;
                            tracing::warn!(code=%error.code,message=%error.message,"partial GLOBAL reconciliation awaits local or authority change; pending edits retained");
                            watch_after=web_time::Instant::now();
                            continue 'worker;
                        }
                        if error.code == "LIX_PARTIAL_REPLICA_REBASE_REQUIRED" && !retry_upload { upload_due = true; }
                        tracing::warn!(code=%error.code, "partial reconciliation retained existing working set");
                        watch_after = web_time::Instant::now() + if error.code == "LIX_PARTIAL_REPLICA_BASELINE_RECOVERY_PENDING" { Duration::from_secs(30) } else { Duration::from_secs(1) };
                    }
                }
            }
            continue;
        }
        let demand = if let Some(demand) = queued_demand.take() {
            Some(demand)
        } else {
            let shutdown = shutdown_rx.changed().fuse();
            let next = demand_rx.recv().fuse();
            let renewal_enabled = changes.is_some() && baseline_expired.is_none();
            let changed = async {
                match changes.as_mut() {
                    Some(receiver) => receiver.changed().await.is_ok(),
                    None => futures_util::future::pending::<bool>().await,
                }
            }
            .fuse();
            let retry_enabled = retry_upload;
            let retry_at = retry_deadline;
            let retry = async move {
                if retry_enabled {
                    sleep(retry_at.saturating_duration_since(web_time::Instant::now())).await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            }
            .fuse();
            let renewal = async {
                if renewal_enabled {
                    sleep(renewal_deadline.saturating_duration_since(web_time::Instant::now()))
                        .await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            }
            .fuse();
            let watch_enabled = engine.is_some() && publication.is_none();
            let watch_ready = async {
                if watch_enabled {
                    sleep(watch_after.saturating_duration_since(web_time::Instant::now())).await;
                } else {
                    futures_util::future::pending::<()>().await;
                }
            }
            .fuse();
            futures_util::pin_mut!(shutdown, next, changed, retry, renewal, watch_ready);
            select_biased! {
                _ = shutdown => break,
                demand = next => demand,
                live = changed => { if live { upload_due = true; blocked_global_cursor=None; force_descriptor_refresh=true; } else { break; } continue; },
                _ = retry => { upload_due = true; continue; },
                _ = renewal => { continue; },
                _ = watch_ready => { continue; },
            }
        };
        let Some(mut demand) = demand else { break };
        if demand.response.is_closed() {
            continue;
        }
        let result = {
            let hydrate = async {
                for attempt in 0..3 {
                    if let Some(engine) = &engine {
                        engine.sync_mode().ensure_partial_admission_healthy()?;
                        let current = engine.sync_mode().partial_admission().ok_or_else(|| {
                            LixError::new(
                                "LIX_PARTIAL_REPLICA_ADMISSION_MISMATCH",
                                "worker lost admission",
                            )
                        })?;
                        if current.as_ref() != state.as_ref() {
                            let lease_changed = current.baseline_lease().lease_id
                                != state.baseline_lease().lease_id;
                            transport = transport
                                .as_ref()
                                .map(|value| {
                                    value.fork_native_baseline_lease(current.baseline_lease())
                                })
                                .transpose()?;
                            state = current;
                            watch_cursor = state.descriptor().cursor;
                            renewal_deadline = web_time::Instant::now()
                                + lease_renewal_delay(state.baseline_lease().expires_at_ms);
                            if lease_changed {
                                baseline_expired = None;
                            }
                        }
                    }
                    let result = async {
                        if demand_is_resident(&storage, &state, &demand.request).await? {
                            return Ok(());
                        }
                        if let Some(error) = &baseline_expired {
                            return Err(error.clone());
                        }
                        if transport.is_none() {
                            let connected = connect().await?;
                            validate_admission(&storage, &state, &connected).await?;
                            transport = Some(connected);
                        }
                        hydrate_demand(
                            &storage,
                            &state,
                            transport.as_ref().expect("connected"),
                            demand.request.clone(),
                        )
                        .await
                    }
                    .await;
                    if result.is_ok() {
                        return result;
                    }
                    let admission_changed = engine.as_ref().is_some_and(|engine| {
                        engine.sync_mode().partial_admission().as_deref() != Some(state.as_ref())
                    });
                    if !admission_changed || attempt == 2 {
                        return result;
                    }
                    // Retain the actual typed demand and retry installation
                    // after refresh. Never report hydration success merely
                    // because publication advanced the serving basis.
                }
                unreachable!("bounded demand retry returns")
            }
            .fuse();
            let shutdown = shutdown_rx.changed().fuse();
            let cancelled = demand.response.closed().fuse();
            futures_util::pin_mut!(hydrate, shutdown, cancelled);
            select_biased! {
                _ = shutdown => Some(Err(stopped_error())),
                _ = cancelled => None,
                result = hydrate => Some(result),
            }
        };
        if let Some(result) = result {
            if let Err(error) = &result {
                if error.code == "LIX_PARTIAL_BASELINE_EXPIRED" {
                    baseline_expired = Some(error.clone());
                    watch_after = web_time::Instant::now();
                }
            }
            if let Err(error) = &result {
                if is_terminal_partial_transport_error(error) {
                    terminal_error = Some(error.clone());
                }
            }
            let _ = demand.response.send(result);
            if terminal_error.is_some() {
                break;
            }
        }
    }
    // Dropping a publication completion future cannot cancel its already
    // spawned commit owner. That owner keeps Engine and both gates alive and
    // does no network work. Shutdown therefore closes transport independently;
    // it is not an acknowledgement that an accepted publication has finished.
    // Waiting here unconditionally could deadlock a caller closing while it
    // still holds an explicit transaction's operation gate.
    demand_rx.close();
    let stopped = terminal_error.clone().unwrap_or_else(stopped_error);
    if let Some(demand) = queued_demand {
        let _ = demand.response.send(Err(stopped.clone()));
    }
    while let Some(demand) = demand_rx.recv().await {
        let _ = demand.response.send(Err(stopped.clone()));
    }
    // Close the authenticated authority session without letting a disconnected
    // network indefinitely delay local shutdown.
    let Some(transport) = transport else {
        return terminal_error.map_or(Ok(()), Err);
    };
    let close = transport.close_session().fuse();
    let deadline = sleep(Duration::from_secs(1)).fuse();
    futures_util::pin_mut!(close, deadline);
    select_biased! { _ = close => {}, _ = deadline => {} }
    terminal_error.map_or(Ok(()), Err)
}

fn is_terminal_partial_transport_error(error: &LixError) -> bool {
    matches!(
        error.code.as_str(),
        super::SYNC_PROTOCOL_MISMATCH_CODE
            | super::SYNC_REPOSITORY_ID_MISMATCH_CODE
            | super::SYNC_IMMUTABLE_OBJECT_MISMATCH_CODE
    )
}

#[cfg(test)]
mod tests {
    use super::super::SyncTransportFuture;
    use super::super::http::{RawHttpRequest, RawHttpResponse};
    use super::super::native_metadata::NativeMetadataResponse;
    use super::super::partial_state::stage_partial_replica_state;
    use super::*;
    use crate::{Memory, open_lix};
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[tokio::test]
    async fn queued_resident_demand_succeeds_offline_without_connecting() {
        let (storage, state, transport, client, address) = fixture(false).await;
        hydrate_metadata(&storage, &state, &transport, address.clone())
            .await
            .unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let connects = Arc::new(AtomicUsize::new(0));
        let connect = {
            let connects = connects.clone();
            move || -> SyncTransportFuture<'static, HttpSyncTransport<Client>> {
                connects.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Err(LixError::unknown("offline")) })
            }
        };
        let (response, done) = tokio::sync::oneshot::channel();
        sender
            .send(SyncDemand {
                request: SyncDemandRequest::NativeMetadata(
                    address,
                    LixError::unknown("stale demand"),
                ),
                response,
            })
            .await
            .unwrap();
        let caller = async {
            done.await.unwrap().unwrap();
            shutdown.send_replace(SyncShutdown::Stop);
        };
        let (result, _) = futures_util::join!(
            run_partial_worker_connecting(storage, state, None, connect, shutdown_rx, receiver),
            caller
        );
        result.unwrap();
        assert_eq!(connects.load(Ordering::SeqCst), 0);
        assert_eq!(client.fetches.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn lazy_worker_connects_only_for_missing_input_and_cancels_connection() {
        let (storage, state, _, client, address) = fixture(false).await;
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let connects = Arc::new(AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let connect = {
            let connects = connects.clone();
            let entered = entered.clone();
            let cancelled = client.cancelled.clone();
            move || -> SyncTransportFuture<'static, HttpSyncTransport<Client>> {
                connects.fetch_add(1, Ordering::SeqCst);
                let entered = entered.clone();
                let cancelled = cancelled.clone();
                Box::pin(async move {
                    let _guard = CancelGuard(cancelled);
                    entered.notify_one();
                    futures_util::future::pending::<Result<HttpSyncTransport<Client>, LixError>>()
                        .await
                })
            }
        };
        let caller = async {
            tokio::task::yield_now().await;
            assert_eq!(connects.load(Ordering::SeqCst), 0);
            let (response, done) = tokio::sync::oneshot::channel();
            sender
                .send(SyncDemand {
                    request: SyncDemandRequest::NativeMetadata(
                        address,
                        LixError::unknown("missing"),
                    ),
                    response,
                })
                .await
                .unwrap();
            entered.notified().await;
            assert_eq!(connects.load(Ordering::SeqCst), 1);
            shutdown.send_replace(SyncShutdown::Stop);
            assert!(done.await.unwrap().is_err());
        };
        let (result, _) = futures_util::join!(
            run_partial_worker_connecting(storage, state, None, connect, shutdown_rx, receiver),
            caller
        );
        result.unwrap();
        assert_eq!(client.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(client.fetches.load(Ordering::SeqCst), 0);
    }

    #[derive(Clone)]
    struct Client {
        state: Arc<PartialReplicaState>,
        metadata: NativeMetadataResponse,
        block: bool,
        entered: Arc<tokio::sync::Notify>,
        cancelled: Arc<AtomicUsize>,
        fetches: Arc<AtomicUsize>,
    }
    struct CancelGuard(Arc<AtomicUsize>);
    impl Drop for CancelGuard {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl RawHttpClient for Client {
        fn send(&self, request: RawHttpRequest) -> SyncTransportFuture<'_, RawHttpResponse> {
            Box::pin(async move {
                let value = if request.method == http::Method::GET {
                    serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": super::super::SYNC_PROTOCOL_VERSION,
                        "lixId": self.state.repository_id(),
                        "sessionId": "partial-worker-test",
                        "activeAccountId": self.state.active_account_id()
                    })
                } else if request.url.ends_with("/sync/native-metadata") {
                    self.fetches.fetch_add(1, Ordering::SeqCst);
                    if self.block {
                        let _guard = CancelGuard(self.cancelled.clone());
                        self.entered.notify_one();
                        futures_util::future::pending::<()>().await;
                    }
                    serde_json::to_value(&self.metadata).unwrap()
                } else {
                    serde_json::json!({})
                };
                Ok(RawHttpResponse {
                    status: 200,
                    status_text: "OK".into(),
                    body: serde_json::to_vec(&value).unwrap(),
                })
            })
        }
    }
    async fn fixture(
        block: bool,
    ) -> (
        StorageAdapter<Memory>,
        Arc<PartialReplicaState>,
        HttpSyncTransport<Client>,
        Client,
        NativeMetadataRef,
    ) {
        let authority = open_lix().await.unwrap();
        let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
        let address =
            NativeMetadataRef::CommitStateHeader(descriptor.selected_branch.head.commit_id.clone());
        let state = Arc::new(
            PartialReplicaState::new(
                format!("https://example.test/lix/{}", authority.lix_id()),
                authority.active_account_id().to_owned(),
                "00000000-0000-7000-8000-000000000499".into(),
                descriptor,
            )
            .unwrap(),
        );
        let metadata = authority
            .read_sync_native_metadata(&NativeMetadataRequest {
                epoch_id: state.epoch_id().to_owned(),
                objects: vec![address.clone()],
            })
            .await
            .unwrap();
        let client = Client {
            state: state.clone(),
            metadata,
            block,
            entered: Arc::default(),
            cancelled: Arc::default(),
            fetches: Arc::default(),
        };
        let transport = HttpSyncTransport::connect_with(client.clone(), state.remote_id())
            .await
            .unwrap();
        transport
            .bind_native_baseline_lease(state.baseline_lease())
            .unwrap();
        let storage = StorageAdapter::new(Memory::new());
        let mut writes = storage.new_write_set();
        let guard = stage_partial_replica_state(&mut writes, &state, None).unwrap();
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions: vec![guard],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        storage.admit_partial_replica_writer(super::super::partial_replica_write_capability());
        (storage, state, transport, client, address)
    }
    #[tokio::test]
    async fn partial_worker_installs_native_metadata_durably() {
        let (storage, state, transport, client, address) = fixture(false).await;
        validate_admission(&storage, &state, &transport)
            .await
            .unwrap();
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let (response, done) = tokio::sync::oneshot::channel();
        sender
            .send(SyncDemand {
                request: SyncDemandRequest::NativeMetadata(
                    address.clone(),
                    LixError::unknown("missing"),
                ),
                response,
            })
            .await
            .unwrap();
        let worker = run_partial_worker(
            storage.clone(),
            state.clone(),
            transport,
            shutdown_rx,
            receiver,
        );
        let caller = async {
            done.await.unwrap().unwrap();
            let (response, done) = tokio::sync::oneshot::channel();
            sender
                .send(SyncDemand {
                    request: SyncDemandRequest::NativeMetadata(
                        address.clone(),
                        LixError::unknown("duplicate demand"),
                    ),
                    response,
                })
                .await
                .unwrap();
            done.await.unwrap().unwrap();
            shutdown.send_replace(SyncShutdown::Stop);
        };
        let (result, _) = futures_util::join!(worker, caller);
        result.unwrap();
        assert_eq!(client.fetches.load(Ordering::SeqCst), 1);
        // Rebuild the adapter from the backing store: no adapter or worker
        // memory cache survives this boundary.
        let backing = storage.storage().clone();
        drop(storage);
        let storage = StorageAdapter::new(backing);
        let transport = HttpSyncTransport::connect_with(client.clone(), state.remote_id())
            .await
            .unwrap();
        validate_admission(&storage, &state, &transport)
            .await
            .unwrap();
        hydrate_metadata(&storage, &state, &transport, address.clone())
            .await
            .unwrap();
        assert_eq!(client.fetches.load(Ordering::SeqCst), 1);
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut writes = storage.new_write_set();
        let guards = stage_native_metadata(
            &read,
            &mut writes,
            &state,
            &NativeMetadataRequest {
                epoch_id: state.epoch_id().to_owned(),
                objects: vec![address],
            },
            &client.metadata,
        )
        .await
        .unwrap();
        assert!(guards.iter().all(|guard| matches!(
            guard,
            crate::storage_adapter::StoragePrecondition::KeyValueEquals { .. }
        )));
    }
    #[tokio::test]
    async fn partial_worker_shutdown_cancels_fetch_and_resolves_waiters() {
        let (storage, state, transport, client, address) = fixture(true).await;
        let (shutdown, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
        let (sender, receiver) = tokio::sync::mpsc::channel(4);
        let mut waiting = Vec::new();
        for _ in 0..2 {
            let (response, done) = tokio::sync::oneshot::channel();
            sender
                .send(SyncDemand {
                    request: SyncDemandRequest::NativeMetadata(
                        address.clone(),
                        LixError::unknown("missing"),
                    ),
                    response,
                })
                .await
                .unwrap();
            waiting.push(done);
        }
        let caller = async {
            client.entered.notified().await;
            shutdown.send_replace(SyncShutdown::Stop);
            for done in waiting {
                assert!(done.await.unwrap().is_err());
            }
        };
        let (result, _) = futures_util::join!(
            run_partial_worker(storage, state, transport, shutdown_rx, receiver),
            caller
        );
        result.unwrap();
        assert_eq!(client.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(client.fetches.load(Ordering::SeqCst), 1);
    }
}
