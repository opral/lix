//! One repository-scoped synchronization state machine for every platform.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;

use futures_util::{FutureExt, select_biased};

use crate::storage_adapter::Storage;
use crate::{Lix, LixError};

use super::platform::{HttpSyncTransport, SyncTask, sleep, spawn_sync_task};
use super::{SyncPushRequest, SyncRepositoryPullResponse, SyncTransport};

const SYNC_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const SYNC_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);
const SYNC_RESPONSE_TOO_LARGE_CODE: &str = "LIX_ERROR_SYNC_RESPONSE_TOO_LARGE";
const SYNC_REQUEST_TOO_LARGE_CODE: &str = "LIX_ERROR_REQUEST_BODY_TOO_LARGE";
const SYNC_ITEM_TOO_LARGE_CODE: &str = "LIX_ERROR_SYNC_ITEM_TOO_LARGE";
const SYNC_SNAPSHOT_TOO_LARGE_CODE: &str = "LIX_ERROR_SYNC_SNAPSHOT_TOO_LARGE";
const SYNC_DEMAND_STALLED_CODE: &str = "LIX_ERROR_SYNC_DEMAND_STALLED";

#[derive(Debug)]
pub(crate) struct SyncRuntime {
    shutdown_tx: tokio::sync::watch::Sender<SyncShutdown>,
    pub(crate) demand_tx: tokio::sync::mpsc::Sender<SyncDemand>,
    completion_rx: Mutex<Option<tokio::sync::oneshot::Receiver<Result<(), LixError>>>>,
    task: SyncTask,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SyncShutdown {
    Running,
    Drain,
    Stop,
}

#[derive(Debug)]
pub(crate) struct SyncDemand {
    request: SyncDemandRequest,
    response: tokio::sync::oneshot::Sender<Result<(), LixError>>,
}

#[cfg(test)]
impl SyncDemand {
    pub(crate) fn succeed_for_test(self) {
        let _ = self.response.send(Ok(()));
    }
}

#[derive(Debug)]
enum SyncDemandRequest {
    History(Vec<String>),
    Chunks(Vec<String>),
}

#[derive(Debug)]
pub(crate) struct PreparedSync {
    transport: HttpSyncTransport,
    snapshot: PreparedRepositorySnapshot,
    lix_id: String,
    pub(crate) default_branch_id: String,
}

#[derive(Debug)]
pub(super) struct PreparedRepositorySnapshot {
    pub(super) metadata: SyncRepositoryPullResponse,
    pub(super) commits: Vec<super::SyncCommit>,
    pub(super) commit_headers: Vec<super::SyncCommitHeader>,
    pub(super) rows: Vec<super::SyncSnapshotRow>,
    pub(super) checkpoint_roots: BTreeMap<String, String>,
}

/// Performs the fresh-store bootstrap network operation exactly once.
pub(crate) async fn prepare_sync_mode(
    server: &crate::ServerOptions,
) -> Result<PreparedSync, LixError> {
    let remote_id = server.url.trim_end_matches('/');
    let transport = HttpSyncTransport::connect(remote_id, &server.headers).await?;
    let (snapshot, lix_id, default_branch_id) = fetch_repository_snapshot(&transport).await?;
    Ok(PreparedSync {
        transport,
        snapshot,
        lix_id,
        default_branch_id,
    })
}

pub(super) async fn fetch_repository_snapshot<Transport>(
    transport: &Transport,
) -> Result<(PreparedRepositorySnapshot, String, String), LixError>
where
    Transport: SyncTransport,
{
    let metadata = transport
        .pull(None, super::MAX_SYNC_REQUEST_ITEMS)
        .await
        .map_err(snapshot_pull_error)?;
    let SyncRepositoryPullResponse::Snapshot {
        lix_id,
        default_branch_id,
        branches,
        ..
    } = &metadata
    else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "initial sync pull did not return a repository snapshot",
        ));
    };
    let lix_id = lix_id.to_owned();
    let default_branch_id = default_branch_id.to_owned();
    if crate::storage_codec::id_string::uuid_bytes_from_canonical(&lix_id).is_none() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync snapshot lixId must be a canonical UUID",
        ));
    }
    super::validate_sync_branch_id(&default_branch_id)?;
    let head_ids = branches
        .iter()
        .flat_map(|branch| {
            [
                branch.head_commit_id.clone(),
                branch.checkpoint_commit_id.clone(),
            ]
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let branch_targets = branches
        .iter()
        .filter_map(|branch| {
            branch
                .head_commit_id
                .as_deref()
                .map(|head| (branch.branch_id.as_str(), head))
        })
        .collect::<Vec<_>>();
    let (history, mut rows) = futures_util::try_join!(
        fetch_history_objects(transport, head_ids, 1),
        fetch_snapshot_rows(transport, &branch_targets),
    )?;
    let mut checkpoint_targets = Vec::new();
    let mut seen_checkpoints = BTreeSet::new();
    let mut checkpoint_roots = BTreeMap::new();
    for branch in branches {
        let (Some(head), Some(checkpoint)) = (
            branch.head_commit_id.as_deref(),
            branch.checkpoint_commit_id.as_deref(),
        ) else {
            continue;
        };
        if head == checkpoint || !seen_checkpoints.insert(checkpoint) {
            continue;
        }
        checkpoint_targets.push((checkpoint, checkpoint));
        checkpoint_roots.insert(
            checkpoint.to_owned(),
            branch.checkpoint_state_root_id.clone(),
        );
    }
    rows.extend(fetch_snapshot_rows(transport, &checkpoint_targets).await?);
    let snapshot = PreparedRepositorySnapshot {
        metadata,
        commits: history.commits,
        commit_headers: history.commit_headers,
        rows,
        checkpoint_roots,
    };
    Ok((snapshot, lix_id, default_branch_id))
}

impl SyncRuntime {
    pub(crate) fn stop(&self) {
        self.shutdown_tx.send_replace(SyncShutdown::Stop);
    }

    pub(crate) async fn drain_and_join(&self) -> Result<(), LixError> {
        self.shutdown_tx.send_replace(SyncShutdown::Drain);
        let completion = self
            .completion_rx
            .lock()
            .map_err(|_| LixError::unknown("sync completion lock is poisoned"))?
            .take();
        self.task.join().await?;
        match completion {
            Some(completion) => completion.await.map_err(|_| {
                LixError::unknown("sync worker stopped without reporting its completion")
            })?,
            None => Ok(()),
        }
    }
}

fn sync_demand_request_for_error(error: &LixError) -> Result<Option<SyncDemandRequest>, LixError> {
    let (field, context, constructor): (_, _, fn(Vec<String>) -> _) = match error.code.as_str() {
        "LIX_SYNC_HISTORY_REQUIRED" => ("commitIds", "history", SyncDemandRequest::History),
        "LIX_SYNC_CHUNKS_REQUIRED" => ("chunkIds", "chunk", SyncDemandRequest::Chunks),
        // A sparse replica deliberately has no record for the parent just
        // beyond its bounded header frontier. Commit-graph readers report the
        // same structured absence as every other missing commit; sync mode is
        // the only layer that needs to reinterpret it as a history fetch.
        LixError::CODE_COMMIT_NOT_FOUND if is_sparse_commit_graph_miss(error) => {
            ("commit_id", "history", SyncDemandRequest::History)
        }
        _ => return Ok(None),
    };
    let value = error
        .details
        .as_ref()
        .and_then(|details| details.get(field))
        .ok_or_else(|| {
            LixError::new(
                SYNC_DEMAND_STALLED_CODE,
                format!("sync {context} demand error omitted {field}"),
            )
        })?;
    let ids = match value {
        serde_json::Value::Array(ids) => ids
            .iter()
            .map(|id| {
                id.as_str().map(str::to_owned).ok_or_else(|| {
                    LixError::new(
                        SYNC_DEMAND_STALLED_CODE,
                        format!("sync {context} demand {field} must be strings"),
                    )
                })
            })
            .collect::<Result<Vec<_>, _>>()?,
        serde_json::Value::String(id) => vec![id.clone()],
        _ => {
            return Err(LixError::new(
                SYNC_DEMAND_STALLED_CODE,
                format!("sync {context} demand {field} must be strings"),
            ));
        }
    };
    if ids.is_empty() {
        return Err(LixError::new(
            SYNC_DEMAND_STALLED_CODE,
            format!("sync {context} demand error contained no {field}"),
        ));
    }
    Ok(Some(constructor(ids)))
}

async fn send_sync_demand(
    demand_tx: &tokio::sync::mpsc::Sender<SyncDemand>,
    request: SyncDemandRequest,
) -> Result<(), LixError> {
    let (response, done) = tokio::sync::oneshot::channel();
    demand_tx
        .send(SyncDemand { request, response })
        .await
        .map_err(|_| LixError::new(LixError::CODE_CLOSED, "sync demand worker is closed"))?;
    done.await.map_err(|_| stopped_error())??;
    Ok(())
}

fn is_sparse_commit_graph_miss(error: &LixError) -> bool {
    error.code == LixError::CODE_COMMIT_NOT_FOUND
        && error.details.as_ref().is_some_and(|details| {
            details.get("operation").and_then(serde_json::Value::as_str)
                == Some("walk_commit_graph")
                && details.get("role").and_then(serde_json::Value::as_str) == Some("graph_node")
        })
}

#[derive(Debug, Default)]
pub(crate) struct SyncDemandRetry {
    seen: BTreeSet<String>,
}

impl SyncDemandRetry {
    fn admit(&mut self, error: LixError) -> Result<SyncDemandRequest, LixError> {
        let Some(request) = sync_demand_request_for_error(&error)? else {
            return Err(error);
        };
        let identity = format!(
            "{}:{}",
            error.code,
            serde_json::to_string(&error.details).unwrap_or_default()
        );
        if !self.seen.insert(identity) {
            return Err(LixError::new(
                SYNC_DEMAND_STALLED_CODE,
                "sync demand hydration did not make progress",
            ));
        }
        Ok(request)
    }

    pub(crate) async fn hydrate_for_retry(
        &mut self,
        demand_tx: Option<&tokio::sync::mpsc::Sender<SyncDemand>>,
        error: LixError,
    ) -> Result<(), LixError> {
        let Some(demand_tx) = demand_tx else {
            return Err(error);
        };
        let request = self.admit(error)?;
        send_sync_demand(demand_tx, request).await
    }
}

impl Drop for SyncRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) async fn activate_sync_mode<StorageImpl>(
    lix: &mut Lix<StorageImpl>,
    server: &crate::ServerOptions,
    prepared: Option<PreparedSync>,
) -> Result<Arc<SyncRuntime>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let remote_id = server.url.trim_end_matches('/').to_owned();
    let headers = server.headers.clone();
    lix.set_sync_role(crate::sync::SyncRole::Replica)?;

    // Reopens remain local. A fresh open hands in the already-fetched snapshot
    // used to choose the repository's default branch during initialization.
    let initial_transport = if let Some(prepared) = prepared {
        register_blob_manifests(
            lix,
            &prepared.transport,
            &prepared.snapshot.commits,
            &prepared.snapshot.rows,
        )
        .await?;
        let authority_lix_id = prepared.lix_id.clone();
        lix.apply_sync_repository_snapshot(
            &remote_id,
            prepared.transport.active_account_id(),
            &prepared.snapshot.metadata,
            &prepared.snapshot.commits,
            &prepared.snapshot.commit_headers,
            &prepared.snapshot.rows,
            &prepared.snapshot.checkpoint_roots,
        )
        .await?;
        lix.align_repository_identity_for_sync(authority_lix_id)?;
        lix.align_primary_account_for_sync(prepared.transport.active_account_id())
            .await?;
        Some(prepared.transport)
    } else {
        None
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(SyncShutdown::Running);
    let (completion_tx, completion_rx) = tokio::sync::oneshot::channel();
    let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(64);
    let worker_lix = lix
        .open_internal_session_suppressed(lix.active_branch_id().await?, lix.active_account_id())
        .await?;
    let task = spawn_sync_task(async move {
        let result = run_sync_worker(
            worker_lix,
            remote_id,
            headers,
            initial_transport,
            shutdown_rx,
            demand_rx,
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

/// Shared bootstrap, outbox, long-poll, reconciliation, and retry policy.
///
/// Platform adapters supply only task spawning, timers, HTTP, and cancellation.
async fn run_sync_worker<StorageImpl>(
    lix: Lix<StorageImpl>,
    remote_id: String,
    headers: Vec<(String, String)>,
    mut transport: Option<HttpSyncTransport>,
    mut shutdown_rx: tokio::sync::watch::Receiver<SyncShutdown>,
    mut demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
    let mut delta_pull_limit = super::MAX_SYNC_REQUEST_ITEMS;
    let mut push_item_limit = super::MAX_SYNC_REQUEST_ITEMS;
    let mut change_watcher = lix.sync_mode_state().change_watcher();
    let mut internal_demand_retry = SyncDemandRetry::default();
    let mut pending_demands = Vec::new();

    let mut terminal_error = None;
    while *shutdown_rx.borrow() == SyncShutdown::Running {
        if transport.is_none() {
            let connected = {
                let connect = HttpSyncTransport::connect(&remote_id, &headers).fuse();
                let shutdown = shutdown_rx.changed().fuse();
                futures_util::pin_mut!(connect, shutdown);
                select_biased! {
                    _ = shutdown => break,
                    connected = connect => connected,
                }
            };
            match connected {
                Ok(connected) => {
                    if let Err(error) = lix
                        .validate_sync_repository_account(&remote_id, connected.active_account_id())
                        .await
                    {
                        tracing::error!(error = ?error, "sync authority account changed");
                        lix.fail_observers_for_sync(error);
                        break;
                    }
                    transport = Some(connected);
                }
                Err(error) => {
                    tracing::warn!(error = ?error, "sync reconnect failed");
                    if !wait_for_sync_retry(&mut retry_backoff, &mut shutdown_rx).await {
                        break;
                    }
                    continue;
                }
            }
        }

        let Some(current) = transport.as_ref() else {
            continue;
        };
        // This outer race covers every phase, including connect, CAS transfer,
        // and push. Dropping an in-flight transport future invokes the
        // adapter's cancellation mechanism.
        let result = {
            let iteration = sync_iteration(
                &lix,
                &remote_id,
                current,
                &mut push_item_limit,
                &mut delta_pull_limit,
                &mut change_watcher,
                &mut demand_rx,
                &mut pending_demands,
            )
            .fuse();
            let shutdown = shutdown_rx.changed().fuse();
            futures_util::pin_mut!(iteration, shutdown);
            select_biased! {
                _ = shutdown => break,
                result = iteration => result,
            }
        };
        match result {
            Ok(()) => {
                internal_demand_retry = SyncDemandRetry::default();
                retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
            }
            Err(error) => {
                let error = match internal_demand_retry.admit(error) {
                    Ok(request) => {
                        let result = {
                            let hydration = async {
                                match request {
                                    SyncDemandRequest::History(ids) => {
                                        hydrate_history_ids(
                                            &lix,
                                            current,
                                            ids.into_iter().collect(),
                                        )
                                        .await
                                    }
                                    SyncDemandRequest::Chunks(ids) => {
                                        hydrate_chunk_ids(&lix, current, ids.into_iter().collect())
                                            .await
                                    }
                                }
                            }
                            .fuse();
                            let shutdown = shutdown_rx.changed().fuse();
                            futures_util::pin_mut!(hydration, shutdown);
                            select_biased! {
                                _ = shutdown => None,
                                result = hydration => Some(result),
                            }
                        };
                        let Some(result) = result else {
                            break;
                        };
                        match result {
                            Ok(()) => {
                                retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
                                continue;
                            }
                            Err(error) => error,
                        }
                    }
                    Err(error) => error,
                };
                internal_demand_retry = SyncDemandRetry::default();
                if is_terminal_sync_error(&error) {
                    tracing::error!(error = ?error, "sync repository cannot make progress");
                    lix.fail_observers_for_sync(error.clone());
                    terminal_error = Some(error);
                    break;
                }
                tracing::warn!(error = ?error, "sync repository iteration failed");
                transport = None;
                if !wait_for_sync_retry(&mut retry_backoff, &mut shutdown_rx).await {
                    break;
                }
            }
        }
    }
    let result = if let Some(error) = terminal_error {
        Err(error)
    } else if *shutdown_rx.borrow() == SyncShutdown::Drain {
        drain_sync_outbox(
            &lix,
            &remote_id,
            transport.as_ref(),
            &mut push_item_limit,
            &mut delta_pull_limit,
        )
        .await
    } else {
        Ok(())
    };
    drop(pending_demands);
    drop(demand_rx);
    let close_result = lix.close().await;
    result.and(close_result)
}

async fn drain_sync_outbox<StorageImpl>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    transport: Option<&HttpSyncTransport>,
    push_item_limit: &mut usize,
    delta_pull_limit: &mut usize,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    // A disconnected persistent replica already has a durable outbox. Do not
    // turn close into an unbounded reconnect attempt; the next warm open will
    // resume it. Fresh in-memory replicas retain their bootstrap transport, so
    // their accepted work still takes this drain path before the store dies.
    let Some(transport) = transport else {
        return Ok(());
    };
    loop {
        if !push_pending_outbox(lix, remote_id, transport, push_item_limit, delta_pull_limit)
            .await?
        {
            return Ok(());
        }
        let cursor = lix
            .load_sync_repository_cursor(remote_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync repository cursor disappeared while draining",
                )
            })?;
        let response = pull_delta_adaptive(transport, cursor, delta_pull_limit).await?;
        validate_delta_after(cursor, &response)?;
        prepare_pull(lix, transport, &response).await?;
        lix.apply_sync_repository_pull(remote_id, &response).await?;
    }
}

pub(super) async fn sync_iteration<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    transport: &Transport,
    push_item_limit: &mut usize,
    delta_pull_limit: &mut usize,
    change_watcher: &mut tokio::sync::watch::Receiver<u64>,
    demand_rx: &mut tokio::sync::mpsc::Receiver<SyncDemand>,
    pending_demands: &mut Vec<SyncDemand>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    if !pending_demands.is_empty() {
        while let Ok(demand) = demand_rx.try_recv() {
            pending_demands.push(demand);
        }
        pending_demands.retain(|demand| !demand.response.is_closed());
        if !pending_demands.is_empty() {
            let demands = std::mem::take(pending_demands);
            let (retry, retry_error) =
                hydrate_and_resolve_sync_demands(lix, transport, demands).await;
            *pending_demands = retry;
            if let Some(error) = retry_error {
                return Err(error);
            }
        }
    }

    // Establish the generation before inspecting the outbox. A commit racing
    // with outbox construction then wakes the select below and cannot remain
    // hidden behind an already-held long poll.
    let _ = change_watcher.borrow_and_update();

    // Publish completed local commits before waiting for remote work. Commit
    // identity and ref compare-and-swap make retry after a lost response safe.
    let ref_conflicted =
        push_pending_outbox(lix, remote_id, transport, push_item_limit, delta_pull_limit).await?;

    let cursor = lix
        .load_sync_repository_cursor(remote_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync repository cursor disappeared after bootstrap",
            )
        })?;
    if ref_conflicted {
        let response = pull_delta_adaptive(transport, cursor, delta_pull_limit).await?;
        validate_delta_after(cursor, &response)?;
        prepare_pull(lix, transport, &response).await?;
        lix.apply_sync_repository_pull(remote_id, &response).await?;
        return Ok(());
    }
    let local_changed = change_watcher.changed().fuse();
    let pull = pull_delta_adaptive(transport, cursor, delta_pull_limit).fuse();
    let demand = demand_rx.recv().fuse();
    futures_util::pin_mut!(local_changed, pull, demand);
    select_biased! {
        demand = demand => {
            pending_demands.push(demand.ok_or_else(|| {
                LixError::new(LixError::CODE_CLOSED, "sync demand channel closed")
            })?);
            Ok(())
        },
        _ = local_changed => Ok(()),
        response = pull => {
            let response = response?;
            validate_delta_after(cursor, &response)?;
            prepare_pull(lix, transport, &response).await?;
            lix.apply_sync_repository_pull(remote_id, &response).await?;
            Ok(())
        }
    }
}

/// Publishes every currently admitted outbox batch. Returns `true` when a ref
/// conflict requires one pull/reconciliation pass before publication resumes.
async fn push_pending_outbox<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    transport: &Transport,
    push_item_limit: &mut usize,
    delta_pull_limit: &mut usize,
) -> Result<bool, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    loop {
        let Some(mut request) = lix.build_sync_push(remote_id, *push_item_limit).await? else {
            return Ok(false);
        };
        let result = push_with_inline_fallback(lix, transport, &mut request).await;
        match result {
            Ok(receipt) => {
                catch_up_to(lix, remote_id, transport, receipt.cursor, delta_pull_limit).await?;
            }
            // A ref moved concurrently. Pulling the authority's intervening
            // events lets the importer reconcile local refs/outbox state; an
            // immediate reconnect/re-push would repeat the same conflict.
            Err(error) if error.code == LixError::CODE_TRANSACTION_CONFLICT => return Ok(true),
            Err(error) if error.code == SYNC_REQUEST_TOO_LARGE_CODE => {
                reduce_push_limit_after_too_large(push_item_limit, error)?;
            }
            Err(error) => return Err(error),
        }
    }
}

async fn catch_up_to<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    transport: &Transport,
    target_cursor: u64,
    delta_pull_limit: &mut usize,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    loop {
        let cursor = lix
            .load_sync_repository_cursor(remote_id)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync repository cursor disappeared after push",
                )
            })?;
        if cursor >= target_cursor {
            return Ok(());
        }
        let response = pull_delta_adaptive(transport, cursor, delta_pull_limit).await?;
        let next = validate_delta_after(cursor, &response)?;
        if next <= cursor {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "sync push acknowledged cursor {target_cursor}, but pull remained at {cursor}"
                ),
            ));
        }
        prepare_pull(lix, transport, &response).await?;
        lix.apply_sync_repository_pull(remote_id, &response).await?;
    }
}

async fn pull_delta_adaptive<Transport>(
    transport: &Transport,
    after: u64,
    limit: &mut usize,
) -> Result<SyncRepositoryPullResponse, LixError>
where
    Transport: SyncTransport,
{
    fetch_adaptive(limit, "repository event", |limit| {
        transport.pull(Some(after), limit)
    })
    .await
}

async fn fetch_snapshot_rows<Transport>(
    transport: &Transport,
    targets: &[(&str, &str)],
) -> Result<Vec<super::SyncSnapshotRow>, LixError>
where
    Transport: SyncTransport,
{
    let mut rows = Vec::new();
    let mut page_limit = super::MAX_SYNC_REQUEST_ITEMS;
    for &(branch_id, head_commit_id) in targets {
        let mut continuation = None;
        let mut seen_continuations = BTreeSet::new();
        loop {
            let page = fetch_adaptive(&mut page_limit, "snapshot row", |limit| {
                transport.snapshot_rows(branch_id, head_commit_id, continuation.as_deref(), limit)
            })
            .await?;
            if page.branch_id != branch_id || page.head_commit_id != head_commit_id {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot row page changed its pinned branch or head",
                ));
            }
            if page.rows.len() > page_limit
                || page.rows.iter().any(|row| row.branch_id != branch_id)
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot row page exceeded its limit or contained another branch",
                ));
            }
            let next = page.continuation;
            if next.is_some() && page.rows.is_empty() {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot row page returned a continuation without rows",
                ));
            }
            if next
                .as_ref()
                .is_some_and(|next| next.len() > 4096 || !seen_continuations.insert(next.clone()))
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync snapshot row continuation is invalid or did not advance",
                ));
            }
            rows.extend(page.rows);
            let Some(next) = next else {
                break;
            };
            continuation = Some(next);
        }
    }
    Ok(rows)
}

async fn fetch_adaptive<T, Fetch, FetchFuture>(
    limit: &mut usize,
    item_kind: &str,
    mut fetch: Fetch,
) -> Result<T, LixError>
where
    Fetch: FnMut(usize) -> FetchFuture,
    FetchFuture: Future<Output = Result<T, LixError>>,
{
    loop {
        match fetch(*limit).await {
            Ok(response) => return Ok(response),
            Err(error) if is_response_too_large(&error) && *limit > 1 => {
                *limit = smaller_page_limit(*limit);
            }
            Err(error) if is_response_too_large(&error) => {
                return Err(sync_item_too_large_error(item_kind, error));
            }
            Err(error) => return Err(error),
        }
    }
}

fn smaller_page_limit(current: usize) -> usize {
    (current / 2).max(1)
}

fn is_response_too_large(error: &LixError) -> bool {
    error.code == SYNC_RESPONSE_TOO_LARGE_CODE
        || error.code == SYNC_REQUEST_TOO_LARGE_CODE
        || (error.code == LixError::CODE_INVALID_PARAM
            && error.message.contains("response exceeds"))
}

fn reduce_push_limit_after_too_large(limit: &mut usize, error: LixError) -> Result<(), LixError> {
    if *limit > 1 {
        *limit = smaller_page_limit(*limit);
        Ok(())
    } else {
        Err(sync_item_too_large_error("push item", error))
    }
}

fn sync_item_too_large_error(kind: &str, source: LixError) -> LixError {
    LixError::new(
        SYNC_ITEM_TOO_LARGE_CODE,
        format!(
            "one sync {kind} exceeds the protocol transfer limit: {}",
            source.message
        ),
    )
    .with_hint(format!(
        "reduce the serialized size of this {kind}; retrying cannot advance synchronization"
    ))
}

fn snapshot_pull_error(error: LixError) -> LixError {
    if !is_response_too_large(&error) {
        return error;
    }
    LixError::new(
        SYNC_SNAPSHOT_TOO_LARGE_CODE,
        format!(
            "the hot-state sync snapshot exceeds the protocol response limit: {}",
            error.message
        ),
    )
    .with_hint("reduce the branch list or add branch-metadata paging before bootstrapping")
}

fn is_terminal_sync_error(error: &LixError) -> bool {
    matches!(
        error.code.as_str(),
        SYNC_ITEM_TOO_LARGE_CODE | SYNC_SNAPSHOT_TOO_LARGE_CODE | SYNC_DEMAND_STALLED_CODE
    )
}

fn is_retryable_sync_transport_error(error: &LixError) -> bool {
    if error.code == super::http::SYNC_TRANSPORT_ERROR_CODE {
        return true;
    }
    let status = error
        .details
        .as_ref()
        .and_then(|details| details.get("httpStatus"))
        .and_then(serde_json::Value::as_u64);
    matches!(status, Some(408 | 429 | 500..=599))
}

async fn hydrate_and_resolve_sync_demands<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    demands: Vec<SyncDemand>,
) -> (Vec<SyncDemand>, Option<LixError>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let mut history_ids = BTreeSet::new();
    let mut chunk_ids = BTreeSet::new();
    for demand in &demands {
        match &demand.request {
            SyncDemandRequest::History(ids) => history_ids.extend(ids),
            SyncDemandRequest::Chunks(ids) => chunk_ids.extend(ids),
        }
    }
    let history_ids = history_ids.into_iter().cloned().collect::<BTreeSet<_>>();
    let chunk_ids = chunk_ids.into_iter().cloned().collect::<BTreeSet<_>>();
    let history_result = if history_ids.is_empty() {
        Ok(())
    } else {
        hydrate_history_ids(lix, transport, history_ids).await
    };
    let chunk_result = if chunk_ids.is_empty() {
        Ok(())
    } else {
        hydrate_chunk_ids(lix, transport, chunk_ids).await
    };
    let mut retry = Vec::new();
    let mut retry_error = None;
    for demand in demands {
        if demand.response.is_closed() {
            continue;
        }
        let result = match &demand.request {
            SyncDemandRequest::History(_) => history_result.clone(),
            SyncDemandRequest::Chunks(_) => chunk_result.clone(),
        };
        match result {
            Err(error) if is_retryable_sync_transport_error(&error) => {
                retry_error.get_or_insert(error);
                retry.push(demand);
            }
            result => {
                let _ = demand.response.send(result);
            }
        }
    }
    (retry, retry_error)
}

async fn hydrate_history_ids<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    requested: BTreeSet<String>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let pending = lix.sync_history_demand_ids(requested).await?;
    if pending.is_empty() {
        return Ok(());
    }
    let fetched =
        fetch_history_objects(transport, pending, super::MAX_SYNC_HISTORY_PAGE_SIZE).await?;
    let boundary_targets = fetched
        .boundaries
        .iter()
        .map(|boundary| (boundary.commit_id.as_str(), boundary.commit_id.as_str()))
        .collect::<Vec<_>>();
    let rows = fetch_snapshot_rows(transport, &boundary_targets).await?;
    register_blob_manifests(lix, transport, &fetched.commits, &rows).await?;
    lix.import_sync_history_headers(&fetched.commit_headers)
        .await?;
    lix.import_sync_history_boundaries(&fetched.commits, &fetched.boundaries, &rows)
        .await
}

#[derive(Debug)]
struct FetchedHistory {
    commits: Vec<super::SyncCommit>,
    commit_headers: Vec<super::SyncCommitHeader>,
    boundaries: Vec<super::SyncHistoryBoundary>,
}

async fn fetch_history_objects<Transport>(
    transport: &Transport,
    pending: BTreeSet<String>,
    requested_page_limit: usize,
) -> Result<FetchedHistory, LixError>
where
    Transport: SyncTransport,
{
    let mut commits = BTreeMap::new();
    let mut commit_headers = BTreeMap::new();
    let mut boundaries = BTreeMap::new();
    let mut history_page_limit = requested_page_limit;
    for head in pending {
        let response =
            fetch_history_page_adaptive(transport, &head, &mut history_page_limit).await?;
        for header in response.commit_headers {
            if let Some(existing) = commit_headers.insert(header.commit_id.clone(), header.clone())
                && existing != header
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync history returned conflicting commit headers",
                ));
            }
        }
        let mut response_commits = response.commits;
        for commit in &response_commits {
            commit.validate()?;
        }
        let returned = response_commits
            .iter()
            .map(|commit| commit.commit_id.clone())
            .collect::<BTreeSet<_>>();
        if !returned.contains(&head) {
            return Err(LixError::new(
                LixError::CODE_COMMIT_NOT_FOUND,
                format!("sync history response omitted requested head '{head}'"),
            ));
        }
        for boundary in response.boundaries {
            if !returned.contains(&boundary.commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync history returned boundary '{}' outside its page",
                        boundary.commit_id
                    ),
                ));
            }
            if let Some(existing) = boundaries.insert(boundary.commit_id.clone(), boundary.clone())
                && existing != boundary
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync history returned conflicting boundary roots",
                ));
            }
        }
        for commit in response_commits.drain(..) {
            if let Some(existing) = commits.insert(commit.commit_id.clone(), commit.clone())
                && existing != commit
            {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync history returned conflicting duplicate commits",
                ));
            }
        }
    }

    Ok(FetchedHistory {
        // Both import paths canonicalize and dependency-order this set before
        // staging it. Keep fetch responsible only for validating and merging
        // transport responses instead of sorting the same graph twice.
        commits: commits.into_values().collect(),
        commit_headers: commit_headers.into_values().collect(),
        boundaries: boundaries.into_values().collect(),
    })
}

async fn fetch_history_page_adaptive<Transport>(
    transport: &Transport,
    head: &str,
    limit: &mut usize,
) -> Result<super::SyncHistoryResponse, LixError>
where
    Transport: SyncTransport,
{
    let response = fetch_adaptive(limit, "history commit", |limit| {
        transport.history(head, limit)
    })
    .await?;
    if response.commits.len() > *limit {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync history response exceeds the requested page limit",
        ));
    }
    Ok(response)
}

pub(super) async fn register_blob_manifests<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    commits: &[super::SyncCommit],
    rows: &[super::SyncSnapshotRow],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let mut blob_ids = super::repository::sync_commit_blob_ids(commits)?;
    blob_ids.extend(blob_ids_from_rows(
        rows.iter()
            .map(|row| (row.schema_key.as_str(), row.snapshot.as_ref())),
    )?);
    ensure_blob_manifests(lix, transport, blob_ids).await
}

async fn hydrate_chunk_ids<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    chunk_ids: BTreeSet<String>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    for chunk_id in chunk_ids {
        super::validate_blake3_id(&chunk_id, "sync demanded chunk id")?;
        if lix.get_sync_chunk(&chunk_id).await?.is_some() {
            continue;
        }
        let bytes = transport
            .get_chunk(&chunk_id)
            .await?
            .ok_or_else(|| missing_chunk_error(&chunk_id, "demand", "remote read"))?;
        lix.put_sync_chunk(&chunk_id, &bytes).await?;
    }
    Ok(())
}

/// Drives the same lazy history/blob hydration path as a live sync worker.
/// Kept test-only so deterministic simulations can advance without spawning
/// background tasks or introducing a second demand implementation.
#[cfg(test)]
pub(super) async fn hydrate_error_for_test<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    error: LixError,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    match sync_demand_request_for_error(&error)? {
        Some(SyncDemandRequest::History(ids)) => {
            hydrate_history_ids(lix, transport, ids.into_iter().collect()).await
        }
        Some(SyncDemandRequest::Chunks(ids)) => {
            hydrate_chunk_ids(lix, transport, ids.into_iter().collect()).await
        }
        None => Err(error),
    }
}

async fn push_request_blobs<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    request: &SyncPushRequest,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let inline_blob_ids = request
        .inline_blobs
        .iter()
        .map(|manifest| manifest.blob_id.as_str())
        .collect::<BTreeSet<_>>();
    for blob_id in super::repository::sync_commit_blob_ids(&request.commits)? {
        if inline_blob_ids.contains(blob_id.as_str()) {
            continue;
        }
        let manifest = lix.get_sync_blob_manifest(&blob_id).await?.ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("local push references missing sync blob '{blob_id}'"),
            )
        })?;
        let mut registration = transport.register_blob(&manifest).await?;
        for chunk_id in registration
            .missing_chunk_ids
            .iter()
            .collect::<BTreeSet<_>>()
        {
            let bytes = lix
                .get_sync_chunk(chunk_id)
                .await?
                .ok_or_else(|| missing_chunk_error(chunk_id, &blob_id, "local push"))?;
            transport.put_chunk(chunk_id, &bytes).await?;
        }
        if !registration.missing_chunk_ids.is_empty() {
            registration = transport.register_blob(&manifest).await?;
        }
        if !registration.missing_chunk_ids.is_empty() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "sync blob '{blob_id}' remained incomplete after uploading requested chunks"
                ),
            ));
        }
    }
    Ok(())
}

async fn push_with_inline_fallback<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    request: &mut SyncPushRequest,
) -> Result<super::SyncPushResponse, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    push_request_blobs(lix, transport, request).await?;
    let first = transport.push(request).await;
    let request_too_large = first
        .as_ref()
        .is_err_and(|error| error.code == SYNC_REQUEST_TOO_LARGE_CODE);
    if request_too_large && !request.inline_blobs.is_empty() {
        // A server may configure a lower request-body cap than the protocol
        // default. Inline blobs are optional acceleration: retry the exact
        // commit/ref request once through the ordinary manifest lane.
        request.inline_blobs.clear();
        push_request_blobs(lix, transport, request).await?;
        return transport.push(request).await;
    }
    first
}

async fn prepare_pull<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    response: &SyncRepositoryPullResponse,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let SyncRepositoryPullResponse::Delta { events, .. } = response else {
        return Ok(());
    };
    let mut blob_ids = BTreeSet::new();
    let mut inline_blob_ids = BTreeSet::new();
    for event in events {
        blob_ids.extend(super::repository::sync_commit_blob_ids(&event.commits)?);
        inline_blob_ids.extend(
            event
                .inline_blobs
                .iter()
                .map(|manifest| manifest.blob_id.clone()),
        );
    }
    blob_ids.retain(|blob_id| !inline_blob_ids.contains(blob_id));
    ensure_blob_manifests(lix, transport, blob_ids).await?;
    let included = events
        .iter()
        .flat_map(|event| event.commits.iter().map(|commit| commit.commit_id.as_str()))
        .collect::<BTreeSet<_>>();
    let targets = events
        .iter()
        .flat_map(|event| &event.ref_updates)
        .flat_map(|update| {
            [
                update.head_commit_id.as_ref(),
                update.checkpoint_commit_id.as_ref(),
            ]
        })
        .flatten()
        .filter(|commit_id| !included.contains(commit_id.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    hydrate_history_ids(lix, transport, targets).await
}

async fn ensure_blob_manifests<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    blob_ids: BTreeSet<String>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let mut missing = Vec::new();
    for blob_id in blob_ids {
        if lix.has_sync_blob_manifest(&blob_id).await? {
            continue;
        }
        missing.push(blob_id);
    }
    for requested in missing.chunks(super::MAX_SYNC_BLOB_BATCH_ITEMS) {
        let manifests = transport.get_blobs(requested).await?;
        if manifests.len() != requested.len() {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync blob batch response omitted or added manifests",
            ));
        }
        for (blob_id, manifest) in requested.iter().zip(manifests) {
            if manifest.blob_id != *blob_id {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "sync blob request '{blob_id}' returned manifest '{}'",
                        manifest.blob_id
                    ),
                ));
            }
            lix.register_deferred_sync_blob_manifest(&manifest).await?;
        }
    }
    Ok(())
}

fn blob_ids_from_rows<'a>(
    rows: impl IntoIterator<Item = (&'a str, Option<&'a serde_json::Value>)>,
) -> Result<BTreeSet<String>, LixError> {
    let mut blob_ids = BTreeSet::new();
    for (schema_key, snapshot) in rows {
        if schema_key != "lix_binary_blob_ref" {
            continue;
        }
        let Some(snapshot) = snapshot else {
            continue;
        };
        let Some(blob_id) = snapshot
            .get("blob_hash")
            .and_then(serde_json::Value::as_str)
        else {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "sync binary blob ref is missing blob_hash",
            ));
        };
        super::validate_blake3_id(blob_id, "sync binary blob ref blob_hash")?;
        blob_ids.insert(blob_id.to_owned());
    }
    Ok(blob_ids)
}

fn missing_chunk_error(chunk_id: &str, blob_id: &str, direction: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{direction} sync blob '{blob_id}' references missing chunk '{chunk_id}'"),
    )
}

fn validate_delta_after(
    previous_cursor: u64,
    response: &SyncRepositoryPullResponse,
) -> Result<u64, LixError> {
    let SyncRepositoryPullResponse::Delta { cursor, events } = response else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "cursor sync pull unexpectedly returned a repository snapshot",
        ));
    };
    let mut expected = previous_cursor.checked_add(1);
    let has_gap = events.iter().any(|event| {
        let matches = expected == Some(event.cursor);
        expected = event.cursor.checked_add(1);
        !matches
    });
    if *cursor < previous_cursor
        || has_gap
        || events.last().is_some_and(|event| event.cursor != *cursor)
        || (events.is_empty() && *cursor != previous_cursor)
    {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "sync pull returned a non-monotonic repository cursor",
        ));
    }
    Ok(*cursor)
}

async fn wait_for_sync_retry(
    retry_backoff: &mut Duration,
    shutdown_rx: &mut tokio::sync::watch::Receiver<SyncShutdown>,
) -> bool {
    if *shutdown_rx.borrow() != SyncShutdown::Running {
        return false;
    }
    let timer = sleep(*retry_backoff).fuse();
    let changed = shutdown_rx.changed().fuse();
    futures_util::pin_mut!(timer, changed);
    let elapsed = select_biased! {
        _ = changed => false,
        _ = timer => true,
    };
    if !elapsed {
        return false;
    }
    *retry_backoff = next_backoff(*retry_backoff);
    true
}

fn next_backoff(current: Duration) -> Duration {
    current
        .checked_mul(2)
        .unwrap_or(SYNC_MAX_RETRY_BACKOFF)
        .min(SYNC_MAX_RETRY_BACKOFF)
}

fn stopped_error() -> LixError {
    LixError::new(LixError::CODE_CLOSED, "sync worker is stopping")
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine as _;
    use std::sync::Mutex;

    use crate::engine::Engine;
    use crate::storage::Memory;
    use crate::{Value, open_lix};

    #[derive(Debug)]
    struct HistoryTransport {
        commits: BTreeMap<String, super::super::SyncCommit>,
        missing: BTreeSet<String>,
        calls: Arc<Mutex<Vec<Vec<String>>>>,
        max_history_items: Option<usize>,
        fail_first_history: bool,
        commit_headers: Vec<super::super::SyncCommitHeader>,
        history_boundaries: BTreeMap<String, super::super::SyncHistoryBoundary>,
        boundary_rows: BTreeMap<String, Vec<super::super::SyncSnapshotRow>>,
        blobs: BTreeMap<String, super::super::SyncBlobManifest>,
        blob_calls: Arc<Mutex<Vec<Vec<String>>>>,
        chunks: BTreeMap<String, Vec<u8>>,
        chunk_calls: Arc<Mutex<Vec<String>>>,
    }

    impl SyncTransport for HistoryTransport {
        fn active_account_id(&self) -> &str {
            crate::ANONYMOUS_ACCOUNT_ID
        }

        fn push<'a>(
            &'a self,
            _request: &'a SyncPushRequest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncPushResponse> {
            Box::pin(async { Err(LixError::unknown("unused history test push")) })
        }

        fn pull(
            &self,
            _after: Option<u64>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'_, SyncRepositoryPullResponse> {
            Box::pin(async { Err(LixError::unknown("unused history test pull")) })
        }

        fn snapshot_rows<'a>(
            &'a self,
            branch_id: &'a str,
            head_commit_id: &'a str,
            continuation: Option<&'a str>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncSnapshotRowPage> {
            Box::pin(async move {
                if continuation.is_some() || branch_id != head_commit_id {
                    return Err(LixError::new(
                        LixError::CODE_INVALID_PARAM,
                        "unexpected history snapshot page request",
                    ));
                }
                Ok(super::super::SyncSnapshotRowPage {
                    branch_id: branch_id.to_owned(),
                    head_commit_id: head_commit_id.to_owned(),
                    rows: self
                        .boundary_rows
                        .get(head_commit_id)
                        .cloned()
                        .unwrap_or_default(),
                    continuation: None,
                })
            })
        }

        fn history<'a>(
            &'a self,
            head: &'a str,
            limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async move {
                let call_number = {
                    let mut calls = self.calls.lock().expect("history calls lock");
                    calls.push(vec![head.to_owned()]);
                    calls.len()
                };
                if self.fail_first_history && call_number == 1 {
                    return Err(LixError::new(
                        super::super::http::SYNC_TRANSPORT_ERROR_CODE,
                        "test transport disconnected",
                    ));
                }
                if self.max_history_items.is_some_and(|cap| limit > cap) {
                    return Err(LixError::new(
                        SYNC_RESPONSE_TOO_LARGE_CODE,
                        "test history response exceeds cap",
                    ));
                }
                if self.missing.contains(head) || !self.commits.contains_key(head) {
                    return Err(LixError::commit_not_found(head, "test_history", "head"));
                }
                let mut next = Some(head.to_owned());
                let mut newest_first = Vec::new();
                while let Some(commit_id) = next
                    && newest_first.len() < limit
                {
                    let Some(commit) = self.commits.get(&commit_id).cloned() else {
                        break;
                    };
                    next = commit.parent_commit_ids.first().cloned();
                    newest_first.push(commit);
                }
                let returned = newest_first
                    .iter()
                    .map(|commit| commit.commit_id.as_str())
                    .collect::<BTreeSet<_>>();
                let boundaries = newest_first
                    .iter()
                    .filter(|commit| {
                        commit
                            .parent_commit_ids
                            .first()
                            .is_some_and(|parent| !returned.contains(parent.as_str()))
                    })
                    .map(|commit| {
                        self.history_boundaries
                            .get(&commit.commit_id)
                            .cloned()
                            .expect("test boundary has a certified live root")
                    })
                    .collect();
                newest_first.reverse();
                Ok(super::super::SyncHistoryResponse {
                    commits: newest_first,
                    commit_headers: self.commit_headers.clone(),
                    boundaries,
                })
            })
        }

        fn get_blobs<'a>(
            &'a self,
            blob_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, Vec<super::super::SyncBlobManifest>> {
            Box::pin(async move {
                self.blob_calls
                    .lock()
                    .expect("blob calls lock")
                    .push(blob_ids.to_vec());
                Ok(blob_ids
                    .iter()
                    .filter_map(|blob_id| self.blobs.get(blob_id).cloned())
                    .collect())
            })
        }

        fn register_blob<'a>(
            &'a self,
            _manifest: &'a super::super::SyncBlobManifest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncBlobRegistration> {
            Box::pin(async { Err(LixError::unknown("unused history test blob register")) })
        }

        fn get_chunk<'a>(
            &'a self,
            chunk_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<Vec<u8>>> {
            Box::pin(async move {
                self.chunk_calls
                    .lock()
                    .expect("chunk calls lock")
                    .push(chunk_id.to_owned());
                Ok(self.chunks.get(chunk_id).cloned())
            })
        }

        fn put_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
            _bytes: &'a [u8],
        ) -> super::super::SyncTransportFuture<'a, ()> {
            Box::pin(async { Err(LixError::unknown("unused history test chunk put")) })
        }
    }

    #[derive(Debug)]
    struct BlobCallTransport {
        manifests: BTreeMap<String, super::super::SyncBlobManifest>,
        get_calls: Arc<Mutex<Vec<Vec<String>>>>,
        register_calls: Arc<Mutex<Vec<String>>>,
        push_calls: Arc<Mutex<Vec<usize>>>,
        reject_inline_push: bool,
    }

    impl SyncTransport for BlobCallTransport {
        fn active_account_id(&self) -> &str {
            crate::ANONYMOUS_ACCOUNT_ID
        }

        fn push<'a>(
            &'a self,
            request: &'a SyncPushRequest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncPushResponse> {
            Box::pin(async move {
                self.push_calls
                    .lock()
                    .expect("push calls lock")
                    .push(request.inline_blobs.len());
                if self.reject_inline_push && !request.inline_blobs.is_empty() {
                    Err(LixError::new(
                        SYNC_REQUEST_TOO_LARGE_CODE,
                        "test server inline body cap",
                    ))
                } else {
                    Ok(super::super::SyncPushResponse { cursor: 1 })
                }
            })
        }

        fn pull(
            &self,
            _after: Option<u64>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'_, SyncRepositoryPullResponse> {
            Box::pin(async { Err(LixError::unknown("unused blob-call pull")) })
        }

        fn snapshot_rows<'a>(
            &'a self,
            _branch_id: &'a str,
            _head_commit_id: &'a str,
            _continuation: Option<&'a str>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncSnapshotRowPage> {
            Box::pin(async { Err(LixError::unknown("unused blob-call snapshot")) })
        }

        fn history<'a>(
            &'a self,
            _head: &'a str,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async { Err(LixError::unknown("unused blob-call history")) })
        }

        fn get_blobs<'a>(
            &'a self,
            blob_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, Vec<super::super::SyncBlobManifest>> {
            Box::pin(async move {
                self.get_calls
                    .lock()
                    .expect("get calls lock")
                    .push(blob_ids.to_vec());
                Ok(blob_ids
                    .iter()
                    .filter_map(|blob_id| self.manifests.get(blob_id).cloned())
                    .collect())
            })
        }

        fn register_blob<'a>(
            &'a self,
            manifest: &'a super::super::SyncBlobManifest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncBlobRegistration> {
            Box::pin(async move {
                self.register_calls
                    .lock()
                    .expect("register calls lock")
                    .push(manifest.blob_id.clone());
                Ok(super::super::SyncBlobRegistration {
                    missing_chunk_ids: Vec::new(),
                })
            })
        }

        fn get_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<Vec<u8>>> {
            Box::pin(async { Err(LixError::unknown("unused blob-call chunk get")) })
        }

        fn put_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
            _bytes: &'a [u8],
        ) -> super::super::SyncTransportFuture<'a, ()> {
            Box::pin(async { Err(LixError::unknown("unused blob-call chunk put")) })
        }
    }

    fn blob_ref_commit(blob_id: &str) -> super::super::SyncCommit {
        super::super::SyncCommit {
            commit_id: crate::changelog::CommitId::for_test_label("runtime-inline-commit")
                .to_string(),
            parent_commit_ids: Vec::new(),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            selected_source_commit_id: None,
            state_alias: None,
            members: vec![super::super::commit::SyncCommitMember {
                change_id: crate::changelog::ChangeId::for_test_label("runtime-inline-change")
                    .to_string(),
                authored: true,
                schema_key: "lix_binary_blob_ref".to_owned(),
                file_id: None,
                row_pk: serde_json::json!(["runtime-inline"]),
                deleted: false,
                snapshot: Some(serde_json::json!({ "blob_hash": blob_id })),
                metadata: None,
                row_created_at: "2026-01-01T00:00:00Z".to_owned(),
                row_updated_at: "2026-01-01T00:00:00Z".to_owned(),
                change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_owned(),
                change_created_at: "2026-01-01T00:00:00Z".to_owned(),
                origin_key: None,
            }],
        }
    }

    fn wire_blob(bytes: &[u8], inline: bool) -> super::super::SyncBlobManifest {
        let canonical = crate::binary_cas::CanonicalBlobManifest::from_bytes(bytes);
        super::super::SyncBlobManifest {
            blob_id: canonical.blob_id.to_hex(),
            size_bytes: canonical.size_bytes,
            chunks: canonical
                .chunks
                .iter()
                .map(|chunk| super::super::SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: inline
                .then(|| base64::engine::general_purpose::STANDARD.encode(bytes)),
        }
    }

    #[tokio::test]
    async fn inline_blobs_skip_manifest_network_lanes_while_large_blobs_fall_back() {
        let lix = open_lix().await.expect("runtime blob fixture opens");
        let inline = wire_blob(b"small inline runtime payload", true);
        let inline_commit = blob_ref_commit(&inline.blob_id);
        let get_calls = Arc::new(Mutex::new(Vec::new()));
        let register_calls = Arc::new(Mutex::new(Vec::new()));
        let push_calls = Arc::new(Mutex::new(Vec::new()));
        let transport = BlobCallTransport {
            manifests: BTreeMap::new(),
            get_calls: Arc::clone(&get_calls),
            register_calls: Arc::clone(&register_calls),
            push_calls: Arc::clone(&push_calls),
            reject_inline_push: false,
        };
        let mut push = SyncPushRequest {
            commits: vec![inline_commit.clone()],
            ref_updates: Vec::new(),
            inline_blobs: vec![inline.clone()],
        };
        push_with_inline_fallback(&lix, &transport, &mut push)
            .await
            .expect("embedded outbound blob pushes directly");
        assert_eq!(*push_calls.lock().expect("push calls lock"), vec![1]);
        let inline_delta = SyncRepositoryPullResponse::Delta {
            cursor: 1,
            events: vec![super::super::SyncEvent {
                cursor: 1,
                commits: vec![inline_commit],
                ref_updates: Vec::new(),
                inline_blobs: vec![inline],
            }],
        };
        prepare_pull(&lix, &transport, &inline_delta)
            .await
            .expect("embedded inbound blob needs no fetch");

        let empty = wire_blob(&[], true);
        let empty_commit = blob_ref_commit(&empty.blob_id);
        let mut empty_push = SyncPushRequest {
            commits: vec![empty_commit.clone()],
            ref_updates: Vec::new(),
            inline_blobs: vec![empty.clone()],
        };
        push_with_inline_fallback(&lix, &transport, &mut empty_push)
            .await
            .expect("empty blob pushes inline without a manifest request");
        prepare_pull(
            &lix,
            &transport,
            &SyncRepositoryPullResponse::Delta {
                cursor: 2,
                events: vec![super::super::SyncEvent {
                    cursor: 2,
                    commits: vec![empty_commit],
                    ref_updates: Vec::new(),
                    inline_blobs: vec![empty],
                }],
            },
        )
        .await
        .expect("empty blob arrives inline without a manifest fetch");
        assert_eq!(*push_calls.lock().expect("push calls lock"), vec![1, 1]);
        assert!(get_calls.lock().expect("get calls lock").is_empty());
        assert!(
            register_calls
                .lock()
                .expect("register calls lock")
                .is_empty()
        );

        let large = wire_blob(&vec![7_u8; 65 * 1024], false);
        let large_commit = blob_ref_commit(&large.blob_id);
        let large_get_calls = Arc::new(Mutex::new(Vec::new()));
        let large_transport = BlobCallTransport {
            manifests: BTreeMap::from([(large.blob_id.clone(), large)]),
            get_calls: Arc::clone(&large_get_calls),
            register_calls: Arc::new(Mutex::new(Vec::new())),
            push_calls: Arc::new(Mutex::new(Vec::new())),
            reject_inline_push: false,
        };
        prepare_pull(
            &lix,
            &large_transport,
            &SyncRepositoryPullResponse::Delta {
                cursor: 2,
                events: vec![super::super::SyncEvent {
                    cursor: 2,
                    commits: vec![large_commit],
                    ref_updates: Vec::new(),
                    inline_blobs: Vec::new(),
                }],
            },
        )
        .await
        .expect("large blob retains manifest fetch lane");
        assert_eq!(
            large_get_calls.lock().expect("large get calls lock").len(),
            1
        );
    }

    #[tokio::test]
    async fn lower_server_body_cap_retries_inline_push_through_manifest_lane() {
        let lix = open_lix().await.expect("inline fallback fixture opens");
        let bytes = b"inline fallback payload";
        lix.execute(
            "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
            &[
                Value::Text("/inline-fallback.bin".to_owned()),
                Value::Blob(bytes.to_vec().into()),
            ],
        )
        .await
        .expect("inline fallback blob should commit locally");
        let inline = wire_blob(bytes, true);
        let mut request = SyncPushRequest {
            commits: vec![blob_ref_commit(&inline.blob_id)],
            ref_updates: Vec::new(),
            inline_blobs: vec![inline],
        };
        let register_calls = Arc::new(Mutex::new(Vec::new()));
        let push_calls = Arc::new(Mutex::new(Vec::new()));
        let transport = BlobCallTransport {
            manifests: BTreeMap::new(),
            get_calls: Arc::new(Mutex::new(Vec::new())),
            register_calls: Arc::clone(&register_calls),
            push_calls: Arc::clone(&push_calls),
            reject_inline_push: true,
        };
        let response = push_with_inline_fallback(&lix, &transport, &mut request)
            .await
            .expect("manifest fallback should fit lower server body cap");
        assert_eq!(response.cursor, 1);
        assert!(request.inline_blobs.is_empty());
        assert_eq!(*push_calls.lock().expect("push calls lock"), vec![1, 0]);
        assert_eq!(register_calls.lock().expect("register calls lock").len(), 1);
    }

    #[derive(Debug)]
    struct CappedPullTransport {
        max_items: usize,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl SyncTransport for CappedPullTransport {
        fn active_account_id(&self) -> &str {
            crate::ANONYMOUS_ACCOUNT_ID
        }

        fn push<'a>(
            &'a self,
            _request: &'a SyncPushRequest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncPushResponse> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull push")) })
        }

        fn pull(
            &self,
            after: Option<u64>,
            limit: usize,
        ) -> super::super::SyncTransportFuture<'_, SyncRepositoryPullResponse> {
            Box::pin(async move {
                self.calls.lock().expect("pull calls lock").push(limit);
                if limit > self.max_items {
                    return Err(LixError::new(
                        SYNC_RESPONSE_TOO_LARGE_CODE,
                        "test pull response exceeds cap",
                    ));
                }
                let after = after.expect("adaptive pull is always a delta");
                Ok(SyncRepositoryPullResponse::Delta {
                    cursor: after + 1,
                    events: vec![super::super::SyncEvent {
                        cursor: after + 1,
                        commits: Vec::new(),
                        ref_updates: Vec::new(),
                        inline_blobs: Vec::new(),
                    }],
                })
            })
        }

        fn snapshot_rows<'a>(
            &'a self,
            _branch_id: &'a str,
            _head_commit_id: &'a str,
            _continuation: Option<&'a str>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncSnapshotRowPage> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull snapshot rows")) })
        }

        fn history<'a>(
            &'a self,
            _head: &'a str,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull history")) })
        }

        fn get_blobs<'a>(
            &'a self,
            _blob_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, Vec<super::super::SyncBlobManifest>> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull blob get")) })
        }

        fn register_blob<'a>(
            &'a self,
            _manifest: &'a super::super::SyncBlobManifest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncBlobRegistration> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull blob register")) })
        }

        fn get_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<Vec<u8>>> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull chunk get")) })
        }

        fn put_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
            _bytes: &'a [u8],
        ) -> super::super::SyncTransportFuture<'a, ()> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull chunk put")) })
        }
    }

    #[derive(Debug)]
    struct PagedSnapshotTransport {
        max_items: usize,
        calls: Arc<Mutex<Vec<(Option<String>, usize)>>>,
        behavior: SnapshotPageBehavior,
    }

    #[derive(Clone, Copy, Debug)]
    enum SnapshotPageBehavior {
        Normal,
        Cycle,
        EmptyContinuation,
    }

    impl SyncTransport for PagedSnapshotTransport {
        fn active_account_id(&self) -> &str {
            crate::ANONYMOUS_ACCOUNT_ID
        }

        fn push<'a>(
            &'a self,
            _request: &'a SyncPushRequest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncPushResponse> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot push")) })
        }

        fn pull(
            &self,
            _after: Option<u64>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'_, SyncRepositoryPullResponse> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot pull")) })
        }

        fn snapshot_rows<'a>(
            &'a self,
            branch_id: &'a str,
            head_commit_id: &'a str,
            continuation: Option<&'a str>,
            limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncSnapshotRowPage> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("snapshot calls lock")
                    .push((continuation.map(str::to_owned), limit));
                if limit > self.max_items {
                    return Err(LixError::new(
                        SYNC_RESPONSE_TOO_LARGE_CODE,
                        "test snapshot row response exceeds cap",
                    ));
                }
                let (row_id, next) = match (self.behavior, continuation) {
                    (SnapshotPageBehavior::Normal, None) => ("first", Some("next".to_owned())),
                    (SnapshotPageBehavior::Normal, Some("next")) => ("second", None),
                    (SnapshotPageBehavior::Cycle, None) => ("first", Some("a".to_owned())),
                    (SnapshotPageBehavior::Cycle, Some("a")) => ("second", Some("b".to_owned())),
                    (SnapshotPageBehavior::Cycle, Some("b")) => ("third", Some("a".to_owned())),
                    (SnapshotPageBehavior::EmptyContinuation, None) => {
                        return Ok(super::super::SyncSnapshotRowPage {
                            branch_id: branch_id.to_owned(),
                            head_commit_id: head_commit_id.to_owned(),
                            rows: Vec::new(),
                            continuation: Some("next".to_owned()),
                        });
                    }
                    _ => {
                        return Err(LixError::new(
                            LixError::CODE_INVALID_PARAM,
                            "unknown test continuation",
                        ));
                    }
                };
                Ok(super::super::SyncSnapshotRowPage {
                    branch_id: branch_id.to_owned(),
                    head_commit_id: head_commit_id.to_owned(),
                    rows: vec![snapshot_row(branch_id, row_id)],
                    continuation: next,
                })
            })
        }

        fn history<'a>(
            &'a self,
            _head: &'a str,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot history")) })
        }

        fn get_blobs<'a>(
            &'a self,
            _blob_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, Vec<super::super::SyncBlobManifest>> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot blob get")) })
        }

        fn register_blob<'a>(
            &'a self,
            _manifest: &'a super::super::SyncBlobManifest,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncBlobRegistration> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot blob register")) })
        }

        fn get_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<Vec<u8>>> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot chunk get")) })
        }

        fn put_chunk<'a>(
            &'a self,
            _chunk_id: &'a str,
            _bytes: &'a [u8],
        ) -> super::super::SyncTransportFuture<'a, ()> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot chunk put")) })
        }
    }

    fn snapshot_row(branch_id: &str, row_id: &str) -> super::super::SyncSnapshotRow {
        super::super::SyncSnapshotRow {
            branch_id: branch_id.to_owned(),
            schema_key: "lix_key_value".to_owned(),
            file_id: None,
            row_pk: serde_json::json!([row_id]),
            snapshot: Some(serde_json::json!({ "key": row_id, "value": row_id })),
            metadata: None,
            change_id: format!("change-{row_id}"),
            commit_id: "head".to_owned(),
            created_at: "2026-01-01T00:00:00Z".to_owned(),
            updated_at: "2026-01-01T00:00:00Z".to_owned(),
            change_account_id: "account".to_owned(),
            change_created_at: "2026-01-01T00:00:00Z".to_owned(),
            origin_key: None,
        }
    }

    async fn history_fixture() -> (
        Lix<Memory>,
        String,
        String,
        BTreeMap<String, super::super::SyncCommit>,
        Vec<super::super::SyncCommitHeader>,
        BTreeMap<String, super::super::SyncHistoryBoundary>,
        BTreeMap<String, Vec<super::super::SyncSnapshotRow>>,
    ) {
        history_fixture_with_depth(1).await
    }

    async fn history_fixture_with_depth(
        commits_after_parent: usize,
    ) -> (
        Lix<Memory>,
        String,
        String,
        BTreeMap<String, super::super::SyncCommit>,
        Vec<super::super::SyncCommitHeader>,
        BTreeMap<String, super::super::SyncHistoryBoundary>,
        BTreeMap<String, Vec<super::super::SyncSnapshotRow>>,
    ) {
        let authority = open_lix().await.expect("authority opens");
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('history-a', 'a')",
                &[],
            )
            .await
            .expect("first history commit");
        let parent = authority
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("parent head")
            .rows()[0]
            .get::<String>("id")
            .expect("parent id");
        for index in 0..commits_after_parent {
            authority
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("history-{index}")),
                        Value::Text(index.to_string()),
                    ],
                )
                .await
                .expect("extend history fixture");
        }
        let snapshot = authority
            .pull_sync_repository(None, 128)
            .await
            .expect("history snapshot");
        let SyncRepositoryPullResponse::Snapshot {
            default_branch_id,
            branches,
            ..
        } = &snapshot
        else {
            panic!("initial pull is snapshot");
        };
        let head = branches
            .iter()
            .find(|branch| branch.branch_id == *default_branch_id)
            .and_then(|branch| branch.head_commit_id.clone())
            .expect("default head");
        let mut snapshot_commits = BTreeMap::new();
        let mut snapshot_headers = BTreeMap::new();
        for branch_head in branches
            .iter()
            .filter_map(|branch| branch.head_commit_id.as_deref())
        {
            let page = authority
                .sync_history(branch_head, 1)
                .await
                .expect("authority history");
            snapshot_commits.extend(
                page.commits
                    .into_iter()
                    .map(|commit| (commit.commit_id.clone(), commit)),
            );
            snapshot_headers.extend(
                page.commit_headers
                    .into_iter()
                    .map(|header| (header.commit_id.clone(), header)),
            );
        }
        let history = super::super::SyncHistoryResponse {
            commits: snapshot_commits.into_values().collect(),
            commit_headers: snapshot_headers.into_values().collect(),
            boundaries: Vec::new(),
        };
        let all_commit_ids = authority
            .execute("SELECT id FROM lix_commit", &[])
            .await
            .expect("authority commit ids")
            .rows()
            .iter()
            .map(|row| row.get::<String>("id").expect("commit id"))
            .collect::<Vec<_>>();
        let mut commits = BTreeMap::new();
        let mut headers = BTreeMap::new();
        let mut history_boundaries = BTreeMap::new();
        let mut boundary_rows = BTreeMap::new();
        for commit_id in all_commit_ids {
            let response = authority
                .sync_history(&commit_id, 1)
                .await
                .expect("authority singleton test history");
            commits.extend(
                response
                    .commits
                    .into_iter()
                    .map(|commit| (commit.commit_id.clone(), commit)),
            );
            for header in response.commit_headers {
                headers.entry(header.commit_id.clone()).or_insert(header);
            }
            for boundary in response.boundaries {
                history_boundaries.insert(boundary.commit_id.clone(), boundary);
            }
            let page = authority
                .pull_sync_snapshot_rows(&commit_id, &commit_id, None, 512)
                .await
                .expect("authority history boundary rows");
            assert!(page.continuation.is_none(), "test boundary fits one page");
            boundary_rows.insert(commit_id, page.rows);
        }
        let commit_headers = headers.into_values().collect();
        let mut rows = Vec::new();
        for branch in branches {
            let Some(head_commit_id) = branch.head_commit_id.as_deref() else {
                continue;
            };
            let page = authority
                .pull_sync_snapshot_rows(&branch.branch_id, head_commit_id, None, 512)
                .await
                .expect("authority snapshot rows");
            assert!(page.continuation.is_none(), "test fixture fits one page");
            rows.extend(page.rows);
        }
        let checkpoint_roots = branches
            .iter()
            .filter_map(|branch| {
                let head = branch.head_commit_id.as_deref()?;
                let checkpoint = branch.checkpoint_commit_id.as_deref()?;
                (head != checkpoint).then(|| {
                    (
                        checkpoint.to_owned(),
                        branch.checkpoint_state_root_id.clone(),
                    )
                })
            })
            .collect::<BTreeMap<_, _>>();
        for checkpoint in checkpoint_roots.keys() {
            let page = authority
                .pull_sync_snapshot_rows(checkpoint, checkpoint, None, 512)
                .await
                .expect("authority checkpoint snapshot rows");
            assert!(page.continuation.is_none(), "test checkpoint fits one page");
            rows.extend(page.rows);
        }

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(default_branch_id))
            .await
            .expect("replica initializes");
        let replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica opens");
        replica
            .set_sync_role(super::super::SyncRole::Replica)
            .expect("replica role");
        replica
            .apply_sync_repository_snapshot(
                "https://sync.example/history",
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
                &checkpoint_roots,
            )
            .await
            .expect("snapshot installs");
        (
            replica,
            parent,
            head,
            commits,
            commit_headers,
            history_boundaries,
            boundary_rows,
        )
    }

    #[test]
    fn accepts_empty_long_poll_heartbeat() {
        validate_delta_after(
            9,
            &SyncRepositoryPullResponse::Delta {
                cursor: 9,
                events: Vec::new(),
            },
        )
        .expect("heartbeat keeps cursor");
    }

    #[test]
    fn rejects_cursor_advance_without_events() {
        let error = validate_delta_after(
            9,
            &SyncRepositoryPullResponse::Delta {
                cursor: 10,
                events: Vec::new(),
            },
        )
        .expect_err("empty page cannot advance cursor");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    }

    #[test]
    fn rejects_a_repository_cursor_gap() {
        let error = validate_delta_after(
            9,
            &SyncRepositoryPullResponse::Delta {
                cursor: 11,
                events: vec![super::super::SyncEvent {
                    cursor: 11,
                    commits: Vec::new(),
                    ref_updates: Vec::new(),
                    inline_blobs: Vec::new(),
                }],
            },
        )
        .expect_err("delta must start at the next repository cursor");
        assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    }

    #[test]
    fn retry_is_bounded() {
        assert_eq!(
            next_backoff(Duration::from_secs(20)),
            Duration::from_secs(30)
        );
        assert_eq!(
            next_backoff(Duration::from_secs(30)),
            Duration::from_secs(30)
        );
    }

    #[tokio::test]
    async fn delta_response_cap_reduces_and_retains_the_page_limit() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = CappedPullTransport {
            max_items: 20,
            calls: Arc::clone(&calls),
        };
        let mut limit = super::super::MAX_SYNC_REQUEST_ITEMS;
        let response = pull_delta_adaptive(&transport, 9, &mut limit)
            .await
            .expect("a smaller delta page fits");
        validate_delta_after(9, &response).expect("adaptive response remains a valid delta");
        assert_eq!(limit, 16);
        assert_eq!(
            *calls.lock().expect("pull calls lock"),
            vec![512, 256, 128, 64, 32, 16]
        );
    }

    #[test]
    fn push_request_cap_halves_then_reports_one_terminal_item() {
        let request_too_large = || {
            LixError::new(
                SYNC_REQUEST_TOO_LARGE_CODE,
                "sync push request exceeds the protocol body limit",
            )
        };
        let mut limit = super::super::MAX_SYNC_REQUEST_ITEMS;
        assert!(is_response_too_large(&request_too_large()));
        reduce_push_limit_after_too_large(&mut limit, request_too_large())
            .expect("a multi-item push should retry with a smaller batch");
        assert_eq!(limit, super::super::MAX_SYNC_REQUEST_ITEMS / 2);

        limit = 1;
        let error = reduce_push_limit_after_too_large(&mut limit, request_too_large())
            .expect_err("one oversized push item cannot be split further");
        assert_eq!(error.code, SYNC_ITEM_TOO_LARGE_CODE);
        assert!(error.message.contains("protocol transfer limit"));
        assert!(is_terminal_sync_error(&error));
    }

    #[test]
    fn live_history_and_snapshot_use_distinct_protocol_limits() {
        assert_eq!(super::super::MAX_SYNC_REQUEST_ITEMS, 512);
        assert_eq!(crate::sync::MAX_SYNC_HISTORY_PAGE_SIZE, 100);
        assert!(crate::sync::MAX_SYNC_HISTORY_PAGE_SIZE < super::super::MAX_SYNC_REQUEST_ITEMS);
    }

    #[tokio::test]
    async fn deep_history_fetches_one_requested_body_with_bounded_headers() {
        let authority = open_lix().await.expect("authority opens");
        for index in 0..32 {
            authority
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ($1, $2)",
                    &[
                        Value::Text(format!("deep-{index}")),
                        Value::Text(index.to_string()),
                    ],
                )
                .await
                .expect("extend deep history");
        }
        let head = authority
            .execute("SELECT lix_active_branch_commit_id() AS id", &[])
            .await
            .expect("read deep head")
            .rows()[0]
            .get::<String>("id")
            .expect("head id");
        let response = authority
            .sync_history(&head, 1)
            .await
            .expect("authority exports one deep head");
        assert_eq!(response.commits.len(), 1);
        assert!(
            response.commit_headers.len() <= 6,
            "header closure must stay bounded independently of history depth",
        );
        let response_boundaries = response
            .boundaries
            .into_iter()
            .map(|boundary| (boundary.commit_id.clone(), boundary))
            .collect();
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits: response
                .commits
                .into_iter()
                .map(|commit| (commit.commit_id.clone(), commit))
                .collect(),
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: None,
            fail_first_history: false,
            commit_headers: response.commit_headers,
            history_boundaries: response_boundaries,
            boundary_rows: BTreeMap::new(),
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let fetched = fetch_history_objects(
            &transport,
            BTreeSet::from([head.clone()]),
            super::super::MAX_SYNC_HISTORY_PAGE_SIZE,
        )
        .await
        .expect("deep head fetch succeeds");
        assert_eq!(fetched.commits.len(), 1);
        assert!(fetched.commit_headers.len() <= 6);
        assert_eq!(
            *calls.lock().expect("history calls lock"),
            vec![vec![head]],
            "ordinary parents remain lazy and do not cause recursive network batches",
        );
    }

    #[tokio::test]
    async fn one_oversized_delta_event_is_terminal() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = CappedPullTransport {
            max_items: 0,
            calls: Arc::clone(&calls),
        };
        let mut limit = 1;
        let error = pull_delta_adaptive(&transport, 9, &mut limit)
            .await
            .expect_err("one oversized event cannot be paged further");
        assert_eq!(error.code, SYNC_ITEM_TOO_LARGE_CODE);
        assert!(is_terminal_sync_error(&error));
        assert_eq!(*calls.lock().expect("pull calls lock"), vec![1]);
    }

    #[tokio::test]
    async fn history_response_cap_reduces_the_page_size() {
        let (_replica, _parent, head, commits, commit_headers, history_boundaries, boundary_rows) =
            history_fixture().await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: Some(2),
            fail_first_history: false,
            commit_headers,
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let mut limit = 4;
        fetch_history_page_adaptive(&transport, &head, &mut limit)
            .await
            .expect("a smaller history page fits");
        assert_eq!(limit, 2);
        assert_eq!(
            calls.lock().expect("history calls lock").len(),
            2,
            "one oversized page is retried at half the size",
        );
    }

    #[tokio::test]
    async fn one_oversized_history_commit_is_terminal() {
        let transport = HistoryTransport {
            commits: BTreeMap::new(),
            missing: BTreeSet::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: Some(0),
            fail_first_history: false,
            commit_headers: Vec::new(),
            history_boundaries: BTreeMap::new(),
            boundary_rows: BTreeMap::new(),
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let mut limit = 1;
        let error = fetch_history_page_adaptive(&transport, "commit-0", &mut limit)
            .await
            .expect_err("one oversized commit cannot be batched further");
        assert_eq!(error.code, SYNC_ITEM_TOO_LARGE_CODE);
    }

    #[tokio::test]
    async fn snapshot_row_response_cap_reduces_the_page_limit() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = PagedSnapshotTransport {
            max_items: 2,
            calls: Arc::clone(&calls),
            behavior: SnapshotPageBehavior::Normal,
        };
        let mut limit = 4;
        let page = fetch_adaptive(&mut limit, "snapshot row", |limit| {
            transport.snapshot_rows("branch", "head", None, limit)
        })
        .await
        .expect("a smaller snapshot row page fits");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(limit, 2);
        assert_eq!(
            *calls.lock().expect("snapshot calls lock"),
            vec![(None, 4), (None, 2)]
        );
    }

    #[tokio::test]
    async fn metadata_only_snapshot_loads_pinned_stateless_row_pages() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = PagedSnapshotTransport {
            max_items: super::super::MAX_SYNC_REQUEST_ITEMS,
            calls: Arc::clone(&calls),
            behavior: SnapshotPageBehavior::Normal,
        };
        let snapshot = SyncRepositoryPullResponse::Snapshot {
            cursor: 7,
            lix_id: "00000000-0000-7000-8000-000000000001".to_owned(),
            default_branch_id: "branch".to_owned(),
            branches: vec![super::super::SyncBranchHead {
                branch_id: "branch".to_owned(),
                head_commit_id: Some("head".to_owned()),
                checkpoint_commit_id: Some("head".to_owned()),
                checkpoint_state_root_id: "0".repeat(64),
                hot_state_root_id: "0".repeat(64),
            }],
        };
        let SyncRepositoryPullResponse::Snapshot {
            branches, cursor, ..
        } = snapshot
        else {
            panic!("snapshot remains a snapshot");
        };
        let targets = branches
            .iter()
            .filter_map(|branch| {
                branch
                    .head_commit_id
                    .as_deref()
                    .map(|head| (branch.branch_id.as_str(), head))
            })
            .collect::<Vec<_>>();
        let rows = fetch_snapshot_rows(&transport, &targets)
            .await
            .expect("snapshot row pages hydrate");
        assert_eq!(cursor, 7, "paging does not publish a different cursor");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            *calls.lock().expect("snapshot calls lock"),
            vec![
                (None, super::super::MAX_SYNC_REQUEST_ITEMS),
                (
                    Some("next".to_owned()),
                    super::super::MAX_SYNC_REQUEST_ITEMS,
                )
            ]
        );
    }

    #[tokio::test]
    async fn snapshot_row_paging_rejects_cycles_and_empty_progress() {
        for behavior in [
            SnapshotPageBehavior::Cycle,
            SnapshotPageBehavior::EmptyContinuation,
        ] {
            let transport = PagedSnapshotTransport {
                max_items: super::super::MAX_SYNC_REQUEST_ITEMS,
                calls: Arc::new(Mutex::new(Vec::new())),
                behavior,
            };
            let error = fetch_snapshot_rows(&transport, &[("branch", "head")])
                .await
                .expect_err("malformed continuation must not hang bootstrap");
            assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
        }
    }

    #[test]
    fn oversized_cursorless_snapshot_has_a_distinct_terminal_error() {
        let error = snapshot_pull_error(LixError::new(
            SYNC_RESPONSE_TOO_LARGE_CODE,
            "test snapshot response exceeds cap",
        ));
        assert_eq!(error.code, SYNC_SNAPSHOT_TOO_LARGE_CODE);
        assert!(is_terminal_sync_error(&error));
    }

    #[test]
    fn malformed_or_repeated_internal_demand_is_terminal() {
        let malformed = LixError::new(
            "LIX_SYNC_CHUNKS_REQUIRED",
            "chunk demand omitted its identifiers",
        );
        let error = SyncDemandRetry::default()
            .admit(malformed)
            .expect_err("malformed demand must stop the worker");
        assert_eq!(error.code, SYNC_DEMAND_STALLED_CODE);
        assert!(is_terminal_sync_error(&error));

        let demand = || {
            LixError::new("LIX_SYNC_CHUNKS_REQUIRED", "chunk demand")
                .with_details(serde_json::json!({ "chunkIds": ["a".repeat(64)] }))
        };
        let mut retry = SyncDemandRetry::default();
        retry.admit(demand()).expect("first demand is admitted");
        let error = retry
            .admit(demand())
            .expect_err("a repeated demand proves hydration made no progress");
        assert_eq!(error.code, SYNC_DEMAND_STALLED_CODE);
        assert!(is_terminal_sync_error(&error));
    }

    #[tokio::test]
    async fn structured_history_error_uses_the_single_demand_channel() {
        let commit_id = uuid::Uuid::now_v7().to_string();
        let error = LixError::new("LIX_SYNC_HISTORY_REQUIRED", "history is deferred")
            .with_details(serde_json::json!({ "commitIds": [commit_id.clone()] }));
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let request = sync_demand_request_for_error(&error)
            .expect("history demand is valid")
            .expect("history demand is classified");
        let waiter = send_sync_demand(&demand_tx, request);
        let responder = async {
            let demand = demand_rx.recv().await.expect("history demand arrives");
            assert!(matches!(
                demand.request,
                SyncDemandRequest::History(ids) if ids == vec![commit_id]
            ));
            demand.response.send(Ok(())).expect("waiter remains live");
        };
        let (hydrated, ()) = tokio::join!(waiter, responder);
        hydrated.expect("history response succeeds");
    }

    #[tokio::test]
    async fn sparse_commit_graph_miss_uses_the_history_demand_channel() {
        let commit_id = uuid::Uuid::now_v7().to_string();
        let error =
            LixError::commit_not_found(commit_id.clone(), "walk_commit_graph", "graph_node");
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let request = sync_demand_request_for_error(&error)
            .expect("sparse graph demand is valid")
            .expect("sparse graph demand is classified");
        let waiter = send_sync_demand(&demand_tx, request);
        let responder = async {
            let demand = demand_rx.recv().await.expect("history demand arrives");
            assert!(matches!(
                demand.request,
                SyncDemandRequest::History(ids) if ids == vec![commit_id]
            ));
            demand.response.send(Ok(())).expect("waiter remains live");
        };
        let (hydrated, ()) = tokio::join!(waiter, responder);
        hydrated.expect("history response succeeds");
    }

    #[tokio::test]
    async fn unrelated_missing_commit_is_not_reclassified_as_lazy_history() {
        let commit_id = uuid::Uuid::now_v7().to_string();
        let error = LixError::commit_not_found(commit_id, "load_branch_head", "head");
        assert!(
            sync_demand_request_for_error(&error)
                .expect("unrelated missing commit remains unrelated")
                .is_none()
        );
    }

    #[tokio::test]
    async fn structured_chunk_error_uses_the_same_demand_channel() {
        let chunk_id = "a".repeat(64);
        let error = LixError::new("LIX_SYNC_CHUNKS_REQUIRED", "chunks are deferred")
            .with_details(serde_json::json!({ "chunkIds": [chunk_id.clone()] }));
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let request = sync_demand_request_for_error(&error)
            .expect("chunk demand is valid")
            .expect("chunk demand is classified");
        let waiter = send_sync_demand(&demand_tx, request);
        let responder = async {
            let demand = demand_rx.recv().await.expect("chunk demand arrives");
            assert!(matches!(
                demand.request,
                SyncDemandRequest::Chunks(ids) if ids == vec![chunk_id]
            ));
            demand.response.send(Ok(())).expect("waiter remains live");
        };
        let (hydrated, ()) = tokio::join!(waiter, responder);
        hydrated.expect("chunk response succeeds");
    }

    #[tokio::test]
    async fn shutdown_result_unblocks_a_pending_demand() {
        let error = LixError::new("LIX_SYNC_CHUNKS_REQUIRED", "chunks are deferred")
            .with_details(serde_json::json!({ "chunkIds": ["a".repeat(64)] }));
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let request = sync_demand_request_for_error(&error)
            .expect("chunk demand is valid")
            .expect("chunk demand is classified");
        let waiter = send_sync_demand(&demand_tx, request);
        let responder = async {
            let demand = demand_rx.recv().await.expect("pending demand arrives");
            drop(demand);
        };
        let (result, ()) = tokio::join!(waiter, responder);
        let error = result.expect_err("worker shutdown is reported to the caller");
        assert_eq!(error.code, LixError::CODE_CLOSED);
        assert_eq!(error.message, "sync worker is stopping");
    }

    #[tokio::test]
    async fn worker_exit_drops_retrying_and_queued_demands_together() {
        let request = || SyncDemandRequest::Chunks(vec!["a".repeat(64)]);
        let (retrying_response, retrying_done) = tokio::sync::oneshot::channel();
        let pending = vec![SyncDemand {
            request: request(),
            response: retrying_response,
        }];
        let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(1);
        let (queued_response, queued_done) = tokio::sync::oneshot::channel();
        demand_tx
            .send(SyncDemand {
                request: request(),
                response: queued_response,
            })
            .await
            .expect("queued demand is admitted");

        drop(pending);
        drop(demand_rx);
        for done in [retrying_done, queued_done] {
            done.await
                .expect_err("worker ownership drop cancels the demand response");
        }
    }

    #[tokio::test]
    async fn worker_exit_unblocks_a_demand_waiting_for_channel_capacity() {
        let request = || SyncDemandRequest::Chunks(vec!["a".repeat(64)]);
        let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(1);
        let (queued_response, queued_done) = tokio::sync::oneshot::channel();
        demand_tx
            .send(SyncDemand {
                request: request(),
                response: queued_response,
            })
            .await
            .expect("first demand fills the channel");

        let mut blocked = Box::pin(send_sync_demand(&demand_tx, request()));
        assert!(
            blocked.as_mut().now_or_never().is_none(),
            "second demand waits for channel capacity",
        );
        drop(demand_rx);

        let error = blocked
            .await
            .expect_err("receiver drop rejects the blocked demand");
        assert_eq!(error.code, LixError::CODE_CLOSED);
        assert_eq!(error.message, "sync demand worker is closed");
        queued_done
            .await
            .expect_err("receiver drop cancels the queued demand response");
    }

    #[tokio::test]
    async fn chunk_demand_hydrates_a_deferred_blob_once() {
        let lix = open_lix().await.expect("replica opens");
        let bytes = (0..5 * 1024 * 1024 + 19)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let canonical = crate::binary_cas::CanonicalBlobManifest::from_bytes(&bytes);
        assert!(canonical.chunks.len() > 1);
        let manifest = super::super::SyncBlobManifest {
            blob_id: canonical.blob_id.to_hex(),
            size_bytes: canonical.size_bytes,
            chunks: canonical
                .chunks
                .iter()
                .map(|chunk| super::super::SyncBlobChunk {
                    chunk_id: chunk.hash.to_hex(),
                    size_bytes: chunk.size_bytes,
                })
                .collect(),
            inline_bytes_base64: None,
        };
        let mut offset = 0usize;
        let mut chunks = BTreeMap::new();
        for chunk in &canonical.chunks {
            let end = offset + usize::try_from(chunk.size_bytes).expect("test chunk fits usize");
            chunks.insert(chunk.hash.to_hex(), bytes[offset..end].to_vec());
            offset = end;
        }
        let blob_calls = Arc::new(Mutex::new(Vec::new()));
        let chunk_calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits: BTreeMap::new(),
            missing: BTreeSet::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: None,
            fail_first_history: false,
            commit_headers: Vec::new(),
            history_boundaries: BTreeMap::new(),
            boundary_rows: BTreeMap::new(),
            blobs: BTreeMap::from([(manifest.blob_id.clone(), manifest)]),
            blob_calls: Arc::clone(&blob_calls),
            chunks,
            chunk_calls: Arc::clone(&chunk_calls),
        };
        let blob_id = canonical.blob_id.to_hex();
        ensure_blob_manifests(&lix, &transport, BTreeSet::from([blob_id.clone()]))
            .await
            .expect("pull registers the manifest without chunks");
        assert_eq!(blob_calls.lock().expect("blob calls lock").len(), 1);
        assert!(chunk_calls.lock().expect("chunk calls lock").is_empty());
        ensure_blob_manifests(&lix, &transport, BTreeSet::from([blob_id]))
            .await
            .expect("known manifest is a local no-op");
        assert_eq!(
            blob_calls.lock().expect("blob calls lock").len(),
            1,
            "manifest availability must dedupe sequential pulls",
        );

        let adapter = lix.storage_adapter();
        let read = adapter
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("read opens");
        let error = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect_err("manifest-only blob demands chunks");
        drop(read);
        assert_eq!(error.code, "LIX_SYNC_CHUNKS_REQUIRED");
        let demanded = error.details.expect("chunk demand details")["chunkIds"]
            .as_array()
            .expect("chunkIds array")
            .iter()
            .map(|id| id.as_str().expect("chunk id string").to_owned())
            .collect::<BTreeSet<_>>();
        let chunk_ids = demanded.iter().cloned().collect::<Vec<_>>();
        let (first_response, first_done) = tokio::sync::oneshot::channel();
        let (second_response, second_done) = tokio::sync::oneshot::channel();
        let (retry, retry_error) = hydrate_and_resolve_sync_demands(
            &lix,
            &transport,
            vec![
                SyncDemand {
                    request: SyncDemandRequest::Chunks(chunk_ids.clone()),
                    response: first_response,
                },
                SyncDemand {
                    request: SyncDemandRequest::Chunks(chunk_ids),
                    response: second_response,
                },
            ],
        )
        .await;
        assert!(retry.is_empty());
        assert!(retry_error.is_none());
        first_done
            .await
            .expect("first demand response arrives")
            .expect("first demand hydrates");
        second_done
            .await
            .expect("second demand response arrives")
            .expect("second demand hydrates");
        let first_call_count = chunk_calls.lock().expect("chunk calls lock").len();
        assert_eq!(
            first_call_count,
            demanded.len(),
            "concurrent identical demands share one fetch per unique chunk",
        );

        let read = adapter
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("read reopens");
        let actual = crate::binary_cas::load_bytes_many(&read, &[canonical.blob_id])
            .await
            .expect("hydrated blob reads")
            .into_vec()
            .into_iter()
            .next()
            .flatten()
            .expect("blob exists");
        assert_eq!(actual, bytes);
        drop(read);

        hydrate_chunk_ids(&lix, &transport, demanded)
            .await
            .expect("sequential duplicate is a local no-op");
        assert_eq!(
            chunk_calls.lock().expect("chunk calls lock").len(),
            first_call_count,
            "already-present chunks must not be fetched twice",
        );
    }

    #[tokio::test]
    async fn blob_manifest_fetches_use_bounded_batches() {
        let lix = open_lix().await.expect("replica opens");
        let mut blobs = BTreeMap::new();
        for index in 0..40 {
            let canonical = crate::binary_cas::CanonicalBlobManifest::from_bytes(
                format!("batch manifest {index}").as_bytes(),
            );
            blobs.insert(
                canonical.blob_id.to_hex(),
                super::super::SyncBlobManifest {
                    blob_id: canonical.blob_id.to_hex(),
                    size_bytes: canonical.size_bytes,
                    chunks: canonical
                        .chunks
                        .iter()
                        .map(|chunk| super::super::SyncBlobChunk {
                            chunk_id: chunk.hash.to_hex(),
                            size_bytes: chunk.size_bytes,
                        })
                        .collect(),
                    inline_bytes_base64: None,
                },
            );
        }
        let blob_calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits: BTreeMap::new(),
            missing: BTreeSet::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: None,
            fail_first_history: false,
            commit_headers: Vec::new(),
            history_boundaries: BTreeMap::new(),
            boundary_rows: BTreeMap::new(),
            blobs: blobs.clone(),
            blob_calls: Arc::clone(&blob_calls),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };

        ensure_blob_manifests(&lix, &transport, blobs.into_keys().collect())
            .await
            .expect("all manifest batches register");

        let batch_sizes = blob_calls
            .lock()
            .expect("blob calls lock")
            .iter()
            .map(Vec::len)
            .collect::<Vec<_>>();
        assert_eq!(batch_sizes, vec![16, 16, 8]);
    }

    #[tokio::test]
    async fn unrelated_error_does_not_enqueue_demand() {
        assert!(
            sync_demand_request_for_error(&LixError::new(
                LixError::CODE_INVALID_PARAM,
                "not history",
            ))
            .expect("unrelated error classification succeeds")
            .is_none(),
        );
    }

    #[test]
    fn blob_ref_discovery_is_deduplicated() {
        let hash = "a".repeat(64);
        let first = serde_json::json!({ "blob_hash": hash });
        let second = first.clone();
        let unrelated = serde_json::json!({ "blob_hash": "b".repeat(64) });
        let blob_ids = blob_ids_from_rows([
            ("lix_binary_blob_ref", Some(&first)),
            ("lix_binary_blob_ref", Some(&second)),
            ("another_schema", Some(&unrelated)),
        ])
        .expect("valid blob refs");
        assert_eq!(blob_ids, BTreeSet::from(["a".repeat(64)]));
    }

    #[test]
    fn malformed_blob_ref_fails_before_apply() {
        let snapshot = serde_json::json!({ "blob_hash": "not-blake3" });
        let error = blob_ids_from_rows([("lix_binary_blob_ref", Some(&snapshot))])
            .expect_err("invalid content identity must fail");
        assert_eq!(error.code, LixError::CODE_INVALID_PARAM);
    }

    #[tokio::test]
    async fn one_history_demand_hydrates_and_retries_sql_while_deduping_callers() {
        let (replica, parent, head, commits, commit_headers, history_boundaries, boundary_rows) =
            history_fixture().await;
        let error = replica
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2)",
                &[Value::Text(parent.clone()), Value::Text(head.clone())],
            )
            .await
            .expect_err("deferred parent must demand history");
        assert_eq!(error.code, "LIX_SYNC_HISTORY_REQUIRED", "{error:?}");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: None,
            fail_first_history: false,
            commit_headers,
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (first_response, first_done) = tokio::sync::oneshot::channel();
        let (second_response, second_done) = tokio::sync::oneshot::channel();
        let (retry, retry_error) = hydrate_and_resolve_sync_demands(
            &replica,
            &transport,
            vec![
                SyncDemand {
                    request: SyncDemandRequest::History(vec![parent.clone()]),
                    response: first_response,
                },
                SyncDemand {
                    request: SyncDemandRequest::History(vec![parent.clone()]),
                    response: second_response,
                },
            ],
        )
        .await;
        assert!(retry.is_empty());
        assert!(retry_error.is_none());
        first_done
            .await
            .expect("first demand response arrives")
            .expect("first demand hydrates");
        second_done
            .await
            .expect("second demand response arrives")
            .expect("second demand hydrates");
        let parent_requests = calls
            .lock()
            .expect("history calls lock")
            .iter()
            .flatten()
            .filter(|commit_id| *commit_id == &parent)
            .count();
        assert_eq!(parent_requests, 1, "concurrent demands share one fetch");
        let calls_after_first_hydration = calls.lock().expect("history calls lock").len();
        let (repeat_response, repeat_done) = tokio::sync::oneshot::channel();
        let (retry, retry_error) = hydrate_and_resolve_sync_demands(
            &replica,
            &transport,
            vec![SyncDemand {
                request: SyncDemandRequest::History(vec![parent.clone()]),
                response: repeat_response,
            }],
        )
        .await;
        assert!(retry.is_empty());
        assert!(retry_error.is_none());
        repeat_done
            .await
            .expect("repeat demand response arrives")
            .expect("repeat demand hydrates");
        assert_eq!(
            calls.lock().expect("history calls lock").len(),
            calls_after_first_hydration,
            "a sequential duplicate must not refetch history",
        );

        replica
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2)",
                &[Value::Text(parent), Value::Text(head)],
            )
            .await
            .expect("same SQL succeeds after history hydration");
    }

    #[tokio::test]
    async fn history_hydration_waits_for_collaboration_read_quiescence() {
        let (replica, parent, head, commits, commit_headers, history_boundaries, boundary_rows) =
            history_fixture().await;
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: None,
            fail_first_history: false,
            commit_headers,
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };

        // Expired auto-commit reads acquire this gate before their final
        // retry. Once they do, sync is allowed to finish fetching immutable
        // transport data, but it must not publish more storage revisions and
        // exhaust the read's bounded retry budget.
        let read_quiescence = replica.lock_collaboration_writes().await;
        let worker_replica = replica.clone();
        let worker_parent = parent.clone();
        let (response, done) = tokio::sync::oneshot::channel();
        let mut hydration = tokio::spawn(async move {
            hydrate_and_resolve_sync_demands(
                &worker_replica,
                &transport,
                vec![SyncDemand {
                    request: SyncDemandRequest::History(vec![worker_parent]),
                    response,
                }],
            )
            .await
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(50), &mut hydration)
                .await
                .is_err(),
            "sync history publication must wait behind read quiescence",
        );
        let error = replica
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2)",
                &[Value::Text(parent.clone()), Value::Text(head.clone())],
            )
            .await
            .expect_err("history remains deferred while publication is gated");
        assert_eq!(error.code, "LIX_SYNC_HISTORY_REQUIRED");

        drop(read_quiescence);
        let (retry, retry_error) = hydration.await.expect("hydration task joins");
        assert!(retry.is_empty());
        assert!(retry_error.is_none());
        done.await
            .expect("history demand response arrives")
            .expect("history demand succeeds");
        replica
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2)",
                &[Value::Text(parent), Value::Text(head)],
            )
            .await
            .expect("history read succeeds after serialized publication");
    }

    #[tokio::test]
    async fn transient_history_hydration_keeps_the_sql_waiter_for_worker_retry() {
        let (replica, parent, _head, commits, commit_headers, history_boundaries, boundary_rows) =
            history_fixture().await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: None,
            fail_first_history: true,
            commit_headers,
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (response, mut done) = tokio::sync::oneshot::channel();
        let mut demands = vec![SyncDemand {
            request: SyncDemandRequest::History(vec![parent]),
            response,
        }];
        let (_demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let mut push_item_limit = super::super::MAX_SYNC_REQUEST_ITEMS;
        let mut delta_pull_limit = super::super::MAX_SYNC_REQUEST_ITEMS;
        let mut change_watcher = replica.sync_mode_state().change_watcher();

        let error = sync_iteration(
            &replica,
            "https://sync.example/history",
            &transport,
            &mut push_item_limit,
            &mut delta_pull_limit,
            &mut change_watcher,
            &mut demand_rx,
            &mut demands,
        )
        .await
        .expect_err("transport failure returns to the worker retry path");
        assert!(is_retryable_sync_transport_error(&error));
        assert_eq!(demands.len(), 1, "transport failure remains pending");
        assert!(
            matches!(
                done.try_recv(),
                Err(tokio::sync::oneshot::error::TryRecvError::Empty)
            ),
            "the SQL caller must not see the transient fetch error",
        );

        let error = sync_iteration(
            &replica,
            "https://sync.example/history",
            &transport,
            &mut push_item_limit,
            &mut delta_pull_limit,
            &mut change_watcher,
            &mut demand_rx,
            &mut demands,
        )
        .await
        .expect_err("the test transport stops after servicing the demand");
        assert!(error.message.contains("unused history test pull"));
        assert!(demands.is_empty());
        done.await
            .expect("SQL waiter receives the retry result")
            .expect("second history fetch succeeds");
        assert_eq!(calls.lock().expect("history calls lock").len(), 2);
    }

    #[tokio::test]
    async fn canceled_demand_does_not_block_or_fetch_before_the_next_iteration() {
        let (replica, parent, _head, commits, commit_headers, history_boundaries, boundary_rows) =
            history_fixture().await;
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: None,
            fail_first_history: false,
            commit_headers,
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (response, done) = tokio::sync::oneshot::channel();
        drop(done);
        let mut demands = vec![SyncDemand {
            request: SyncDemandRequest::History(vec![parent]),
            response,
        }];
        let (_demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let mut push_item_limit = super::super::MAX_SYNC_REQUEST_ITEMS;
        let mut delta_pull_limit = super::super::MAX_SYNC_REQUEST_ITEMS;
        let mut change_watcher = replica.sync_mode_state().change_watcher();

        let error = sync_iteration(
            &replica,
            "https://sync.example/history",
            &transport,
            &mut push_item_limit,
            &mut delta_pull_limit,
            &mut change_watcher,
            &mut demand_rx,
            &mut demands,
        )
        .await
        .expect_err("the test transport stops after pruning the canceled demand");
        assert!(error.message.contains("unused history test pull"));
        assert!(demands.is_empty());
        assert!(calls.lock().expect("history calls lock").is_empty());
    }

    #[test]
    fn demand_retry_distinguishes_transport_from_semantic_errors() {
        assert!(is_retryable_sync_transport_error(&LixError::new(
            super::super::http::SYNC_TRANSPORT_ERROR_CODE,
            "disconnected",
        )));
        assert!(is_retryable_sync_transport_error(
            &LixError::new("LIX_REMOTE_OVERLOADED", "try later")
                .with_details(serde_json::json!({ "httpStatus": 503 })),
        ));
        assert!(!is_retryable_sync_transport_error(
            &LixError::new(LixError::CODE_COMMIT_NOT_FOUND, "missing history")
                .with_details(serde_json::json!({ "httpStatus": 404 })),
        ));
        assert!(!is_retryable_sync_transport_error(&LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "missing chunk"
        ),));
    }

    #[tokio::test]
    async fn missing_sparse_boundary_hydrates_through_shared_retry_path() {
        let (replica, _parent, _head, commits, commit_headers, history_boundaries, boundary_rows) =
            history_fixture_with_depth(32).await;

        // A history query over the sparse graph must drive cold-history
        // hydration on a fresh replica.
        let sql = "SELECT COUNT(DISTINCT history.id) AS entries \
                   FROM lix_checkpoint AS checkpoint \
                   LEFT JOIN lix_history('lix_file') AS history \
                     ON history.lixcol_observed_commit_id = checkpoint.commit_id";
        let mut error = replica
            .execute(sql, &[])
            .await
            .expect_err("the sparse graph boundary is initially absent");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND, "{error:?}");

        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: None,
            fail_first_history: false,
            commit_headers,
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let mut retry = SyncDemandRetry::default();
        loop {
            let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
            let hydrate = retry.hydrate_for_retry(Some(&demand_tx), error);
            let serve = async {
                let demand = demand_rx.recv().await.expect("history demand arrives");
                let (pending, retry_error) =
                    hydrate_and_resolve_sync_demands(&replica, &transport, vec![demand]).await;
                assert!(pending.is_empty());
                assert!(retry_error.is_none());
            };
            let (retry_result, _hydration_result) = tokio::join!(hydrate, serve);
            retry_result.expect("shared retry accepts the graph miss");
            match replica.execute(sql, &[]).await {
                Ok(_) => break,
                Err(next) => error = next,
            }
        }
        let hydrated_ids = {
            let calls = calls.lock().expect("history calls lock");
            assert!(!calls.is_empty());
            assert!(calls.iter().all(|request| request.len() == 1));
            calls.iter().flatten().cloned().collect::<BTreeSet<_>>()
        };

        let adapter = replica.storage_adapter();
        let read = adapter
            .begin_read(crate::storage_adapter::StorageReadOptions::default())
            .await
            .expect("history manifest read opens");
        for commit_id in hydrated_ids {
            let commit_id =
                crate::changelog::CommitId::parse_lix(&commit_id, "hydrated history manifest")
                    .expect("hydrated id is canonical");
            let manifest = crate::tracked_state::load_commit_state_manifest(&read, commit_id)
                .await
                .expect("hydrated manifest loads")
                .expect("hydrated boundary has a manifest");
            assert!(
                manifest.snapshot_root.is_some(),
                "history boundaries must be complete rooted snapshots",
            );
            assert_eq!(manifest.replay_debt.depth, 0);
            assert_eq!(manifest.replay_debt.rows, 0);
            assert_eq!(manifest.replay_debt.bytes, 0);
        }
        drop(read);

        replica
            .execute(sql, &[])
            .await
            .expect("Atelier History query reconstructs from hydrated cold commits");
    }

    #[tokio::test]
    async fn missing_history_id_leaves_its_deferred_marker() {
        let (replica, parent, _head, commits, _commit_headers, history_boundaries, boundary_rows) =
            history_fixture().await;
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::from([parent.clone()]),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: None,
            fail_first_history: false,
            commit_headers: Vec::new(),
            history_boundaries,
            boundary_rows,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (response, done) = tokio::sync::oneshot::channel();
        let (retry, retry_error) = hydrate_and_resolve_sync_demands(
            &replica,
            &transport,
            vec![SyncDemand {
                request: SyncDemandRequest::History(vec![parent.clone()]),
                response,
            }],
        )
        .await;
        assert!(retry.is_empty());
        assert!(retry_error.is_none());
        let error = done
            .await
            .expect("demand response arrives")
            .expect_err("missing authority history fails");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
        assert_eq!(
            replica
                .sync_history_demand_ids(BTreeSet::from([parent.clone()]))
                .await
                .expect("marker query"),
            BTreeSet::from([parent]),
            "a missing history response must not clear the local demand marker",
        );

        let sparse_boundary = uuid::Uuid::now_v7().to_string();
        assert_eq!(
            replica
                .sync_history_demand_ids(BTreeSet::from([sparse_boundary.clone()]))
                .await
                .expect("missing record query"),
            BTreeSet::from([sparse_boundary]),
            "a commit beyond the bounded header frontier must remain demandable",
        );
    }

    #[tokio::test]
    async fn explicit_transaction_surfaces_structured_history_demand() {
        let (replica, parent, head, _commits, _commit_headers, _history_boundaries, _boundary_rows) =
            history_fixture().await;
        let mut transaction = replica
            .begin_transaction()
            .await
            .expect("transaction begins");
        let error = transaction
            .execute(
                "SELECT COUNT(*) AS entries FROM lix_diff($1, $2)",
                &[Value::Text(parent.clone()), Value::Text(head)],
            )
            .await
            .expect_err("explicit transactions do not retry against a pinned snapshot");
        assert_eq!(error.code, "LIX_SYNC_HISTORY_REQUIRED");
        assert_eq!(
            error.details.expect("history demand details")["commitIds"],
            serde_json::json!([parent]),
        );
        transaction
            .rollback()
            .await
            .expect("transaction rolls back");
    }
}
