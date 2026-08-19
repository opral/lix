//! One repository-scoped synchronization state machine for every platform.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{FutureExt, select_biased};

use crate::storage_adapter::Storage;
use crate::{Lix, LixError};

use super::platform::{HttpSyncTransport, SyncTask, sleep, spawn_sync_task};
use super::{SyncPushRequest, SyncRepositoryPullResponse, SyncTransport};

const SYNC_DELTA_PULL_LIMIT: usize = 512;
const SYNC_SNAPSHOT_ROW_LIMIT: usize = 512;
const SYNC_RETRY_INITIAL_BACKOFF: Duration = Duration::from_millis(100);
const SYNC_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);
const SYNC_RESPONSE_TOO_LARGE_CODE: &str = "LIX_ERROR_SYNC_RESPONSE_TOO_LARGE";
const SYNC_REQUEST_TOO_LARGE_CODE: &str = "LIX_ERROR_REQUEST_BODY_TOO_LARGE";
const SYNC_ITEM_TOO_LARGE_CODE: &str = "LIX_ERROR_SYNC_ITEM_TOO_LARGE";
const SYNC_SNAPSHOT_TOO_LARGE_CODE: &str = "LIX_ERROR_SYNC_SNAPSHOT_TOO_LARGE";

#[derive(Debug)]
pub(crate) struct SyncRuntime {
    shutdown_tx: tokio::sync::watch::Sender<bool>,
    demand_tx: tokio::sync::mpsc::Sender<SyncDemand>,
    task: SyncTask,
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
    default_branch_id: String,
}

#[derive(Debug)]
struct PreparedRepositorySnapshot {
    metadata: SyncRepositoryPullResponse,
    commits: Vec<super::SyncCommit>,
    commit_headers: Vec<super::SyncCommitHeader>,
    rows: Vec<super::SyncSnapshotRow>,
}

impl PreparedSync {
    pub(crate) fn default_branch_id(&self) -> &str {
        &self.default_branch_id
    }

    pub(crate) fn lix_id(&self) -> &str {
        &self.lix_id
    }

    pub(crate) fn active_account_id(&self) -> &str {
        self.transport.active_account_id()
    }
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

async fn fetch_repository_snapshot<Transport>(
    transport: &Transport,
) -> Result<(PreparedRepositorySnapshot, String, String), LixError>
where
    Transport: SyncTransport,
{
    let metadata = transport
        .pull(None, SYNC_DELTA_PULL_LIMIT)
        .await
        .map_err(snapshot_pull_error)?;
    let (lix_id, default_branch_id) = snapshot_repository_identity(&metadata)?;
    let lix_id = lix_id.to_owned();
    let default_branch_id = default_branch_id.to_owned();
    if crate::storage_codec::id_string::uuid_bytes_from_canonical(&lix_id).is_none() {
        return Err(LixError::new(
            LixError::CODE_INVALID_PARAM,
            "sync snapshot lixId must be a canonical UUID",
        ));
    }
    super::validate_sync_branch_id(&default_branch_id)?;
    let snapshot = prepare_repository_snapshot(transport, metadata).await?;
    Ok((snapshot, lix_id, default_branch_id))
}

fn snapshot_repository_identity(
    response: &SyncRepositoryPullResponse,
) -> Result<(&str, &str), LixError> {
    match response {
        SyncRepositoryPullResponse::Snapshot {
            lix_id,
            default_branch_id,
            ..
        } => Ok((lix_id, default_branch_id)),
        SyncRepositoryPullResponse::Delta { .. } => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "initial sync pull did not return a repository snapshot",
        )),
    }
}

async fn prepare_repository_snapshot<Transport>(
    transport: &Transport,
    metadata: SyncRepositoryPullResponse,
) -> Result<PreparedRepositorySnapshot, LixError>
where
    Transport: SyncTransport,
{
    let SyncRepositoryPullResponse::Snapshot { branches, .. } = &metadata else {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "repository bootstrap metadata was not a snapshot",
        ));
    };
    let head_ids = branches
        .iter()
        .filter_map(|branch| branch.head_commit_id.clone())
        .collect::<BTreeSet<_>>();
    let (history, rows) = futures_util::try_join!(
        fetch_history_objects(transport, head_ids),
        fetch_snapshot_rows(transport, branches),
    )?;
    Ok(PreparedRepositorySnapshot {
        metadata,
        commits: history.commits,
        commit_headers: history.commit_headers,
        rows,
    })
}

impl SyncRuntime {
    pub(crate) fn stop(&self) {
        self.shutdown_tx.send_replace(true);
    }

    pub(crate) async fn stop_and_join(&self) -> Result<(), LixError> {
        self.stop();
        self.task.join().await
    }

    pub(crate) fn demand_sender(&self) -> tokio::sync::mpsc::Sender<SyncDemand> {
        self.demand_tx.clone()
    }
}

pub(crate) async fn demand_sync_for_error(
    demand_tx: &tokio::sync::mpsc::Sender<SyncDemand>,
    error: &LixError,
) -> Result<bool, LixError> {
    let (field, context) = match error.code.as_str() {
        "LIX_SYNC_HISTORY_REQUIRED" => ("commitIds", "history"),
        "LIX_SYNC_CHUNKS_REQUIRED" => ("chunkIds", "chunk"),
        _ => return Ok(false),
    };
    let ids = error
        .details
        .as_ref()
        .and_then(|details| details.get(field))
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("sync {context} demand error omitted {field}"),
            )
        })?
        .iter()
        .map(|id| {
            id.as_str().map(str::to_owned).ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("sync {context} demand {field} must be strings"),
                )
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    if ids.is_empty() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("sync {context} demand error contained no {field}"),
        ));
    }
    let request = match error.code.as_str() {
        "LIX_SYNC_HISTORY_REQUIRED" => SyncDemandRequest::History(ids),
        "LIX_SYNC_CHUNKS_REQUIRED" => SyncDemandRequest::Chunks(ids),
        _ => unreachable!("demand error code was classified above"),
    };
    let (response, done) = tokio::sync::oneshot::channel();
    demand_tx
        .send(SyncDemand { request, response })
        .await
        .map_err(|_| LixError::new(LixError::CODE_CLOSED, "sync demand worker is closed"))?;
    done.await
        .map_err(|_| LixError::new(LixError::CODE_CLOSED, "sync demand worker stopped"))??;
    Ok(true)
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
    lix.set_sync_role(crate::sync::SyncRole::Replica {
        remote_id: remote_id.clone(),
    })?;

    // Reopens remain local. A fresh open hands in the already-fetched snapshot
    // used to choose the repository's default branch during initialization.
    let initial_transport = if let Some(prepared) = prepared {
        if prepared.transport.remote_id() != remote_id {
            return Err(LixError::new(
                LixError::CODE_INVALID_PARAM,
                "prepared sync repository does not match ServerOptions",
            ));
        }
        register_commit_blob_manifests(lix, &prepared.transport, &prepared.snapshot.commits)
            .await?;
        register_snapshot_row_blob_manifests(lix, &prepared.transport, &prepared.snapshot.rows)
            .await?;
        let authority_lix_id = prepared.lix_id().to_owned();
        lix.apply_sync_repository_snapshot(
            &remote_id,
            prepared.active_account_id(),
            &prepared.snapshot.metadata,
            &prepared.snapshot.commits,
            &prepared.snapshot.commit_headers,
            &prepared.snapshot.rows,
        )
        .await?;
        lix.align_repository_identity_for_sync(authority_lix_id)?;
        lix.align_primary_account_for_sync(prepared.active_account_id())
            .await?;
        Some(prepared.transport)
    } else {
        None
    };

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let (demand_tx, demand_rx) = tokio::sync::mpsc::channel(64);
    let worker_lix = lix.clone();
    let task = spawn_sync_task(async move {
        run_sync_worker(
            worker_lix,
            remote_id,
            headers,
            initial_transport,
            shutdown_rx,
            demand_rx,
        )
        .await;
    })?;

    Ok(Arc::new(SyncRuntime {
        shutdown_tx,
        demand_tx,
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
    mut shutdown_rx: tokio::sync::watch::Receiver<bool>,
    mut demand_rx: tokio::sync::mpsc::Receiver<SyncDemand>,
) where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let mut retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
    let mut delta_pull_limit = SYNC_DELTA_PULL_LIMIT;
    let mut push_item_limit = super::MAX_SYNC_REQUEST_ITEMS;
    let mut change_watcher = lix.sync_mode_state().change_watcher();

    while !*shutdown_rx.borrow() {
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
                        break;
                    }
                    transport = Some(connected);
                }
                Err(error) => {
                    tracing::warn!(error = ?error, "sync reconnect failed");
                    if wait_for_retry_or_shutdown(retry_backoff, &mut shutdown_rx)
                        .await
                        .is_err()
                    {
                        break;
                    }
                    retry_backoff = next_backoff(retry_backoff);
                    continue;
                }
            }
        }

        let Some(current) = transport.clone() else {
            continue;
        };
        // This outer race covers every phase, including connect, CAS transfer,
        // and push. Dropping an in-flight transport future invokes the
        // adapter's cancellation mechanism.
        let result = {
            let iteration = sync_iteration(
                &lix,
                &remote_id,
                &current,
                &mut push_item_limit,
                &mut delta_pull_limit,
                &mut change_watcher,
                &mut demand_rx,
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
            Ok(IterationResult::Applied | IterationResult::LocalChange) => {
                retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
            }
            Ok(IterationResult::Demand(first)) => {
                let mut demands = vec![first];
                while let Ok(demand) = demand_rx.try_recv() {
                    demands.push(demand);
                }
                let (result, should_stop) = {
                    let hydration = hydrate_sync_demands(&lix, &current, &demands).fuse();
                    let shutdown = shutdown_rx.changed().fuse();
                    futures_util::pin_mut!(hydration, shutdown);
                    select_biased! {
                        _ = shutdown => (
                            (0..demands.len()).map(|_| Err(stopped_error())).collect(),
                            true,
                        ),
                        result = hydration => (result, false),
                    }
                };
                for (demand, result) in demands.into_iter().zip(result) {
                    let _ = demand.response.send(result);
                }
                if should_stop {
                    break;
                }
                retry_backoff = SYNC_RETRY_INITIAL_BACKOFF;
            }
            Err(error) => {
                if is_terminal_sync_error(&error) {
                    tracing::error!(error = ?error, "sync repository cannot make progress");
                    break;
                }
                tracing::warn!(error = ?error, "sync repository iteration failed");
                transport = None;
                if wait_for_retry_or_shutdown(retry_backoff, &mut shutdown_rx)
                    .await
                    .is_err()
                {
                    break;
                }
                retry_backoff = next_backoff(retry_backoff);
            }
        }
    }
}

#[derive(Debug)]
enum IterationResult {
    Applied,
    LocalChange,
    Demand(SyncDemand),
}

async fn sync_iteration<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    transport: &Transport,
    push_item_limit: &mut usize,
    delta_pull_limit: &mut usize,
    change_watcher: &mut tokio::sync::watch::Receiver<u64>,
    demand_rx: &mut tokio::sync::mpsc::Receiver<SyncDemand>,
) -> Result<IterationResult, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    // Establish the generation before inspecting the outbox. A commit racing
    // with outbox construction then wakes the select below and cannot remain
    // hidden behind an already-held long poll.
    let _ = change_watcher.borrow_and_update();

    // A cursor-less replica always bootstraps before it can publish local
    // work. This prevents an engine's synthetic initialization commit from
    // being mistaken for user-authored repository history.
    if lix.load_sync_repository_cursor(remote_id).await?.is_none() {
        let (snapshot, _lix_id, _default_branch_id) =
            fetch_repository_snapshot(transport).await?;
        register_commit_blob_manifests(lix, transport, &snapshot.commits).await?;
        register_snapshot_row_blob_manifests(lix, transport, &snapshot.rows).await?;
        lix.apply_sync_repository_snapshot(
            remote_id,
            transport.active_account_id(),
            &snapshot.metadata,
            &snapshot.commits,
            &snapshot.commit_headers,
            &snapshot.rows,
        )
        .await?;
        return Ok(IterationResult::Applied);
    }

    // Publish completed local commits before waiting for remote work. Commit
    // identity and ref compare-and-swap make retry after a lost response safe.
    loop {
        let Some(request) = lix
            .build_sync_push_bounded(remote_id, *push_item_limit)
            .await?
        else {
            break;
        };
        push_request_blobs(lix, transport, &request).await?;
        match transport.push(&request).await {
            Ok(receipt) => {
                catch_up_to(lix, remote_id, transport, receipt.cursor, delta_pull_limit).await?;
            }
            // A ref moved concurrently. Pulling the authority's intervening
            // events lets the importer reconcile local refs/outbox state; an
            // immediate reconnect/re-push would repeat the same conflict.
            Err(error) if error.code == LixError::CODE_TRANSACTION_CONFLICT => break,
            Err(error) if is_request_too_large(&error) => {
                reduce_push_limit_after_too_large(push_item_limit, error)?;
            }
            Err(error) => return Err(error),
        }
    }

    let cursor = lix
        .load_sync_repository_cursor(remote_id)
        .await?
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "sync repository cursor disappeared after bootstrap",
            )
        })?;
    let local_changed = change_watcher.changed().fuse();
    let pull = pull_delta_adaptive(transport, cursor, delta_pull_limit).fuse();
    let demand = demand_rx.recv().fuse();
    futures_util::pin_mut!(local_changed, pull, demand);
    select_biased! {
        demand = demand => demand.map(IterationResult::Demand).ok_or_else(|| {
            LixError::new(LixError::CODE_CLOSED, "sync demand channel closed")
        }),
        _ = local_changed => Ok(IterationResult::LocalChange),
        response = pull => {
            let response = response?;
            validate_delta_after(cursor, &response)?;
            register_pull_blob_manifests(lix, transport, &response).await?;
            lix.apply_sync_repository_pull(remote_id, &response).await?;
            Ok(IterationResult::Applied)
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
        validate_delta_after(cursor, &response)?;
        let next = repository_cursor(&response);
        if next <= cursor {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "sync push acknowledged cursor {target_cursor}, but pull remained at {cursor}"
                ),
            ));
        }
        register_pull_blob_manifests(lix, transport, &response).await?;
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
    loop {
        match transport.pull(Some(after), *limit).await {
            Ok(response) => return Ok(response),
            Err(error) if is_response_too_large(&error) && *limit > 1 => {
                *limit = smaller_page_limit(*limit);
            }
            Err(error) if is_response_too_large(&error) => {
                return Err(sync_item_too_large_error("repository event", error));
            }
            Err(error) => return Err(error),
        }
    }
}

async fn fetch_snapshot_rows<Transport>(
    transport: &Transport,
    branches: &[super::SyncBranchHead],
) -> Result<Vec<super::SyncSnapshotRow>, LixError>
where
    Transport: SyncTransport,
{
    let targets = branches
        .iter()
        .filter_map(|branch| {
            branch
                .head_commit_id
                .as_ref()
                .map(|head| (branch.branch_id.clone(), head.clone()))
        })
        .collect::<Vec<_>>();
    let mut rows = Vec::new();
    let mut page_limit = SYNC_SNAPSHOT_ROW_LIMIT;
    for (branch_id, head_commit_id) in targets {
        let mut continuation = None;
        let mut seen_continuations = BTreeSet::new();
        loop {
            let page = fetch_snapshot_row_page_adaptive(
                transport,
                &branch_id,
                &head_commit_id,
                continuation.as_deref(),
                &mut page_limit,
            )
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

async fn fetch_snapshot_row_page_adaptive<Transport>(
    transport: &Transport,
    branch_id: &str,
    head_commit_id: &str,
    continuation: Option<&str>,
    limit: &mut usize,
) -> Result<super::SyncSnapshotRowPage, LixError>
where
    Transport: SyncTransport,
{
    loop {
        match transport
            .snapshot_rows(branch_id, head_commit_id, continuation, *limit)
            .await
        {
            Ok(page) => return Ok(page),
            Err(error) if is_response_too_large(&error) && *limit > 1 => {
                *limit = smaller_page_limit(*limit);
            }
            Err(error) if is_response_too_large(&error) => {
                return Err(sync_item_too_large_error("snapshot row", error));
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

fn is_request_too_large(error: &LixError) -> bool {
    error.code == SYNC_REQUEST_TOO_LARGE_CODE
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
        SYNC_ITEM_TOO_LARGE_CODE | SYNC_SNAPSHOT_TOO_LARGE_CODE
    )
}

async fn hydrate_sync_demands<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    demands: &[SyncDemand],
) -> Vec<Result<(), LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let history_ids = demands
        .iter()
        .filter_map(|demand| match &demand.request {
            SyncDemandRequest::History(ids) => Some(ids.as_slice()),
            SyncDemandRequest::Chunks(_) => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let chunk_ids = demands
        .iter()
        .filter_map(|demand| match &demand.request {
            SyncDemandRequest::Chunks(ids) => Some(ids.as_slice()),
            SyncDemandRequest::History(_) => None,
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    let history_result = if history_ids.is_empty() {
        Ok(())
    } else {
        hydrate_history_ids(lix, transport, history_ids.into_iter().cloned().collect()).await
    };
    let chunk_result = if chunk_ids.is_empty() {
        Ok(())
    } else {
        hydrate_chunk_ids(lix, transport, chunk_ids.into_iter().cloned().collect()).await
    };
    demands
        .iter()
        .map(|demand| match &demand.request {
            SyncDemandRequest::History(_) => history_result.clone(),
            SyncDemandRequest::Chunks(_) => chunk_result.clone(),
        })
        .collect()
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
    let pending = lix
        .deferred_sync_history_ids(&requested)
        .await?
        .into_iter()
        .collect::<BTreeSet<_>>();
    if pending.is_empty() {
        return Ok(());
    }
    let fetched = fetch_history_objects(transport, pending).await?;
    register_commit_blob_manifests(lix, transport, &fetched.commits).await?;
    lix.import_sync_history_headers(&fetched.commit_headers)
        .await?;
    lix.import_sync_history(&fetched.commits).await
}

#[derive(Debug)]
struct FetchedHistory {
    commits: Vec<super::SyncCommit>,
    commit_headers: Vec<super::SyncCommitHeader>,
}

async fn fetch_history_objects<Transport>(
    transport: &Transport,
    mut pending: BTreeSet<String>,
) -> Result<FetchedHistory, LixError>
where
    Transport: SyncTransport,
{
    let mut commits = BTreeMap::new();
    let mut commit_headers = BTreeMap::new();
    let mut history_batch_limit = super::MAX_SYNC_HISTORY_COMMIT_IDS;
    while !pending.is_empty() {
        let (batch, response) =
            fetch_history_batch_adaptive(transport, &pending, &mut history_batch_limit).await?;
        for commit_id in &batch {
            pending.remove(commit_id);
        }
        if !response.missing_commit_ids.is_empty() {
            return Err(LixError::new(
                LixError::CODE_COMMIT_NOT_FOUND,
                format!(
                    "sync history is missing commits: {}",
                    response.missing_commit_ids.join(", ")
                ),
            ));
        }
        let requested = batch.iter().cloned().collect::<BTreeSet<_>>();
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
        if !requested.is_subset(&returned) {
            return Err(LixError::new(
                LixError::CODE_COMMIT_NOT_FOUND,
                "sync history response omitted a requested commit",
            ));
        }
        for commit in response_commits.drain(..) {
            if !requested.contains(&commit.commit_id) {
                return Err(LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    format!(
                        "sync history returned unrelated commit '{}'",
                        commit.commit_id
                    ),
                ));
            }
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

    let mut ordered = Vec::with_capacity(commits.len());
    let mut remaining = commits;
    while !remaining.is_empty() {
        let ready = remaining
            .iter()
            .find(|(_, commit)| {
                commit
                    .parent_commit_ids
                    .iter()
                    .all(|dependency| !remaining.contains_key(dependency))
            })
            .map(|(commit_id, _)| commit_id.clone())
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INVALID_PARAM,
                    "sync history dependency graph contains a cycle",
                )
            })?;
        ordered.push(remaining.remove(&ready).expect("ready commit exists"));
    }
    Ok(FetchedHistory {
        commits: ordered,
        commit_headers: commit_headers.into_values().collect(),
    })
}

async fn fetch_history_batch_adaptive<Transport>(
    transport: &Transport,
    pending: &BTreeSet<String>,
    limit: &mut usize,
) -> Result<(Vec<String>, super::SyncHistoryResponse), LixError>
where
    Transport: SyncTransport,
{
    loop {
        let batch = pending.iter().take(*limit).cloned().collect::<Vec<_>>();
        match transport.history(&batch).await {
            Ok(response) => return Ok((batch, response)),
            Err(error) if is_response_too_large(&error) && *limit > 1 => {
                *limit = smaller_page_limit(*limit);
            }
            Err(error) if is_response_too_large(&error) => {
                return Err(sync_item_too_large_error("history commit", error));
            }
            Err(error) => return Err(error),
        }
    }
}

async fn register_commit_blob_manifests<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    commits: &[super::SyncCommit],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    let blob_ids = blob_ids_from_commits(commits)?;
    ensure_blob_manifests(lix, transport, blob_ids).await
}

async fn register_snapshot_row_blob_manifests<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    rows: &[super::SyncSnapshotRow],
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    ensure_blob_manifests(
        lix,
        transport,
        blob_ids_from_rows(
            rows.iter()
                .map(|row| (row.schema_key.as_str(), row.snapshot.as_ref())),
        )?,
    )
    .await
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

async fn push_request_blobs<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    request: &SyncPushRequest,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    for blob_id in blob_ids_from_commits(&request.commits)? {
        let manifest = lix
            .get_sync_blob_manifest(&blob_id)
            .await?
            .ok_or_else(|| missing_blob_error(&blob_id, "local push"))?;
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
        if !registration.complete {
            registration = transport.register_blob(&manifest).await?;
        }
        require_complete_registration(&blob_id, &registration)?;
    }
    Ok(())
}

async fn register_pull_blob_manifests<StorageImpl, Transport>(
    lix: &Lix<StorageImpl>,
    transport: &Transport,
    response: &SyncRepositoryPullResponse,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    Transport: SyncTransport,
{
    ensure_blob_manifests(lix, transport, blob_ids_from_pull(response)?).await
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
    for blob_id in blob_ids {
        if lix.has_sync_blob_manifest(&blob_id).await? {
            continue;
        }
        let manifest = transport
            .get_blob(&blob_id)
            .await?
            .ok_or_else(|| missing_blob_error(&blob_id, "remote pull"))?;
        if manifest.blob_id != blob_id {
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
    Ok(())
}

fn blob_ids_from_pull(response: &SyncRepositoryPullResponse) -> Result<BTreeSet<String>, LixError> {
    match response {
        SyncRepositoryPullResponse::Snapshot { .. } => Ok(BTreeSet::new()),
        SyncRepositoryPullResponse::Delta { events, .. } => {
            let mut blob_ids = BTreeSet::new();
            for event in events {
                blob_ids.extend(blob_ids_from_commits(&event.commits)?);
            }
            Ok(blob_ids)
        }
    }
}

fn blob_ids_from_commits(commits: &[super::SyncCommit]) -> Result<BTreeSet<String>, LixError> {
    blob_ids_from_rows(commits.iter().flat_map(|commit| {
        commit
            .members
            .iter()
            .filter(|member| !member.deleted)
            .map(|member| (member.schema_key.as_str(), member.snapshot.as_ref()))
    }))
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

fn require_complete_registration(
    blob_id: &str,
    registration: &super::SyncBlobRegistration,
) -> Result<(), LixError> {
    if registration.complete && registration.missing_chunk_ids.is_empty() {
        return Ok(());
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("sync blob '{blob_id}' remained incomplete after uploading requested chunks"),
    ))
}

fn missing_blob_error(blob_id: &str, direction: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{direction} references missing sync blob '{blob_id}'"),
    )
}

fn missing_chunk_error(chunk_id: &str, blob_id: &str, direction: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("{direction} sync blob '{blob_id}' references missing chunk '{chunk_id}'"),
    )
}

fn repository_cursor(response: &SyncRepositoryPullResponse) -> u64 {
    match response {
        SyncRepositoryPullResponse::Snapshot { cursor, .. }
        | SyncRepositoryPullResponse::Delta { cursor, .. } => *cursor,
    }
}

fn validate_delta_after(
    previous_cursor: u64,
    response: &SyncRepositoryPullResponse,
) -> Result<(), LixError> {
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
    Ok(())
}

async fn wait_for_retry_or_shutdown(
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
        commit_headers: Vec<super::super::SyncCommitHeader>,
        blobs: BTreeMap<String, super::super::SyncBlobManifest>,
        blob_calls: Arc<Mutex<Vec<String>>>,
        chunks: BTreeMap<String, Vec<u8>>,
        chunk_calls: Arc<Mutex<Vec<String>>>,
    }

    impl SyncTransport for HistoryTransport {
        fn remote_id(&self) -> &str {
            "https://sync.example/history"
        }

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
            _branch_id: &'a str,
            _head_commit_id: &'a str,
            _continuation: Option<&'a str>,
            _limit: usize,
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncSnapshotRowPage> {
            Box::pin(async { Err(LixError::unknown("unused history snapshot rows")) })
        }

        fn history<'a>(
            &'a self,
            commit_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async move {
                self.calls
                    .lock()
                    .expect("history calls lock")
                    .push(commit_ids.to_vec());
                if self
                    .max_history_items
                    .is_some_and(|limit| commit_ids.len() > limit)
                {
                    return Err(LixError::new(
                        SYNC_RESPONSE_TOO_LARGE_CODE,
                        "test history response exceeds cap",
                    ));
                }
                let mut commits = Vec::new();
                let mut missing_commit_ids = Vec::new();
                for commit_id in commit_ids {
                    if self.missing.contains(commit_id) {
                        missing_commit_ids.push(commit_id.clone());
                    } else if let Some(commit) = self.commits.get(commit_id) {
                        commits.push(commit.clone());
                    } else {
                        missing_commit_ids.push(commit_id.clone());
                    }
                }
                Ok(super::super::SyncHistoryResponse {
                    commits,
                    commit_headers: self.commit_headers.clone(),
                    missing_commit_ids,
                })
            })
        }

        fn get_blob<'a>(
            &'a self,
            blob_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<super::super::SyncBlobManifest>> {
            Box::pin(async move {
                self.blob_calls
                    .lock()
                    .expect("blob calls lock")
                    .push(blob_id.to_owned());
                Ok(self.blobs.get(blob_id).cloned())
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
    struct CappedPullTransport {
        max_items: usize,
        calls: Arc<Mutex<Vec<usize>>>,
    }

    impl SyncTransport for CappedPullTransport {
        fn remote_id(&self) -> &str {
            "https://sync.example/capped-pull"
        }

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
            _commit_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async { Err(LixError::unknown("unused capped-pull history")) })
        }

        fn get_blob<'a>(
            &'a self,
            _blob_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<super::super::SyncBlobManifest>> {
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
        fn remote_id(&self) -> &str {
            "https://sync.example/paged-snapshot"
        }

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
            _commit_ids: &'a [String],
        ) -> super::super::SyncTransportFuture<'a, super::super::SyncHistoryResponse> {
            Box::pin(async { Err(LixError::unknown("unused paged-snapshot history")) })
        }

        fn get_blob<'a>(
            &'a self,
            _blob_id: &'a str,
        ) -> super::super::SyncTransportFuture<'a, Option<super::super::SyncBlobManifest>> {
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
        authority
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('history-b', 'b')",
                &[],
            )
            .await
            .expect("second history commit");
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
        let ids = branches
            .iter()
            .filter_map(|branch| branch.head_commit_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let history = authority
            .sync_history(&ids)
            .await
            .expect("authority history");
        let deferred_parent = authority
            .sync_history(std::slice::from_ref(&parent))
            .await
            .expect("authority deferred parent");
        let commits = history
            .commits
            .iter()
            .chain(deferred_parent.commits.iter())
            .cloned()
            .map(|commit| (commit.commit_id.clone(), commit))
            .collect();
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

        let storage = Memory::new();
        Engine::initialize_with_main_branch_id(storage.clone(), Some(default_branch_id))
            .await
            .expect("replica initializes");
        let replica = open_lix()
            .with_storage(storage)
            .await
            .expect("replica opens");
        replica
            .set_sync_role(super::super::SyncRole::Replica {
                remote_id: "https://sync.example/history".to_owned(),
            })
            .expect("replica role");
        replica
            .apply_sync_repository_snapshot(
                "https://sync.example/history",
                crate::ANONYMOUS_ACCOUNT_ID,
                &snapshot,
                &history.commits,
                &history.commit_headers,
                &rows,
            )
            .await
            .expect("snapshot installs");
        (replica, parent, head, commits)
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
        let mut limit = SYNC_DELTA_PULL_LIMIT;
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
        assert_eq!(SYNC_DELTA_PULL_LIMIT, 512);
        assert_eq!(SYNC_SNAPSHOT_ROW_LIMIT, 512);
        assert_eq!(crate::sync::MAX_SYNC_HISTORY_COMMIT_IDS, 128);
        assert!(crate::sync::MAX_SYNC_HISTORY_COMMIT_IDS < SYNC_DELTA_PULL_LIMIT);
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
            .sync_history(std::slice::from_ref(&head))
            .await
            .expect("authority exports one deep head");
        assert_eq!(response.commits.len(), 1);
        assert!(
            response.commit_headers.len() <= 6,
            "header closure must stay bounded independently of history depth",
        );
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
            commit_headers: response.commit_headers,
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let fetched = fetch_history_objects(&transport, BTreeSet::from([head.clone()]))
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
    async fn history_response_cap_reduces_the_requested_commit_batch() {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let transport = HistoryTransport {
            commits: BTreeMap::new(),
            missing: BTreeSet::new(),
            calls: Arc::clone(&calls),
            max_history_items: Some(2),
            commit_headers: Vec::new(),
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let pending = (0..4).map(|index| format!("commit-{index}")).collect();
        let mut limit = 4;
        let (batch, _) = fetch_history_batch_adaptive(&transport, &pending, &mut limit)
            .await
            .expect("a smaller history batch fits");
        assert_eq!(limit, 2);
        assert_eq!(batch.len(), 2);
        assert_eq!(
            calls
                .lock()
                .expect("history calls lock")
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![4, 2]
        );
    }

    #[tokio::test]
    async fn one_oversized_history_commit_is_terminal() {
        let transport = HistoryTransport {
            commits: BTreeMap::new(),
            missing: BTreeSet::new(),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: Some(0),
            commit_headers: Vec::new(),
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let pending = BTreeSet::from(["commit-0".to_owned()]);
        let mut limit = 1;
        let error = fetch_history_batch_adaptive(&transport, &pending, &mut limit)
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
        let page = fetch_snapshot_row_page_adaptive(&transport, "branch", "head", None, &mut limit)
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
            max_items: SYNC_SNAPSHOT_ROW_LIMIT,
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
                hot_state_root_id: "0".repeat(64),
            }],
        };
        let SyncRepositoryPullResponse::Snapshot {
            branches, cursor, ..
        } = snapshot
        else {
            panic!("snapshot remains a snapshot");
        };
        let rows = fetch_snapshot_rows(&transport, &branches)
            .await
            .expect("snapshot row pages hydrate");
        assert_eq!(cursor, 7, "paging does not publish a different cursor");
        assert_eq!(rows.len(), 2);
        assert_eq!(
            *calls.lock().expect("snapshot calls lock"),
            vec![
                (None, SYNC_SNAPSHOT_ROW_LIMIT),
                (Some("next".to_owned()), SYNC_SNAPSHOT_ROW_LIMIT)
            ]
        );
    }

    #[tokio::test]
    async fn snapshot_row_paging_rejects_cycles_and_empty_progress() {
        let branches = vec![super::super::SyncBranchHead {
            branch_id: "branch".to_owned(),
            head_commit_id: Some("head".to_owned()),
            hot_state_root_id: "0".repeat(64),
        }];
        for behavior in [
            SnapshotPageBehavior::Cycle,
            SnapshotPageBehavior::EmptyContinuation,
        ] {
            let transport = PagedSnapshotTransport {
                max_items: SYNC_SNAPSHOT_ROW_LIMIT,
                calls: Arc::new(Mutex::new(Vec::new())),
                behavior,
            };
            let error = fetch_snapshot_rows(&transport, &branches)
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

    #[tokio::test]
    async fn structured_history_error_uses_the_single_demand_channel() {
        let commit_id = uuid::Uuid::now_v7().to_string();
        let error = LixError::new("LIX_SYNC_HISTORY_REQUIRED", "history is deferred")
            .with_details(serde_json::json!({ "commitIds": [commit_id.clone()] }));
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let waiter = demand_sync_for_error(&demand_tx, &error);
        let responder = async {
            let demand = demand_rx.recv().await.expect("history demand arrives");
            assert!(matches!(
                demand.request,
                SyncDemandRequest::History(ids) if ids == vec![commit_id]
            ));
            demand.response.send(Ok(())).expect("waiter remains live");
        };
        let (hydrated, ()) = tokio::join!(waiter, responder);
        assert!(hydrated.expect("history response succeeds"));
    }

    #[tokio::test]
    async fn structured_chunk_error_uses_the_same_demand_channel() {
        let chunk_id = "a".repeat(64);
        let error = LixError::new("LIX_SYNC_CHUNKS_REQUIRED", "chunks are deferred")
            .with_details(serde_json::json!({ "chunkIds": [chunk_id.clone()] }));
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let waiter = demand_sync_for_error(&demand_tx, &error);
        let responder = async {
            let demand = demand_rx.recv().await.expect("chunk demand arrives");
            assert!(matches!(
                demand.request,
                SyncDemandRequest::Chunks(ids) if ids == vec![chunk_id]
            ));
            demand.response.send(Ok(())).expect("waiter remains live");
        };
        let (hydrated, ()) = tokio::join!(waiter, responder);
        assert!(hydrated.expect("chunk response succeeds"));
    }

    #[tokio::test]
    async fn shutdown_result_unblocks_a_pending_demand() {
        let error = LixError::new("LIX_SYNC_CHUNKS_REQUIRED", "chunks are deferred")
            .with_details(serde_json::json!({ "chunkIds": ["a".repeat(64)] }));
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        let waiter = demand_sync_for_error(&demand_tx, &error);
        let responder = async {
            let demand = demand_rx.recv().await.expect("pending demand arrives");
            demand
                .response
                .send(Err(stopped_error()))
                .expect("pending waiter remains live");
        };
        let (result, ()) = tokio::join!(waiter, responder);
        let error = result.expect_err("worker shutdown is reported to the caller");
        assert_eq!(error.code, LixError::CODE_CLOSED);
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
            commit_headers: Vec::new(),
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
        hydrate_chunk_ids(&lix, &transport, demanded.clone())
            .await
            .expect("chunk hydration succeeds");
        let first_call_count = chunk_calls.lock().expect("chunk calls lock").len();
        assert_eq!(first_call_count, demanded.len());

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
    async fn unrelated_error_does_not_enqueue_demand() {
        let (demand_tx, mut demand_rx) = tokio::sync::mpsc::channel(1);
        assert!(
            !demand_sync_for_error(
                &demand_tx,
                &LixError::new(LixError::CODE_INVALID_PARAM, "not history"),
            )
            .await
            .expect("unrelated error classification succeeds"),
        );
        assert!(demand_rx.try_recv().is_err());
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

    #[test]
    fn complete_blob_registration_is_accepted() {
        require_complete_registration(
            &"a".repeat(64),
            &super::super::SyncBlobRegistration {
                missing_chunk_ids: Vec::new(),
                complete: true,
            },
        )
        .expect("complete manifest registration");
    }

    #[test]
    fn prepared_snapshot_uses_explicit_repository_identity() {
        let lix_id = uuid::Uuid::now_v7().to_string();
        let default_branch_id = uuid::Uuid::now_v7().to_string();
        let snapshot = SyncRepositoryPullResponse::Snapshot {
            cursor: 3,
            lix_id: lix_id.clone(),
            default_branch_id: default_branch_id.clone(),
            branches: Vec::new(),
        };
        assert_eq!(
            snapshot_repository_identity(&snapshot).expect("snapshot identity"),
            (lix_id.as_str(), default_branch_id.as_str())
        );
    }

    #[tokio::test]
    async fn one_history_demand_hydrates_and_retries_sql_while_deduping_callers() {
        let (replica, parent, head, commits) = history_fixture().await;
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
            commit_headers: Vec::new(),
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (first_response, _first_done) = tokio::sync::oneshot::channel();
        let (second_response, _second_done) = tokio::sync::oneshot::channel();
        let results = hydrate_sync_demands(
            &replica,
            &transport,
            &[
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
        assert!(results.into_iter().all(|result| result.is_ok()));
        let parent_requests = calls
            .lock()
            .expect("history calls lock")
            .iter()
            .flatten()
            .filter(|commit_id| *commit_id == &parent)
            .count();
        assert_eq!(parent_requests, 1, "concurrent demands share one fetch");
        let calls_after_first_hydration = calls.lock().expect("history calls lock").len();
        let (repeat_response, _repeat_done) = tokio::sync::oneshot::channel();
        let results = hydrate_sync_demands(
            &replica,
            &transport,
            &[SyncDemand {
                request: SyncDemandRequest::History(vec![parent.clone()]),
                response: repeat_response,
            }],
        )
        .await;
        assert!(results.into_iter().all(|result| result.is_ok()));
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
    async fn missing_history_id_leaves_its_deferred_marker() {
        let (replica, parent, _head, commits) = history_fixture().await;
        let transport = HistoryTransport {
            commits,
            missing: BTreeSet::from([parent.clone()]),
            calls: Arc::new(Mutex::new(Vec::new())),
            max_history_items: None,
            commit_headers: Vec::new(),
            blobs: BTreeMap::new(),
            blob_calls: Arc::new(Mutex::new(Vec::new())),
            chunks: BTreeMap::new(),
            chunk_calls: Arc::new(Mutex::new(Vec::new())),
        };
        let (response, _done) = tokio::sync::oneshot::channel();
        let errors = hydrate_sync_demands(
            &replica,
            &transport,
            &[SyncDemand {
                request: SyncDemandRequest::History(vec![parent.clone()]),
                response,
            }],
        )
        .await;
        let error = errors
            .into_iter()
            .next()
            .expect("one result")
            .expect_err("missing authority history fails");
        assert_eq!(error.code, LixError::CODE_COMMIT_NOT_FOUND);
        assert_eq!(
            replica
                .deferred_sync_history_ids(&BTreeSet::from([parent.clone()]))
                .await
                .expect("marker query"),
            vec![parent],
            "a missing history response must not clear the local demand marker",
        );
    }

    #[tokio::test]
    async fn explicit_transaction_surfaces_structured_history_demand() {
        let (replica, parent, head, _commits) = history_fixture().await;
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
