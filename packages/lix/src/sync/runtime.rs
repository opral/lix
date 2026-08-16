use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use crate::storage_adapter::Storage;
use crate::{CreateBranchOptions, Lix, LixError, SwitchBranchOptions, Value};

use super::transport::HttpSyncTransport;

const SYNC_POLL_INTERVAL: Duration = Duration::from_millis(100);
const SYNC_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);
const SYNC_MAX_IDLE_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(crate) struct SyncRuntime {
    shutdown: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
    finished_notify: Arc<tokio::sync::Notify>,
    worker: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl SyncRuntime {
    pub(crate) fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub(crate) async fn stop_and_join(&self) -> Result<(), LixError> {
        self.stop();
        {
            let worker = self.worker.lock().map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync worker lock is poisoned",
                )
            })?;
            if let Some(worker) = worker.as_ref() {
                worker.thread().unpark();
            }
        }
        loop {
            let notified = self.finished_notify.notified();
            if self.finished.load(Ordering::Acquire) {
                break;
            }
            notified.await;
        }
        let worker = self
            .worker
            .lock()
            .map_err(|_| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    "sync worker lock is poisoned",
                )
            })?
            .take();
        if let Some(worker) = worker {
            worker.join().map_err(|_| {
                LixError::new(LixError::CODE_INTERNAL_ERROR, "sync worker panicked")
            })?;
        }
        Ok(())
    }
}

impl Drop for SyncRuntime {
    fn drop(&mut self) {
        self.stop();
    }
}

pub(crate) async fn activate_sync_mode<StorageImpl>(
    lix: &Lix<StorageImpl>,
    server_url: &str,
) -> Result<Arc<SyncRuntime>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let remote_id = server_url.trim_end_matches('/').to_owned();
    // Keep a reopened replica on the branch it last used. New replicas have
    // no durable mapping yet and therefore let the server choose its default
    // branch during the first handshake.
    let remembered_branch = lix.load_sync_replica_branch(&remote_id).await?;
    let initial_transport =
        match HttpSyncTransport::connect(&remote_id, remembered_branch.as_deref()).await {
            Ok(transport) => {
                select_server_branch(lix, transport.branch_id()).await?;
                let mut client = lix.sync_lifecycle(transport.clone()).await?;
                // A first online open returns only after the local replica has
                // caught up to the head visible during the handshake.
                client.flush().await?;
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
        };
    lix.set_sync_role(crate::sync::SyncRole::Replica {
        remote_id: remote_id.clone(),
    })?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown);
    let finished = Arc::new(AtomicBool::new(false));
    let worker_finished = Arc::clone(&finished);
    let finished_notify = Arc::new(tokio::sync::Notify::new());
    let worker_finished_notify = Arc::clone(&finished_notify);
    let worker_lix = lix.clone();
    // Plugin reconciliation and the nested single-thread Tokio runtime use
    // more than Rust's 2 MiB default stack under concurrent replicas. Keep a
    // bounded per-replica stack so parallel sync workers fail gracefully
    // rather than aborting the process with a stack overflow.
    let worker = std::thread::Builder::new()
        .name("lix-sync".to_owned())
        .stack_size(4 * 1024 * 1024)
        .spawn(move || {
            let _done = WorkerDone {
                finished: worker_finished,
                notify: worker_finished_notify,
            };
            let mut transport = initial_transport;
            let mut retry_backoff = SYNC_POLL_INTERVAL;
            let mut idle_delay = SYNC_POLL_INTERVAL;
            let mut last_cursor = None;
            let worker_runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(_) => return,
            };
            while !worker_shutdown.load(Ordering::Acquire) {
                let result = worker_runtime.block_on(async {
                    let active_branch_id = worker_lix.active_branch_id().await?;
                    // A branch switch is session-local. Drop the old HTTP
                    // session before flushing so the next handshake is pinned
                    // to the newly selected branch.
                    if transport
                        .as_ref()
                        .is_some_and(|current| current.branch_id() != active_branch_id)
                    {
                        transport = None;
                    }
                    let current = match transport.clone() {
                        Some(current) => current,
                        None => {
                            let current =
                                HttpSyncTransport::connect(&remote_id, Some(&active_branch_id))
                                    .await?;
                            if active_branch_id != current.branch_id() {
                                return Err(LixError::new(
                                    LixError::CODE_INVALID_PARAM,
                                    "sync server branch changed while the replica was offline",
                                ));
                            }
                            worker_lix
                                .persist_sync_replica_config(&remote_id, current.branch_id())
                                .await?;
                            transport = Some(current.clone());
                            current
                        }
                    };
                    // Keep the polling writer on its own logical session. A
                    // Lix handle rejects reads/observations while that handle
                    // owns an explicit transaction; sharing the application
                    // session would make `observe()` race the sync apply
                    // transaction and intermittently return
                    // LIX_INVALID_TRANSACTION_STATE.
                    let worker_session = worker_lix
                        .open_internal_session(
                            active_branch_id,
                            worker_lix.active_account_id().to_owned(),
                        )
                        .await?;
                    let mut client = worker_session.sync_lifecycle(current).await?;
                    let result = client.flush().await;
                    worker_session.close().await?;
                    result
                });
                let delay = match result {
                    Ok(receipt) => {
                        retry_backoff = SYNC_POLL_INTERVAL;
                        if last_cursor == Some(receipt.cursor) {
                            idle_delay = idle_delay
                                .checked_mul(2)
                                .unwrap_or(SYNC_MAX_IDLE_INTERVAL)
                                .min(SYNC_MAX_IDLE_INTERVAL);
                        } else {
                            last_cursor = Some(receipt.cursor);
                            idle_delay = SYNC_POLL_INTERVAL;
                        }
                        idle_delay
                    }
                    Err(error) => {
                        // The server may have restarted or evicted this
                        // session. Re-handshake on the next iteration; durable
                        // local state keeps the cursor and pending queue
                        // authoritative. Backoff also prevents a permanent
                        // validation/bootstrap error from becoming a 10 Hz
                        // reconnect storm.
                        transport = None;
                        idle_delay = SYNC_POLL_INTERVAL;
                        let delay = if error.code == LixError::CODE_INVALID_PARAM {
                            retry_backoff.max(Duration::from_secs(5))
                        } else {
                            retry_backoff
                        };
                        retry_backoff = retry_backoff
                            .checked_mul(2)
                            .unwrap_or(SYNC_MAX_RETRY_BACKOFF)
                            .min(SYNC_MAX_RETRY_BACKOFF);
                        delay
                    }
                };
                std::thread::park_timeout(delay);
            }
        })
        .map_err(|error| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!("start sync worker: {error}"),
            )
        })?;
    Ok(Arc::new(SyncRuntime {
        shutdown,
        finished,
        finished_notify,
        worker: Mutex::new(Some(worker)),
    }))
}

struct WorkerDone {
    finished: Arc<AtomicBool>,
    notify: Arc<tokio::sync::Notify>,
}

impl Drop for WorkerDone {
    fn drop(&mut self) {
        self.finished.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

async fn select_server_branch<StorageImpl>(
    lix: &Lix<StorageImpl>,
    server_branch_id: &str,
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
            name: "sync".to_owned(),
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
