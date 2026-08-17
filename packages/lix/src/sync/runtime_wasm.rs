use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use js_sys::{Function, Promise, Reflect};
use wasm_bindgen::{JsCast, JsValue};
use wasm_bindgen_futures::{JsFuture, spawn_local};

use crate::branch::{branch_descriptor_tombstone_row, branch_ref_tombstone_row};
use crate::storage_adapter::Storage;
use crate::transaction_types::RawWriteBatch;
use crate::{
    CreateBranchOptions, GLOBAL_BRANCH_ID, Lix, LixError, ServerOptions, SwitchBranchOptions,
    Value,
};

use super::transport_wasm::HttpSyncTransport;
use super::{SyncBranch, SyncTransport, reconcile_sync_branches};

const SYNC_POLL_INTERVAL: Duration = Duration::from_millis(250);
const SYNC_MAX_RETRY_BACKOFF: Duration = Duration::from_secs(30);

#[derive(Debug)]
pub(crate) struct SyncRuntime {
    shutdown: Arc<AtomicBool>,
    finished: Arc<AtomicBool>,
}

impl SyncRuntime {
    pub(crate) fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    pub(crate) async fn stop_and_join(&self) -> Result<(), LixError> {
        self.stop();
        while !self.finished.load(Ordering::Acquire) {
            sleep(Duration::from_millis(10)).await?;
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
    server: &ServerOptions,
) -> Result<Arc<SyncRuntime>, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let remote_id = server.url.trim_end_matches('/').to_owned();
    let headers = server.headers.clone();
    let remembered_branch = lix.load_sync_replica_branch(&remote_id).await?;
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
        Some(lix.active_branch_id().await?)
    } else {
        None
    };
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
                let server_branch = transport
                    .list_branches()
                    .await?
                    .into_iter()
                    .find(|branch| branch.id == transport.branch_id());
                let fresh_bootstrap = remembered_branch.is_none()
                    && local_default_branch.as_deref() != Some(transport.branch_id());
                if fresh_bootstrap {
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
                    reconcile_initial_server_branch(lix, metadata, None).await?;
                }
                if fresh_local_seed_commit_id.is_some() {
                    lix.mark_sync_bootstrap_commits_hidden().await?;
                }
                let mut client = lix.sync_lifecycle(transport.clone()).await?;
                if let Some(seed_commit_id) = fresh_local_seed_commit_id {
                    client.mark_fresh_bootstrap_cleanup(seed_commit_id);
                }
                client.flush_without_wait().await?;
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
    lix.set_sync_role(super::SyncRole::Replica {
        remote_id: remote_id.clone(),
    })?;
    lix.restore_sync_file_projections(&remote_id).await?;

    let shutdown = Arc::new(AtomicBool::new(false));
    let finished = Arc::new(AtomicBool::new(false));
    let worker_shutdown = Arc::clone(&shutdown);
    let worker_finished = Arc::clone(&finished);
    let worker_lix = lix.clone();
    spawn_local(async move {
        let mut transport = initial_transport;
        let mut retry_backoff = SYNC_POLL_INTERVAL;
        while !worker_shutdown.load(Ordering::Acquire) {
            let result = sync_once(
                &worker_lix,
                &remote_id,
                &headers,
                &mut transport,
            )
            .await;
            let delay = match result {
                Ok(()) => {
                    retry_backoff = SYNC_POLL_INTERVAL;
                    SYNC_POLL_INTERVAL
                }
                Err(error) => {
                    tracing::warn!(error = ?error, "browser sync runtime iteration failed");
                    transport = None;
                    let delay = retry_backoff;
                    retry_backoff = retry_backoff
                        .checked_mul(2)
                        .unwrap_or(SYNC_MAX_RETRY_BACKOFF)
                        .min(SYNC_MAX_RETRY_BACKOFF);
                    delay
                }
            };
            if !worker_shutdown.load(Ordering::Acquire)
                && let Err(error) = sleep(delay).await
            {
                tracing::warn!(error = ?error, "browser sync timer failed");
                break;
            }
        }
        worker_finished.store(true, Ordering::Release);
    });

    Ok(Arc::new(SyncRuntime { shutdown, finished }))
}

async fn sync_once<StorageImpl>(
    lix: &Lix<StorageImpl>,
    remote_id: &str,
    headers: &[(String, String)],
    transport: &mut Option<HttpSyncTransport>,
) -> Result<(), LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
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
            current
        }
    };
    let worker_session = lix
        .open_internal_session_suppressed(
            active_branch_id.clone(),
            lix.active_account_id().to_owned(),
        )
        .await?;
    let result = async {
        reconcile_sync_branches(&worker_session, &current, false).await?;
        let mut client = worker_session.sync_lifecycle(current).await?;
        client.flush_without_wait().await?;
        lix.restore_sync_file_projections(remote_id).await?;
        reconcile_sync_branches(&worker_session, transport.as_ref().expect("transport"), true)
            .await?;
        Ok(())
    }
    .await;
    let close = worker_session.close().await;
    match (result, close) {
        (Err(error), _) => Err(error),
        (Ok(()), Err(error)) => Err(error),
        (Ok(()), Ok(())) => Ok(()),
    }
}

pub(super) async fn sleep(duration: Duration) -> Result<(), LixError> {
    let promise = Promise::new(&mut |resolve, _reject| {
        let global = js_sys::global();
        if let Ok(timer) = Reflect::get(&global, &"setTimeout".into())
            && let Ok(timer) = timer.dyn_into::<Function>()
        {
            let _ = timer.call2(
                &global,
                &resolve,
                &JsValue::from_f64(duration.as_millis() as f64),
            );
        }
    });
    JsFuture::from(promise).await.map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("browser sync timer failed: {error:?}"),
        )
    })?;
    Ok(())
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
    Ok(())
}
