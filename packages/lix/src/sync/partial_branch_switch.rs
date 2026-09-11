//! Explicit, online admission of an existing authority branch. No pending lane
//! is settled here and no local branch is discarded to manufacture cleanliness.
use super::SyncTransport;
use super::http::{HttpSyncTransport, RawHttpClient};
use super::partial_state::PartialReplicaState;
use crate::engine::Engine;
use crate::storage_adapter::Storage;
use crate::{LixError, ServerOptions};
use std::sync::Arc;

/// The session and primary switch gates travel into the owned commit task.
/// Cancellation of the public future cannot release them before selector and
/// durable admission agree.
pub(crate) struct PartialBranchSwitchCompletion {
    pub(crate) branch: crate::session::SessionBranch,
    pub(crate) target: String,
    pub(crate) _primary_guard: Option<tokio::sync::OwnedMutexGuard<()>>,
    pub(crate) _session_guard: tokio::sync::OwnedMutexGuard<()>,
}
impl PartialBranchSwitchCompletion {
    pub(super) fn complete(&self) -> Result<(), LixError> {
        self.branch.set(self.target.clone())
    }
}
fn conflict(message: &str) -> LixError {
    LixError::new(LixError::CODE_TRANSACTION_CONFLICT, message)
}

pub(crate) async fn switch_existing_branch<S>(
    engine: Arc<Engine<S>>,
    server: ServerOptions,
    completion: PartialBranchSwitchCompletion,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    super::validate_sync_branch_id(&completion.target)?;
    let mode = engine.sync_mode();
    mode.ensure_partial_admission_healthy()?;
    let previous = mode
        .partial_admission()
        .ok_or_else(|| conflict("branch switch has no partial admission"))?;
    if super::http::normalize_sync_locator(&server.url)?.protocol_url
        != super::http::normalize_sync_locator(previous.remote_id())?.protocol_url
    {
        return Err(conflict(
            "branch switch authority differs from admitted repository",
        ));
    }
    super::partial_publication::require_clean_switch_source(&engine, &previous).await?;
    let owner = engine.partial_owner();
    let owner_guard = if owner.is_installed() {
        Some(owner.retain_for_owned_work()?)
    } else {
        None
    };
    let (mut sender, receiver) = tokio::sync::oneshot::channel();
    let task = super::platform::spawn_sync_task(async move {
        let _owner_guard = owner_guard;
        let connect = super::platform::HttpSyncTransport::connect(&server.url, &server.headers);
        futures_util::pin_mut!(connect);
        let connected = match futures_util::future::select(connect, Box::pin(sender.closed())).await
        {
            futures_util::future::Either::Left((result, _)) => Some(result),
            futures_util::future::Either::Right(_) => None,
        };
        let transport = match connected {
            Some(Ok(transport)) => transport,
            Some(Err(error)) => {
                let _ = sender.send(Err(error));
                return;
            }
            None => return,
        };
        let result = async {
            if transport.lix_id() != previous.repository_id()
                || transport.active_account_id() != previous.active_account_id()
            {
                return Err(conflict(
                    "branch switch authenticated another repository or account",
                ));
            }
            let target = completion.target.clone();
            let prepare = prepare_existing_branch(engine.clone(), previous, &transport, &target);
            futures_util::pin_mut!(prepare);
            let prepared =
                match futures_util::future::select(prepare, Box::pin(sender.closed())).await {
                    futures_util::future::Either::Left((result, _)) => result?,
                    futures_util::future::Either::Right(_) => {
                        return Err(conflict("branch switch cancelled before publication"));
                    }
                };
            // Publication owns its task and both selector guards. Do not race
            // this await against cancellation once durable work can begin.
            super::partial_publication::publish_prepared_partial(
                engine,
                prepared.with_branch_switch_completion(completion),
            )
            .await
        }
        .await;
        let close = transport.close_session();
        let timeout = super::platform::sleep(std::time::Duration::from_secs(1));
        futures_util::pin_mut!(close, timeout);
        if let futures_util::future::Either::Left((Err(error), _)) =
            futures_util::future::select(close, timeout).await
        {
            tracing::warn!(code = %error.code, "branch admission session cleanup failed");
        }
        let _ = sender.send(result);
    })?;
    let result = receiver
        .await
        .map_err(|_| conflict("branch switch stopped without an outcome"))?;
    task.join().await?;
    result
}

async fn prepare_existing_branch<S, C>(
    engine: Arc<Engine<S>>,
    previous: Arc<PartialReplicaState>,
    transport: &HttpSyncTransport<C>,
    target: &str,
) -> Result<super::partial_publication::PreparedPartialPublication, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    // Reject dirty source before network metadata or candidate hydration. The
    // final preparation repeats these reads and carries their exact CAS tokens.
    super::partial_publication::require_clean_switch_source(&engine, &previous).await?;
    let wrapper = transport.partial_replica_descriptor(Some(target)).await?;
    wrapper.wire.validate(
        previous.repository_id(),
        previous.active_account_id(),
        Some(target),
    )?;
    let deadline = wrapper.deadline;
    let next = Arc::new(previous.with_selected_branch(wrapper.wire, target)?);
    let candidate = transport.fork_native_baseline_lease(next.baseline_lease())?;
    let storage = engine.storage();
    let mut demands = std::collections::BTreeSet::new();
    for _ in 0..4096 {
        deadline.check(&next.baseline_lease().lease_id)?;
        if engine.sync_mode().partial_admission().as_deref() != Some(previous.as_ref()) {
            return Err(conflict(
                "source admission changed during branch preparation",
            ));
        }
        let error = match super::partial_publication::prepare_branch_switch_publication(
            &engine,
            next.clone(),
            deadline.clone(),
        )
        .await
        {
            Ok(prepared) => return Ok(prepared),
            Err(error) => error,
        };
        let Some(demand) = super::runtime::native_sync_demand_request_for_error(&error)? else {
            return Err(error);
        };
        if !demands.insert(format!("{demand:?}")) {
            return Err(LixError::new(
                "LIX_PARTIAL_SCOPE_PREPARATION_STALLED",
                "branch switch repeated a hydrated dependency",
            ));
        }
        let hydrate =
            super::partial_runtime::hydrate_demand(&storage, &previous, &candidate, demand);
        futures_util::pin_mut!(hydrate);
        match futures_util::future::select(
            hydrate,
            Box::pin(super::platform::sleep(deadline.remaining()?)),
        )
        .await
        {
            futures_util::future::Either::Left((result, _)) => result?,
            futures_util::future::Either::Right(_) => {
                return Err(LixError::new(
                    "LIX_PARTIAL_CANDIDATE_EXPIRED",
                    "branch switch exceeded its original authority lease",
                ));
            }
        }
    }
    Err(LixError::new(
        "LIX_PARTIAL_SCOPE_PREPARATION_LIMIT",
        "branch switch exceeded native dependency bound",
    ))
}
