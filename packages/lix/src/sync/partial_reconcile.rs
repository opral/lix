//! Background reconciliation of an authority descriptor into the retained SQL
//! working set. Every network step precedes the final owned publication task.
use super::http::{HttpSyncTransport, RawHttpClient};
use super::partial_state::PartialReplicaState;
use crate::LixError;
use crate::engine::Engine;
use crate::storage_adapter::Storage;
use std::sync::Arc;

pub(super) enum PreparedDescriptor {
    NoChange,
    /// Durable bookkeeping advanced while native serving controls stayed local.
    LocalProgress,
    Ready(super::partial_publication::PreparedPartialPublication),
}

pub(super) async fn prepare_clean_descriptor<S, C>(
    engine: Arc<Engine<S>>,
    previous: Arc<PartialReplicaState>,
    transport: &HttpSyncTransport<C>,
    wrapper: super::http::TimedLeasedPartialDescriptor,
    recovery: super::partial_publication::PartialRecoveryPolicy,
) -> Result<PreparedDescriptor, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    C: RawHttpClient + Clone + 'static,
{
    let deadline = wrapper.deadline;
    deadline.check(&wrapper.wire.lease.lease_id)?;
    if engine.sync_mode().partial_admission().as_deref() != Some(previous.as_ref()) {
        return Err(LixError::new(
            LixError::CODE_TRANSACTION_CONFLICT,
            "lease recovery source changed",
        ));
    }
    if recovery == super::partial_publication::PartialRecoveryPolicy::ExpiredBaseline
        && super::partial_publication::same_serving_basis(
            previous.descriptor(),
            &wrapper.wire.descriptor,
        )
    {
        return super::partial_publication::prepare_clean_lease_reacquisition(
            &engine,
            &wrapper.wire,
            deadline,
        )
        .await
        .map(PreparedDescriptor::Ready);
    }
    let next = Arc::new(previous.with_leased_descriptor_and_fresh_generations(wrapper.wire)?);
    let candidate = transport.fork_native_baseline_lease(next.baseline_lease())?;
    let storage = engine.storage();
    let mut demands = std::collections::BTreeSet::new();
    for _ in 0..4096 {
        deadline.check(&next.baseline_lease().lease_id)?;
        engine.sync_mode().ensure_partial_admission_healthy()?;
        if engine.sync_mode().partial_admission().as_deref() != Some(previous.as_ref()) {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "candidate source admission changed",
            ));
        }
        if next.descriptor().cursor < previous.descriptor().cursor {
            return Err(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "candidate descriptor cursor regressed",
            ));
        }
        let error = match super::partial_publication::prepare_partial_publication(
            &engine,
            next.clone(),
            deadline.clone(),
            recovery,
        )
        .await
        {
            Ok(None) => return Ok(PreparedDescriptor::NoChange),
            Ok(Some(prepared)) => {
                return Ok(PreparedDescriptor::Ready(prepared));
            }
            Err(error) => error,
        };
        let Some(demand) = super::runtime::native_sync_demand_request_for_error(&error)? else {
            return Err(error);
        };
        // Same typed immutable input cannot be demanded twice by one candidate.
        // Native corruption and unsupported history remain terminal errors.
        let fingerprint = format!("{demand:?}");
        if !demands.insert(fingerprint) {
            return Err(LixError::new(
                "LIX_PARTIAL_SCOPE_PREPARATION_STALLED",
                "candidate repeated a hydrated native dependency",
            ));
        }
        // Keep the original HTTP observation's monotonic budget across every
        // hydration retry. Neither partial progress nor transport cancellation
        // renews authority retention for this candidate.
        use futures_util::FutureExt;
        let hydrate =
            super::partial_runtime::hydrate_demand(&storage, &previous, &candidate, demand).fuse();
        let expires = super::platform::sleep(deadline.remaining()?).fuse();
        futures_util::pin_mut!(hydrate, expires);
        futures_util::select_biased! {
            _ = expires => return Err(LixError::new("LIX_PARTIAL_CANDIDATE_EXPIRED", "candidate hydration exceeded its original baseline deadline")),
            result = hydrate => result?,
        }
        deadline.check(&next.baseline_lease().lease_id)?;
    }
    Err(LixError::new(
        "LIX_PARTIAL_SCOPE_PREPARATION_LIMIT",
        "candidate preparation exceeded bounded dependency count",
    ))
}
