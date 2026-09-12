use super::*;
use crate::sync::{MAX_PARTIAL_UPDATE_RESPONSE_BYTES, PartialUpdateRequest, PartialUpdateResponse};

pub(super) async fn update<S>(
    lease: SessionLease<S>,
    Json(request): Json<PartialUpdateRequest>,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let interests = request.snapshot()?;
    let descriptor = descriptor_wait::load_descriptor_until(
        lease.clone(),
        Some(request.branch_id),
        request.after,
        tokio::time::Instant::now() + SYNC_LONG_POLL_TIMEOUT,
    )
    .await?;
    // An idle poll does not reevaluate the working set or send its payload.
    let bundle = if descriptor.descriptor.cursor == request.known_cursor {
        crate::sync::WorkingSetBundle::default()
    } else {
        let pinned = descriptor.clone();
        lease
            .run_cancellable_read(move |lix| async move {
                lix.collect_partial_working_set(&pinned, &interests).await
            })
            .await?
    };
    bounded_sync_json_response(
        PartialUpdateResponse { descriptor, bundle },
        "partial working-set update",
        MAX_PARTIAL_UPDATE_RESPONSE_BYTES,
    )
}
