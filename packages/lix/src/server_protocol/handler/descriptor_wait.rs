use super::*;

pub(super) async fn wait_descriptor<S>(
    lease: SessionLease<S>,
    branch_id: Option<String>,
    after: Option<u64>,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    wait_descriptor_until(
        lease,
        branch_id,
        after,
        tokio::time::Instant::now() + SYNC_LONG_POLL_TIMEOUT,
    )
    .await
}

pub(super) async fn wait_descriptor_until<S>(
    lease: SessionLease<S>,
    branch_id: Option<String>,
    after: Option<u64>,
    deadline: tokio::time::Instant,
) -> Result<Response, ApiError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if after.is_some() && branch_id.is_none() {
        return Err(ApiError::bad_request(
            "descriptor continuation requires the selected branchId",
        ));
    }
    if after.is_none() {
        let descriptor = lease
            .run_cancellable_read(move |lix| async move {
                lix.leased_partial_replica_descriptor(branch_id.as_deref())
                    .await
            })
            .await?;
        return bounded_sync_json_response(
            descriptor,
            "partial replica descriptor",
            crate::sync::MAX_LEASED_DESCRIPTOR_BYTES,
        );
    }
    // Subscribe first: a publication during/between descriptor read and wait
    // leaves an unseen channel version and makes changed() immediately ready.
    let mut changed = lease.record.lix.sync_mode_state().change_watcher();
    let mut finish = false;
    loop {
        let branch_id = branch_id.clone();
        let descriptor = lease
            .run_cancellable_read(move |lix| async move {
                lix.partial_replica_descriptor(branch_id.as_deref()).await
            })
            .await?;
        if after.is_some_and(|after| descriptor.cursor < after) {
            return Err(ApiError::from(LixError::new(
                LixError::CODE_TRANSACTION_CONFLICT,
                "partial descriptor cursor is ahead of the authority; admitted baseline must be reconciled",
            ).with_details(serde_json::json!({ "after":after, "cursor":descriptor.cursor, "partialBaselineResetRequired":true }))));
        }
        if after.is_none_or(|after| descriptor.cursor > after)
            || finish
            || tokio::time::Instant::now() >= deadline
        {
            let branch = descriptor.selected_branch.branch_id;
            let leased = lease
                .run_cancellable_read(move |lix| async move {
                    lix.leased_partial_replica_descriptor(Some(&branch)).await
                })
                .await?;
            return bounded_sync_json_response(
                leased,
                "partial replica descriptor",
                crate::sync::MAX_LEASED_DESCRIPTOR_BYTES,
            );
        }
        tokio::select! {
            result = changed.changed() => { finish = result.is_err(); },
            _ = tokio::time::sleep_until(deadline) => { finish = true; },
        }
        // Re-read even on timeout/closed notifier. The response always contains
        // one final coherent descriptor, without resetting the fixed deadline.
    }
}
