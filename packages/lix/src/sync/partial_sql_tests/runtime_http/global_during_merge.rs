use super::*;

pub(super) async fn create_branch(
    session: &SessionContext<Memory>,
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<Client>,
) -> String {
    let id = uuid::Uuid::now_v7().to_string();
    for _ in 0..256 {
        match session
            .create_branch(crate::CreateBranchOptions {
                id: Some(id.clone()),
                name: "created-from-pending-L2".into(),
                from_commit_id: None,
            })
            .await
        {
            Ok(branch) => return branch.id,
            Err(error) => hydrate(storage, state, transport, error).await,
        }
    }
    panic!("branch creation exceeded dependency bound")
}

pub(super) async fn upload_if_ready(
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<Client>,
) {
    for _ in 0..256 {
        let result = crate::sync::partial_upload_cycle::upload_partial_once(
            storage,
            state,
            crate::GLOBAL_BRANCH_ID,
            uuid::Uuid::now_v7().to_string(),
            32,
            1024 * 1024,
            |request| async move {
                crate::sync::partial_blob_upload::push_partial_with_blobs(
                    storage, state, transport, &request,
                )
                .await
            },
        )
        .await;
        match result {
            Ok(_) => return,
            Err(error) if error.code == "LIX_PARTIAL_CREATED_REF_SOURCE_PENDING" => return,
            Err(error) => hydrate(storage, state, transport, error).await,
        }
    }
    panic!("GLOBAL upload exceeded dependency bound")
}

async fn hydrate(
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    transport: &HttpSyncTransport<Client>,
    error: LixError,
) {
    let demand = crate::sync::runtime::native_sync_demand_request_for_error(&error)
        .unwrap()
        .unwrap_or_else(|| panic!("unexpected dependency error: {error:?}"));
    crate::sync::partial_runtime::hydrate_demand(storage, state, transport, demand)
        .await
        .unwrap();
}
