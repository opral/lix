use super::*;

async fn fixture() -> (ReadFulfillmentRequest, ReadFulfillmentResponse) {
    let authority = crate::open_lix().await.unwrap();
    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
    let branch_id = descriptor.selected_branch.branch_id.clone();
    let bytes = b"candidate installer test chunk".to_vec();
    let address = ReadInputAddress::BlobChunk(*blake3::hash(&bytes).as_bytes());
    let request = ReadFulfillmentRequest {
        operation_id: uuid::Uuid::now_v7().to_string(),
        release: false,
        operation_expires_at_ms: crate::telemetry::unix_time_ms() + 60_000,
        epoch_id: uuid::Uuid::now_v7().to_string(),
        descriptor,
        interests: vec![LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![branch_id],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        }],
        required: vec![address.clone()],
        continuation: None,
    };
    let inputs = vec![ReadInput { address, bytes }];
    let response = ReadFulfillmentResponse {
        frame: None,
        lix_id: request.descriptor.lix_id.clone(),
        epoch_id: request.epoch_id.clone(),
        request_digest: request.digest().unwrap(),
        closure_digest: input_digest(&request, &inputs).unwrap(),
        inputs,
        profile: Default::default(),
        continuation: None,
        outcome: ReadFulfillmentOutcome::Complete,
    };
    authority.close().await.unwrap();
    (request, response)
}

fn candidate_state(request: &ReadFulfillmentRequest) -> PartialReplicaState {
    PartialReplicaState::new(
        format!("https://example.test/lix/{}", request.descriptor.lix_id),
        "00000000-0000-4000-8000-000000000004".to_owned(),
        request.epoch_id.clone(),
        request.descriptor.clone(),
    )
    .unwrap()
}

fn working_diff_request(state: &PartialReplicaState) -> ReadFulfillmentRequest {
    ReadFulfillmentRequest {
        operation_id: uuid::Uuid::now_v7().to_string(),
        release: false,
        operation_expires_at_ms: state.baseline_lease().expires_at_ms,
        epoch_id: state.epoch_id().to_owned(),
        descriptor: state.descriptor().clone(),
        interests: vec![LogicalReadInterest::Diff {
            branch_id: Some(state.descriptor().selected_branch.branch_id.clone()),
            relation: "lix_file".to_owned(),
            from: crate::hot_state::DiffInterestEndpoint::WorkingCheckpoint,
            to: crate::hot_state::DiffInterestEndpoint::ActiveHead,
            filter: crate::tracked_state::TrackedStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: Vec::new(),
            limit: None,
        }],
        required: vec![ReadInputAddress::Metadata(
            NativeMetadataRef::CommitStateHeader(
                state.descriptor().selected_branch.head.commit_id.clone(),
            ),
        )],
        continuation: None,
    }
}

#[tokio::test]
async fn candidate_install_rejects_non_working_diff_interest() {
    let (request, response) = fixture().await;
    let state = candidate_state(&request);
    let error = validate_candidate_immutable_basis(&state, &state, &request, &response)
        .expect_err("filesystem metadata is not a working-diff candidate");
    assert!(error.message.contains("outside the working-diff basis"));
}

#[tokio::test]
async fn candidate_install_requires_the_exact_next_head_header() {
    let (base_request, response) = fixture().await;
    let state = candidate_state(&base_request);
    let mut request = working_diff_request(&state);
    request.required = vec![ReadInputAddress::Metadata(
        NativeMetadataRef::CommitStateHeader(uuid::Uuid::now_v7().to_string()),
    )];
    let error = validate_candidate_immutable_basis(&state, &state, &request, &response)
        .expect_err("candidate must pin the selected head, not the checkpoint");
    assert!(error.message.contains("exact next selected-head header"));
}

#[tokio::test]
async fn candidate_install_rejects_change_records_before_closure_validation() {
    let (base_request, mut response) = fixture().await;
    let state = candidate_state(&base_request);
    let request = working_diff_request(&state);
    response.inputs.push(ReadInput {
        address: ReadInputAddress::ChangeRecord {
            change_id: "00000000-0000-7000-8000-000000000010".to_owned(),
            source_commit_id: state.descriptor().selected_branch.head.commit_id.clone(),
            branch_id: state.descriptor().selected_branch.branch_id.clone(),
            schema_key: "lix_file".to_owned(),
            file_id: None,
            row_pk: crate::row_pk::RowPk::single("candidate-row"),
            updated_at: "2024-01-01T00:00:00.000Z".to_owned(),
            payload_digest: [0; 32],
        },
        bytes: Vec::new(),
    });
    let error = validate_candidate_immutable_basis(&state, &state, &request, &response)
        .expect_err("mutable changelog records cannot be installed for a candidate");
    assert!(error.message.contains("cannot contain change records"));
}

#[tokio::test]
async fn candidate_install_rejects_admission_identity_changes() {
    let (request, response) = fixture().await;
    let previous = candidate_state(&request);
    let next = PartialReplicaState::new(
        previous.remote_id().to_owned(),
        "00000000-0000-4000-8000-000000000005".to_owned(),
        previous.epoch_id().to_owned(),
        previous.descriptor().clone(),
    )
    .unwrap();
    let candidate_request = working_diff_request(&next);
    let error = validate_candidate_immutable_basis(&previous, &next, &candidate_request, &response)
        .expect_err("candidate must retain the current admission identity");
    assert!(error.message.contains("changed its admission identity"));
}

#[tokio::test]
async fn candidate_install_warms_dependencies_without_publishing_admission() {
    let authority = crate::open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let initial = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file(path, content) VALUES('/candidate-install.txt', CAST('text' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
    let leased = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    let epoch_id = uuid::Uuid::now_v7().to_string();
    let remote_id = format!("https://example.test/lix/{}", leased.descriptor.lix_id);
    let previous = PartialReplicaState::new(
        remote_id.clone(),
        authority.active_account_id().to_owned(),
        epoch_id.clone(),
        initial.descriptor,
    )
    .unwrap();
    let next = PartialReplicaState::new(
        remote_id,
        authority.active_account_id().to_owned(),
        epoch_id,
        leased.descriptor.clone(),
    )
    .unwrap();
    assert_ne!(
        previous.descriptor().selected_branch.head.commit_id,
        next.descriptor().selected_branch.head.commit_id,
        "fixture must advance the candidate head"
    );
    let mut request = working_diff_request(&next);
    request.operation_expires_at_ms = leased.lease.expires_at_ms;
    let response = authority
        .read_sync_fulfillment(&request, &leased.lease.lease_id)
        .await
        .unwrap();
    assert_eq!(response.outcome, ReadFulfillmentOutcome::Complete);

    // A concurrent admission change must stop candidate warmup before it
    // installs any dependency.
    let changed_storage = StorageAdapter::new(Memory::new());
    let mut changed_writes = changed_storage.new_write_set();
    let changed_admission =
        crate::sync::partial_state::stage_partial_replica_state(&mut changed_writes, &next, None)
            .unwrap();
    let mut changed_raw = changed_storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![changed_admission],
            ..Default::default()
        })
        .await
        .unwrap();
    changed_writes.lower_into(&mut changed_raw).await.unwrap();
    changed_raw.commit().await.unwrap();
    changed_storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    let error =
        install_candidate_immutable(&changed_storage, &previous, &next, &request, &response)
            .await
            .expect_err("candidate install must fence the previous admission receipt");
    assert_eq!(
        error.code,
        super::super::runtime::PARTIAL_ADMISSION_CHANGED_CODE
    );
    let changed_read = changed_storage
        .begin_read(Default::default())
        .await
        .unwrap();
    let (space, key) = ReadInputAddress::Metadata(NativeMetadataRef::CommitStateHeader(
        next.descriptor().selected_branch.head.commit_id.clone(),
    ))
    .coordinate()
    .unwrap();
    assert!(
        PointReadPlan::new(space, std::slice::from_ref(&key))
            .materialize(&changed_read, Default::default())
            .await
            .unwrap()
            .value
            .pop()
            .flatten()
            .is_none(),
        "stale candidate receipt must not install its head header"
    );

    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    let admission =
        crate::sync::partial_state::stage_partial_replica_state(&mut writes, &previous, None)
            .unwrap();
    let mut raw = storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![admission],
            ..Default::default()
        })
        .await
        .unwrap();
    writes.lower_into(&mut raw).await.unwrap();
    raw.commit().await.unwrap();
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());

    install_candidate_immutable(&storage, &previous, &next, &request, &response)
        .await
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (installed_state, _) = crate::sync::partial_state::load_partial_replica_state(&read)
        .await
        .unwrap()
        .expect("candidate warmup must leave admission present");
    assert_eq!(
        installed_state, previous,
        "warmup must not publish candidate state"
    );
    let installed = PointReadPlan::new(space, std::slice::from_ref(&key))
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten();
    assert!(
        installed.is_some(),
        "candidate warmup should install its head header"
    );
    authority.close().await.unwrap();
}

#[tokio::test]
async fn candidate_install_prefetches_graph_metadata_without_publishing_controls() {
    let authority = crate::open_lix().await.unwrap();
    authority
        .set_sync_role(crate::sync::SyncRole::Authority)
        .unwrap();
    let initial = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    authority
        .execute(
            "INSERT INTO lix_file(path, content) VALUES('/candidate-graph.txt', CAST('text' AS BYTEA))",
            &[],
        )
        .await
        .unwrap();
    let leased = authority
        .leased_partial_replica_descriptor(None)
        .await
        .unwrap();
    let epoch_id = uuid::Uuid::now_v7().to_string();
    let remote_id = format!("https://example.test/lix/{}", leased.descriptor.lix_id);
    let previous = PartialReplicaState::new(
        remote_id.clone(),
        authority.active_account_id().to_owned(),
        epoch_id.clone(),
        initial.descriptor,
    )
    .unwrap();
    let next = PartialReplicaState::new(
        remote_id,
        authority.active_account_id().to_owned(),
        epoch_id,
        leased.descriptor.clone(),
    )
    .unwrap();
    let mut request = working_diff_request(&next);
    request.operation_expires_at_ms = leased.lease.expires_at_ms;
    let mut response = authority
        .read_sync_fulfillment(&request, &leased.lease.lease_id)
        .await
        .unwrap();
    assert_eq!(response.outcome, ReadFulfillmentOutcome::Complete);

    let next_head = NativeMetadataRef::CommitGraphRecord(
        next.descriptor().selected_branch.head.commit_id.clone(),
    );
    let old_head = NativeMetadataRef::CommitGraphRecord(
        previous.descriptor().selected_branch.head.commit_id.clone(),
    );
    async fn authority_graph_bytes(
        authority: &crate::Lix<Memory>,
        request: &ReadFulfillmentRequest,
        lease_id: &str,
        address: NativeMetadataRef,
    ) -> Vec<u8> {
        let metadata_request = crate::sync::native_metadata::NativeMetadataRequest {
            epoch_id: request.epoch_id.clone(),
            objects: vec![address],
        };
        authority
            .read_sync_native_metadata_leased(&metadata_request, lease_id)
            .await
            .unwrap()
            .objects
            .into_iter()
            .next()
            .unwrap()
            .bytes
    }
    let next_graph_bytes = authority_graph_bytes(
        &authority,
        &request,
        &leased.lease.lease_id,
        next_head.clone(),
    )
    .await;
    let old_graph_bytes = authority_graph_bytes(
        &authority,
        &request,
        &leased.lease.lease_id,
        old_head.clone(),
    )
    .await;
    for (address, bytes) in [
        (next_head.clone(), next_graph_bytes.clone()),
        (old_head.clone(), old_graph_bytes.clone()),
    ] {
        if !response
            .inputs
            .iter()
            .any(|input| input.address == ReadInputAddress::Metadata(address.clone()))
        {
            response.inputs.push(ReadInput {
                address: ReadInputAddress::Metadata(address),
                bytes,
            });
        }
    }
    response.closure_digest = input_digest(&request, &response.inputs).unwrap();

    let durable = crate::sync::durable_memory_for_test(Memory::new());
    let installed = crate::migration::install_fresh_partial_epoch(durable, &previous)
        .await
        .unwrap();
    let storage = installed.adapter;

    // Preserve a valid locally selected graph overlay at the old head. The
    // candidate may use this metadata only as a dependency; it cannot replace
    // the retained local representation.
    let mut local_record: crate::changelog::CommitRecord =
        crate::storage_codec::decode("commit record", &old_graph_bytes).unwrap();
    local_record.account_id = "local-graph-overlay".to_owned();
    let local_graph_bytes = crate::storage_codec::encode("commit record", &local_record).unwrap();
    let old_commit = crate::changelog::CommitId::parse_lix(
        previous
            .descriptor()
            .selected_branch
            .head
            .commit_id
            .as_str(),
        "candidate old head",
    )
    .unwrap();
    let old_graph_key = StorageKey(Bytes::from(crate::changelog::commit_key(old_commit)));
    let mut overlay_writes = storage.new_write_set();
    overlay_writes.put(
        crate::changelog::COMMIT_SPACE,
        old_graph_key.clone(),
        local_graph_bytes.as_slice(),
    );
    let mut overlay_write = storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![StoragePrecondition::KeyAbsent {
                space: crate::changelog::COMMIT_SPACE,
                key: old_graph_key.clone(),
            }],
            ..Default::default()
        })
        .await
        .unwrap();
    overlay_writes.lower_into(&mut overlay_write).await.unwrap();
    overlay_write.commit().await.unwrap();

    let control_key =
        crate::branch::branch_head_control_key(&previous.descriptor().selected_branch.branch_id)
            .unwrap();
    let before = storage.begin_read(Default::default()).await.unwrap();
    let (before_state, before_receipt) =
        crate::sync::partial_state::load_partial_replica_state(&before)
            .await
            .unwrap()
            .unwrap();
    let before_control = PointReadPlan::new(
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
        &[StorageKey(Bytes::from(control_key.clone()))],
    )
    .materialize(&before, Default::default())
    .await
    .unwrap()
    .value
    .pop()
    .flatten();
    assert!(
        before_control.is_some(),
        "fixture must have an admitted serving control"
    );
    drop(before);

    install_candidate_immutable(&storage, &previous, &next, &request, &response)
        .await
        .unwrap();

    let after = storage.begin_read(Default::default()).await.unwrap();
    let (after_state, after_receipt) =
        crate::sync::partial_state::load_partial_replica_state(&after)
            .await
            .unwrap()
            .unwrap();
    assert_eq!(after_state, before_state);
    assert_eq!(
        after_receipt, before_receipt,
        "candidate prefetch must retain the admission receipt"
    );
    let after_control = PointReadPlan::new(
        crate::branch::BRANCH_HEAD_CONTROL_SPACE,
        &[StorageKey(Bytes::from(control_key))],
    )
    .materialize(&after, Default::default())
    .await
    .unwrap()
    .value
    .pop()
    .flatten();
    assert_eq!(
        after_control, before_control,
        "candidate prefetch must retain serving controls"
    );
    let graph_space = crate::sync::native_metadata::space(&next_head);
    let next_graph_key = crate::sync::native_metadata::key(&next_head).unwrap();
    let next_graph = PointReadPlan::new(graph_space, std::slice::from_ref(&next_graph_key))
        .materialize(&after, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten();
    assert!(
        matches!(
            next_graph,
            Some(StorageProjectedValue::FullValue(bytes)) if bytes.as_ref() == next_graph_bytes
        ),
        "absent candidate graph dependencies should be installed under their exact key"
    );
    let old_graph = PointReadPlan::new(
        crate::changelog::COMMIT_SPACE,
        std::slice::from_ref(&old_graph_key),
    )
    .materialize(&after, Default::default())
    .await
    .unwrap()
    .value
    .pop()
    .flatten();
    assert!(
        matches!(
            old_graph,
            Some(StorageProjectedValue::FullValue(bytes)) if bytes.as_ref() == local_graph_bytes
        ),
        "candidate prefetch must preserve a resident mutable graph overlay"
    );
    drop(after);
    authority.close().await.unwrap();
}
