use super::*;
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

pub(super) async fn fixture() -> (
    StorageAdapter<Memory>,
    PartialReplicaState,
    ReadFulfillmentRequest,
) {
    let authority = crate::open_lix().await.unwrap();
    let descriptor = authority.partial_replica_descriptor(None).await.unwrap();
    let state = PartialReplicaState::new(
        format!("https://example.test/lix/{}", descriptor.lix_id),
        authority.active_account_id().into(),
        uuid::Uuid::now_v7().to_string(),
        descriptor.clone(),
    )
    .unwrap();
    let request = ReadFulfillmentRequest {
        operation_id: uuid::Uuid::now_v7().to_string(),
        release: false,
        release_completed: false,
        operation_expires_at_ms: state.baseline_lease().expires_at_ms,
        epoch_id: state.epoch_id().into(),
        descriptor,
        interests: vec![],
        required: vec![],
        continuation: None,
    };
    let storage = StorageAdapter::new(Memory::new());
    let mut writes = storage.new_write_set();
    let guard =
        crate::sync::partial_state::stage_partial_replica_state(&mut writes, &state, None).unwrap();
    let mut raw = storage
        .begin_migration_write(StorageWriteOptions {
            preconditions: vec![guard],
            ..Default::default()
        })
        .await
        .unwrap();
    writes.lower_into(&mut raw).await.unwrap();
    raw.commit().await.unwrap();
    storage.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    authority.close().await.unwrap();
    (storage, state, request)
}

pub(super) async fn stage(
    storage: &StorageAdapter<Memory>,
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
) -> StagedClosure<Memory> {
    let (id, permit) = lifecycle::reserve(storage, state).await.unwrap();
    StagedClosure::new(
        storage,
        state,
        ReadFulfillmentResponse {
            frame: None,
            lix_id: request.descriptor.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            inputs: vec![],
            profile: Default::default(),
            closure_digest: input_digest(request, &[]).unwrap(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::Complete,
        },
        id,
        permit,
    )
}
fn chunk(index: u32, bytes: usize) -> ReadInput {
    let mut data = vec![0x5a; bytes];
    data[..4].copy_from_slice(&index.to_be_bytes());
    ReadInput {
        address: ReadInputAddress::BlobChunk(*blake3::hash(&data).as_bytes()),
        bytes: data,
    }
}
fn update_digest(digest: &mut blake3::Hasher, input: &ReadInput) {
    let address = serde_json::to_vec(&input.address).unwrap();
    digest.update(&(address.len() as u64).to_be_bytes());
    digest.update(&address);
    digest.update(&(input.bytes.len() as u64).to_be_bytes());
    digest.update(&input.bytes);
}

#[tokio::test]
async fn private_128_mib_closure_promotes_only_after_terminal_validation_and_reaps_scratch() {
    let (storage, state, request) = fixture().await;
    let mut stage = stage(&storage, &state, &request).await;
    let mut digest = blake3::Hasher::new();
    digest.update(request.digest().unwrap().as_bytes());
    let mut addresses = Vec::new();
    for index in 0..128 {
        let input = chunk(index, 1024 * 1024);
        addresses.push(input.address.coordinate().unwrap());
        update_digest(&mut digest, &input);
        stage.append_page(vec![input]).await.unwrap();
    }
    let read = storage.begin_read(Default::default()).await.unwrap();
    for (space, key) in &addresses {
        assert!(
            PointReadPlan::new(*space, std::slice::from_ref(key))
                .materialize(&read, Default::default())
                .await
                .unwrap()
                .value[0]
                .is_none(),
            "private transport pages must not warm canonical storage"
        );
    }
    drop(read);
    stage.header.closure_digest = digest.finalize().to_hex().to_string();
    let hydrated = stage.promote(&request, false).await.unwrap();
    assert!(
        addresses
            .iter()
            .all(|coordinate| hydrated.keys.contains(coordinate)),
        "promotion must receipt every canonical chunk coordinate"
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    for (space, key) in &addresses {
        assert!(
            PointReadPlan::new(*space, std::slice::from_ref(key))
                .materialize(&read, Default::default())
                .await
                .unwrap()
                .value[0]
                .is_some()
        );
    }
    let mut cursor = read
        .begin_scan(
            STAGING_SPACE,
            StorageKeyRange {
                lower: std::ops::Bound::Unbounded,
                upper: std::ops::Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (rows, more) = cursor.next_page(32).await.unwrap().into_parts();
    assert!(!more);
    assert_eq!(rows.len(), 1, "only the empty ownership ledger remains");
}

#[tokio::test]
async fn corrupt_private_page_never_publishes_a_canonical_member() {
    let (storage, state, request) = fixture().await;
    let mut stage = stage(&storage, &state, &request).await;
    let input = chunk(1, 1024);
    let coordinate = input.address.coordinate().unwrap();
    stage.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    stage.append_page(vec![input]).await.unwrap();
    let mut writes = storage.new_write_set();
    writes.put(
        STAGING_SPACE,
        stage.inputs[0].frames[0].clone(),
        b"corrupt".to_vec(),
    );
    storage
        .commit_partial_replica_write_set(
            crate::sync::partial_replica_write_capability(),
            writes,
            Default::default(),
        )
        .await
        .unwrap();
    assert!(stage.promote(&request, false).await.is_err());
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        PointReadPlan::new(coordinate.0, &[coordinate.1])
            .materialize(&read, Default::default())
            .await
            .unwrap()
            .value[0]
            .is_none()
    );
    drop(read);
    lifecycle::release(storage.clone(), stage.id).await.unwrap();
    stage.released = true;
}

#[tokio::test]
async fn repository_scratch_admission_is_bounded_and_release_restores_capacity() {
    let (storage, state, _) = fixture().await;
    let (first, first_permit) = lifecycle::reserve(&storage, &state).await.unwrap();
    let (second, second_permit) = lifecycle::reserve(&storage, &state).await.unwrap();
    assert!(lifecycle::reserve(&storage, &state).await.is_err());
    lifecycle::release(storage.clone(), first).await.unwrap();
    drop(first_permit);
    let (third, third_permit) = lifecycle::reserve(&storage, &state).await.unwrap();
    lifecycle::release(storage.clone(), second).await.unwrap();
    lifecycle::release(storage, third).await.unwrap();
    drop((second_permit, third_permit));
}

#[tokio::test]
async fn framed_large_typed_member_validates_before_atomic_payload_and_locator_promotion() {
    let (storage, state, mut request) = fixture().await;
    let change_id = crate::changelog::ChangeId::for_test_label("large-framed-change");
    let owner = crate::changelog::CommitId::for_test_label("large-framed-owner");
    let row_pk = crate::row_pk::RowPk::single("large-framed-row");
    let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(7);
    let snapshot =
        serde_json::json!({"key":"large-framed-row", "value":"x".repeat(8 * 1024 * 1024)});
    let typed =
        crate::row_payload::TypedRow::from_builtin_json("lix_key_value", &row_pk, &snapshot)
            .unwrap();
    let compressed = typed.durable_payload().unwrap();
    let payload = crate::row_payload::decompress_engine_row_payload(&compressed)
        .unwrap()
        .to_vec();
    let record = crate::changelog::ChangeRecord {
        format_version: 2,
        change_id,
        account_id: crate::ANONYMOUS_ACCOUNT_ID.into(),
        schema_key: "lix_key_value".into(),
        row_pk: row_pk.clone(),
        file_id: None,
        metadata: None,
        snapshot: Some(payload),
        created_at,
        origin_key: None,
    };
    let bytes = crate::changelog::encode_change_record(&record).unwrap();
    assert!(bytes.len() > 8 * 1024 * 1024);
    let input = ReadInput {
        address: ReadInputAddress::ChangeRecord {
            change_id: change_id.to_string(),
            source_commit_id: owner.to_string(),
            branch_id: request.descriptor.selected_branch.branch_id.clone(),
            schema_key: record.schema_key.clone(),
            file_id: None,
            row_pk: row_pk.clone(),
            updated_at: created_at.to_string(),
            payload_digest: *blake3::hash(&bytes).as_bytes(),
        },
        bytes,
    };
    request.interests = vec![LogicalReadInterest::Scan {
        request: crate::hot_state::HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                schema_keys: vec![record.schema_key.clone()],
                row_pks: vec![row_pk],
                branch_ids: vec![request.descriptor.selected_branch.branch_id.clone()],
                file_ids: vec![crate::NullableKeyFilter::Null],
                ..Default::default()
            },
            ..Default::default()
        },
        domain: InterestDomain::Combined,
    }];
    let locator = ReadInput {
        address: ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(
            change_id.to_string(),
        )),
        bytes: crate::tracked_state::encode_change_locator(
            crate::tracked_state::CommitDeltaChangeLocator {
                change_id,
                commit_id: owner,
                segment_index: 0,
                ordinal: 0,
            },
        ),
    };
    let mut stage = stage(&storage, &state, &request).await;
    let mut digest = blake3::Hasher::new();
    digest.update(request.digest().unwrap().as_bytes());
    update_digest(&mut digest, &input);
    update_digest(&mut digest, &locator);
    for (index, part) in input.bytes.chunks(FRAME_BYTES).enumerate() {
        stage
            .append_frame(ReadInputFrame {
                address: input.address.clone(),
                total_bytes: input.bytes.len(),
                offset: index * FRAME_BYTES,
                digest: *blake3::hash(&input.bytes).as_bytes(),
                bytes: part.to_vec(),
            })
            .await
            .unwrap();
    }
    stage.append_page(vec![locator.clone()]).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    for address in [&input.address, &locator.address] {
        let (space, key) = address.coordinate().unwrap();
        assert!(
            PointReadPlan::new(space, &[key])
                .materialize(&read, Default::default())
                .await
                .unwrap()
                .value[0]
                .is_none()
        );
    }
    drop(read);
    stage.header.closure_digest = digest.finalize().to_hex().to_string();
    stage.promote(&request, false).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    for expected in [&input, &locator] {
        let (space, key) = expected.address.coordinate().unwrap();
        let values = PointReadPlan::new(space, &[key])
            .materialize(&read, Default::default())
            .await
            .unwrap()
            .value;
        assert_eq!(
            values,
            vec![Some(StorageProjectedValue::FullValue(
                Bytes::copy_from_slice(&expected.bytes)
            ))]
        );
    }
}

struct ExpiringReadClient {
    lix_id: String,
    account: String,
    inputs: Vec<ReadInput>,
    repeated: bool,
    attempts: Arc<AtomicUsize>,
    releases: Arc<AtomicUsize>,
}
impl crate::sync::http::RawHttpClient for ExpiringReadClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        Box::pin(async move {
            use std::sync::atomic::Ordering;
            let (status, body) = if raw.method == http::Method::GET {
                (
                    200,
                    serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": self.lix_id, "sessionId": "scratch-test-session",
                        "activeAccountId": self.account,
                    }),
                )
            } else {
                assert!(raw.url.ends_with("/sync/read-fulfillment"));
                let request: ReadFulfillmentRequest =
                    serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
                let attempt = if !request.release && request.continuation.is_none() {
                    self.attempts.fetch_add(1, Ordering::SeqCst) + 1
                } else {
                    self.attempts.load(Ordering::SeqCst)
                };
                if !request.release
                    && request.continuation.is_some()
                    && (attempt == 1 || self.repeated)
                {
                    (
                        410,
                        serde_json::json!({"error":{"code":"LIX_READ_FULFILLMENT_RESTART","message":"sealed operation expired"}}),
                    )
                } else {
                    let inputs = if request.release {
                        self.releases.fetch_add(1, Ordering::SeqCst);
                        vec![]
                    } else if attempt == 1 || self.repeated {
                        vec![self.inputs[0].clone()]
                    } else {
                        self.inputs.clone()
                    };
                    let closure_digest =
                        input_digest(&request, if request.release { &[] } else { &self.inputs })
                            .unwrap();
                    let continuation = if !request.release && (attempt == 1 || self.repeated) {
                        Some(ReadContinuation {
                            next_input: 1,
                            next_offset: 0,
                            spool_id: uuid::Uuid::now_v7().to_string(),
                            closure_digest: closure_digest.clone(),
                        })
                    } else {
                        None
                    };
                    (
                        200,
                        serde_json::to_value(ReadFulfillmentResponse {
                            frame: None,
                            lix_id: self.lix_id.clone(),
                            epoch_id: request.epoch_id.clone(),
                            request_digest: request.digest().unwrap(),
                            inputs,
                            profile: Default::default(),
                            closure_digest,
                            continuation,
                            outcome: ReadFulfillmentOutcome::Complete,
                        })
                        .unwrap(),
                    )
                }
            };
            Ok(crate::sync::http::RawHttpResponse {
                status,
                status_text: "scratch fixture".into(),
                body: serde_json::to_vec(&body).unwrap(),
            })
        })
    }
}
#[derive(Clone)]
struct LifecycleReadClient {
    lix_id: String,
    account: String,
    inputs: Vec<ReadInput>,
    paginate: bool,
    malformed_first_once: Arc<AtomicBool>,
    active_spools: Arc<AtomicUsize>,
    peak_spools: Arc<AtomicUsize>,
    completed_releases: Arc<AtomicUsize>,
    cancelled_releases: Arc<AtomicUsize>,
}

impl LifecycleReadClient {
    fn response(
        &self,
        request: &ReadFulfillmentRequest,
        inputs: Vec<ReadInput>,
        continuation: Option<ReadContinuation>,
    ) -> crate::sync::http::RawHttpResponse {
        let closure_inputs = if request.release {
            &[][..]
        } else {
            &self.inputs
        };
        let response = ReadFulfillmentResponse {
            frame: None,
            lix_id: self.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            inputs,
            profile: Default::default(),
            closure_digest: input_digest(request, closure_inputs).unwrap(),
            continuation,
            outcome: ReadFulfillmentOutcome::Complete,
        };
        crate::sync::http::RawHttpResponse {
            status: 200,
            status_text: "lifecycle test fixture".into(),
            body: serde_json::to_vec(&response).unwrap(),
        }
    }

    fn note_spool(&self) {
        let current = self.active_spools.fetch_add(1, Ordering::SeqCst) + 1;
        self.peak_spools.fetch_max(current, Ordering::SeqCst);
    }

    fn retire_spool(&self) {
        let _ = self
            .active_spools
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
                Some(current.saturating_sub(1))
            });
    }
}

impl crate::sync::http::RawHttpClient for LifecycleReadClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        let client = self.clone();
        Box::pin(async move {
            if raw.method == http::Method::GET {
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "lifecycle test handshake".into(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": client.lix_id,
                        "sessionId": "lifecycle-test-session",
                        "activeAccountId": client.account,
                    }))
                    .unwrap(),
                });
            }
            let request: ReadFulfillmentRequest =
                serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
            if request.release {
                if request.release_completed {
                    assert!(request.continuation.is_some());
                    client.completed_releases.fetch_add(1, Ordering::SeqCst);
                } else {
                    client.cancelled_releases.fetch_add(1, Ordering::SeqCst);
                }
                client.retire_spool();
                return Ok(client.response(&request, Vec::new(), None));
            }
            if request.continuation.is_some() {
                return Ok(client.response(&request, vec![client.inputs[1].clone()], None));
            }
            if client.malformed_first_once.swap(false, Ordering::SeqCst) {
                client.note_spool();
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "simulated malformed first page".into(),
                    body: b"{".to_vec(),
                });
            }
            if client.paginate {
                client.note_spool();
                let closure_digest = input_digest(&request, &client.inputs).unwrap();
                return Ok(client.response(
                    &request,
                    vec![client.inputs[0].clone()],
                    Some(ReadContinuation {
                        next_input: 1,
                        next_offset: 0,
                        spool_id: uuid::Uuid::now_v7().to_string(),
                        closure_digest,
                    }),
                ));
            }
            Ok(client.response(&request, client.inputs.clone(), None))
        })
    }
}

fn lifecycle_client(
    request: &ReadFulfillmentRequest,
    inputs: Vec<ReadInput>,
    paginate: bool,
    malformed_first_once: bool,
) -> LifecycleReadClient {
    LifecycleReadClient {
        lix_id: request.descriptor.lix_id.clone(),
        account: crate::SYSTEM_ACCOUNT_ID.into(),
        inputs,
        paginate,
        malformed_first_once: Arc::new(AtomicBool::new(malformed_first_once)),
        active_spools: Arc::new(AtomicUsize::new(0)),
        peak_spools: Arc::new(AtomicUsize::new(0)),
        completed_releases: Arc::new(AtomicUsize::new(0)),
        cancelled_releases: Arc::new(AtomicUsize::new(0)),
    }
}

async fn lifecycle_transport(
    request: &ReadFulfillmentRequest,
    client: LifecycleReadClient,
) -> crate::sync::http::HttpSyncTransport<LifecycleReadClient> {
    let account = client.account.clone();
    let transport = crate::sync::http::HttpSyncTransport::connect_with(
        client,
        &format!("https://example.test/lix/{}", request.descriptor.lix_id),
    )
    .await
    .unwrap();
    let lease =
        crate::sync::LeasedPartialReplicaDescriptor::for_test(request.descriptor.clone(), &account);
    transport.bind_native_baseline_lease(&lease.lease).unwrap();
    transport
}

fn lifecycle_request(
    mut request: ReadFulfillmentRequest,
    inputs: &[ReadInput],
) -> ReadFulfillmentRequest {
    request.required = inputs.iter().map(|input| input.address.clone()).collect();
    request.interests = vec![LogicalReadInterest::Scan {
        request: Default::default(),
        domain: InterestDomain::Combined,
    }];
    request
}

#[tokio::test]
async fn successful_paginated_operations_complete_more_than_64_times() {
    let (storage, state, request) = fixture().await;
    let mut inputs = vec![chunk(200, 1024), chunk(201, 1024)];
    inputs.sort_by_key(|input| input.address.coordinate().unwrap());
    let request = lifecycle_request(request, &inputs);
    let client = lifecycle_client(&request, inputs, true, false);
    let completed_releases = client.completed_releases.clone();
    let peak_spools = client.peak_spools.clone();
    let transport = lifecycle_transport(&request, client).await;
    let started = std::time::Instant::now();
    for _ in 0..65 {
        let mut operation_request = request.clone();
        operation_request.operation_id = uuid::Uuid::now_v7().to_string();
        let mut stage = fetch_staged(&storage, &state, &transport, &operation_request)
            .await
            .unwrap();
        stage.promote(&operation_request, false).await.unwrap();
    }
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    assert_eq!(completed_releases.load(Ordering::SeqCst), 65);
    assert_eq!(peak_spools.load(Ordering::SeqCst), 1);
    println!(
        "READ_OPERATION_RETIREMENT_PROFILE_JSON={}",
        serde_json::json!({
            "completedPaginatedOperations": 65,
            "completedReleases": completed_releases.load(Ordering::SeqCst),
            "peakActiveSpools": peak_spools.load(Ordering::SeqCst),
            "elapsedMs": elapsed_ms,
        })
    );
}

#[tokio::test]
async fn malformed_first_response_releases_possible_remote_operation() {
    let (storage, state, request) = fixture().await;
    let mut inputs = vec![chunk(202, 1024), chunk(203, 1024)];
    inputs.sort_by_key(|input| input.address.coordinate().unwrap());
    let request = lifecycle_request(request, &inputs);
    let client = lifecycle_client(&request, inputs, false, true);
    let active_spools = client.active_spools.clone();
    let cancelled_releases = client.cancelled_releases.clone();
    let transport = lifecycle_transport(&request, client).await;

    let error = fetch_staged(&storage, &state, &transport, &request)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    assert_eq!(active_spools.load(Ordering::SeqCst), 0);
    assert_eq!(cancelled_releases.load(Ordering::SeqCst), 1);

    let mut healthy_request = request.clone();
    healthy_request.operation_id = uuid::Uuid::now_v7().to_string();
    let mut stage = fetch_staged(&storage, &state, &transport, &healthy_request)
        .await
        .unwrap();
    stage.promote(&healthy_request, false).await.unwrap();
}

async fn exercise_cursor_restart(repeated: bool) {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    let (storage, state, mut request) = fixture().await;
    let mut inputs = vec![chunk(90, 1024), chunk(91, 1024)];
    inputs.sort_by_key(|input| input.address.coordinate().unwrap());
    request.required = inputs.iter().map(|input| input.address.clone()).collect();
    request.interests = vec![LogicalReadInterest::Scan {
        request: Default::default(),
        domain: InterestDomain::Combined,
    }];
    let account = crate::SYSTEM_ACCOUNT_ID.to_owned();
    let attempts = Arc::new(AtomicUsize::new(0));
    let releases = Arc::new(AtomicUsize::new(0));
    let client = ExpiringReadClient {
        lix_id: request.descriptor.lix_id.clone(),
        account: account.clone(),
        inputs: inputs.clone(),
        repeated,
        attempts: attempts.clone(),
        releases: releases.clone(),
    };
    let transport = crate::sync::http::HttpSyncTransport::connect_with(
        client,
        &format!("https://example.test/lix/{}", request.descriptor.lix_id),
    )
    .await
    .unwrap();
    let lease =
        crate::sync::LeasedPartialReplicaDescriptor::for_test(request.descriptor.clone(), &account);
    transport.bind_native_baseline_lease(&lease.lease).unwrap();
    let result = fetch_staged(&storage, &state, &transport, &request).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(
        releases.load(Ordering::SeqCst),
        if repeated { 2 } else { 1 }
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    for input in &inputs {
        let (space, key) = input.address.coordinate().unwrap();
        assert!(
            read.get_many(&[StorageGetManyRequest {
                space,
                keys: &[key],
                opts: Default::default()
            }])
            .await
            .unwrap()
            .values[0]
                .is_none(),
            "no content is canonical before terminal validation and promotion"
        );
    }
    drop(read);
    if repeated {
        assert_eq!(result.err().unwrap().code, "LIX_READ_FULFILLMENT_RESTART");
    } else {
        result.unwrap().promote(&request, false).await.unwrap();
    }
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut cursor = read
        .begin_scan(
            STAGING_SPACE,
            StorageKeyRange {
                lower: std::ops::Bound::Unbounded,
                upper: std::ops::Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: StorageCoreProjection::KeyOnly,
                ..Default::default()
            },
        )
        .await
        .unwrap();
    let (rows, _) = cursor.next_page(32).await.unwrap().into_parts();
    assert!(
        rows.iter().all(|row| row.key.0.as_ref() == b"operations"),
        "attempt frames must be removed"
    );
}
#[tokio::test]
async fn lost_cursor_restarts_once_on_the_same_lease_and_reaps_previous_frames() {
    exercise_cursor_restart(false).await;
}
#[tokio::test]
async fn repeated_cursor_expiry_stops_after_two_attempts_and_reaps_all_frames() {
    exercise_cursor_restart(true).await;
}

#[tokio::test]
async fn cancelling_a_stage_cleans_private_frames_without_publishing_them() {
    let (storage, state, request) = fixture().await;
    let mut stage = stage(&storage, &state, &request).await;
    stage.append_page(vec![chunk(99, 1024)]).await.unwrap();
    let key = stage.inputs[0].frames[0].clone();
    let canonical = stage.inputs[0].address.coordinate().unwrap();
    drop(stage);
    for attempt in 0..100 {
        let read = storage.begin_read(Default::default()).await.unwrap();
        let values = read
            .get_many(&[StorageGetManyRequest {
                space: STAGING_SPACE,
                keys: std::slice::from_ref(&key),
                opts: Default::default(),
            }])
            .await
            .unwrap();
        if values.values[0].is_none() {
            break;
        }
        assert!(attempt < 99, "cancelled frame cleanup did not finish");
        drop(read);
        crate::sync::platform::sleep(std::time::Duration::from_millis(20)).await;
    }
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        read.get_many(&[StorageGetManyRequest {
            space: canonical.0,
            keys: &[canonical.1],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values[0]
            .is_none()
    );
}
