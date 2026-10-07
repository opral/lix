use super::*;
use std::sync::{
    Arc,
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::future::Future;

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

#[derive(Clone)]
struct CountingStorage {
    memory: Memory,
    canonical_commits: Arc<Mutex<Vec<Vec<(StorageSpace, StorageKey)>>>>,
}

struct CountingWrite<W> {
    inner: W,
    canonical_commits: Arc<Mutex<Vec<Vec<(StorageSpace, StorageKey)>>>>,
    canonical_write: bool,
    canonical_keys: Vec<(StorageSpace, StorageKey)>,
}

impl Storage for CountingStorage {
    type Read<'a> = <Memory as Storage>::Read<'a> where Self: 'a;
    type Write<'a> = CountingWrite<<Memory as Storage>::Write<'a>> where Self: 'a;

    fn acquire_session(
        &self,
    ) -> impl Future<
        Output = Result<StorageSessionToken, StorageError>,
    > + Send {
        self.memory.acquire_session()
    }

    fn acquire_partial_replica_owner(
        &self,
        session: StorageSessionToken,
    ) -> impl Future<
        Output = Result<StorageOwnerLease, StorageError>,
    > + Send {
        self.memory.acquire_partial_replica_owner(session)
    }

    fn begin_read(
        &self,
        opts: StorageReadOptions,
    ) -> impl Future<
        Output = Result<Self::Read<'_>, StorageError>,
    > + Send {
        self.memory.begin_read(opts)
    }

    fn begin_write(
        &self,
        opts: StorageWriteOptions,
    ) -> impl Future<
        Output = Result<Self::Write<'_>, StorageError>,
    > + Send {
        let write = self.memory.begin_write(opts);
        let canonical_commits = Arc::clone(&self.canonical_commits);
        async move {
            Ok(CountingWrite {
                inner: write.await?,
                canonical_commits,
                canonical_write: false,
                canonical_keys: Vec::new(),
            })
        }
    }
}

impl<W: StorageWrite> StorageWrite for CountingWrite<W> {
    fn put_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space != STAGING_SPACE {
            self.canonical_write = true;
            self.canonical_keys.extend(
                entries
                    .entries
                    .iter()
                    .map(|entry| (space, entry.key.clone())),
            );
        }
        self.inner.put_many(space, entries)
    }

    fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space != STAGING_SPACE {
            self.canonical_write = true;
            self.canonical_keys.extend(
                entries
                    .entries
                    .iter()
                    .map(|entry| (space, entry.key.clone())),
            );
        }
        self.inner.replace_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[StorageKey],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space != STAGING_SPACE {
            self.canonical_write = true;
            self.canonical_keys
                .extend(keys.iter().cloned().map(|key| (space, key)));
        }
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: StorageKeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space != STAGING_SPACE {
            self.canonical_write = true;
        }
        self.inner.delete_range(space, range)
    }

    fn commit(
        self,
    ) -> impl Future<
        Output = Result<StorageCommitResult, StorageError>,
    > + Send {
        async move {
            let result = self.inner.commit().await?;
            if self.canonical_write {
                self.canonical_commits
                    .lock()
                    .expect("test commit counter is not poisoned")
                    .push(self.canonical_keys);
            }
            Ok(result)
        }
    }

    fn rollback(self) -> impl Future<Output = Result<(), StorageError>> + Send {
        self.inner.rollback()
    }
}

async fn counting_fixture() -> (
    StorageAdapter<CountingStorage>,
    PartialReplicaState,
    ReadFulfillmentRequest,
    Arc<Mutex<Vec<Vec<(StorageSpace, StorageKey)>>>>,
) {
    let (storage, state, request) = fixture().await;
    let canonical_commits = Arc::new(Mutex::new(Vec::new()));
    let counted = StorageAdapter::new(CountingStorage {
        memory: storage.storage().clone(),
        canonical_commits: Arc::clone(&canonical_commits),
    });
    counted.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    (counted, state, request, canonical_commits)
}

pub(super) async fn stage<S>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
) -> StagedClosure<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
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

fn staged_ref(index: usize, len: usize) -> StagedInputRef {
    StagedInputRef {
        address: ReadInputAddress::BlobChunk(*blake3::hash(&(index as u64).to_be_bytes()).as_bytes()),
        len,
        received: len,
        digest: [0; 32],
        frames: Vec::new(),
    }
}

#[test]
fn promotion_packs_whole_owner_units_into_bounded_commit_batches() {
    let mut inputs = Vec::new();
    let mut units = Vec::new();
    for _ in 0..18 {
        let index = inputs.len();
        inputs.push(staged_ref(index, 256));
        units.push(vec![index]);
    }
    for _ in 0..9 {
        let mut unit = Vec::new();
        for _ in 0..3 {
            let index = inputs.len();
            inputs.push(staged_ref(index, 256));
            unit.push(index);
        }
        units.push(unit);
    }
    assert_eq!(units.len(), 27);
    assert_eq!(inputs.len(), 45);

    // Two ordinary codec pages model the captured closure's two large plugin
    // blob groups. They remain intact while the small owner units are packed
    // around the same 32-input and two-MiB promotion limits.
    let mut first_page = Vec::new();
    let index = inputs.len();
    inputs.push(staged_ref(index, 1_123_269));
    first_page.push(index);
    for _ in 0..22 {
        let index = inputs.len();
        inputs.push(staged_ref(index, 700));
        first_page.push(index);
    }
    units.push(first_page);
    let index = inputs.len();
    inputs.push(staged_ref(index, 1_302_845));
    units.push(vec![index]);

    assert_eq!(inputs.len(), 69);
    assert_eq!(units.len(), 29);
    let batches = pack_promotion_groups(units.clone(), &inputs).unwrap();
    assert_eq!(
        batches.len(),
        4,
        "one promotion commit per packed batch replaces 29 per-unit commits"
    );

    let mut seen = BTreeSet::new();
    for batch in &batches {
        assert!(batch.len() <= PROMOTION_ITEMS);
        let (encoded, decoded) = promotion_unit_weights(batch, &inputs).unwrap();
        assert!(encoded.saturating_add(2) <= PROMOTION_BYTES);
        assert!(decoded <= PROMOTION_BYTES);
        for &index in batch {
            assert!(seen.insert(index), "an input appears in two commits");
        }
    }
    assert_eq!(seen.len(), inputs.len());
    for unit in units {
        assert!(
            batches
                .iter()
                .any(|batch| unit.iter().all(|index| batch.contains(index))),
            "promotion split an indivisible owner or ordinary unit"
        );
    }
}

#[test]
fn promotion_rejects_an_oversized_unit_before_batching_any_unit() {
    let inputs = vec![staged_ref(1, 256), staged_ref(2, MAX_INPUT_BYTES + 1)];
    let units = vec![vec![0], vec![1]];
    let error = validate_promotion_unit_sizes(&units, &inputs).unwrap_err();
    assert_eq!(error.code, "LIX_READ_FULFILLMENT_INVALID");
    assert!(
        error.message.contains("codec allocation budget"),
        "the whole closure is refused before the earlier valid unit can be installed"
    );
}

#[tokio::test]
async fn promotion_preflights_oversized_owner_before_installing_earlier_valid_change() {
    let (storage, state, mut request, canonical_commits) = counting_fixture().await;
    let branch_id = request.descriptor.selected_branch.branch_id.clone();
    let row_pk = crate::row_pk::RowPk::single("preflight-row");
    let change_id = crate::changelog::ChangeId::for_test_label("preflight-change");
    let owner = crate::changelog::CommitId::for_test_label("preflight-change-owner");
    let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(1);
    let typed = crate::row_payload::TypedRow::from_builtin_json(
        "lix_key_value",
        &row_pk,
        &serde_json::json!({"key":"preflight-row", "value":1}),
    )
    .unwrap();
    let payload = typed.durable_payload().unwrap().to_vec();
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
    let record_bytes = crate::changelog::encode_change_record(&record).unwrap();
    let record_input = ReadInput {
        address: ReadInputAddress::ChangeRecord {
            change_id: record.change_id.to_string(),
            source_commit_id: owner.to_string(),
            branch_id: branch_id.clone(),
            schema_key: record.schema_key.clone(),
            file_id: None,
            row_pk: row_pk.clone(),
            updated_at: created_at.to_string(),
            payload_digest: *blake3::hash(&record_bytes).as_bytes(),
        },
        bytes: record_bytes,
    };
    let locator_input = ReadInput {
        address: ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(
            record.change_id.to_string(),
        )),
        bytes: crate::tracked_state::encode_change_locator(
            crate::tracked_state::CommitDeltaChangeLocator {
                change_id: record.change_id,
                commit_id: owner,
                segment_index: 0,
                ordinal: 0,
            },
        ),
    };
    let earlier_pair = (
        record_input.address.coordinate().unwrap(),
        locator_input.address.coordinate().unwrap(),
    );
    request.interests = vec![LogicalReadInterest::Scan {
        request: crate::hot_state::HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                schema_keys: vec!["lix_key_value".into()],
                row_pks: vec![row_pk],
                branch_ids: vec![branch_id],
                file_ids: vec![crate::NullableKeyFilter::Null],
                ..Default::default()
            },
            ..Default::default()
        },
        domain: InterestDomain::Combined,
    }];

    let mut stage = stage(&storage, &state, &request).await;
    stage
        .append_page(vec![record_input, locator_input])
        .await
        .unwrap();

    // Model two members of a single validated owner bundle. Each member is
    // individually within MAX_INPUT_BYTES, while the indivisible bundle is
    // over budget. The valid ChangeRecord+locator unit sorts before this owner
    // group and would otherwise be durably installed first.
    let oversized_owner = uuid::Uuid::from_u128(2).into_bytes();
    let large_members = [
        ReadInputAddress::Object(NativeObjectRef::MutationCatalog {
            commit_id: oversized_owner,
            expected_digest: [2; 32],
        }),
        ReadInputAddress::Object(NativeObjectRef::CommitDeltaPart {
            commit_id: oversized_owner,
            part_index: 0,
            expected_digest: [3; 32],
            replacement: false,
        }),
    ];
    stage.inputs.extend(large_members.into_iter().map(|address| StagedInputRef {
        address,
        len: 33 * 1024 * 1024,
        received: 0,
        digest: [0; 32],
        frames: Vec::new(),
    }));
    stage.validated = true;

    let error = stage.promote(&request, false).await.unwrap_err();
    assert_eq!(error.code, "LIX_READ_FULFILLMENT_INVALID");
    assert!(error.message.contains("codec allocation budget"));
    assert_eq!(
        canonical_commits
            .lock()
            .unwrap()
            .iter()
            .filter(|commit| {
                commit.contains(&earlier_pair.0) || commit.contains(&earlier_pair.1)
            })
            .count(),
        0,
        "oversized later unit must be refused before the earlier valid pair is published"
    );
}

#[tokio::test]
async fn promotion_co_packs_valid_change_locator_units_in_two_canonical_commits() {
    let (storage, state, mut request, canonical_commits) = counting_fixture().await;
    let branch_id = request.descriptor.selected_branch.branch_id.clone();
    let mut inputs = Vec::new();
    let mut expected_pairs = Vec::new();
    let mut row_pks = Vec::new();
    for index in 0..27 {
        let row_key = format!("batched-row-{index:02}");
        let row_pk = crate::row_pk::RowPk::single(row_key.clone());
        row_pks.push(row_pk.clone());
        let change_id = crate::changelog::ChangeId::for_test_label(&format!(
            "batched-change-{index:02}"
        ));
        let owner = crate::changelog::CommitId::for_test_label(&format!(
            "batched-owner-{index:02}"
        ));
        let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(
            100 + index as i64,
        );
        let typed = crate::row_payload::TypedRow::from_builtin_json(
            "lix_key_value",
            &row_pk,
            &serde_json::json!({"key": row_key, "value": index}),
        )
        .unwrap();
        let payload = typed.durable_payload().unwrap().to_vec();
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
        let record_bytes = crate::changelog::encode_change_record(&record).unwrap();
        let record_input = ReadInput {
            address: ReadInputAddress::ChangeRecord {
                change_id: record.change_id.to_string(),
                source_commit_id: owner.to_string(),
                branch_id: branch_id.clone(),
                schema_key: record.schema_key.clone(),
                file_id: None,
                row_pk,
                updated_at: created_at.to_string(),
                payload_digest: *blake3::hash(&record_bytes).as_bytes(),
            },
            bytes: record_bytes,
        };
        let locator_input = ReadInput {
            address: ReadInputAddress::Metadata(NativeMetadataRef::ChangeLocator(
                record.change_id.to_string(),
            )),
            bytes: crate::tracked_state::encode_change_locator(
                crate::tracked_state::CommitDeltaChangeLocator {
                    change_id: record.change_id,
                    commit_id: owner,
                    segment_index: 0,
                    ordinal: 0,
                },
            ),
        };
        expected_pairs.push((
            record_input.address.coordinate().unwrap(),
            locator_input.address.coordinate().unwrap(),
        ));
        inputs.extend([record_input, locator_input]);
    }
    request.interests = vec![LogicalReadInterest::Scan {
        request: crate::hot_state::HotStateScanRequest {
            filter: crate::hot_state::HotStateFilter {
                schema_keys: vec!["lix_key_value".into()],
                row_pks,
                branch_ids: vec![branch_id],
                file_ids: vec![crate::NullableKeyFilter::Null],
                ..Default::default()
            },
            ..Default::default()
        },
        domain: InterestDomain::Combined,
    }];

    let mut stage = stage(&storage, &state, &request).await;
    stage.header.closure_digest = input_digest(&request, &inputs).unwrap();
    stage.append_page(inputs.clone()).await.unwrap();
    let commits_before = canonical_commits.lock().unwrap().len();
    let hydrated = stage.promote(&request, false).await.unwrap();
    let commits = canonical_commits.lock().unwrap();
    let promoted_commits = commits[commits_before..]
        .iter()
        .filter(|commit| {
            expected_pairs
                .iter()
                .any(|(record, locator)| commit.contains(record) || commit.contains(locator))
        })
        .collect::<Vec<_>>();
    assert_eq!(promoted_commits.len(), 2);
    for (record, locator) in &expected_pairs {
        assert!(promoted_commits.iter().any(|commit| {
            commit.contains(record) && commit.contains(locator)
        }), "selected payload and locator must publish atomically");
    }
    for input in &inputs {
        assert!(hydrated.keys.contains(&input.address.coordinate().unwrap()));
    }
}

#[tokio::test]
async fn promotion_rejects_changed_admission_before_installing_inputs() {
    let (storage, state, request) = fixture().await;
    let mut stage = stage(&storage, &state, &request).await;
    let input = chunk(240, 1024);
    let coordinate = input.address.coordinate().unwrap();
    stage.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    stage.append_page(vec![input]).await.unwrap();

    let mut descriptor = state.descriptor().clone();
    descriptor.cursor = descriptor.cursor.saturating_add(1);
    let changed = state
        .with_descriptor_and_fresh_generations(descriptor)
        .unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let (_, previous) = super::super::super::partial_state::load_partial_replica_state(&read)
        .await
        .unwrap()
        .unwrap();
    drop(read);
    let mut writes = storage.new_write_set();
    let precondition = super::super::super::partial_state::stage_partial_replica_state(
        &mut writes,
        &changed,
        Some(previous),
    )
    .unwrap();
    storage
        .commit_migration_write_set(
            writes,
            StorageWriteOptions {
                preconditions: vec![precondition],
                await_durable: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

    let error = stage.promote(&request, false).await.unwrap_err();
    assert_eq!(
        error.code,
        crate::sync::runtime::PARTIAL_ADMISSION_CHANGED_CODE
    );
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        PointReadPlan::new(coordinate.0, std::slice::from_ref(&coordinate.1))
            .materialize(&read, Default::default())
            .await
            .unwrap()
            .value
            .pop()
            .flatten()
            .is_none(),
        "a stale admission cannot publish any batch"
    );
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
    let (storage, state, mut request, canonical_commits) = counting_fixture().await;
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
    let commits_before = canonical_commits.lock().unwrap().len();
    stage.promote(&request, false).await.unwrap();
    let record_coordinate = input.address.coordinate().unwrap();
    let locator_coordinate = locator.address.coordinate().unwrap();
    {
        let commits = canonical_commits.lock().unwrap();
        let promoted_commits = commits[commits_before..]
            .iter()
            .filter(|commit| {
                commit.contains(&record_coordinate) || commit.contains(&locator_coordinate)
            })
            .collect::<Vec<_>>();
        assert_eq!(
            promoted_commits.len(),
            1,
            "an oversized indivisible pair is published in one commit"
        );
        assert!(promoted_commits[0].contains(&record_coordinate));
        assert!(promoted_commits[0].contains(&locator_coordinate));
    }
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
    release_requests: Arc<AtomicUsize>,
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
                client.release_requests.fetch_add(1, Ordering::SeqCst);
                client.retire_spool();
                return Ok(client.response(&request, Vec::new(), None));
            }
            if request.continuation.is_some() {
                client.retire_spool();
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
        release_requests: Arc::new(AtomicUsize::new(0)),
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

#[derive(Clone)]
struct NestedFallbackReadClient {
    lix_id: String,
    account: String,
    fallback_outcome: ReadFulfillmentOutcome,
    locator: ReadInput,
    fallback_requests: Arc<AtomicUsize>,
    current_requests: Arc<AtomicUsize>,
}

impl NestedFallbackReadClient {
    fn response(
        &self,
        request: &ReadFulfillmentRequest,
        inputs: Vec<ReadInput>,
        outcome: ReadFulfillmentOutcome,
    ) -> crate::sync::http::RawHttpResponse {
        let response = ReadFulfillmentResponse {
            frame: None,
            lix_id: self.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(request, &inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation: None,
            outcome,
        };
        crate::sync::http::RawHttpResponse {
            status: 200,
            status_text: "nested-fallback test fixture".into(),
            body: serde_json::to_vec(&response).unwrap(),
        }
    }
}

impl crate::sync::http::RawHttpClient for NestedFallbackReadClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        let client = self.clone();
        Box::pin(async move {
            if raw.method == http::Method::GET {
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "nested-fallback test handshake".into(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": client.lix_id,
                        "sessionId": "nested-fallback-test-session",
                        "activeAccountId": client.account,
                    }))
                    .unwrap(),
                });
            }
            let request: ReadFulfillmentRequest =
                serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
            if request
                .interests
                .iter()
                .any(|interest| matches!(interest, LogicalReadInterest::History { .. }))
            {
                client.fallback_requests.fetch_add(1, Ordering::SeqCst);
                return Ok(client.response(&request, Vec::new(), client.fallback_outcome));
            }
            client.current_requests.fetch_add(1, Ordering::SeqCst);
            let inputs = if request.required.contains(&client.locator.address) {
                vec![client.locator.clone()]
            } else {
                Vec::new()
            };
            Ok(client.response(&request, inputs, ReadFulfillmentOutcome::Complete))
        })
    }
}

fn nested_fallback_request(
    mut request: ReadFulfillmentRequest,
) -> (ReadFulfillmentRequest, NativeMetadataRef, ReadInput) {
    let branch = &request.descriptor.selected_branch;
    request.interests = vec![
        LogicalReadInterest::History {
            branch_id: branch.branch_id.clone(),
            commit_ids: vec![branch.head.commit_id.clone()],
            relation: "lix_file".into(),
            filter: crate::tracked_state::TrackedStateFilter {
                file_ids: vec![crate::NullableKeyFilter::Null],
                include_tombstones: true,
                ..Default::default()
            },
            retain_payloads: false,
            projected_columns: Vec::new(),
            limit: None,
        },
        LogicalReadInterest::FilesystemMetadata {
            directory: false,
            branch_ids: vec![branch.branch_id.clone()],
            file_ids: None,
            directory_ids: None,
            root_directory: false,
            path_predicate: crate::hot_state::FilePathInterest::All,
        },
    ];
    let change_id = crate::changelog::ChangeId::for_test_label("nested-fallback-change");
    let locator = NativeMetadataRef::ChangeLocator(change_id.to_string());
    request.required = vec![ReadInputAddress::Metadata(locator.clone())];
    let owner = crate::changelog::CommitId::parse_lix(
        &request.descriptor.selected_branch.head.commit_id,
        "nested-fallback owner",
    )
    .unwrap();
    let input = ReadInput {
        address: ReadInputAddress::Metadata(locator.clone()),
        bytes: crate::tracked_state::encode_change_locator(
            crate::tracked_state::CommitDeltaChangeLocator {
                change_id,
                commit_id: owner,
                segment_index: 0,
                ordinal: 0,
            },
        ),
    };
    (request, locator, input)
}

async fn durable_scratch_owner_count(storage: &StorageAdapter<Memory>) -> usize {
    let key = StorageKey(Bytes::from_static(b"operations"));
    let read = storage.begin_read(Default::default()).await.unwrap();
    let values = read
        .get_many(&[StorageGetManyRequest {
            space: STAGING_SPACE,
            keys: &[key],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values;
    let Some(StorageProjectedValue::FullValue(bytes)) = values[0].as_ref() else {
        return 0;
    };
    serde_json::from_slice::<serde_json::Value>(bytes)
        .unwrap()
        .as_object()
        .unwrap()
        .len()
}

#[tokio::test]
async fn staged_fallback_releases_both_quotas_before_nested_current_payload_fetch() {
    for fallback_outcome in [
        ReadFulfillmentOutcome::NativeFallback,
        ReadFulfillmentOutcome::OperationFallback,
    ] {
        let (storage, state, request) = fixture().await;
        let mut blocker = stage(&storage, &state, &request).await;
        let (request, locator, locator_input) = nested_fallback_request(request);
        let account = crate::SYSTEM_ACCOUNT_ID.to_owned();
        let fallback_requests = Arc::new(AtomicUsize::new(0));
        let current_requests = Arc::new(AtomicUsize::new(0));
        let client = NestedFallbackReadClient {
            lix_id: request.descriptor.lix_id.clone(),
            account: account.clone(),
            fallback_outcome,
            locator: locator_input.clone(),
            fallback_requests: fallback_requests.clone(),
            current_requests: current_requests.clone(),
        };
        let transport = crate::sync::http::HttpSyncTransport::connect_with(
            client,
            &format!("https://example.test/lix/{}", request.descriptor.lix_id),
        )
        .await
        .unwrap();
        let lease = crate::sync::LeasedPartialReplicaDescriptor::for_test(
            request.descriptor.clone(),
            &account,
        );
        transport.bind_native_baseline_lease(&lease.lease).unwrap();

        let fallback_started = std::time::Instant::now();
        let fallback = fetch_staged(&storage, &state, &transport, &request)
            .await
            .unwrap();
        let fallback_ms = fallback_started.elapsed().as_secs_f64() * 1000.0;
        let owners_before_child = durable_scratch_owner_count(&storage).await;
        println!(
            "STAGED_FALLBACK_PRE_CHILD_JSON={}",
            serde_json::json!({
                "outcome": format!("{fallback_outcome:?}"),
                "durableOwnerCount": owners_before_child,
                "fallbackPermitHeld": fallback.permit.is_some(),
                "expectedBlockerCount": 1,
            })
        );
        assert_eq!(fallback.outcome(), fallback_outcome);
        assert!(fallback.released);
        assert!(fallback.permit.is_none());
        assert_eq!(owners_before_child, 1, "only the blocker remains owned");
        assert_eq!(fallback_requests.load(Ordering::SeqCst), 1);

        let current_request =
            current_payload_request_after_native_fallback(&request, &locator).unwrap();
        let child_started = std::time::Instant::now();
        let mut child = fetch_staged(&storage, &state, &transport, &current_request)
            .await
            .expect("nested current-payload fetch should fit after fallback retirement");
        let child_ms = child_started.elapsed().as_secs_f64() * 1000.0;
        let promoted = child.promote(&current_request, false).await.unwrap();
        assert!(
            promoted
                .keys
                .contains(&locator_input.address.coordinate().unwrap())
        );
        assert_eq!(durable_scratch_owner_count(&storage).await, 1);

        let mut normal_request = current_request.clone();
        normal_request.operation_id = uuid::Uuid::now_v7().to_string();
        let normal_started = std::time::Instant::now();
        let mut normal = fetch_staged(&storage, &state, &transport, &normal_request)
            .await
            .unwrap();
        normal.promote(&normal_request, false).await.unwrap();
        let normal_ms = normal_started.elapsed().as_secs_f64() * 1000.0;
        assert_eq!(current_requests.load(Ordering::SeqCst), 2);
        assert_eq!(durable_scratch_owner_count(&storage).await, 1);

        drop(fallback);
        blocker.release_scratch().await.unwrap();
        drop(blocker);
        assert_eq!(durable_scratch_owner_count(&storage).await, 0);
        println!(
            "STAGED_FALLBACK_PROFILE_JSON={}",
            serde_json::json!({
                "outcome": format!("{fallback_outcome:?}"),
                "fallbackFetchAndRetirementMs": fallback_ms,
                "nestedCurrentPayloadFetchMs": child_ms,
                "normalCompleteFetchAndPromoteMs": normal_ms,
            })
        );
    }
}

#[derive(Clone, Copy)]
enum TerminalLossMode {
    SinglePage,
    PaginatedTerminalPage,
}

struct TerminalLossState {
    mode: TerminalLossMode,
    network_loss_injected: AtomicBool,
    completed_operation_ids: Mutex<BTreeSet<String>>,
    requests: Mutex<Vec<(String, bool, bool)>>,
    release_requests: AtomicUsize,
}

#[derive(Clone)]
struct TerminalLossReadClient {
    lix_id: String,
    account: String,
    inputs: Vec<ReadInput>,
    state: Arc<TerminalLossState>,
}

impl TerminalLossReadClient {
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
            status_text: "terminal-loss test fixture".into(),
            body: serde_json::to_vec(&response).unwrap(),
        }
    }

    fn restart() -> crate::sync::http::RawHttpResponse {
        crate::sync::http::RawHttpResponse {
			status: 410,
			status_text: "retired operation".into(),
			body: serde_json::to_vec(&serde_json::json!({
				"error": {"code":"LIX_READ_FULFILLMENT_RESTART", "message":"operation already retired"}
			})).unwrap(),
		}
    }
}

impl crate::sync::http::RawHttpClient for TerminalLossReadClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        let client = self.clone();
        Box::pin(async move {
            if raw.method == http::Method::GET {
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "terminal-loss test handshake".into(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": client.lix_id,
                        "sessionId": "terminal-loss-test-session",
                        "activeAccountId": client.account,
                    }))
                    .unwrap(),
                });
            }
            let request: ReadFulfillmentRequest =
                serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
            client.state.requests.lock().unwrap().push((
                request.operation_id.clone(),
                request.continuation.is_some(),
                request.release,
            ));
            if request.release {
                client.state.release_requests.fetch_add(1, Ordering::SeqCst);
                return Ok(client.response(&request, Vec::new(), None));
            }
            if client
                .state
                .completed_operation_ids
                .lock()
                .unwrap()
                .contains(&request.operation_id)
            {
                return Ok(Self::restart());
            }
            match client.state.mode {
                TerminalLossMode::SinglePage => {
                    if !client
                        .state
                        .network_loss_injected
                        .swap(true, Ordering::SeqCst)
                    {
                        // A one-page closure retains no server resource. Once the
                        // response is lost, retrying the same ID may re-run it.
                        return Err(LixError::new(
                            "LIX_TRANSPORT_NETWORK",
                            "injected lost one-page response",
                        ));
                    }
                    Ok(client.response(&request, client.inputs.clone(), None))
                }
                TerminalLossMode::PaginatedTerminalPage => {
                    if request.continuation.is_none() {
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
                    if !client
                        .state
                        .network_loss_injected
                        .swap(true, Ordering::SeqCst)
                    {
                        client
                            .state
                            .completed_operation_ids
                            .lock()
                            .unwrap()
                            .insert(request.operation_id.clone());
                        return Err(LixError::new(
                            "LIX_TRANSPORT_NETWORK",
                            "injected lost terminal page response",
                        ));
                    }
                    Ok(client.response(&request, vec![client.inputs[1].clone()], None))
                }
            }
        })
    }
}

async fn terminal_loss_transport(
    request: &ReadFulfillmentRequest,
    inputs: Vec<ReadInput>,
    mode: TerminalLossMode,
) -> (
    crate::sync::http::HttpSyncTransport<TerminalLossReadClient>,
    Arc<TerminalLossState>,
) {
    let account = crate::SYSTEM_ACCOUNT_ID.to_owned();
    let state = Arc::new(TerminalLossState {
        mode,
        network_loss_injected: AtomicBool::new(false),
        completed_operation_ids: Mutex::new(BTreeSet::new()),
        requests: Mutex::new(Vec::new()),
        release_requests: AtomicUsize::new(0),
    });
    let client = TerminalLossReadClient {
        lix_id: request.descriptor.lix_id.clone(),
        account: account.clone(),
        inputs,
        state: state.clone(),
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
    (transport, state)
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
    let release_requests = client.release_requests.clone();
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
    assert_eq!(release_requests.load(Ordering::SeqCst), 0);
    assert_eq!(peak_spools.load(Ordering::SeqCst), 1);
    println!(
        "READ_OPERATION_RETIREMENT_PROFILE_JSON={}",
        serde_json::json!({
            "completedPaginatedOperations": 65,
            "terminalReleaseRequests": release_requests.load(Ordering::SeqCst),
            "peakActiveSpools": peak_spools.load(Ordering::SeqCst),
            "elapsedMs": elapsed_ms,
        })
    );
}

#[tokio::test]
async fn lost_terminal_response_retries_once_then_restarts_with_a_fresh_id() {
    for mode in [
        TerminalLossMode::SinglePage,
        TerminalLossMode::PaginatedTerminalPage,
    ] {
        let (storage, state, request) = fixture().await;
        let mut inputs = match mode {
            TerminalLossMode::SinglePage => vec![chunk(210, 1024)],
            TerminalLossMode::PaginatedTerminalPage => {
                vec![chunk(211, 1024), chunk(212, 1024)]
            }
        };
        inputs.sort_by_key(|input| input.address.coordinate().unwrap());
        let request = lifecycle_request(request, &inputs);
        let expected_digest = input_digest(&request, &inputs).unwrap();
        let (transport, fault_state) =
            terminal_loss_transport(&request, inputs.clone(), mode).await;

        let mut stage = fetch_staged(&storage, &state, &transport, &request)
            .await
            .unwrap();
        assert_eq!(stage.header.closure_digest, expected_digest);
        let promoted = stage.promote(&request, false).await.unwrap();
        assert!(
            inputs
                .iter()
                .all(|input| { promoted.keys.contains(&input.address.coordinate().unwrap()) }),
            "every member of the complete validated closure is promoted"
        );
        let observed_requests = fault_state.requests.lock().unwrap().clone();
        assert_eq!(
            fault_state.release_requests.load(Ordering::SeqCst),
            usize::from(matches!(mode, TerminalLossMode::PaginatedTerminalPage))
        );
        match mode {
            TerminalLossMode::SinglePage => {
                assert_eq!(
                    observed_requests,
                    vec![
                        (request.operation_id.clone(), false, false),
                        (request.operation_id.clone(), false, false),
                    ],
                    "a resource-free one-page operation can recompute under the same ID"
                );
            }
            TerminalLossMode::PaginatedTerminalPage => {
                assert_eq!(observed_requests.len(), 6);
                assert_eq!(
                    observed_requests[0],
                    (request.operation_id.clone(), false, false)
                );
                assert_eq!(
                    observed_requests[1],
                    (request.operation_id.clone(), true, false)
                );
                assert_eq!(
                    observed_requests[2],
                    (request.operation_id.clone(), false, false)
                );
                assert_eq!(
                    observed_requests[3],
                    (request.operation_id.clone(), false, true)
                );
                let restarted_id = observed_requests[4].0.clone();
                assert_ne!(restarted_id, request.operation_id);
                assert_eq!(observed_requests[4], (restarted_id.clone(), false, false));
                assert_eq!(observed_requests[5], (restarted_id, true, false));
                assert_eq!(
                    fault_state.completed_operation_ids.lock().unwrap().len(),
                    1,
                    "the lost terminal response retires only the original operation"
                );
            }
        }
    }
}

#[tokio::test]
async fn malformed_first_response_releases_possible_remote_operation() {
    let (storage, state, request) = fixture().await;
    let mut inputs = vec![chunk(202, 1024), chunk(203, 1024)];
    inputs.sort_by_key(|input| input.address.coordinate().unwrap());
    let request = lifecycle_request(request, &inputs);
    let client = lifecycle_client(&request, inputs, false, true);
    let active_spools = client.active_spools.clone();
    let release_requests = client.release_requests.clone();
    let transport = lifecycle_transport(&request, client).await;

    let error = fetch_staged(&storage, &state, &transport, &request)
        .await
        .err()
        .unwrap();
    assert_eq!(error.code, LixError::CODE_INTERNAL_ERROR);
    assert_eq!(active_spools.load(Ordering::SeqCst), 0);
    assert_eq!(release_requests.load(Ordering::SeqCst), 1);

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
