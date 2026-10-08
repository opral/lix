use super::*;
use std::sync::{
    Arc,
    Mutex,
    OnceLock,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::future::Future;

static RETAINED_PAYLOAD_TEST_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

async fn retained_payload_test_guard() -> tokio::sync::MutexGuard<'static, ()> {
    RETAINED_PAYLOAD_TEST_LOCK
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

fn assert_retained_payload_slot_available() {
    let permit = super::super::super::transfer::RetainedPayloadPermit::try_acquire()
        .expect("retained payload slot is restored after the operation");
    drop(permit);
}

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
    commits: Arc<Mutex<Vec<CountedCommit>>>,
    conflict_on_joined_commit: Arc<Mutex<Option<(StorageSpace, StorageKey, Bytes)>>>,
    joined_commit_attempts: Arc<AtomicUsize>,
    commit_ack_gate: Arc<Mutex<Option<Arc<CommitAckGate>>>>,
    fail_scratch_payload_ack_after_commit: Arc<AtomicBool>,
    closed: Arc<AtomicBool>,
    close_when_ledger_empty: Arc<AtomicBool>,
}

struct CommitAckGate {
    committed: Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    acknowledge: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
}

impl CommitAckGate {
    fn new() -> (Arc<Self>, tokio::sync::oneshot::Receiver<()>, tokio::sync::oneshot::Sender<()>) {
        let (committed_sender, committed_receiver) = tokio::sync::oneshot::channel();
        let (acknowledge_sender, acknowledge_receiver) = tokio::sync::oneshot::channel();
        (
            Arc::new(Self {
                committed: Mutex::new(Some(committed_sender)),
                acknowledge: Mutex::new(Some(acknowledge_receiver)),
            }),
            committed_receiver,
            acknowledge_sender,
        )
    }
}

#[derive(Clone, Debug)]
struct CountedCommit {
    canonical_keys: Vec<(StorageSpace, StorageKey)>,
    scratch_keys: Vec<StorageKey>,
    await_durable: bool,
}

fn is_canonical_test_key(space: StorageSpace, key: &StorageKey) -> bool {
    space != STAGING_SPACE
        && !is_non_content_revision_key(space.id.0, key.0.as_ref())
}

struct CountingWrite<W> {
    inner: W,
    canonical_commits: Arc<Mutex<Vec<Vec<(StorageSpace, StorageKey)>>>>,
    commits: Arc<Mutex<Vec<CountedCommit>>>,
    conflict_on_joined_commit: Arc<Mutex<Option<(StorageSpace, StorageKey, Bytes)>>>,
    joined_commit_attempts: Arc<AtomicUsize>,
    commit_ack_gate: Arc<Mutex<Option<Arc<CommitAckGate>>>>,
    fail_scratch_payload_ack_after_commit: Arc<AtomicBool>,
    memory: Memory,
    closed: Arc<AtomicBool>,
    close_when_ledger_empty: Arc<AtomicBool>,
    await_durable: bool,
    canonical_write: bool,
    canonical_keys: Vec<(StorageSpace, StorageKey)>,
    scratch_keys: Vec<StorageKey>,
    scratch_ledger_write: bool,
    scratch_ledger_owner_count: Option<usize>,
}

fn note_scratch_ledger<W: StorageWrite>(write: &mut CountingWrite<W>, entries: &[PutEntry]) {
    for entry in entries {
        if entry.key.0.as_ref() != b"operations" {
            continue;
        }
        write.scratch_ledger_write = true;
        write.scratch_ledger_owner_count = serde_json::from_slice::<serde_json::Value>(
            entry.value.bytes.as_ref(),
        )
        .ok()
        .and_then(|ledger| ledger.as_object().map(serde_json::Map::len));
    }
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
        let memory = self.memory.clone();
        let closed = Arc::clone(&self.closed);
        async move {
            if closed.load(Ordering::SeqCst) {
                return Err(StorageError::Closed("staging close fixture".into()));
            }
            memory.begin_read(opts).await
        }
    }

    fn begin_write(
        &self,
        opts: StorageWriteOptions,
    ) -> impl Future<
        Output = Result<Self::Write<'_>, StorageError>,
    > + Send {
        let await_durable = opts.await_durable;
        let write = self.memory.begin_write(opts);
        let canonical_commits = Arc::clone(&self.canonical_commits);
        let commits = Arc::clone(&self.commits);
        let conflict_on_joined_commit = Arc::clone(&self.conflict_on_joined_commit);
        let joined_commit_attempts = Arc::clone(&self.joined_commit_attempts);
        let commit_ack_gate = Arc::clone(&self.commit_ack_gate);
        let fail_scratch_payload_ack_after_commit =
            Arc::clone(&self.fail_scratch_payload_ack_after_commit);
        let closed = Arc::clone(&self.closed);
        let close_when_ledger_empty = Arc::clone(&self.close_when_ledger_empty);
        let memory = self.memory.clone();
        async move {
            Ok(CountingWrite {
                inner: write.await?,
                canonical_commits,
                commits,
                conflict_on_joined_commit,
                joined_commit_attempts,
                commit_ack_gate,
                fail_scratch_payload_ack_after_commit,
                memory,
                closed,
                close_when_ledger_empty,
                await_durable,
                canonical_write: false,
                canonical_keys: Vec::new(),
                scratch_keys: Vec::new(),
                scratch_ledger_write: false,
                scratch_ledger_owner_count: None,
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
        if space == STAGING_SPACE {
            self.scratch_keys
                .extend(entries.entries.iter().map(|entry| entry.key.clone()));
            note_scratch_ledger(self, &entries.entries);
        } else {
            let canonical = entries
                .entries
                .iter()
                .filter(|entry| is_canonical_test_key(space, &entry.key))
                .map(|entry| (space, entry.key.clone()))
                .collect::<Vec<_>>();
            self.canonical_write |= !canonical.is_empty();
            self.canonical_keys.extend(canonical);
        }
        self.inner.put_many(space, entries)
    }

    fn replace_many(
        &mut self,
        space: StorageSpace,
        entries: PutBatch,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space == STAGING_SPACE {
            self.scratch_keys
                .extend(entries.entries.iter().map(|entry| entry.key.clone()));
            note_scratch_ledger(self, &entries.entries);
        } else {
            let canonical = entries
                .entries
                .iter()
                .filter(|entry| is_canonical_test_key(space, &entry.key))
                .map(|entry| (space, entry.key.clone()))
                .collect::<Vec<_>>();
            self.canonical_write |= !canonical.is_empty();
            self.canonical_keys.extend(canonical);
        }
        self.inner.replace_many(space, entries)
    }

    fn delete_many(
        &mut self,
        space: StorageSpace,
        keys: &[StorageKey],
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space == STAGING_SPACE {
            self.scratch_keys.extend(keys.iter().cloned());
            let ledger_deleted = keys.iter().any(|key| key.0.as_ref() == b"operations");
            self.scratch_ledger_write |= ledger_deleted;
            if ledger_deleted {
                self.scratch_ledger_owner_count = Some(0);
            }
        } else {
            let canonical = keys
                .iter()
                .filter(|key| is_canonical_test_key(space, key))
                .cloned()
                .map(|key| (space, key))
                .collect::<Vec<_>>();
            self.canonical_write |= !canonical.is_empty();
            self.canonical_keys.extend(canonical);
        }
        self.inner.delete_many(space, keys)
    }

    fn delete_range(
        &mut self,
        space: StorageSpace,
        range: StorageKeyRange,
    ) -> impl Future<Output = Result<(), StorageError>> + Send {
        if space == STAGING_SPACE {
            self.scratch_keys.push(StorageKey(Bytes::new()));
        } else {
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
            if self.canonical_write && self.scratch_ledger_write {
                self.joined_commit_attempts.fetch_add(1, Ordering::Relaxed);
            }
            if self.canonical_write && self.scratch_ledger_write {
                let conflict = {
                    self.conflict_on_joined_commit
                        .lock()
                        .expect("commit conflict hook is not poisoned")
                        .take()
                };
                if let Some((space, key, bytes)) = conflict {
                    let mut conflict = self
                        .memory
                        .begin_write(StorageWriteOptions::default())
                        .await?;
                    conflict
                        .put_many(
                            space,
                            PutBatch {
                                entries: vec![PutEntry {
                                    key,
                                    value: StorageValue { bytes },
                                }],
                            },
                        )
                        .await?;
                    conflict.commit().await?;
                }
            }
            let result = self.inner.commit().await?;
            if self
                .scratch_keys
                .iter()
                .any(|key| !key.0.is_empty() && key.0.as_ref() != b"operations")
                && self
                    .fail_scratch_payload_ack_after_commit
                    .swap(false, Ordering::SeqCst)
            {
                return Err(StorageError::Closed(
                    "injected ambiguous scratch payload acknowledgement".into(),
                ));
            }
            let gate = if self.scratch_ledger_write {
                self.commit_ack_gate
                    .lock()
                    .expect("commit acknowledgement gate is not poisoned")
                    .take()
            } else {
                None
            };
            if let Some(gate) = gate {
                if let Some(committed) = gate
                    .committed
                    .lock()
                    .expect("commit acknowledgement signal is not poisoned")
                    .take()
                {
                    let _ = committed.send(());
                }
                let acknowledge = gate
                    .acknowledge
                    .lock()
                    .expect("commit acknowledgement receiver is not poisoned")
                    .take();
                if let Some(acknowledge) = acknowledge {
                    let _ = acknowledge.await;
                }
            }
            if self.close_when_ledger_empty.load(Ordering::SeqCst)
                && self.scratch_ledger_owner_count == Some(0)
            {
                self.closed.store(true, Ordering::SeqCst);
            }
            if self.canonical_write {
                self.canonical_commits
                    .lock()
                    .expect("test commit counter is not poisoned")
                    .push(self.canonical_keys.clone());
            }
            self.commits
                .lock()
                .expect("test commit records are not poisoned")
                .push(CountedCommit {
                    canonical_keys: self.canonical_keys,
                    scratch_keys: self.scratch_keys,
                    await_durable: self.await_durable,
                });
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
    let commits = Arc::new(Mutex::new(Vec::new()));
    let conflict_on_joined_commit = Arc::new(Mutex::new(None));
    let joined_commit_attempts = Arc::new(AtomicUsize::new(0));
    let counted = StorageAdapter::new(CountingStorage {
        memory: storage.storage().clone(),
        canonical_commits: Arc::clone(&canonical_commits),
        commits,
        conflict_on_joined_commit,
        joined_commit_attempts,
        commit_ack_gate: Arc::new(Mutex::new(None)),
        fail_scratch_payload_ack_after_commit: Arc::new(AtomicBool::new(false)),
        closed: Arc::new(AtomicBool::new(false)),
        close_when_ledger_empty: Arc::new(AtomicBool::new(false)),
    });
    counted.admit_partial_replica_writer(crate::sync::partial_replica_write_capability());
    (counted, state, request, canonical_commits)
}

fn staged_payload_put_keys(storage: &StorageAdapter<CountingStorage>) -> Vec<StorageKey> {
    storage
        .storage()
        .commits
        .lock()
        .expect("test commit records are not poisoned")
        .iter()
        .flat_map(|commit| commit.scratch_keys.iter())
        .filter(|key| !key.0.is_empty() && key.0.as_ref() != b"operations")
        .cloned()
        .collect()
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
        None,
        None,
    )
}

async fn stage_with_retained_payload<S>(
    storage: &StorageAdapter<S>,
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
) -> StagedClosure<S>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (id, permit) = lifecycle::reserve(storage, state).await.unwrap();
    let retained = super::super::super::transfer::RetainedPayloadPermit::try_acquire()
        .expect("retained payload test owns the bounded slot");
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
        Some(retained),
        None,
    )
}

fn required_tree_chunk(request: &mut ReadFulfillmentRequest) -> ReadInput {
    let bytes = b"validated immutable read input".to_vec();
    let address = ReadInputAddress::Object(NativeObjectRef::TrackedStateTreeChunk(
        *blake3::hash(&bytes).as_bytes(),
    ));
    request.required = vec![address.clone()];
    request.interests = vec![LogicalReadInterest::CollectionGeneration {
        branch_id: request.descriptor.selected_branch.branch_id.clone(),
        schema_key: "lix_key_value".into(),
        file_id: None,
    }];
    ReadInput { address, bytes }
}

async fn owner_is_reaping<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    id: uuid::Uuid,
) -> bool {
    let key = StorageKey(Bytes::from_static(b"operations"));
    let read = storage.begin_read(Default::default()).await.unwrap();
    let bytes = PointReadPlan::new(STAGING_SPACE, std::slice::from_ref(&key))
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes),
            StorageProjectedValue::KeyOnly => None,
        })
        .expect("scratch ownership ledger is present");
    let ledger: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    let owner = id.to_string();
    ledger[owner.as_str()]["reaping"]
        .as_bool()
        .expect("scratch owner has a reaping state")
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
async fn final_install_claims_scratch_reaping_in_the_same_durable_commit() {
    let (storage, state, mut request, _) = counting_fixture().await;
    let input = required_tree_chunk(&mut request);
    let coordinate = input.address.coordinate().unwrap();
    let mut stage = stage(&storage, &state, &request).await;
    stage.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    stage.append_page(vec![input]).await.unwrap();

    stage.promote(&request, false).await.unwrap();

    let commits = storage.storage().commits.lock().unwrap().clone();
    let joined = commits
        .iter()
        .filter(|commit| {
            commit.canonical_keys.contains(&coordinate)
                && commit
                    .scratch_keys
                    .iter()
                    .any(|key| key.0.as_ref() == b"operations")
        })
        .collect::<Vec<_>>();
    assert_eq!(joined.len(), 1, "final install and reaping claim share one commit");
    assert!(joined[0].await_durable, "the joined commit remains strictly durable");
}

#[tokio::test]
async fn crash_after_joined_commit_keeps_install_and_resumes_scratch_cleanup() {
    let (storage, state, mut request, _) = counting_fixture().await;
    let input = required_tree_chunk(&mut request);
    let coordinate = input.address.coordinate().unwrap();
    let mut stage = stage(&storage, &state, &request).await;
    let owner = stage.id;
    stage.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    stage.append_page(vec![input]).await.unwrap();

    let inputs = stage.read_many(&[0]).await.unwrap();
    let mut response = stage.header.clone();
    response.inputs = inputs;
    let capability = ScratchOwnerFinalizeCapability { owner };
    install_inputs(
        &storage,
        &state,
        &request,
        &response,
        false,
        Some(&capability),
    )
    .await
    .unwrap();
    assert!(owner_is_reaping(&storage, owner).await);

    // Model process loss after the atomic canonical install/reaping commit.
    reap_expired(&storage).await.unwrap();

    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(PointReadPlan::new(coordinate.0, std::slice::from_ref(&coordinate.1))
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .is_some());
    let frame = &stage.inputs[0].frames[0];
    assert!(PointReadPlan::new(STAGING_SPACE, std::slice::from_ref(frame))
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .is_none());
    let ledger_key = StorageKey(Bytes::from_static(b"operations"));
    assert!(PointReadPlan::new(STAGING_SPACE, std::slice::from_ref(&ledger_key))
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .is_none());
}

#[tokio::test]
async fn empty_finalization_uses_one_guarded_owner_claim_commit() {
    let (storage, state, request, _) = counting_fixture().await;
    let mut stage = stage(&storage, &state, &request).await;
    // This directly exercises the empty-promotion branch; the transport
    // request validator normally rejects a request without required inputs.
    stage.validated = true;

    stage.promote(&request, false).await.unwrap();

    let commits = storage.storage().commits.lock().unwrap().clone();
    assert_eq!(commits.len(), 3, "reserve, final owner claim, and ledger removal");
    assert!(commits.iter().all(|commit| commit.await_durable));
    assert_eq!(
        commits
            .iter()
            .filter(|commit| {
                commit
                    .scratch_keys
                    .iter()
                    .any(|key| key.0.as_ref() == b"operations")
            })
            .count(),
        3,
        "empty promotion has no separate renewal or reaping-claim write"
    );
}

#[tokio::test]
async fn failed_last_install_never_claims_scratch_reaping() {
    let (storage, state, mut request, _) = counting_fixture().await;
    let input = required_tree_chunk(&mut request);
    let coordinate = input.address.coordinate().unwrap();
    let mut stage = stage(&storage, &state, &request).await;
    let owner = stage.id;
    stage.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    stage.append_page(vec![input]).await.unwrap();
    *storage
        .storage()
        .conflict_on_joined_commit
        .lock()
        .unwrap() = Some((
        coordinate.0,
        coordinate.1,
        Bytes::from_static(b"racing conflicting immutable value"),
    ));

    let error = stage.promote(&request, false).await.unwrap_err();

    assert_eq!(error.code, "LIX_READ_FULFILLMENT_INVALID");
    assert!(!owner_is_reaping(&storage, owner).await);
    assert_eq!(
        storage
            .storage()
            .joined_commit_attempts
            .load(Ordering::Relaxed),
        1,
        "the conflicting final install is rejected atomically"
    );
}

#[tokio::test]
async fn final_install_retries_a_concurrent_scratch_ledger_renewal() {
    let (storage, state, mut request, _) = counting_fixture().await;
    let input = required_tree_chunk(&mut request);
    let coordinate = input.address.coordinate().unwrap();
    let mut stage = stage(&storage, &state, &request).await;
    let owner = stage.id;
    stage.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    stage.append_page(vec![input]).await.unwrap();

    let ledger_key = StorageKey(Bytes::from_static(b"operations"));
    let read = storage.begin_read(Default::default()).await.unwrap();
    let ledger_bytes = PointReadPlan::new(STAGING_SPACE, std::slice::from_ref(&ledger_key))
        .materialize(&read, Default::default())
        .await
        .unwrap()
        .value
        .pop()
        .flatten()
        .and_then(|value| match value {
            StorageProjectedValue::FullValue(bytes) => Some(bytes),
            StorageProjectedValue::KeyOnly => None,
        })
        .unwrap();
    drop(read);
    let mut ledger: serde_json::Value = serde_json::from_slice(&ledger_bytes).unwrap();
    let expires = ledger[&owner.to_string()]["expires_at_ms"]
        .as_u64()
        .unwrap();
    ledger[&owner.to_string()]["expires_at_ms"] = serde_json::json!(expires + 1);
    *storage
        .storage()
        .conflict_on_joined_commit
        .lock()
        .unwrap() = Some((
        STAGING_SPACE,
        ledger_key,
        Bytes::from(serde_json::to_vec(&ledger).unwrap()),
    ));

    stage.promote(&request, false).await.unwrap();

    assert_eq!(
        storage
            .storage()
            .joined_commit_attempts
            .load(Ordering::Relaxed),
        2,
        "the raced ledger CAS is retried before final publication"
    );
    let commits = storage.storage().commits.lock().unwrap().clone();
    assert_eq!(
        commits
            .iter()
            .filter(|commit| {
                commit.canonical_keys.contains(&coordinate)
                    && commit
                        .scratch_keys
                        .iter()
                        .any(|key| key.0.as_ref() == b"operations")
            })
            .count(),
        1
    );
}

#[tokio::test]
async fn promotion_rejects_changed_admission_before_installing_inputs() {
    let (storage, state, request) = fixture().await;
    let mut stage = stage(&storage, &state, &request).await;
    let owner = stage.id;
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
    assert!(!owner_is_reaping(&storage, owner).await);
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
    assert!(rows.is_empty(), "completed ownership and its empty ledger are reaped");
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
    let _retained_guard = retained_payload_test_guard().await;
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
    let mut stage = stage_with_retained_payload(&storage, &state, &request).await;
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
    assert!(stage.retained_inputs.is_none());
    assert!(stage.retained_payload_permit.is_none());
    assert_retained_payload_slot_available();
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

#[derive(Clone)]
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
    corrupt_closure_digest: bool,
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
        let mut closure_digest = input_digest(request, closure_inputs).unwrap();
        if self.corrupt_closure_digest {
            let replacement = if closure_digest.starts_with('0') { '1' } else { '0' };
            closure_digest.replace_range(..1, &replacement.to_string());
        }
        let response = ReadFulfillmentResponse {
            frame: None,
            lix_id: self.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            inputs,
            profile: Default::default(),
            closure_digest,
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
        corrupt_closure_digest: false,
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

async fn durable_scratch_owner_count<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
) -> usize {
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
    let _retained_guard = retained_payload_test_guard().await;
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
        let fallback = fetch_staged_with_retained_payload_permit(
            &storage,
            &state,
            &transport,
            &request,
            lifecycle::Permit::acquire().unwrap(),
        )
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
        assert!(fallback.retained_inputs.is_none());
        assert!(fallback.retained_payload_permit.is_none());
        assert_retained_payload_slot_available();
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

#[tokio::test]
async fn retained_endpoint_closure_avoids_scratch_and_overflow_spills_exact_inputs() {
    let _retained_guard = retained_payload_test_guard().await;

    let (storage, state, request, _) = counting_fixture().await;
    let input = chunk(431, 64 * 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let client = lifecycle_client(&request, vec![input.clone()], false, false);
    let transport = lifecycle_transport(&request, client).await;
    let mut staged = fetch_staged_with_retained_payload_permit(
        &storage,
        &state,
        &transport,
        &request,
        lifecycle::Permit::acquire().unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(staged.retained_payload_bytes, input.bytes.len());
    assert_eq!(staged.retained_inputs.as_ref().unwrap().len(), 1);
    assert!(staged_payload_put_keys(&storage).is_empty());

    let coordinate = input.address.coordinate().unwrap();
    let hydrated = staged.promote(&request, false).await.unwrap();
    assert!(hydrated.keys.contains(&coordinate));
    assert!(staged.retained_inputs.is_none());
    assert!(staged.retained_payload_permit.is_none());
    assert!(staged_payload_put_keys(&storage).is_empty());
    assert_retained_payload_slot_available();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let ReadInputAddress::BlobChunk(hash) = input.address else {
        unreachable!();
    };
    assert_eq!(
        crate::binary_cas::load_verified_chunk(
            &read,
            crate::binary_cas::ChunkHash::from_bytes(hash),
        )
        .await
        .unwrap(),
        Some(input.bytes)
    );

    let (storage, state, request, _) = counting_fixture().await;
    let occupied = super::super::super::transfer::RetainedPayloadPermit::try_acquire()
        .expect("retained payload test owns the bounded slot");
    let input = chunk(438, 40 * 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let client = lifecycle_client(&request, vec![input.clone()], false, false);
    let transport = lifecycle_transport(&request, client).await;
    let mut staged = fetch_staged_with_permit(
        &storage,
        &state,
        &transport,
        &request,
        lifecycle::Permit::acquire().unwrap(),
    )
    .await
    .unwrap();
    assert!(staged.retained_inputs.is_none());
    assert!(staged.retained_payload_permit.is_none());
    assert_eq!(staged_payload_put_keys(&storage).len(), 1);
    let hydrated = staged.promote(&request, false).await.unwrap();
    let coordinate = input.address.coordinate().unwrap();
    assert!(hydrated.keys.contains(&coordinate));
    let read = storage.begin_read(Default::default()).await.unwrap();
    let ReadInputAddress::BlobChunk(hash) = input.address else {
        unreachable!();
    };
    assert_eq!(
        crate::binary_cas::load_verified_chunk(
            &read,
            crate::binary_cas::ChunkHash::from_bytes(hash),
        )
        .await
        .unwrap(),
        Some(input.bytes)
    );
    assert!(super::super::super::transfer::RetainedPayloadPermit::try_acquire().is_none());
    drop(occupied);
    assert_retained_payload_slot_available();

    let (storage, state, request, _) = counting_fixture().await;
    let input = chunk(434, 32 * 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let mut client = lifecycle_client(&request, vec![input.clone()], false, false);
    client.corrupt_closure_digest = true;
    let transport = lifecycle_transport(&request, client).await;
    let error = fetch_staged_with_retained_payload_permit(
        &storage,
        &state,
        &transport,
        &request,
        lifecycle::Permit::acquire().unwrap(),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.code, "LIX_READ_FULFILLMENT_INVALID");
    assert!(staged_payload_put_keys(&storage).is_empty());
    let read = storage.begin_read(Default::default()).await.unwrap();
    let coordinate = input.address.coordinate().unwrap();
    assert!(
        read.get_many(&[StorageGetManyRequest {
            space: coordinate.0,
            keys: &[coordinate.1],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values[0]
            .is_none(),
        "a bad full-closure digest cannot publish retained content"
    );
    assert_retained_payload_slot_available();

    let (storage, state, request, _) = counting_fixture().await;
    let inputs = vec![
        chunk(432, 5 * 1024 * 1024 / 2),
        chunk(433, 5 * 1024 * 1024 / 2),
    ];
    let request = lifecycle_request(request, &inputs);
    let client = lifecycle_client(&request, inputs.clone(), true, false);
    let transport = lifecycle_transport(&request, client).await;
    let mut staged = fetch_staged_with_retained_payload_permit(
        &storage,
        &state,
        &transport,
        &request,
        lifecycle::Permit::acquire().unwrap(),
    )
    .await
    .unwrap();
    assert!(staged.retained_inputs.is_none());
    assert!(staged.retained_payload_permit.is_none());
    assert_eq!(staged_payload_put_keys(&storage).len(), inputs.len());
    assert_eq!(
        staged
            .read_many(&[0, 1])
            .await
            .unwrap()
            .iter()
            .map(|input| input.bytes.as_slice())
            .collect::<Vec<_>>(),
        inputs
            .iter()
            .map(|input| input.bytes.as_slice())
            .collect::<Vec<_>>()
    );
    let hydrated = staged.promote(&request, false).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    for input in inputs {
        let coordinate = input.address.coordinate().unwrap();
        assert!(hydrated.keys.contains(&coordinate));
        let ReadInputAddress::BlobChunk(hash) = input.address else {
            unreachable!();
        };
        assert_eq!(
            crate::binary_cas::load_verified_chunk(
                &read,
                crate::binary_cas::ChunkHash::from_bytes(hash),
            )
            .await
            .unwrap(),
            Some(input.bytes)
        );
    }
    assert_retained_payload_slot_available();

    let (storage, state, request, _) = counting_fixture().await;
    storage
        .storage()
        .fail_scratch_payload_ack_after_commit
        .store(true, Ordering::SeqCst);
    let inputs = vec![
        chunk(436, 5 * 1024 * 1024 / 2),
        chunk(437, 5 * 1024 * 1024 / 2),
    ];
    let request = lifecycle_request(request, &inputs);
    let client = lifecycle_client(&request, inputs.clone(), true, false);
    let transport = lifecycle_transport(&request, client).await;
    assert!(
        fetch_staged_with_retained_payload_permit(
            &storage,
            &state,
            &transport,
            &request,
            lifecycle::Permit::acquire().unwrap(),
        )
        .await
        .is_err(),
        "an ambiguous durable spill aborts the fetch attempt"
    );
    assert!(
        !storage
            .storage()
            .fail_scratch_payload_ack_after_commit
            .load(Ordering::SeqCst),
        "the spill commit was applied before its acknowledgement failed"
    );
    assert_eq!(durable_scratch_owner_count(&storage).await, 0);
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut scratch = read
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
    let (scratch_rows, more) = scratch.next_page(32).await.unwrap().into_parts();
    assert!(!more);
    assert!(scratch_rows.iter().all(|row| row.key.0.as_ref() == b"operations"));
    for input in inputs {
        let coordinate = input.address.coordinate().unwrap();
        assert!(
            read.get_many(&[StorageGetManyRequest {
                space: coordinate.0,
                keys: &[coordinate.1],
                opts: Default::default(),
            }])
            .await
            .unwrap()
            .values[0]
                .is_none(),
            "a failed scratch spill never partially promotes canonical data"
        );
    }
    assert_retained_payload_slot_available();
}

#[tokio::test]
async fn dropping_retained_payload_attempt_releases_slot_without_publication() {
    let _retained_guard = retained_payload_test_guard().await;
    let (storage, state, request, _) = counting_fixture().await;
    let input = chunk(435, 48 * 1024);
    let coordinate = input.address.coordinate().unwrap();
    let mut staged = stage_with_retained_payload(&storage, &state, &request).await;
    staged.header.closure_digest = input_digest(&request, std::slice::from_ref(&input)).unwrap();
    staged.append_page(vec![input]).await.unwrap();
    assert!(staged_payload_put_keys(&storage).is_empty());
    assert!(staged.retained_inputs.is_some());

    drop(staged);
    assert_retained_payload_slot_available();
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        read.get_many(&[StorageGetManyRequest {
            space: coordinate.0,
            keys: &[coordinate.1],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values[0]
            .is_none(),
        "dropping an unvalidated retained closure cannot publish its bytes"
    );
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
    let _retained_guard = retained_payload_test_guard().await;
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

        let mut stage = fetch_staged_with_retained_payload_permit(
            &storage,
            &state,
            &transport,
            &request,
            lifecycle::Permit::acquire().unwrap(),
        )
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

struct HeldReadState {
    entered: AtomicUsize,
    entered_notify: tokio::sync::Notify,
    allow_response: AtomicBool,
    response_notify: tokio::sync::Notify,
    release_attempts: AtomicUsize,
    release_requests: AtomicUsize,
    fail_release_once: AtomicBool,
    release_has_tokio_runtime: AtomicBool,
    operation_ids: Mutex<Vec<(String, bool)>>,
    release_lease_ids: Mutex<Vec<Option<String>>>,
    close_requests: AtomicUsize,
    wire_events: Mutex<Vec<(String, Option<String>, Option<bool>)>>,
}

impl HeldReadState {
    fn new(allow_response: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicUsize::new(0),
            entered_notify: tokio::sync::Notify::new(),
            allow_response: AtomicBool::new(allow_response),
            response_notify: tokio::sync::Notify::new(),
            release_attempts: AtomicUsize::new(0),
            release_requests: AtomicUsize::new(0),
            fail_release_once: AtomicBool::new(false),
            release_has_tokio_runtime: AtomicBool::new(false),
            operation_ids: Mutex::new(Vec::new()),
            release_lease_ids: Mutex::new(Vec::new()),
            close_requests: AtomicUsize::new(0),
            wire_events: Mutex::new(Vec::new()),
        })
    }
}

#[derive(Clone)]
struct HeldReadClient {
    lix_id: String,
    account_id: String,
    inputs: Vec<ReadInput>,
    state: Arc<HeldReadState>,
}

impl HeldReadClient {
    fn response(
        &self,
        request: &ReadFulfillmentRequest,
    ) -> crate::sync::http::RawHttpResponse {
        let inputs = if request.release {
            Vec::new()
        } else {
            self.inputs.clone()
        };
        let response = ReadFulfillmentResponse {
            frame: None,
            lix_id: self.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(request, &inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::Complete,
        };
        crate::sync::http::RawHttpResponse {
            status: 200,
            status_text: "staged cancellation fixture".into(),
            body: serde_json::to_vec(&response).unwrap(),
        }
    }
}

impl crate::sync::http::RawHttpClient for HeldReadClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        let client = self.clone();
        Box::pin(async move {
            let session_id = raw
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("lix-session-id"))
                .map(|(_, value)| value.clone());
            if raw.method == http::Method::GET {
                client
                    .state
                    .wire_events
                    .lock()
                    .unwrap()
                    .push(("GET".into(), session_id, None));
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "staged cancellation handshake".into(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": client.lix_id,
                        "sessionId": "staged-cancellation-test-session",
                        "activeAccountId": client.account_id,
                    }))
                    .unwrap(),
                });
            }
            if raw.method == http::Method::DELETE {
                client
                    .state
                    .wire_events
                    .lock()
                    .unwrap()
                    .push(("DELETE".into(), session_id, None));
                client.state.close_requests.fetch_add(1, Ordering::SeqCst);
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 204,
                    status_text: "session closed".into(),
                    body: Vec::new(),
                });
            }
            let request: ReadFulfillmentRequest =
                serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
            client
                .state
                .wire_events
                .lock()
                .unwrap()
                .push((
                    "POST".into(),
                    session_id,
                    Some(request.release),
                ));
            let lease_id = raw
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("lix-native-baseline-lease"))
                .map(|(_, value)| value.clone());
            client
                .state
                .operation_ids
                .lock()
                .unwrap()
                .push((request.operation_id.clone(), request.release));
            if request.release {
                client
                    .state
                    .release_attempts
                    .fetch_add(1, Ordering::SeqCst);
                client
                    .state
                    .release_lease_ids
                    .lock()
                    .unwrap()
                    .push(lease_id);
                client
                    .state
                    .release_has_tokio_runtime
                    .store(tokio::runtime::Handle::try_current().is_ok(), Ordering::SeqCst);
                if client
                    .state
                    .fail_release_once
                    .swap(false, Ordering::SeqCst)
                {
                    return Err(LixError::new(
                        "LIX_TRANSPORT_NETWORK",
                        "simulated lost release request",
                    ));
                }
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
                client
                    .state
                    .release_requests
                    .fetch_add(1, Ordering::SeqCst);
                return Ok(client.response(&request));
            }
            client.state.entered.fetch_add(1, Ordering::SeqCst);
            client.state.entered_notify.notify_waiters();
            loop {
                let notified = client.state.response_notify.notified();
                if client.state.allow_response.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
            Ok(client.response(&request))
        })
    }
}

async fn held_read_transport(
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    inputs: Vec<ReadInput>,
    client_state: Arc<HeldReadState>,
) -> crate::sync::http::HttpSyncTransport<HeldReadClient> {
    let account_id = state.baseline_lease().account_id.clone();
    let client = HeldReadClient {
        lix_id: request.descriptor.lix_id.clone(),
        account_id,
        inputs,
        state: client_state,
    };
    let transport = crate::sync::http::HttpSyncTransport::connect_with(
        client,
        &format!("https://example.test/lix/{}", request.descriptor.lix_id),
    )
    .await
    .unwrap();
    transport
        .bind_native_baseline_lease(state.baseline_lease())
        .unwrap();
    transport
}

async fn wait_for_counter(counter: &AtomicUsize, expected: usize) {
    for attempt in 0..200 {
        if counter.load(Ordering::SeqCst) >= expected {
            return;
        }
        assert!(attempt < 199, "staged cancellation fixture did not progress");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

async fn wait_for_owner_count<S: Storage + Clone + Send + Sync + 'static>(
    storage: &StorageAdapter<S>,
    expected: usize,
) {
    for attempt in 0..200 {
        if durable_scratch_owner_count(storage).await == expected {
            return;
        }
        assert!(attempt < 199, "scratch owner cleanup did not finish");
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }
}

struct RetryReservationFailureState {
    read_requests: AtomicUsize,
    release_requests: AtomicUsize,
    release_acks: AtomicUsize,
    release_entered: AtomicUsize,
    release_entered_notify: tokio::sync::Notify,
    allow_release: AtomicBool,
    allow_release_notify: tokio::sync::Notify,
}

impl RetryReservationFailureState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            read_requests: AtomicUsize::new(0),
            release_requests: AtomicUsize::new(0),
            release_acks: AtomicUsize::new(0),
            release_entered: AtomicUsize::new(0),
            release_entered_notify: tokio::sync::Notify::new(),
            allow_release: AtomicBool::new(false),
            allow_release_notify: tokio::sync::Notify::new(),
        })
    }
}

#[derive(Clone)]
struct RetryReservationFailureClient {
    lix_id: String,
    account_id: String,
    state: Arc<RetryReservationFailureState>,
}

impl RetryReservationFailureClient {
    fn release_response(
        &self,
        request: &ReadFulfillmentRequest,
    ) -> crate::sync::http::RawHttpResponse {
        let response = ReadFulfillmentResponse {
            frame: None,
            lix_id: self.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(request, &[]).unwrap(),
            inputs: Vec::new(),
            profile: Default::default(),
            continuation: None,
            outcome: ReadFulfillmentOutcome::Complete,
        };
        crate::sync::http::RawHttpResponse {
            status: 200,
            status_text: "retry reservation release ack".into(),
            body: serde_json::to_vec(&response).unwrap(),
        }
    }
}

impl crate::sync::http::RawHttpClient for RetryReservationFailureClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        let client = self.clone();
        Box::pin(async move {
            if raw.method == http::Method::GET {
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "retry reservation handshake".into(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": client.lix_id,
                        "sessionId": "retry-reservation-session",
                        "activeAccountId": client.account_id,
                    }))
                    .unwrap(),
                });
            }
            let request: ReadFulfillmentRequest =
                serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
            if request.release {
                client.state.release_requests.fetch_add(1, Ordering::SeqCst);
                client.state.release_entered.fetch_add(1, Ordering::SeqCst);
                client.state.release_entered_notify.notify_waiters();
                loop {
                    let notified = client.state.allow_release_notify.notified();
                    if client.state.allow_release.load(Ordering::SeqCst) {
                        break;
                    }
                    notified.await;
                }
                client.state.release_acks.fetch_add(1, Ordering::SeqCst);
                return Ok(client.release_response(&request));
            }
            let attempt = client.state.read_requests.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                return Err(LixError::new(
                    "LIX_TRANSPORT_NETWORK",
                    "simulated ambiguous first-page response",
                ));
            }
            Err(LixError::new(
                "LIX_TEST_UNEXPECTED_RETRY_HTTP",
                "the retried reservation must fail before another HTTP request",
            ))
        })
    }
}

async fn retry_reservation_failure_transport(
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    client_state: Arc<RetryReservationFailureState>,
) -> crate::sync::http::HttpSyncTransport<RetryReservationFailureClient> {
    let client = RetryReservationFailureClient {
        lix_id: request.descriptor.lix_id.clone(),
        account_id: state.baseline_lease().account_id.clone(),
        state: client_state,
    };
    let transport = crate::sync::http::HttpSyncTransport::connect_with(
        client,
        &format!("https://example.test/lix/{}", request.descriptor.lix_id),
    )
    .await
    .unwrap();
    transport
        .bind_native_baseline_lease(state.baseline_lease())
        .unwrap();
    transport
}

#[tokio::test]
async fn canceled_fetch_keeps_reservation_owned_until_durable_commit_acknowledges() {
    let _retained_guard = retained_payload_test_guard().await;
    let (storage, state, request, _) = counting_fixture().await;
    let input = chunk(301, 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let baseline_lease_id = state.baseline_lease().lease_id.clone();
    let (gate, committed, acknowledge) = CommitAckGate::new();
    *storage
        .storage()
        .commit_ack_gate
        .lock()
        .expect("commit acknowledgement gate is not poisoned") = Some(gate);
    let client_state = HeldReadState::new(true);
    let transport = held_read_transport(
        &state,
        &request,
        vec![input.clone()],
        Arc::clone(&client_state),
    )
    .await;
    let task_storage = storage.clone();
    let task_state = state.clone();
    let task_transport = transport.clone();
    let task_request = request.clone();
    let owner_permit = lifecycle::Permit::acquire().unwrap();
    let task = tokio::spawn(async move {
        fetch_staged_with_retained_payload_permit(
            &task_storage,
            &task_state,
            &task_transport,
            &task_request,
            owner_permit,
        )
        .await
    });

    committed.await.unwrap();
    assert_eq!(durable_scratch_owner_count(&storage).await, 1);
    assert!(super::super::super::transfer::RetainedPayloadPermit::try_acquire().is_none());
    task.abort();
    let _ = task.await;
    assert_eq!(
        durable_scratch_owner_count(&storage).await,
        1,
        "cancellation cannot release an owner while its commit acknowledgement is unresolved"
    );
    assert_eq!(client_state.entered.load(Ordering::SeqCst), 0);

    acknowledge.send(()).unwrap();
    wait_for_owner_count(&storage, 0).await;
    assert_retained_payload_slot_available();
    assert_eq!(client_state.entered.load(Ordering::SeqCst), 0);
    assert_eq!(state.baseline_lease().lease_id, baseline_lease_id);
    let coordinate = input.address.coordinate().unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        read.get_many(&[StorageGetManyRequest {
            space: coordinate.0,
            keys: &[coordinate.1],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values[0]
            .is_none(),
        "cancelled staged content never crosses the private-to-canonical barrier"
    );
}

#[tokio::test]
async fn closed_storage_hands_acknowledged_owner_to_reopen_reaper() {
    let (storage, state, request, _) = counting_fixture().await;
    let mut staged = stage(&storage, &state, &request).await;
    let input = chunk(303, 1024);
    staged.append_page(vec![input]).await.unwrap();
    let scratch_key = staged.inputs[0].frames[0].clone();

    // The durable reservation commit has already returned. A closed adapter
    // cannot acknowledge local reaping, so cleanup retains the ledger record
    // for the next exclusive owner and releases its in-memory permit.
    storage
        .storage()
        .closed
        .store(true, Ordering::SeqCst);
    staged.release_scratch().await.unwrap();

    storage
        .storage()
        .closed
        .store(false, Ordering::SeqCst);
    assert_eq!(durable_scratch_owner_count(&storage).await, 1);
    reap_abandoned(&storage).await.unwrap();
    assert_eq!(durable_scratch_owner_count(&storage).await, 0);
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        read.get_many(&[StorageGetManyRequest {
            space: STAGING_SPACE,
            keys: &[scratch_key],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values[0]
        .is_none()
    );
    drop(read);
}

#[tokio::test]
async fn repeated_http_cancellation_releases_exact_lease_on_tokio_and_holds_quota() {
    let (storage, state, request) = fixture().await;
    let input = chunk(302, 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let client_state = HeldReadState::new(false);
    let transport = held_read_transport(
        &state,
        &request,
        vec![input.clone()],
        Arc::clone(&client_state),
    )
    .await;
    let baseline_lease_id = state.baseline_lease().lease_id.clone();

    let mut tasks = Vec::new();
    let mut operation_ids = Vec::new();
    for _ in 0..2 {
        let mut operation_request = request.clone();
        operation_request.operation_id = uuid::Uuid::now_v7().to_string();
        operation_ids.push(operation_request.operation_id.clone());
        let task_storage = storage.clone();
        let task_state = state.clone();
        let task_transport = transport.clone();
        tasks.push(tokio::spawn(async move {
            fetch_staged(
                &task_storage,
                &task_state,
                &task_transport,
                &operation_request,
            )
            .await
        }));
    }
    wait_for_counter(&client_state.entered, 2).await;
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
    assert_eq!(durable_scratch_owner_count(&storage).await, 2);

    let mut denied_request = request.clone();
    denied_request.operation_id = uuid::Uuid::now_v7().to_string();
    let denied = fetch_staged(&storage, &state, &transport, &denied_request)
        .await
        .err()
        .expect("the two owned operations retain the repository quota");
    assert_eq!(denied.code, "LIX_NATIVE_RECIPE_WORK_BOUND");
    assert_eq!(durable_scratch_owner_count(&storage).await, 2);

    client_state.allow_response.store(true, Ordering::SeqCst);
    client_state.response_notify.notify_waiters();
    wait_for_counter(&client_state.release_requests, 2).await;
    wait_for_owner_count(&storage, 0).await;
    assert!(client_state.release_has_tokio_runtime.load(Ordering::SeqCst));
    let released = client_state
        .operation_ids
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, is_release)| *is_release)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    assert_eq!(released.len(), 2);
    assert!(operation_ids.iter().all(|id| released.contains(id)));
    assert_eq!(
        client_state.release_lease_ids.lock().unwrap().as_slice(),
        &[Some(baseline_lease_id.clone()), Some(baseline_lease_id.clone())]
    );
    let coordinate = input.address.coordinate().unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    assert!(
        read.get_many(&[StorageGetManyRequest {
            space: coordinate.0,
            keys: &[coordinate.1],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values[0]
            .is_none(),
        "canceled payload remains outside canonical storage"
    );
    assert_eq!(state.baseline_lease().lease_id, baseline_lease_id);
}

#[tokio::test]
async fn canceled_fetch_retries_one_lost_remote_release_with_the_same_operation_id() {
    let (storage, state, request) = fixture().await;
    let input = chunk(304, 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let client_state = HeldReadState::new(false);
    client_state
        .fail_release_once
        .store(true, Ordering::SeqCst);
    let transport = held_read_transport(
        &state,
        &request,
        vec![input],
        Arc::clone(&client_state),
    )
    .await;

    let task_storage = storage.clone();
    let task_state = state.clone();
    let task_transport = transport.clone();
    let task_request = request.clone();
    let task = tokio::spawn(async move {
        fetch_staged(&task_storage, &task_state, &task_transport, &task_request).await
    });
    wait_for_counter(&client_state.entered, 1).await;
    task.abort();
    let _ = task.await;

    client_state.allow_response.store(true, Ordering::SeqCst);
    client_state.response_notify.notify_waiters();
    wait_for_counter(&client_state.release_requests, 1).await;
    wait_for_owner_count(&storage, 0).await;

    let release_ids = client_state
        .operation_ids
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, is_release)| *is_release)
        .map(|(id, _)| id.clone())
        .collect::<Vec<_>>();
    assert_eq!(client_state.release_attempts.load(Ordering::SeqCst), 2);
    assert_eq!(release_ids, vec![request.operation_id.clone(), request.operation_id]);
}

#[tokio::test]
async fn close_defers_session_delete_until_original_operation_and_scratch_are_released() {
    let (storage, state, request) = fixture().await;
    let input = chunk(305, 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let client_state = HeldReadState::new(false);
    let transport = held_read_transport(
        &state,
        &request,
        vec![input],
        Arc::clone(&client_state),
    )
    .await;

    let task_storage = storage.clone();
    let task_state = state.clone();
    let task_transport = transport.clone();
    let task_request = request.clone();
    let task = tokio::spawn(async move {
        fetch_staged(&task_storage, &task_state, &task_transport, &task_request).await
    });
    wait_for_counter(&client_state.entered, 1).await;
    task.abort();
    let _ = task.await;

    let canceled_close_transport = transport.clone();
    let canceled_close = tokio::spawn(async move { canceled_close_transport.close_session().await });
    tokio::task::yield_now().await;
    assert!(!canceled_close.is_finished());
    assert_eq!(client_state.close_requests.load(Ordering::SeqCst), 0);
    assert_eq!(durable_scratch_owner_count(&storage).await, 1);
    canceled_close.abort();
    let _ = canceled_close.await;

    let waiting_close_transport = transport.clone();
    let waiting_close = tokio::spawn(async move { waiting_close_transport.close_session().await });
    tokio::task::yield_now().await;
    assert!(!waiting_close.is_finished());

    client_state.allow_response.store(true, Ordering::SeqCst);
    client_state.response_notify.notify_waiters();
    wait_for_counter(&client_state.release_requests, 1).await;
    wait_for_owner_count(&storage, 0).await;
    waiting_close.await.unwrap().unwrap();
    wait_for_counter(&client_state.close_requests, 1).await;

    let events = client_state.wire_events.lock().unwrap().clone();
    assert_eq!(events.len(), 4);
    assert_eq!(events[0], ("GET".into(), None, None));
    assert_eq!(events[1].0, "POST");
    assert_eq!(events[1].1.as_deref(), Some("staged-cancellation-test-session"));
    assert_eq!(events[1].2, Some(false));
    assert_eq!(events[2].0, "POST");
    assert_eq!(events[2].1.as_deref(), Some("staged-cancellation-test-session"));
    assert_eq!(events[2].2, Some(true));
    assert_eq!(
        events[3],
        (
            "DELETE".into(),
            Some("staged-cancellation-test-session".into()),
            None,
        )
    );
    assert_eq!(
        events.iter().filter(|(method, _, _)| method == "GET").count(),
        1,
        "cleanup after close must not open or recover another auth session"
    );
}

struct ContinuationRecoveryState {
    events: Mutex<Vec<(String, Option<String>, Option<bool>, bool)>>,
    handshakes: AtomicUsize,
    continuation_entered: AtomicUsize,
    continuation_notify: tokio::sync::Notify,
    allow_continuation: AtomicBool,
    allow_continuation_notify: tokio::sync::Notify,
    release_requests: AtomicUsize,
    close_requests: AtomicUsize,
}

impl ContinuationRecoveryState {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            events: Mutex::new(Vec::new()),
            handshakes: AtomicUsize::new(0),
            continuation_entered: AtomicUsize::new(0),
            continuation_notify: tokio::sync::Notify::new(),
            allow_continuation: AtomicBool::new(false),
            allow_continuation_notify: tokio::sync::Notify::new(),
            release_requests: AtomicUsize::new(0),
            close_requests: AtomicUsize::new(0),
        })
    }
}

#[derive(Clone)]
struct ContinuationRecoveryClient {
    lix_id: String,
    account_id: String,
    inputs: Vec<ReadInput>,
    state: Arc<ContinuationRecoveryState>,
}

impl ContinuationRecoveryClient {
    fn response(
        &self,
        request: &ReadFulfillmentRequest,
        inputs: Vec<ReadInput>,
        continuation: Option<ReadContinuation>,
    ) -> crate::sync::http::RawHttpResponse {
        let closure_inputs = if request.release { &[][..] } else { &self.inputs };
        let response = ReadFulfillmentResponse {
            frame: None,
            lix_id: self.lix_id.clone(),
            epoch_id: request.epoch_id.clone(),
            request_digest: request.digest().unwrap(),
            closure_digest: input_digest(request, closure_inputs).unwrap(),
            inputs,
            profile: Default::default(),
            continuation,
            outcome: ReadFulfillmentOutcome::Complete,
        };
        crate::sync::http::RawHttpResponse {
            status: 200,
            status_text: "continuation recovery test fixture".into(),
            body: serde_json::to_vec(&response).unwrap(),
        }
    }

    fn session_gone() -> crate::sync::http::RawHttpResponse {
        crate::sync::http::RawHttpResponse {
            status: 410,
            status_text: "expired session".into(),
            body: serde_json::to_vec(&serde_json::json!({
                "error": {
                    "code": "LIX_ERROR_PROTOCOL_SESSION_GONE",
                    "message": "session expired before continuation execution"
                }
            }))
            .unwrap(),
        }
    }
}

impl crate::sync::http::RawHttpClient for ContinuationRecoveryClient {
    fn send(
        &self,
        raw: crate::sync::http::RawHttpRequest,
    ) -> crate::sync::SyncTransportFuture<'_, crate::sync::http::RawHttpResponse> {
        let client = self.clone();
        Box::pin(async move {
            let session_id = raw
                .headers
                .iter()
                .find(|(name, _)| name.eq_ignore_ascii_case("lix-session-id"))
                .map(|(_, value)| value.clone());
            if raw.method == http::Method::GET {
                let handshake = client.state.handshakes.fetch_add(1, Ordering::SeqCst);
                client.state.events.lock().unwrap().push((
                    "GET".into(),
                    None,
                    None,
                    false,
                ));
                let session_id = if handshake == 0 {
                    "expired-read-session"
                } else {
                    "replacement-read-session"
                };
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 200,
                    status_text: "continuation recovery handshake".into(),
                    body: serde_json::to_vec(&serde_json::json!({
                        "protocolVersion": crate::SERVER_PROTOCOL_VERSION,
                        "syncProtocolVersion": crate::sync::SYNC_PROTOCOL_VERSION,
                        "lixId": client.lix_id,
                        "sessionId": session_id,
                        "activeAccountId": client.account_id,
                    }))
                    .unwrap(),
                });
            }
            if raw.method == http::Method::DELETE {
                client.state.events.lock().unwrap().push((
                    "DELETE".into(),
                    session_id,
                    None,
                    false,
                ));
                client.state.close_requests.fetch_add(1, Ordering::SeqCst);
                return Ok(crate::sync::http::RawHttpResponse {
                    status: 204,
                    status_text: "replacement session closed".into(),
                    body: Vec::new(),
                });
            }

            let request: ReadFulfillmentRequest =
                serde_json::from_slice(raw.body.as_ref().unwrap()).unwrap();
            let is_continuation = request.continuation.is_some();
            client.state.events.lock().unwrap().push((
                "POST".into(),
                session_id.clone(),
                Some(request.release),
                is_continuation,
            ));
            if request.release {
                client.state.release_requests.fetch_add(1, Ordering::SeqCst);
                return Ok(client.response(&request, Vec::new(), None));
            }
            if is_continuation && session_id.as_deref() == Some("expired-read-session") {
                return Ok(Self::session_gone());
            }
            if !is_continuation {
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

            client
                .state
                .continuation_entered
                .fetch_add(1, Ordering::SeqCst);
            client.state.continuation_notify.notify_waiters();
            loop {
                let notified = client.state.allow_continuation_notify.notified();
                if client.state.allow_continuation.load(Ordering::SeqCst) {
                    break;
                }
                notified.await;
            }
            Ok(client.response(
                &request,
                vec![client.inputs[1].clone()],
                None,
            ))
        })
    }
}

async fn continuation_recovery_transport(
    state: &PartialReplicaState,
    request: &ReadFulfillmentRequest,
    inputs: Vec<ReadInput>,
    client_state: Arc<ContinuationRecoveryState>,
) -> crate::sync::http::HttpSyncTransport<ContinuationRecoveryClient> {
    let client = ContinuationRecoveryClient {
        lix_id: request.descriptor.lix_id.clone(),
        account_id: state.baseline_lease().account_id.clone(),
        inputs,
        state: client_state,
    };
    let transport = crate::sync::http::HttpSyncTransport::connect_with(
        client,
        &format!("https://example.test/lix/{}", request.descriptor.lix_id),
    )
    .await
    .unwrap();
    transport
        .bind_native_baseline_lease(state.baseline_lease())
        .unwrap();
    transport
}

#[tokio::test]
async fn continuation_session_recovery_releases_on_replacement_before_close() {
    let (storage, state, request) = fixture().await;
    let inputs = vec![chunk(306, 1024), chunk(307, 2048)];
    let request = lifecycle_request(request, &inputs);
    let client_state = ContinuationRecoveryState::new();
    let transport = continuation_recovery_transport(
        &state,
        &request,
        inputs,
        Arc::clone(&client_state),
    )
    .await;

    let task_storage = storage.clone();
    let task_state = state.clone();
    let task_transport = transport.clone();
    let task_request = request.clone();
    let task = tokio::spawn(async move {
        fetch_staged(&task_storage, &task_state, &task_transport, &task_request).await
    });
    wait_for_counter(&client_state.continuation_entered, 1).await;
    task.abort();
    let _ = task.await;

    let close_transport = transport.clone();
    let close = tokio::spawn(async move { close_transport.close_session().await });
    tokio::task::yield_now().await;
    assert!(!close.is_finished());
    assert_eq!(client_state.close_requests.load(Ordering::SeqCst), 0);
    assert_eq!(client_state.release_requests.load(Ordering::SeqCst), 0);

    client_state.allow_continuation.store(true, Ordering::SeqCst);
    client_state.allow_continuation_notify.notify_waiters();
    wait_for_counter(&client_state.release_requests, 1).await;
    wait_for_owner_count(&storage, 0).await;
    close.await.unwrap().unwrap();
    wait_for_counter(&client_state.close_requests, 1).await;

    let events = client_state.events.lock().unwrap().clone();
    assert_eq!(events.len(), 7);
    assert_eq!(events[0], ("GET".into(), None, None, false));
    assert_eq!(events[1], (
        "POST".into(),
        Some("expired-read-session".into()),
        Some(false),
        false,
    ));
    assert_eq!(events[2], (
        "POST".into(),
        Some("expired-read-session".into()),
        Some(false),
        true,
    ));
    assert_eq!(events[3], ("GET".into(), None, None, false));
    assert_eq!(events[4], (
        "POST".into(),
        Some("replacement-read-session".into()),
        Some(false),
        true,
    ));
    assert_eq!(events[5], (
        "POST".into(),
        Some("replacement-read-session".into()),
        Some(true),
        false,
    ));
    assert_eq!(events[6], (
        "DELETE".into(),
        Some("replacement-read-session".into()),
        None,
        false,
    ));
    assert_eq!(client_state.handshakes.load(Ordering::SeqCst), 2);
    assert_eq!(client_state.release_requests.load(Ordering::SeqCst), 1);
    assert_eq!(client_state.close_requests.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn retry_reservation_failure_keeps_process_permit_until_remote_release_ack() {
    let (storage, state, request, _) = counting_fixture().await;
    storage
        .storage()
        .close_when_ledger_empty
        .store(true, Ordering::SeqCst);
    let input = chunk(308, 1024);
    let request = lifecycle_request(request, std::slice::from_ref(&input));
    let client_state = RetryReservationFailureState::new();
    let transport = retry_reservation_failure_transport(
        &state,
        &request,
        Arc::clone(&client_state),
    )
    .await;
    let active_test_permits = Arc::new(AtomicUsize::new(0));
    let permit = lifecycle::Permit::acquire_observed(Arc::clone(&active_test_permits)).unwrap();

    let task_storage = storage.clone();
    let task_state = state.clone();
    let task_request = request.clone();
    let task = tokio::spawn(async move {
        fetch_staged_with_permit(
            &task_storage,
            &task_state,
            &transport,
            &task_request,
            permit,
        )
        .await
    });
    wait_for_counter(&client_state.release_entered, 1).await;
    assert_eq!(client_state.read_requests.load(Ordering::SeqCst), 1);
    assert_eq!(client_state.release_requests.load(Ordering::SeqCst), 1);
    assert_eq!(client_state.release_acks.load(Ordering::SeqCst), 0);
    assert!(
        storage.storage().closed.load(Ordering::SeqCst),
        "the retry reservation fails only after the first local owner was acknowledged"
    );
    assert_eq!(
        active_test_permits.load(Ordering::SeqCst),
        1,
        "the same-ID retry retains its process admission while remote cleanup is held"
    );

    client_state.allow_release.store(true, Ordering::SeqCst);
    client_state.allow_release_notify.notify_waiters();
    let result = task.await.unwrap();
    assert!(result.is_err());
    assert_eq!(client_state.release_acks.load(Ordering::SeqCst), 1);
    assert_eq!(
        active_test_permits.load(Ordering::SeqCst),
        0,
        "process admission is returned only after the release acknowledgment"
    );
}
