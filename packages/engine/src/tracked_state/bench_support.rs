use crate::changelog::{ChangeId, CommitId};
use crate::entity_pk::EntityPk;
use crate::json_store::{
    JsonRef, JsonSlotRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageReadOptions, StorageWriteOptions,
    StorageWriteSet,
};
use crate::tracked_state::{CommitStateManifest, TrackedStateCommitDeltaRef, TrackedStateDeltaRef};

fn stage_bench_commit_deltas(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
) -> Result<Vec<super::storage::CommitDeltaChangeLocator>, crate::LixError> {
    let staged = super::storage::stage_commit_deltas_for_commit_state(writes, deltas)?;
    let commit_id = deltas
        .first()
        .map(|delta| delta.delta.commit_id)
        .unwrap_or_default();
    let mutations = staged.mutation_inventory().clone();
    let current_state_catalog = Box::new(
        super::current_state_part::empty_current_state_catalog_root(None, commit_id)?,
    );
    super::storage::stage_commit_state_manifest(
        writes,
        &CommitStateManifest {
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            state_parent_commit_id: None,
            commit_change_id: ChangeId::for_test_label(&format!("{commit_id}:bench-commit")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
            mutations,
            current_state_catalog,
        },
    )?;
    Ok(staged.locators)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchCurrentStatePointMode {
    PersistentCatalog,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchCurrentStateSparseShape {
    UnrelatedScopes,
    TouchedScope,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchCurrentStatePointTarget {
    ColdUntouched,
    HotMutated,
}

#[expect(missing_debug_implementations)]
pub struct BenchCurrentStatePointFixture<StorageImpl: Storage> {
    storage: StorageAdapter<StorageImpl>,
    current_manifest: CommitStateManifest,
    encoded_key: bytes::Bytes,
    catalog_entry_count: usize,
    catalog_manifest_bytes: u64,
    catalog_staged_encoded_bytes: u64,
    directory_staged_encoded_bytes: u64,
    sparse_staged_puts: u64,
    sparse_written_bytes: u64,
    first_sparse_elapsed_nanos: u64,
    first_sparse_staged_puts: u64,
    first_sparse_written_bytes: u64,
}

pub async fn seed_current_state_point_fixture<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    replacement_rows: usize,
    unrelated_sparse_commits: usize,
    catalog_entry_count: usize,
    sparse_shape: BenchCurrentStateSparseShape,
    point_target: BenchCurrentStatePointTarget,
) -> BenchCurrentStatePointFixture<StorageImpl>
where
    StorageImpl: Storage,
{
    assert!(replacement_rows > 0);
    assert!(catalog_entry_count > 0);
    let _ = crate::storage_bench::take_crud_current_state_catalog_bytes();
    let _ = crate::storage_bench::take_crud_current_state_directory_bytes();
    let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(11);
    let updated_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(22);
    let parent_id = bench_addressable_commit_id("persistent-catalog-parent");
    let entity_pks = (0..replacement_rows)
        .map(|index| EntityPk::single(format!("entity-{index:09}")))
        .collect::<Vec<_>>();
    let parent_rows = entity_pks
        .iter()
        .enumerate()
        .map(|(index, entity_pk)| TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "bench_current_state_alpha",
                file_id: None,
                entity_pk,
                change_id: ChangeId::for_test_label(&format!("catalog-parent-{index}")),
                commit_id: parent_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        })
        .collect::<Vec<_>>();
    let scope = super::types::CommitDeltaReplacementScope {
        schema_key: "bench_current_state_alpha".to_string(),
        file_id: None,
    };
    let generation = super::storage::CommitDeltaReplacementGeneration {
        scope: scope.clone(),
        lifecycle_summary: super::storage::CommitDeltaLifecycleSummary {
            scope,
            ordered_identity_digest: [17; 32],
            uniform_created_at: created_at,
        },
    };
    let mut writes = storage.new_write_set();
    let parent_stage = super::storage::stage_ordered_addressable_replacement_parts(
        &mut writes,
        parent_rows.iter().copied().map(Ok),
        &generation,
    )
    .expect("stage benchmark replacement parts");
    let mut parent_mutations = parent_stage.mutation_inventory().clone();
    let parent_publication = super::storage::stage_current_state_catalog_from_published_parent(
        &storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open benchmark root read"),
        &mut writes,
        None,
        parent_id,
        &parent_mutations,
        &[],
        None,
    )
    .await
    .expect("certify benchmark parent catalog");
    let parent_catalog = parent_publication.root();
    parent_mutations.parts.clear();
    let mut current_manifest =
        bench_current_state_manifest(parent_id, None, parent_mutations, parent_catalog);
    super::storage::stage_certified_commit_state_manifest(
        &mut writes,
        &current_manifest,
        &parent_publication,
    )
    .expect("stage benchmark parent manifest");
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit benchmark parent");

    for index in 1..catalog_entry_count {
        let authority_commit_id = bench_addressable_commit_id(&format!("catalog-scope-{index}"));
        let schema_key = format!("bench_scope_{index:08}");
        let file_id = format!("file-{:05}", index % 10_000);
        let entity_pk = EntityPk::single("entity");
        let scope = super::types::CommitDeltaReplacementScope {
            schema_key: schema_key.clone(),
            file_id: Some(file_id.clone()),
        };
        let row = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &schema_key,
                file_id: Some(&file_id),
                entity_pk: &entity_pk,
                change_id: ChangeId::for_test_label(&format!("catalog-scope-change-{index}")),
                commit_id: authority_commit_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let generation = super::storage::CommitDeltaReplacementGeneration {
            scope: scope.clone(),
            lifecycle_summary: super::storage::CommitDeltaLifecycleSummary {
                scope,
                ordered_identity_digest: *blake3::hash(schema_key.as_bytes()).as_bytes(),
                uniform_created_at: created_at,
            },
        };
        let mut authority_writes = storage.new_write_set();
        let staged = super::storage::stage_ordered_addressable_replacement_parts(
            &mut authority_writes,
            std::iter::once(Ok(row)),
            &generation,
        )
        .expect("stage valid benchmark scope replacement");
        let mut mutations = staged.mutation_inventory().clone();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open benchmark scope parent read");
        let published_parent =
            super::storage::load_published_commit_state_manifest(&read, current_manifest.commit_id)
                .await
                .expect("load benchmark scope parent authority")
                .expect("benchmark scope parent authority exists");
        let publication = super::storage::stage_current_state_catalog_from_published_parent(
            &read,
            &mut authority_writes,
            Some(&published_parent),
            authority_commit_id,
            &mutations,
            &[],
            None,
        )
        .await
        .expect("certify benchmark scope catalog");
        let catalog = publication.root();
        mutations.parts.clear();
        let authority_manifest = bench_current_state_manifest(
            authority_commit_id,
            Some(current_manifest.commit_id),
            mutations,
            catalog,
        );
        super::storage::stage_certified_commit_state_manifest(
            &mut authority_writes,
            &authority_manifest,
            &publication,
        )
        .expect("stage benchmark scope authority");
        drop(read);
        storage
            .commit_write_set(authority_writes, StorageWriteOptions::default())
            .await
            .expect("commit benchmark scope authority");
        current_manifest = authority_manifest;
    }

    let mut catalog_manifest_bytes = 0u64;
    let mut sparse_staged_puts = 0u64;
    let mut sparse_written_bytes = 0u64;
    let mut first_sparse_elapsed_nanos = 0u64;
    let mut first_sparse_staged_puts = 0u64;
    let mut first_sparse_written_bytes = 0u64;
    let beta_pk = EntityPk::single("unrelated");
    for index in 0..unrelated_sparse_commits {
        let sparse_started = Instant::now();
        let commit_id = bench_addressable_commit_id(&format!("catalog-child-{index}"));
        let touches_alpha = sparse_shape == BenchCurrentStateSparseShape::TouchedScope;
        let present_scope = !touches_alpha && catalog_entry_count > 1 && index % 4 == 0;
        let schema_key = if touches_alpha {
            "bench_current_state_alpha".to_string()
        } else if present_scope {
            format!("bench_scope_{:08}", 1 + index % (catalog_entry_count - 1))
        } else {
            "bench_current_state_beta".to_string()
        };
        let file_id = present_scope.then(|| {
            format!(
                "file-{:05}",
                (1 + index % (catalog_entry_count - 1)) % 10_000
            )
        });
        let sparse_pk = if touches_alpha {
            &entity_pks[replacement_rows / 4]
        } else {
            &beta_pk
        };
        let row = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
                entity_pk: sparse_pk,
                change_id: ChangeId::for_test_label(&format!("catalog-child-change-{index}")),
                commit_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{}"),
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let mut writes = storage.new_write_set();
        let stage = super::storage::stage_ordered_addressable_commit_deltas(
            &mut writes,
            std::iter::once(Ok(row)),
            true,
            false,
        )
        .expect("stage benchmark sparse mutation")
        .expect("benchmark sparse mutation is addressable");
        let mut mutations = stage.mutation_inventory().clone();
        let encoded_key = super::codec::encode_key_ref(super::types::TrackedStateKeyRef {
            schema_key: &schema_key,
            file_id: file_id.as_deref(),
            entity_pk: sparse_pk,
        });
        let value = super::types::TrackedStateIndexValueRef {
            change_id: row.delta.change_id,
            commit_id,
            deleted: false,
            created_at,
            updated_at,
        };
        let (encoded_state, _) =
            super::current_state_data_part::encode_authoritative_arrow_state_rows(
                &super::types::CommitDeltaReplacementScope {
                    schema_key: schema_key.clone(),
                    file_id: file_id.clone(),
                },
                &[super::current_state_data_part::ArrowStateInputRowRef {
                    encoded_key: &encoded_key,
                    value,
                    snapshot: JsonSlotRef::Inline("{}"),
                    metadata: JsonSlotRef::None,
                }],
            )
            .expect("encode benchmark sparse Arrow mutation");
        let mut arrow_mutations = crate::live_state::EntityColumnarWriteSets::new();
        arrow_mutations.insert_scope(
            commit_id,
            super::types::CommitDeltaReplacementScope {
                schema_key: schema_key.clone(),
                file_id: file_id.clone(),
            },
            encoded_state,
        );
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open benchmark parent read");
        let published_parent =
            super::storage::load_published_commit_state_manifest(&read, current_manifest.commit_id)
                .await
                .expect("load benchmark sparse parent authority")
                .expect("benchmark sparse parent authority exists");
        let planned_members = super::storage::staged_commit_delta_members_for_write(
            &read,
            &mut writes,
            commit_id,
            &mutations,
        )
        .await
        .expect("load benchmark sparse Arrow event members");
        let publication = super::storage::stage_current_state_catalog_from_published_parent(
            &read,
            &mut writes,
            Some(&published_parent),
            commit_id,
            &mut mutations,
            &planned_members,
            Some(&arrow_mutations),
        )
        .await
        .expect("reuse benchmark persistent catalog");
        let catalog = publication.root();
        drop(read);
        current_manifest = bench_current_state_manifest(
            commit_id,
            Some(current_manifest.commit_id),
            mutations,
            catalog,
        );
        catalog_manifest_bytes += u64::try_from(bench_manifest_encoded_len(&current_manifest))
            .expect("benchmark manifest length fits u64");
        super::storage::stage_certified_commit_state_manifest(
            &mut writes,
            &current_manifest,
            &publication,
        )
        .expect("stage benchmark sparse manifest");
        let (_, stats) = storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit benchmark sparse child");
        sparse_staged_puts += stats.staged_puts;
        sparse_written_bytes += stats.written_bytes;
        if index == 0 {
            first_sparse_elapsed_nanos =
                u64::try_from(sparse_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            first_sparse_staged_puts = stats.staged_puts;
            first_sparse_written_bytes = stats.written_bytes;
        }
    }

    let target_index = if sparse_shape == BenchCurrentStateSparseShape::TouchedScope
        && point_target == BenchCurrentStatePointTarget::HotMutated
    {
        replacement_rows / 4
    } else {
        replacement_rows / 2
    };
    let target = &entity_pks[target_index];
    let encoded_key = bytes::Bytes::from(super::codec::encode_key_ref(
        super::types::TrackedStateKeyRef {
            schema_key: "bench_current_state_alpha",
            file_id: None,
            entity_pk: target,
        },
    ));
    BenchCurrentStatePointFixture {
        storage,
        current_manifest,
        encoded_key,
        catalog_entry_count,
        catalog_manifest_bytes,
        catalog_staged_encoded_bytes: crate::storage_bench::take_crud_current_state_catalog_bytes(),
        directory_staged_encoded_bytes:
            crate::storage_bench::take_crud_current_state_directory_bytes(),
        sparse_staged_puts,
        sparse_written_bytes,
        first_sparse_elapsed_nanos,
        first_sparse_staged_puts,
        first_sparse_written_bytes,
    }
}

impl<StorageImpl> BenchCurrentStatePointFixture<StorageImpl>
where
    StorageImpl: Storage,
{
    pub fn catalog_entry_count(&self) -> usize {
        self.catalog_entry_count
    }

    pub fn catalog_manifest_bytes(&self) -> u64 {
        self.catalog_manifest_bytes
    }

    pub fn catalog_staged_encoded_bytes(&self) -> u64 {
        self.catalog_staged_encoded_bytes
    }

    pub fn directory_staged_encoded_bytes(&self) -> u64 {
        self.directory_staged_encoded_bytes
    }

    pub fn sparse_staged_puts(&self) -> u64 {
        self.sparse_staged_puts
    }

    pub fn sparse_written_bytes(&self) -> u64 {
        self.sparse_written_bytes
    }

    pub fn first_sparse_elapsed_nanos(&self) -> u64 {
        self.first_sparse_elapsed_nanos
    }

    pub fn first_sparse_staged_puts(&self) -> u64 {
        self.first_sparse_staged_puts
    }

    pub fn first_sparse_written_bytes(&self) -> u64 {
        self.first_sparse_written_bytes
    }

    pub async fn read_point(&self, mode: BenchCurrentStatePointMode) -> usize {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin benchmark current-state point read");
        match mode {
            BenchCurrentStatePointMode::PersistentCatalog => {
                let state = super::storage::load_published_commit_state_manifest(
                    &read,
                    self.current_manifest.commit_id,
                )
                .await
                .expect("load benchmark current-state manifest")
                .expect("benchmark current-state manifest exists");
                let rows =
                    super::storage::load_complete_current_state_rows_with_coordinates_encoded(
                        &read,
                        &state,
                        std::slice::from_ref(&self.encoded_key),
                    )
                    .await
                    .expect("read benchmark Arrow current-state row");
                usize::from(rows[0].is_some())
            }
        }
    }
}

fn bench_manifest_encoded_len(manifest: &CommitStateManifest) -> usize {
    5 + crate::storage_codec::encode("benchmark commit-state manifest", manifest)
        .expect("encode benchmark commit-state manifest")
        .len()
}

fn bench_addressable_commit_id(label: &str) -> CommitId {
    let labeled = CommitId::for_test_label(label);
    CommitId::with_change_address_space(*labeled.as_uuid())
}

fn bench_current_state_manifest(
    commit_id: CommitId,
    parent_commit_id: Option<CommitId>,
    mutations: super::types::CommitStateMutationInventory,
    current_state_catalog: Box<super::types::CurrentStateCatalogRoot>,
) -> CommitStateManifest {
    CommitStateManifest {
        commit_id,
        generation: 0,
        state_parent_commit_id: parent_commit_id,
        parent_commit_ids: parent_commit_id.into_iter().collect(),
        commit_change_id: ChangeId::for_test_label(&format!("{commit_id}:bench-state")),
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
        mutations,
        current_state_catalog,
    }
}

#[derive(Clone, Copy, Debug)]
pub struct BenchWriteAccounting {
    pub logical_rows: usize,
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub touched_spaces: u64,
    pub storage_calls: u64,
    pub put_batches: u64,
    pub delete_batches: u64,
    pub written_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct BenchLayoutAccounting {
    pub space_id: u32,
    pub space: &'static str,
    pub rows: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
}

#[derive(Clone, Copy, Debug)]
pub struct BenchPackedHistoryAccounting {
    pub changes: usize,
    pub commits: usize,
    pub staged_puts: u64,
    pub written_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchPackedHistoryShape {
    UniqueInserts,
    RepeatedUpdates,
    DeleteReinsert,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchPackedHistoryPayload {
    None,
    SmallInline,
    SharedLarge,
}

#[derive(Clone, Copy, Debug)]
pub struct BenchPackedHistoryOptions<'a> {
    pub shape: BenchPackedHistoryShape,
    pub payload: BenchPackedHistoryPayload,
    pub live_entities: usize,
    pub shared_large_payload: Option<&'a str>,
}

impl Default for BenchPackedHistoryOptions<'_> {
    fn default() -> Self {
        Self {
            shape: BenchPackedHistoryShape::UniqueInserts,
            payload: BenchPackedHistoryPayload::None,
            live_entities: 10_000,
            shared_large_payload: None,
        }
    }
}

/// Seeds the authoritative packed-history plane without commit headers, roots,
/// or live-state indexes.
///
/// This deliberately isolates the physical packed scan from public commit
/// facts and live-state work. `storage_batch_changes` controls only how many
/// logical changes share one backend transaction; it does not change the
/// commit-delta layout.
pub async fn seed_packed_history<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    changes: usize,
    commit_width: usize,
    storage_batch_changes: usize,
) -> BenchPackedHistoryAccounting
where
    StorageImpl: Storage,
{
    seed_packed_history_with_options(
        storage,
        changes,
        commit_width,
        storage_batch_changes,
        BenchPackedHistoryOptions::default(),
    )
    .await
}

pub async fn seed_packed_history_with_options<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    changes: usize,
    commit_width: usize,
    storage_batch_changes: usize,
    options: BenchPackedHistoryOptions<'_>,
) -> BenchPackedHistoryAccounting
where
    StorageImpl: Storage,
{
    assert!(changes > 0, "packed-history changes must be positive");
    assert!(
        commit_width > 0,
        "packed-history commit width must be positive"
    );
    assert!(
        changes.is_multiple_of(commit_width),
        "packed-history changes must divide evenly by commit width"
    );
    assert!(
        storage_batch_changes >= commit_width,
        "storage batch must hold at least one logical commit"
    );
    validate_packed_history_shape(changes, commit_width, options.shape, options.live_entities);
    assert_eq!(
        matches!(options.payload, BenchPackedHistoryPayload::SharedLarge),
        options.shared_large_payload.is_some(),
        "shared-large payload mode requires exactly one payload"
    );

    let commits = changes / commit_width;
    let commits_per_storage_batch = (storage_batch_changes / commit_width).max(1);
    let mut staged_puts = 0;
    let mut written_bytes = 0;
    let shared_large_ref = if let Some(payload) = options.shared_large_payload {
        let mut writes = storage.new_write_set();
        let [json_ref] = JsonStoreContext::new()
            .writer()
            .stage_batch(
                &mut writes,
                JsonWritePlacementRef::OutOfBand,
                [NormalizedJsonRef::new(payload)],
            )
            .expect("stage packed-history shared large payload")
            .try_into()
            .expect("one shared payload produces one JSON ref");
        let (_commit, stats) = storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit packed-history shared large payload");
        staged_puts += stats.staged_puts;
        written_bytes += stats.written_bytes;
        Some(json_ref)
    } else {
        None
    };
    for commit_batch_start in (0..commits).step_by(commits_per_storage_batch) {
        let commit_batch_end = (commit_batch_start + commits_per_storage_batch).min(commits);
        let mut writes = storage.new_write_set();
        for commit_index in commit_batch_start..commit_batch_end {
            let owned = (0..commit_width)
                .map(|row_index| {
                    PackedHistoryDelta::new(
                        commit_index,
                        row_index,
                        commit_width,
                        options,
                        shared_large_ref,
                    )
                })
                .collect::<Vec<_>>();
            let deltas = owned
                .iter()
                .map(PackedHistoryDelta::as_commit_ref)
                .collect::<Vec<_>>();
            let locators = stage_bench_commit_deltas(&mut writes, &deltas)
                .expect("stage packed-history commit delta");
            super::storage::stage_change_locators(&mut writes, &locators);
        }
        let (_commit, stats) = storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit packed-history storage batch");
        staged_puts += stats.staged_puts;
        written_bytes += stats.written_bytes;
    }
    BenchPackedHistoryAccounting {
        changes,
        commits,
        staged_puts,
        written_bytes,
    }
}

fn validate_packed_history_shape(
    changes: usize,
    commit_width: usize,
    shape: BenchPackedHistoryShape,
    live_entities: usize,
) {
    let required_generations = match shape {
        BenchPackedHistoryShape::UniqueInserts => return,
        BenchPackedHistoryShape::RepeatedUpdates => 2,
        BenchPackedHistoryShape::DeleteReinsert => 3,
    };
    assert!(
        live_entities >= commit_width,
        "repeated packed-history shapes require at least one live entity per commit row"
    );
    let required_changes = live_entities
        .checked_mul(required_generations)
        .expect("packed-history generation size should not overflow");
    assert!(
        changes >= required_changes,
        "{shape:?} requires at least {required_generations} complete generations \
         ({required_changes} changes for {live_entities} live entities)"
    );
}

pub async fn scan_packed_history<StorageImpl>(storage: &StorageAdapter<StorageImpl>) -> usize
where
    StorageImpl: Storage,
{
    let read = SharedStorageAdapterRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin packed-history scan"),
    );
    let mut count = 0usize;
    super::storage::visit_change_records_from_commit_deltas(&read, |_| {
        count += 1;
        Ok(())
    })
    .await
    .expect("scan packed history");
    count
}

pub async fn load_packed_change<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
    commit_index: usize,
    row_index: usize,
) -> bool
where
    StorageImpl: Storage,
{
    let read = SharedStorageAdapterRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin packed-history exact read"),
    );
    super::storage::load_change_record_by_id(
        &read,
        packed_history_change_id(commit_index, row_index),
    )
    .await
    .expect("load packed-history change by id")
    .is_some()
}

pub async fn packed_history_layout<StorageImpl>(
    storage: &StorageAdapter<StorageImpl>,
) -> Vec<BenchLayoutAccounting>
where
    StorageImpl: Storage,
{
    let read = SharedStorageAdapterRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin packed-history layout scan"),
    );
    crate::storage_bench::layout_accounting(&read)
        .await
        .into_iter()
        .map(|space| BenchLayoutAccounting {
            space_id: space.space_id,
            space: space.space,
            rows: space.rows,
            key_bytes: space.key_bytes,
            value_bytes: space.value_bytes,
        })
        .collect()
}

struct PackedHistoryDelta {
    change_id: ChangeId,
    commit_id: CommitId,
    entity_pk: EntityPk,
    schema_key: String,
    deleted: bool,
    payload: BenchPackedHistoryPayload,
    shared_large_ref: Option<JsonRef>,
    created_at: crate::common::LixTimestamp,
    updated_at: crate::common::LixTimestamp,
}

impl PackedHistoryDelta {
    fn new(
        commit_index: usize,
        row_index: usize,
        commit_width: usize,
        options: BenchPackedHistoryOptions<'_>,
        shared_large_ref: Option<JsonRef>,
    ) -> Self {
        let change_index = commit_index
            .checked_mul(commit_width)
            .and_then(|index| index.checked_add(row_index))
            .expect("packed-history change index should not overflow");
        let entity_pk = match options.shape {
            BenchPackedHistoryShape::UniqueInserts => {
                format!("packed-history-entity-{commit_index}-{row_index}")
            }
            BenchPackedHistoryShape::RepeatedUpdates | BenchPackedHistoryShape::DeleteReinsert => {
                format!(
                    "packed-history-entity-{}",
                    change_index % options.live_entities
                )
            }
        };
        let deleted = matches!(options.shape, BenchPackedHistoryShape::DeleteReinsert)
            && (change_index / options.live_entities) % 2 == 1;
        Self {
            change_id: packed_history_change_id(commit_index, row_index),
            commit_id: CommitId::new(packed_history_uuid(commit_index, 0, 0x43)),
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "packed_history".to_string(),
            deleted,
            payload: options.payload,
            shared_large_ref,
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-07-28T00:00:00.000Z",
            ),
            updated_at: crate::common::LixTimestamp::expect_parse(
                "updated_at",
                "2026-07-28T00:00:00.000Z",
            ),
        }
    }

    fn as_commit_ref(&self) -> TrackedStateCommitDeltaRef<'_> {
        const SMALL_PAYLOAD: &str = r#"{"value":"small"}"#;
        let snapshot = if self.deleted {
            JsonSlotRef::None
        } else {
            match self.payload {
                BenchPackedHistoryPayload::None => JsonSlotRef::None,
                BenchPackedHistoryPayload::SmallInline => JsonSlotRef::Inline(SMALL_PAYLOAD),
                BenchPackedHistoryPayload::SharedLarge => JsonSlotRef::Ref(
                    self.shared_large_ref
                        .as_ref()
                        .expect("shared-large packed history has a JSON ref"),
                ),
            }
        };
        TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &self.schema_key,
                file_id: None,
                entity_pk: &self.entity_pk,
                change_id: self.change_id,
                commit_id: self.commit_id,
                deleted: self.deleted,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
            snapshot,
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        }
    }
}

fn packed_history_change_id(commit_index: usize, row_index: usize) -> ChangeId {
    ChangeId::new(packed_history_uuid(commit_index, row_index, 0x68))
}

fn packed_history_uuid(commit_index: usize, ordinal: usize, discriminator: u8) -> uuid::Uuid {
    let timestamp = 0x0192_0000_0000u64
        .checked_add(u64::try_from(commit_index).expect("benchmark commit index fits u64"))
        .expect("benchmark timestamp does not overflow");
    static DETERMINISTIC_IDS: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    let deterministic = *DETERMINISTIC_IDS.get_or_init(|| {
        std::env::var("LIX_PACKED_HISTORY_ID_SHAPE").map_or(true, |shape| shape != "system")
    });
    let mut bytes = [0u8; 16];
    bytes[..6].copy_from_slice(&timestamp.to_be_bytes()[2..]);
    if deterministic {
        bytes[6] = 0x70;
        bytes[7] = 0;
    } else {
        bytes[6] = 0x70 | u8::try_from((ordinal >> 8) & 0x0f).expect("ordinal nibble fits u8");
        bytes[7] = u8::try_from(ordinal & 0xff).expect("ordinal byte fits u8");
    }
    let mixed = if deterministic {
        (u64::try_from(commit_index).expect("commit index fits u64") << 20)
            | (u64::try_from(ordinal).expect("ordinal fits u64") << 1)
            | u64::from(discriminator == 0x68)
    } else {
        let mut mixed = (u64::try_from(commit_index).expect("commit index fits u64") << 16)
            ^ u64::try_from(ordinal).expect("ordinal fits u64")
            ^ u64::from(discriminator);
        mixed = mixed.wrapping_add(0x9e37_79b9_7f4a_7c15);
        mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        mixed ^ (mixed >> 31)
    };
    bytes[8..].copy_from_slice(&mixed.to_be_bytes());
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    uuid::Uuid::from_bytes(bytes)
}

#[cfg(test)]
mod packed_history_tests {
    use super::*;

    fn options(
        shape: BenchPackedHistoryShape,
        payload: BenchPackedHistoryPayload,
    ) -> BenchPackedHistoryOptions<'static> {
        BenchPackedHistoryOptions {
            shape,
            payload,
            live_entities: 10,
            shared_large_payload: matches!(payload, BenchPackedHistoryPayload::SharedLarge)
                .then_some(r#"{"payload":"large"}"#),
        }
    }

    #[test]
    fn repeated_updates_reuse_entity_identity() {
        let first = PackedHistoryDelta::new(
            0,
            0,
            10,
            options(
                BenchPackedHistoryShape::RepeatedUpdates,
                BenchPackedHistoryPayload::None,
            ),
            None,
        );
        let second = PackedHistoryDelta::new(
            1,
            0,
            10,
            options(
                BenchPackedHistoryShape::RepeatedUpdates,
                BenchPackedHistoryPayload::None,
            ),
            None,
        );

        assert_eq!(first.entity_pk, second.entity_pk);
        assert!(!first.deleted);
        assert!(!second.deleted);
    }

    #[test]
    fn delete_reinsert_alternates_generations() {
        let options = options(
            BenchPackedHistoryShape::DeleteReinsert,
            BenchPackedHistoryPayload::SmallInline,
        );
        let insert = PackedHistoryDelta::new(0, 0, 10, options, None);
        let delete = PackedHistoryDelta::new(1, 0, 10, options, None);
        let reinsert = PackedHistoryDelta::new(2, 0, 10, options, None);

        assert_eq!(insert.entity_pk, delete.entity_pk);
        assert_eq!(delete.entity_pk, reinsert.entity_pk);
        assert!(!insert.deleted);
        assert!(delete.deleted);
        assert!(!reinsert.deleted);
        assert_eq!(
            insert.as_commit_ref().snapshot,
            JsonSlotRef::Inline(r#"{"value":"small"}"#)
        );
        assert_eq!(delete.as_commit_ref().snapshot, JsonSlotRef::None);
        assert_eq!(
            reinsert.as_commit_ref().snapshot,
            JsonSlotRef::Inline(r#"{"value":"small"}"#)
        );
    }

    #[test]
    #[should_panic(expected = "RepeatedUpdates requires at least 2 complete generations")]
    fn repeated_updates_require_an_update_generation() {
        validate_packed_history_shape(10, 10, BenchPackedHistoryShape::RepeatedUpdates, 10);
    }

    #[test]
    #[should_panic(expected = "DeleteReinsert requires at least 3 complete generations")]
    fn delete_reinsert_requires_a_reinsert_generation() {
        validate_packed_history_shape(20, 10, BenchPackedHistoryShape::DeleteReinsert, 10);
    }

    #[test]
    fn shared_large_payload_uses_one_json_reference() {
        let json_ref = JsonRef::from_hash_bytes([7; 32]);
        let delta = PackedHistoryDelta::new(
            0,
            0,
            1,
            options(
                BenchPackedHistoryShape::UniqueInserts,
                BenchPackedHistoryPayload::SharedLarge,
            ),
            Some(json_ref),
        );

        assert_eq!(delta.as_commit_ref().snapshot, JsonSlotRef::Ref(&json_ref));
    }
}

use std::time::Instant;
