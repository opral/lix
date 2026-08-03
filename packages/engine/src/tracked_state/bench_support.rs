use crate::changelog::{ChangeId, CommitId};
use crate::entity_pk::EntityPk;
use crate::json_store::{
    JsonRef, JsonSlotRef, JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef,
};
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageAdapterRead, StorageReadOptions,
    StorageWriteOptions, StorageWriteSet, StorageWriteSetStats,
};
use crate::tracked_state::{
    CommitStateManifest, CommitStateReplayDebt, TrackedStateCommitDeltaRef, TrackedStateContext,
    TrackedStateDeltaRef, TrackedStateFilter, TrackedStateKey, TrackedStateReadColumns,
    TrackedStateScanRequest,
};

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
    super::storage::stage_commit_state_manifest(
        writes,
        &CommitStateManifest {
            commit_id,
            generation: 0,
            parent_commit_ids: Vec::new(),
            commit_change_id: ChangeId::for_test_label(&format!("{commit_id}:bench-commit")),
            account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            current_state_catalog: None,
            current_state_coverage_anchor: None,
            snapshot_root: None,
        },
    )?;
    Ok(staged.locators)
}

#[derive(Clone, Debug)]
pub struct BenchTrackedRow {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub entity_pk: String,
    pub value: Vec<u8>,
    pub updated_value: Vec<u8>,
}

#[expect(missing_debug_implementations)]
pub struct BenchTrackedFixture<StorageImpl: Storage> {
    storage: StorageAdapter<StorageImpl>,
    context: TrackedStateContext,
    rows: Vec<BenchTrackedRow>,
    current_commit_id: Option<String>,
    next_commit_index: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchCurrentStatePointMode {
    PersistentCatalog,
    CatalogThenReplay,
    FirstParentReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchCurrentStateDirectoryDiffMode {
    Flatten,
    Merkle,
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
    commits: Vec<CommitId>,
    base_manifest: CommitStateManifest,
    current_manifest: CommitStateManifest,
    encoded_key: bytes::Bytes,
    catalog_entry_count: usize,
    catalog_manifest_bytes: u64,
    replay_manifest_bytes: u64,
    catalog_staged_encoded_bytes: u64,
    directory_staged_encoded_bytes: u64,
    sparse_directory_staged_encoded_bytes: u64,
    sparse_staged_puts: u64,
    sparse_written_bytes: u64,
    sparse_directory_nodes_loaded: u64,
    sparse_directory_descriptors_visited: u64,
    sparse_directory_nodes_encoded: u64,
    sparse_publication_p50_nanos: u64,
    sparse_publication_p95_nanos: u64,
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
        fallback_commit_id: None,
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
        crate::ANONYMOUS_ACCOUNT_ID,
        &parent_mutations,
    )
    .await
    .expect("certify benchmark parent catalog");
    let (parent_catalog, parent_anchor) = parent_publication.parts();
    parent_mutations.parts.clear();
    let mut current_manifest = bench_current_state_manifest(
        parent_id,
        None,
        1,
        parent_mutations,
        parent_catalog,
        parent_anchor,
    );
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
            fallback_commit_id: None,
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
            crate::ANONYMOUS_ACCOUNT_ID,
            &mutations,
        )
        .await
        .expect("certify benchmark scope catalog");
        let (catalog, anchor) = publication.parts();
        mutations.parts.clear();
        let authority_manifest = bench_current_state_manifest(
            authority_commit_id,
            Some(current_manifest.commit_id),
            u16::try_from(index + 1)
                .expect("benchmark catalog construction depth fits u16")
                .min(super::COMMIT_STATE_MAX_REPLAY_DEPTH),
            mutations,
            catalog,
            anchor,
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

    let replay_base_id = if catalog_entry_count == 1 {
        parent_id
    } else {
        let refresh_id = bench_addressable_commit_id("persistent-catalog-alpha-refresh");
        let refresh_rows = entity_pks
            .iter()
            .enumerate()
            .map(|(index, entity_pk)| TrackedStateCommitDeltaRef {
                delta: TrackedStateDeltaRef {
                    schema_key: "bench_current_state_alpha",
                    file_id: None,
                    entity_pk,
                    change_id: ChangeId::for_test_label(&format!("catalog-refresh-{index}")),
                    commit_id: refresh_id,
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
        let mut refresh_writes = storage.new_write_set();
        let refresh_stage = super::storage::stage_ordered_addressable_replacement_parts(
            &mut refresh_writes,
            refresh_rows.iter().copied().map(Ok),
            &generation,
        )
        .expect("stage benchmark alpha refresh");
        let mut refresh_mutations = refresh_stage.mutation_inventory().clone();
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open benchmark refresh parent read");
        let published_parent =
            super::storage::load_published_commit_state_manifest(&read, current_manifest.commit_id)
                .await
                .expect("load benchmark refresh parent authority")
                .expect("benchmark refresh parent authority exists");
        let publication = super::storage::stage_current_state_catalog_from_published_parent(
            &read,
            &mut refresh_writes,
            Some(&published_parent),
            refresh_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &refresh_mutations,
        )
        .await
        .expect("certify benchmark alpha refresh");
        let (catalog, anchor) = publication.parts();
        refresh_mutations.parts.clear();
        let refresh_manifest = bench_current_state_manifest(
            refresh_id,
            Some(current_manifest.commit_id),
            1,
            refresh_mutations,
            catalog,
            anchor,
        );
        super::storage::stage_certified_commit_state_manifest(
            &mut refresh_writes,
            &refresh_manifest,
            &publication,
        )
        .expect("stage benchmark alpha refresh authority");
        drop(read);
        storage
            .commit_write_set(refresh_writes, StorageWriteOptions::default())
            .await
            .expect("commit benchmark alpha refresh");
        current_manifest = refresh_manifest;
        refresh_id
    };

    let base_manifest = current_manifest.clone();
    let mut commits = vec![replay_base_id];
    let mut catalog_manifest_bytes = 0u64;
    let mut replay_manifest_bytes = 0u64;
    let mut sparse_staged_puts = 0u64;
    let mut sparse_written_bytes = 0u64;
    let mut sparse_directory_nodes_loaded = 0u64;
    let mut sparse_directory_descriptors_visited = 0u64;
    let mut sparse_directory_nodes_encoded = 0u64;
    let mut sparse_publication_nanos = Vec::with_capacity(unrelated_sparse_commits);
    let mut first_sparse_elapsed_nanos = 0u64;
    let mut first_sparse_staged_puts = 0u64;
    let mut first_sparse_written_bytes = 0u64;
    let _ = crate::storage_bench::take_crud_current_state_directory_accounting();
    let base_directory_staged_encoded_bytes =
        crate::storage_bench::take_crud_current_state_directory_bytes();
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
        let read = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open benchmark parent read");
        let published_parent =
            super::storage::load_published_commit_state_manifest(&read, current_manifest.commit_id)
                .await
                .expect("load benchmark sparse parent authority")
                .expect("benchmark sparse parent authority exists");
        let publication_started = Instant::now();
        let publication = super::storage::stage_current_state_catalog_from_published_parent(
            &read,
            &mut writes,
            Some(&published_parent),
            commit_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &mut mutations,
        )
        .await
        .expect("reuse benchmark persistent catalog");
        sparse_publication_nanos
            .push(u64::try_from(publication_started.elapsed().as_nanos()).unwrap_or(u64::MAX));
        let (catalog, anchor) = publication.parts();
        drop(read);
        current_manifest = bench_current_state_manifest(
            commit_id,
            commits.last().copied(),
            u16::try_from(commits.len() + 1)
                .expect("benchmark replay depth fits u16")
                .min(super::COMMIT_STATE_MAX_REPLAY_DEPTH),
            mutations,
            catalog,
            anchor,
        );
        catalog_manifest_bytes += u64::try_from(bench_manifest_encoded_len(&current_manifest))
            .expect("benchmark manifest length fits u64");
        let mut replay_manifest = current_manifest.clone();
        replay_manifest.current_state_catalog = None;
        replay_manifest_bytes += u64::try_from(bench_manifest_encoded_len(&replay_manifest))
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
        let directory_accounting =
            crate::storage_bench::take_crud_current_state_directory_accounting();
        sparse_directory_nodes_loaded += directory_accounting.nodes_loaded;
        sparse_directory_descriptors_visited += directory_accounting.descriptors_visited;
        sparse_directory_nodes_encoded += directory_accounting.nodes_encoded;
        if index == 0 {
            first_sparse_elapsed_nanos =
                u64::try_from(sparse_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            first_sparse_staged_puts = stats.staged_puts;
            first_sparse_written_bytes = stats.written_bytes;
        }
        commits.push(commit_id);
    }
    sparse_publication_nanos.sort_unstable();
    let sparse_publication_p50_nanos = sparse_publication_nanos
        .get(sparse_publication_nanos.len() / 2)
        .copied()
        .unwrap_or_default();
    let sparse_publication_p95_nanos = sparse_publication_nanos
        .get(
            sparse_publication_nanos
                .len()
                .saturating_mul(95)
                .div_ceil(100)
                .saturating_sub(1),
        )
        .copied()
        .unwrap_or_default();
    let sparse_directory_staged_encoded_bytes =
        crate::storage_bench::take_crud_current_state_directory_bytes();

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
        commits,
        base_manifest,
        current_manifest,
        encoded_key,
        catalog_entry_count,
        catalog_manifest_bytes,
        replay_manifest_bytes,
        catalog_staged_encoded_bytes: crate::storage_bench::take_crud_current_state_catalog_bytes(),
        directory_staged_encoded_bytes: base_directory_staged_encoded_bytes
            .saturating_add(sparse_directory_staged_encoded_bytes),
        sparse_directory_staged_encoded_bytes,
        sparse_staged_puts,
        sparse_written_bytes,
        sparse_directory_nodes_loaded,
        sparse_directory_descriptors_visited,
        sparse_directory_nodes_encoded,
        sparse_publication_p50_nanos,
        sparse_publication_p95_nanos,
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

    pub fn replay_manifest_bytes(&self) -> u64 {
        self.replay_manifest_bytes
    }

    pub fn catalog_staged_encoded_bytes(&self) -> u64 {
        self.catalog_staged_encoded_bytes
    }

    pub fn directory_staged_encoded_bytes(&self) -> u64 {
        self.directory_staged_encoded_bytes
    }

    pub fn sparse_directory_staged_encoded_bytes(&self) -> u64 {
        self.sparse_directory_staged_encoded_bytes
    }

    pub fn sparse_staged_puts(&self) -> u64 {
        self.sparse_staged_puts
    }

    pub fn sparse_written_bytes(&self) -> u64 {
        self.sparse_written_bytes
    }

    pub fn sparse_directory_nodes_loaded(&self) -> u64 {
        self.sparse_directory_nodes_loaded
    }

    pub fn sparse_directory_descriptors_visited(&self) -> u64 {
        self.sparse_directory_descriptors_visited
    }

    pub fn sparse_directory_nodes_encoded(&self) -> u64 {
        self.sparse_directory_nodes_encoded
    }

    pub fn sparse_publication_p50_nanos(&self) -> u64 {
        self.sparse_publication_p50_nanos
    }

    pub fn sparse_publication_p95_nanos(&self) -> u64 {
        self.sparse_publication_p95_nanos
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
            BenchCurrentStatePointMode::PersistentCatalog
            | BenchCurrentStatePointMode::CatalogThenReplay => {
                if mode == BenchCurrentStatePointMode::CatalogThenReplay {
                    let head = *self.commits.last().expect("benchmark has a head commit");
                    let values = super::storage::load_commit_delta_values_encoded_with_cache(
                        &read,
                        head,
                        std::slice::from_ref(&self.encoded_key),
                        None,
                    )
                    .await
                    .expect("probe benchmark head delta");
                    if values[0].is_some() {
                        return 1;
                    }
                }
                let state = super::storage::load_published_commit_state_manifest(
                    &read,
                    self.current_manifest.commit_id,
                )
                .await
                .expect("load benchmark current-state manifest")
                .expect("benchmark current-state manifest exists");
                let values = if mode == BenchCurrentStatePointMode::PersistentCatalog {
                    super::storage::load_complete_current_state_values_from_published_manifest(
                        &read,
                        &state,
                        std::slice::from_ref(&self.encoded_key),
                    )
                    .await
                } else {
                    super::storage::load_complete_current_state_values_from_published_replay_manifest(
                        &read,
                        &state,
                        std::slice::from_ref(&self.encoded_key),
                    )
                    .await
                }
                .expect("read benchmark current-state catalog");
                if let Some(values) = values {
                    return usize::from(values[0].is_some());
                }
                if mode == BenchCurrentStatePointMode::PersistentCatalog {
                    panic!("benchmark point is not covered");
                }
                self.read_point_by_replay(&read).await
            }
            BenchCurrentStatePointMode::FirstParentReplay => self.read_point_by_replay(&read).await,
        }
    }

    pub async fn diff_current_state_directory(
        &self,
        mode: BenchCurrentStateDirectoryDiffMode,
    ) -> [u8; 32] {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin benchmark current-state directory diff");
        let scope = super::types::CommitDeltaReplacementScope {
            schema_key: "bench_current_state_alpha".to_string(),
            file_id: None,
        };
        let left_state = super::storage::load_published_commit_state_manifest(
            &read,
            self.base_manifest.commit_id,
        )
        .await
        .expect("load benchmark base published manifest")
        .expect("benchmark base published manifest exists");
        let right_state = super::storage::load_published_commit_state_manifest(
            &read,
            self.current_manifest.commit_id,
        )
        .await
        .expect("load benchmark head published manifest")
        .expect("benchmark head published manifest exists");
        let left = super::storage::load_complete_current_state_part_set_from_published_manifest(
            &read,
            &left_state,
            &scope,
        )
        .await
        .expect("authenticate benchmark base current-state entry")
        .expect("benchmark base scope is covered");
        let right = super::storage::load_complete_current_state_part_set_from_published_manifest(
            &read,
            &right_state,
            &scope,
        )
        .await
        .expect("authenticate benchmark head current-state entry")
        .expect("benchmark head scope is covered");
        let windows = match mode {
            BenchCurrentStateDirectoryDiffMode::Flatten => {
                let left = super::current_state_part::load_current_state_part_descriptors(
                    &read,
                    &left.directory,
                )
                .await
                .expect("flatten benchmark base directory");
                let right = super::current_state_part::load_current_state_part_descriptors(
                    &read,
                    &right.directory,
                )
                .await
                .expect("flatten benchmark head directory");
                super::current_state_part::diff_current_state_part_descriptor_slices(&left, &right)
                    .expect("compare flattened benchmark directories")
            }
            BenchCurrentStateDirectoryDiffMode::Merkle => {
                super::current_state_part::diff_current_state_part_descriptors(
                    &read,
                    &left.directory,
                    &right.directory,
                )
                .await
                .expect("Merkle-diff benchmark directories")
            }
        };
        let mut digest = blake3::Hasher::new_derive_key(
            "lix benchmark current-state descriptor diff windows v1",
        );
        for window in windows {
            digest.update(&(window.first_key.len() as u64).to_be_bytes());
            digest.update(&window.first_key);
            digest.update(&(window.last_key.len() as u64).to_be_bytes());
            digest.update(&window.last_key);
            let left = crate::storage_codec::encode(
                "benchmark current-state descriptor diff left window",
                &window.left,
            )
            .expect("encode benchmark left descriptor window");
            let right = crate::storage_codec::encode(
                "benchmark current-state descriptor diff right window",
                &window.right,
            )
            .expect("encode benchmark right descriptor window");
            digest.update(&(left.len() as u64).to_be_bytes());
            digest.update(&left);
            digest.update(&(right.len() as u64).to_be_bytes());
            digest.update(&right);
        }
        *digest.finalize().as_bytes()
    }

    async fn read_point_by_replay(&self, read: &(impl StorageAdapterRead + ?Sized)) -> usize {
        for commit_id in self.commits.iter().rev() {
            let values = super::storage::load_commit_delta_values_encoded_with_cache(
                read,
                *commit_id,
                std::slice::from_ref(&self.encoded_key),
                None,
            )
            .await
            .expect("replay benchmark commit point");
            if values[0].is_some() {
                return 1;
            }
        }
        0
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
    replay_depth: u16,
    mutations: super::types::CommitStateMutationInventory,
    current_state_catalog: Option<Box<super::types::CurrentStateCatalogRoot>>,
    current_state_coverage_anchor: Option<Box<super::types::CurrentStateCoverageAnchor>>,
) -> CommitStateManifest {
    CommitStateManifest {
        commit_id,
        generation: 0,
        parent_commit_ids: parent_commit_id.into_iter().collect(),
        commit_change_id: ChangeId::for_test_label(&format!("{commit_id}:bench-state")),
        account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        created_at: crate::common::LixTimestamp::from_unix_millis_utc_lossy(0),
        replay_debt: CommitStateReplayDebt {
            depth: replay_depth,
            rows: u64::from(mutations.member_count),
            bytes: u64::from(mutations.member_count),
        },
        mutations,
        current_state_catalog,
        current_state_coverage_anchor,
        snapshot_root: None,
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

struct BenchWriteOutcome {
    logical_rows: usize,
    stats: StorageWriteSetStats,
}

impl BenchWriteOutcome {
    fn accounting(&self) -> BenchWriteAccounting {
        BenchWriteAccounting {
            logical_rows: self.logical_rows,
            staged_puts: self.stats.staged_puts,
            staged_deletes: self.stats.staged_deletes,
            touched_spaces: self.stats.touched_spaces,
            storage_calls: self.stats.storage_calls,
            put_batches: self.stats.put_batches,
            delete_batches: self.stats.delete_batches,
            written_bytes: self.stats.written_bytes,
        }
    }
}

impl<StorageImpl> BenchTrackedFixture<StorageImpl>
where
    StorageImpl: Storage,
{
    pub fn new(storage: StorageAdapter<StorageImpl>, rows: Vec<BenchTrackedRow>) -> Self {
        Self {
            storage,
            context: TrackedStateContext::new(),
            rows,
            current_commit_id: None,
            next_commit_index: 0,
        }
    }

    pub async fn seed(&mut self) -> usize {
        self.insert_all().await
    }

    pub async fn insert_all(&mut self) -> usize {
        self.insert_all_accounting().await.logical_rows
    }

    pub async fn insert_all_accounting(&mut self) -> BenchWriteAccounting {
        let rows = self.rows.clone();
        self.stage_rows(rows, None).await.accounting()
    }

    pub async fn update_all(&mut self) -> usize {
        self.update_all_accounting().await.logical_rows
    }

    pub async fn update_all_accounting(&mut self) -> BenchWriteAccounting {
        let rows = self
            .rows
            .iter()
            .cloned()
            .map(|mut row| {
                row.value = row.updated_value.clone();
                row
            })
            .collect::<Vec<_>>();
        self.stage_rows(rows, self.current_commit_id.clone())
            .await
            .accounting()
    }

    pub async fn update_one_by_pk(&mut self) -> usize {
        self.update_one_by_pk_accounting().await.logical_rows
    }

    pub async fn update_one_by_pk_accounting(&mut self) -> BenchWriteAccounting {
        let mut row = self.rows[self.rows.len() / 2].clone();
        row.value = row.updated_value.clone();
        self.stage_rows(vec![row], self.current_commit_id.clone())
            .await
            .accounting()
    }

    pub async fn delete_all(&mut self) -> usize {
        self.delete_all_accounting().await.logical_rows
    }

    pub async fn delete_all_accounting(&mut self) -> BenchWriteAccounting {
        let rows = self
            .rows
            .iter()
            .cloned()
            .map(|mut row| {
                row.value.clear();
                row
            })
            .collect::<Vec<_>>();
        self.stage_rows_as_deletes(rows, self.current_commit_id.clone())
            .await
            .accounting()
    }

    pub async fn delete_one_by_pk(&mut self) -> usize {
        self.delete_one_by_pk_accounting().await.logical_rows
    }

    pub async fn delete_one_by_pk_accounting(&mut self) -> BenchWriteAccounting {
        let mut row = self.rows[self.rows.len() / 2].clone();
        row.value.clear();
        self.stage_rows_as_deletes(vec![row], self.current_commit_id.clone())
            .await
            .accounting()
    }

    pub async fn read_all(&self) -> usize {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin tracked-state read"),
        );
        let mut reader = self.context.reader(read);
        let rows = reader
            .scan_batch_at_commit(
                self.current_commit_id(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter::default(),
                    read_columns: TrackedStateReadColumns::default(),
                    limit: None,
                },
            )
            .await
            .expect("scan tracked-state rows")
            .into_rows();
        assert_eq!(rows.len(), self.rows.len());
        rows.len()
    }

    pub async fn read_all_by_pk(&self) -> usize {
        let keys = self.rows.iter().map(row_key).collect::<Vec<_>>();
        self.read_by_pk(&keys).await
    }

    pub async fn read_one_by_pk(&self) -> usize {
        let key = row_key(&self.rows[self.rows.len() / 2]);
        self.read_by_pk(&[key]).await
    }

    async fn read_by_pk(&self, keys: &[TrackedStateKey]) -> usize {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin tracked-state read"),
        );
        let mut reader = self.context.reader(read);
        let rows = reader
            .load_batch_at_commit(self.current_commit_id(), keys)
            .await
            .expect("load tracked-state rows")
            .into_rows();
        assert!(rows.iter().all(Option::is_some));
        rows.len()
    }

    async fn stage_rows(
        &mut self,
        rows: Vec<BenchTrackedRow>,
        parent_commit_id: Option<String>,
    ) -> BenchWriteOutcome {
        self.stage_rows_inner(rows, parent_commit_id, false).await
    }

    async fn stage_rows_as_deletes(
        &mut self,
        rows: Vec<BenchTrackedRow>,
        parent_commit_id: Option<String>,
    ) -> BenchWriteOutcome {
        self.stage_rows_inner(rows, parent_commit_id, true).await
    }

    async fn stage_rows_inner(
        &mut self,
        rows: Vec<BenchTrackedRow>,
        parent_commit_id: Option<String>,
        deleted: bool,
    ) -> BenchWriteOutcome {
        let commit_id = self.next_commit_id();
        let mut writes = self.storage.new_write_set();
        let owned = rows
            .into_iter()
            .enumerate()
            .map(|(index, row)| OwnedDelta::new(row, &commit_id, index, deleted, &mut writes))
            .collect::<Vec<_>>();
        let deltas = owned.iter().map(OwnedDelta::as_ref).collect::<Vec<_>>();
        {
            let read = SharedStorageAdapterRead::new(
                self.storage
                    .begin_read(StorageReadOptions::default())
                    .await
                    .expect("begin tracked-state write read"),
            );
            let mut writer = self.context.writer(&read, &mut writes);
            writer
                .stage_commit_root(&commit_id, parent_commit_id.as_deref(), deltas)
                .await
                .expect("stage tracked-state commit root");
        }
        let (_commit, stats) = self
            .storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit tracked-state writes");
        assert!(
            stats.staged_puts > 0,
            "tracked-state write should stage physical puts"
        );
        self.current_commit_id = Some(commit_id);
        BenchWriteOutcome {
            logical_rows: owned.len(),
            stats,
        }
    }

    pub async fn layout_accounting(&self) -> Vec<BenchLayoutAccounting> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin tracked-state layout accounting read"),
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

    fn next_commit_id(&mut self) -> String {
        self.next_commit_index += 1;
        format!("tracked-crud-commit-{}", self.next_commit_index)
    }

    fn current_commit_id(&self) -> &str {
        self.current_commit_id
            .as_deref()
            .expect("tracked-state fixture should be seeded")
    }
}

struct OwnedDelta {
    change_id: ChangeId,
    commit_id: CommitId,
    entity_pk: EntityPk,
    schema_key: String,
    file_id: Option<String>,
    deleted: bool,
    created_at: crate::common::LixTimestamp,
    updated_at: crate::common::LixTimestamp,
}

impl OwnedDelta {
    fn new(
        row: BenchTrackedRow,
        commit_id: &str,
        index: usize,
        deleted: bool,
        _writes: &mut StorageWriteSet,
    ) -> Self {
        let change_id = format!("tracked-crud-change-{commit_id}-{index}");
        Self {
            change_id: ChangeId::for_test_label(&change_id),
            commit_id: CommitId::for_test_label(commit_id),
            entity_pk: EntityPk::single(row.entity_pk),
            schema_key: row.schema_key,
            file_id: row.file_id,
            deleted,
            created_at: crate::common::LixTimestamp::expect_parse(
                "created_at",
                "2026-05-19T00:00:00.000Z",
            ),
            updated_at: crate::common::LixTimestamp::expect_parse(
                "updated_at",
                "2026-05-19T00:00:00.000Z",
            ),
        }
    }

    fn as_ref(&self) -> TrackedStateDeltaRef<'_> {
        TrackedStateDeltaRef {
            schema_key: &self.schema_key,
            file_id: self.file_id.as_deref(),
            entity_pk: &self.entity_pk,
            change_id: self.change_id,
            commit_id: self.commit_id,
            deleted: self.deleted,
            created_at: self.created_at,
            updated_at: self.updated_at,
        }
    }
}

fn row_key(row: &BenchTrackedRow) -> TrackedStateKey {
    TrackedStateKey {
        schema_key: row.schema_key.clone(),
        entity_pk: EntityPk::single(row.entity_pk.clone()),
        file_id: row.file_id.clone(),
    }
}
use std::time::Instant;
