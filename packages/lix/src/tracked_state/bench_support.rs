use crate::changelog::{ChangeId, CommitId};
use crate::row_pk::RowPk;
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

pub use super::mutation_directory::MutationDirectoryReadAccounting;

/// Resets the feature-gated authenticated mutation-directory counters for one
/// benchmark phase. Production builds do not compile this module.
pub fn reset_mutation_directory_read_accounting() {
    super::mutation_directory::reset_mutation_directory_read_accounting();
}

/// Stops and snapshots the feature-gated authenticated mutation-directory
/// counters for the completed benchmark phase.
pub fn snapshot_mutation_directory_read_accounting() -> MutationDirectoryReadAccounting {
    super::mutation_directory::snapshot_mutation_directory_read_accounting()
}

fn stage_bench_commit_deltas(
    writes: &mut StorageWriteSet,
    deltas: &[TrackedStateCommitDeltaRef<'_>],
) -> Result<Vec<super::storage::CommitDeltaChangeLocator>, crate::LixError> {
    let (mutations, locators) = if packed_history_addressable_ids() {
        let staged = super::storage::stage_ordered_addressable_commit_deltas(
            writes,
            deltas.iter().copied().map(Ok),
            false,
            false,
        )?
        .ok_or_else(|| {
            crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                "addressable packed-history fixture is not strictly ordered",
            )
        })?;
        (staged.mutation_inventory().clone(), Vec::new())
    } else {
        let staged = super::storage::stage_commit_deltas_for_commit_state(writes, deltas)?;
        (staged.mutation_inventory().clone(), staged.locators)
    };
    let commit_id = deltas
        .first()
        .map(|delta| delta.delta.commit_id)
        .unwrap_or_default();
    super::storage::stage_commit_state_manifest(
        writes,
        &CommitStateManifest {
            commit_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(mutations.member_count),
                bytes: u64::from(mutations.member_count),
            },
            mutations,
            touched_scope_filter: Default::default(),
            current_state_scoped_ranges: None,
            snapshot_root: None,
        },
    )?;
    Ok(locators)
}

#[derive(Clone, Debug)]
pub struct BenchTrackedRow {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub row_pk: String,
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
    ScopedRange,
    FirstParentReplay,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BenchCurrentStateSparseShape {
    UnrelatedScopes,
    TouchedScope,
    TouchedScopeDistinct,
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
    scope_count: usize,
    scoped_manifest_bytes: u64,
    replay_manifest_bytes: u64,
    scoped_range_staged_puts: u64,
    scoped_range_staged_bytes: u64,
    sparse_scoped_range_staged_puts: u64,
    sparse_scoped_range_staged_bytes: u64,
    sparse_staged_puts: u64,
    sparse_written_bytes: u64,
    sparse_publication_p50_nanos: u64,
    sparse_publication_p95_nanos: u64,
    first_sparse_elapsed_nanos: u64,
    first_sparse_staged_puts: u64,
    first_sparse_written_bytes: u64,
    current_state_part_count: u64,
}

pub async fn seed_current_state_point_fixture<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    replacement_rows: usize,
    unrelated_sparse_commits: usize,
    scope_count: usize,
    sparse_shape: BenchCurrentStateSparseShape,
    point_target: BenchCurrentStatePointTarget,
) -> BenchCurrentStatePointFixture<StorageImpl>
where
    StorageImpl: Storage,
{
    assert!(replacement_rows > 0);
    assert!(scope_count > 0);
    let created_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(11);
    let updated_at = crate::common::LixTimestamp::from_unix_millis_utc_lossy(22);
    let row_pks = (0..replacement_rows)
        .map(|index| RowPk::single(format!("row-{index:09}")))
        .collect::<Vec<_>>();
    let alpha_scope = super::types::CommitDeltaReplacementScope {
        schema_key: "bench_current_state_alpha".to_string(),
        file_id: None,
    };
    let alpha_generation = super::storage::CommitDeltaReplacementGeneration {
        scope: alpha_scope.clone(),
        fallback_commit_id: None,
        lifecycle_summary: super::storage::CommitDeltaLifecycleSummary {
            scope: alpha_scope,
            ordered_identity_digest: [17; 32],
            uniform_created_at: created_at,
        },
    };

    let parent_id = bench_addressable_commit_id("scoped-range-parent");
    let parent_rows = row_pks
        .iter()
        .enumerate()
        .map(|(index, row_pk)| TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: "bench_current_state_alpha",
                file_id: None,
                row_pk,
                change_id: ChangeId::for_test_label(&format!("scoped-parent-{index}")),
                commit_id: parent_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{}"),
            typed_snapshot: None,
            typed_payload: None,
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        })
        .collect::<Vec<_>>();
    let mut writes = storage.new_write_set();
    let staged = super::storage::stage_ordered_addressable_replacement_parts(
        &mut writes,
        parent_rows.iter().copied().map(Ok),
        &alpha_generation,
    )
    .expect("stage benchmark replacement parts");
    let (mut current_manifest, mut scoped_range_staged_puts, mut scoped_range_staged_bytes, _, _) =
        publish_bench_current_state_commit(
            &storage,
            writes,
            None,
            parent_id,
            1,
            staged.mutation_inventory().clone(),
        )
        .await;
    let mut commits = vec![parent_id];

    for index in 1..scope_count {
        let commit_id = bench_addressable_commit_id(&format!("scoped-range-scope-{index}"));
        let schema_key = format!("bench_scope_{index:08}");
        let file_id = format!("file-{:05}", index % 10_000);
        let row_pk = RowPk::single("row");
        let scope = super::types::CommitDeltaReplacementScope {
            schema_key: schema_key.clone(),
            file_id: Some(file_id.clone()),
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
        let row = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &schema_key,
                file_id: Some(&file_id),
                row_pk: &row_pk,
                change_id: ChangeId::for_test_label(&format!("scoped-scope-change-{index}")),
                commit_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{}"),
            typed_snapshot: None,
            typed_payload: None,
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let mut writes = storage.new_write_set();
        let staged = super::storage::stage_ordered_addressable_replacement_parts(
            &mut writes,
            std::iter::once(Ok(row)),
            &generation,
        )
        .expect("stage benchmark scope replacement");
        let (manifest, puts, bytes, _, _) = publish_bench_current_state_commit(
            &storage,
            writes,
            Some(&current_manifest),
            commit_id,
            u16::try_from(index + 1)
                .expect("benchmark scope depth fits u16")
                .min(super::COMMIT_STATE_MAX_REPLAY_DEPTH),
            staged.mutation_inventory().clone(),
        )
        .await;
        current_manifest = manifest;
        scoped_range_staged_puts = scoped_range_staged_puts.saturating_add(puts);
        scoped_range_staged_bytes = scoped_range_staged_bytes.saturating_add(bytes);
        commits.push(commit_id);
    }
    let base_manifest = current_manifest.clone();

    let beta_pk = RowPk::single("unrelated");
    let mut scoped_manifest_bytes = 0u64;
    let mut replay_manifest_bytes = 0u64;
    let mut sparse_scoped_range_staged_puts = 0u64;
    let mut sparse_scoped_range_staged_bytes = 0u64;
    let mut sparse_staged_puts = 0u64;
    let mut sparse_written_bytes = 0u64;
    let mut sparse_publication_nanos = Vec::with_capacity(unrelated_sparse_commits);
    let mut first_sparse_elapsed_nanos = 0u64;
    let mut first_sparse_staged_puts = 0u64;
    let mut first_sparse_written_bytes = 0u64;
    for index in 0..unrelated_sparse_commits {
        let sparse_started = Instant::now();
        let commit_id = bench_addressable_commit_id(&format!("scoped-range-child-{index}"));
        let touches_alpha = matches!(
            sparse_shape,
            BenchCurrentStateSparseShape::TouchedScope
                | BenchCurrentStateSparseShape::TouchedScopeDistinct
        );
        let present_scope = !touches_alpha && scope_count > 1 && index % 4 == 0;
        let schema_key = if touches_alpha {
            "bench_current_state_alpha".to_string()
        } else if present_scope {
            format!("bench_scope_{:08}", 1 + index % (scope_count - 1))
        } else {
            "bench_current_state_beta".to_string()
        };
        let file_id =
            present_scope.then(|| format!("file-{:05}", (1 + index % (scope_count - 1)) % 10_000));
        let sparse_pk = if sparse_shape == BenchCurrentStateSparseShape::TouchedScopeDistinct {
            &row_pks[(replacement_rows / 4 + index) % replacement_rows]
        } else if touches_alpha {
            &row_pks[replacement_rows / 4]
        } else {
            &beta_pk
        };
        let row = TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &schema_key,
                file_id: file_id.as_deref(),
                row_pk: sparse_pk,
                change_id: ChangeId::for_test_label(&format!("scoped-child-change-{index}")),
                commit_id,
                deleted: false,
                created_at,
                updated_at,
            },
            snapshot: JsonSlotRef::Inline("{}"),
            typed_snapshot: None,
            typed_payload: None,
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        };
        let mut writes = storage.new_write_set();
        let staged = super::storage::stage_ordered_addressable_commit_deltas(
            &mut writes,
            std::iter::once(Ok(row)),
            true,
            false,
        )
        .expect("stage benchmark sparse mutation")
        .expect("benchmark sparse mutation is addressable");
        let (manifest, range_puts, range_bytes, publication_nanos, stats) =
            publish_bench_current_state_commit(
                &storage,
                writes,
                Some(&current_manifest),
                commit_id,
                u16::try_from(commits.len() + 1)
                    .expect("benchmark replay depth fits u16")
                    .min(super::COMMIT_STATE_MAX_REPLAY_DEPTH),
                staged.mutation_inventory().clone(),
            )
            .await;
        let elapsed = u64::try_from(sparse_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
        sparse_publication_nanos.push(publication_nanos);
        sparse_scoped_range_staged_puts =
            sparse_scoped_range_staged_puts.saturating_add(range_puts);
        sparse_scoped_range_staged_bytes =
            sparse_scoped_range_staged_bytes.saturating_add(range_bytes);
        sparse_staged_puts = sparse_staged_puts.saturating_add(stats.staged_puts);
        sparse_written_bytes = sparse_written_bytes.saturating_add(stats.written_bytes);
        scoped_manifest_bytes = scoped_manifest_bytes.saturating_add(
            u64::try_from(bench_manifest_encoded_len(&manifest))
                .expect("benchmark manifest length fits u64"),
        );
        let mut replay_manifest = manifest.clone();
        replay_manifest.current_state_scoped_ranges = None;
        replay_manifest_bytes = replay_manifest_bytes.saturating_add(
            u64::try_from(bench_manifest_encoded_len(&replay_manifest))
                .expect("benchmark replay manifest length fits u64"),
        );
        if index == 0 {
            first_sparse_elapsed_nanos = elapsed;
            first_sparse_staged_puts = stats.staged_puts;
            first_sparse_written_bytes = stats.written_bytes;
        }
        current_manifest = manifest;
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

    let target_index = if sparse_shape == BenchCurrentStateSparseShape::TouchedScopeDistinct
        && point_target == BenchCurrentStatePointTarget::HotMutated
    {
        (replacement_rows / 4 + unrelated_sparse_commits.saturating_sub(1)) % replacement_rows
    } else if sparse_shape == BenchCurrentStateSparseShape::TouchedScope
        && point_target == BenchCurrentStatePointTarget::HotMutated
    {
        replacement_rows / 4
    } else {
        replacement_rows / 2
    };
    let encoded_key = bytes::Bytes::from(super::codec::encode_key_ref(
        super::types::TrackedStateKeyRef {
            schema_key: "bench_current_state_alpha",
            file_id: None,
            row_pk: &row_pks[target_index],
        },
    ));
    let current_state_part_count = current_manifest
        .current_state_scoped_ranges
        .as_ref()
        .map_or(0, |root| u64::from(root.tree.part_count));
    BenchCurrentStatePointFixture {
        storage,
        commits,
        base_manifest,
        current_manifest,
        encoded_key,
        scope_count,
        scoped_manifest_bytes,
        replay_manifest_bytes,
        scoped_range_staged_puts,
        scoped_range_staged_bytes,
        sparse_scoped_range_staged_puts,
        sparse_scoped_range_staged_bytes,
        sparse_staged_puts,
        sparse_written_bytes,
        sparse_publication_p50_nanos,
        sparse_publication_p95_nanos,
        first_sparse_elapsed_nanos,
        first_sparse_staged_puts,
        first_sparse_written_bytes,
        current_state_part_count,
    }
}

async fn publish_bench_current_state_commit<StorageImpl: Storage>(
    storage: &StorageAdapter<StorageImpl>,
    mut writes: StorageWriteSet,
    parent: Option<&CommitStateManifest>,
    commit_id: CommitId,
    replay_depth: u16,
    mut mutations: super::types::CommitStateMutationInventory,
) -> (CommitStateManifest, u64, u64, u64, StorageWriteSetStats) {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open benchmark scoped-range parent read");
    let published_parent = match parent {
        Some(parent) => Some(
            super::storage::load_published_commit_state_manifest(&read, parent.commit_id)
                .await
                .expect("load benchmark scoped-range parent")
                .expect("benchmark scoped-range parent exists"),
        ),
        None => None,
    };
    let before = writes.stats();
    let publication_started = Instant::now();
    let publication = super::storage::stage_current_state_scoped_ranges_from_published_parent(
        &read,
        &mut writes,
        published_parent.as_ref(),
        commit_id,
        crate::ANONYMOUS_ACCOUNT_ID,
        &mutations,
    )
    .await
    .expect("stage benchmark production scoped-range publication");
    let publication_nanos =
        u64::try_from(publication_started.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let after = writes.stats();
    let range_puts = after.staged_puts.saturating_sub(before.staged_puts);
    let range_bytes = after.written_bytes.saturating_sub(before.written_bytes);
    if mutations.replacement_generation.is_some() {
        mutations.parts.clear();
    }
    let manifest = bench_current_state_manifest(
        commit_id,
        parent.map(|parent| parent.commit_id),
        replay_depth,
        mutations,
        publication.touched_scope_filter().clone(),
        publication.root(),
    );
    super::storage::stage_certified_commit_state_manifest(&mut writes, &manifest, &publication)
        .expect("stage benchmark certified scoped-range manifest");
    drop(read);
    let (_, stats) = storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit benchmark scoped-range authority");
    (manifest, range_puts, range_bytes, publication_nanos, stats)
}

impl<StorageImpl> BenchCurrentStatePointFixture<StorageImpl>
where
    StorageImpl: Storage,
{
    /// Appends one production-shaped empty graph merge whose first parent is
    /// the fixture's current state. Before serving-base lineage this topology
    /// edge discarded the physical root and forced replay; the v56 path
    /// re-attests the unchanged tree against the merge authority.
    pub async fn append_empty_merge(mut self) -> Self {
        let other_id = bench_addressable_commit_id("scoped-range-merge-other");
        let other = bench_current_state_manifest(
            other_id,
            None,
            1,
            super::types::CommitStateMutationInventory::default(),
            Default::default(),
            None,
        );
        let mut other_writes = self.storage.new_write_set();
        super::storage::stage_commit_state_manifest(&mut other_writes, &other)
            .expect("stage benchmark merge parent authority");
        self.storage
            .commit_write_set(other_writes, StorageWriteOptions::default())
            .await
            .expect("commit benchmark merge parent authority");

        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("open benchmark merge publication read");
        let current = super::storage::load_published_commit_state_manifest(
            &read,
            self.current_manifest.commit_id,
        )
        .await
        .expect("load benchmark merge target")
        .expect("benchmark merge target exists");
        let other = super::storage::load_published_commit_state_manifest(&read, other_id)
            .await
            .expect("load benchmark second merge parent")
            .expect("benchmark second merge parent exists");
        let merge_id = bench_addressable_commit_id("scoped-range-empty-merge");
        let mutations = super::types::CommitStateMutationInventory::default();
        let mut writes = self.storage.new_write_set();
        let publication = super::storage::stage_current_state_scoped_ranges_from_topology(
            &read,
            &mut writes,
            &[
                super::storage::CertifiedCommitStateTopologyParent::Published(&current),
                super::storage::CertifiedCommitStateTopologyParent::Published(&other),
            ],
            None,
            merge_id,
            crate::ANONYMOUS_ACCOUNT_ID,
            &mutations,
        )
        .await
        .expect("stage benchmark merge serving root");
        let merged = CommitStateManifest {
            commit_id: merge_id,
            change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            replay_debt: CommitStateReplayDebt {
                depth: self
                    .current_manifest
                    .replay_debt
                    .depth
                    .saturating_add(1)
                    .min(super::COMMIT_STATE_MAX_REPLAY_DEPTH),
                rows: self.current_manifest.replay_debt.rows,
                bytes: self.current_manifest.replay_debt.bytes,
            },
            mutations,
            touched_scope_filter: publication.touched_scope_filter().clone(),
            current_state_scoped_ranges: publication.root(),
            snapshot_root: None,
        };
        super::storage::stage_certified_commit_state_manifest(&mut writes, &merged, &publication)
            .expect("stage certified benchmark merge manifest");
        drop(read);
        self.storage
            .commit_write_set(writes, StorageWriteOptions::default())
            .await
            .expect("commit benchmark merge serving root");
        self.current_manifest = merged;
        self.commits.push(merge_id);
        self.current_state_part_count = self
            .current_manifest
            .current_state_scoped_ranges
            .as_ref()
            .map_or(0, |root| u64::from(root.tree.part_count));
        self
    }

    pub fn scope_count(&self) -> usize {
        self.scope_count
    }

    pub fn scoped_manifest_bytes(&self) -> u64 {
        self.scoped_manifest_bytes
    }

    pub fn replay_manifest_bytes(&self) -> u64 {
        self.replay_manifest_bytes
    }

    pub fn scoped_range_staged_puts(&self) -> u64 {
        self.scoped_range_staged_puts
    }

    pub fn scoped_range_staged_bytes(&self) -> u64 {
        self.scoped_range_staged_bytes
    }

    pub fn sparse_scoped_range_staged_puts(&self) -> u64 {
        self.sparse_scoped_range_staged_puts
    }

    pub fn sparse_scoped_range_staged_bytes(&self) -> u64 {
        self.sparse_scoped_range_staged_bytes
    }

    pub fn sparse_staged_puts(&self) -> u64 {
        self.sparse_staged_puts
    }

    pub fn sparse_written_bytes(&self) -> u64 {
        self.sparse_written_bytes
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

    pub fn current_state_part_count(&self) -> u64 {
        self.current_state_part_count
    }

    pub async fn read_point(&self, mode: BenchCurrentStatePointMode) -> usize {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin benchmark current-state point read");
        match mode {
            BenchCurrentStatePointMode::ScopedRange => {
                let state = super::storage::load_published_commit_state_manifest(
                    &read,
                    self.current_manifest.commit_id,
                )
                .await
                .expect("load benchmark scoped-range manifest")
                .expect("benchmark scoped-range manifest exists");
                let values =
                    super::storage::load_complete_current_state_values_from_published_manifest(
                        &read,
                        &state,
                        std::slice::from_ref(&self.encoded_key),
                    )
                    .await
                    .expect("read benchmark production scoped-range tree")
                    .expect("benchmark production scope is covered");
                usize::from(values[0].is_some())
            }
            BenchCurrentStatePointMode::FirstParentReplay => self.read_point_by_replay(&read).await,
        }
    }

    /// Hashes the production scope scan at both benchmark endpoints. This is
    /// useful for profiling broad serving-index reads without retaining the
    /// removed layout-specific diff mini-engine.
    pub async fn scan_current_state_scope(&self) -> [u8; 32] {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin benchmark current-state scope scan");
        let scope = super::types::CommitDeltaReplacementScope {
            schema_key: "bench_current_state_alpha".to_string(),
            file_id: None,
        };
        let prefix = super::current_state_envelope::current_state_scope_prefix(&scope)
            .expect("encode benchmark scope prefix");
        let mut digest =
            blake3::Hasher::new_derive_key("lix benchmark current-state scoped-range scan v1");
        for manifest in [&self.base_manifest, &self.current_manifest] {
            let published =
                super::storage::load_published_commit_state_manifest(&read, manifest.commit_id)
                    .await
                    .expect("load benchmark scope-scan manifest")
                    .expect("benchmark scope-scan manifest exists");
            let root = published
                .current_state_scoped_ranges
                .as_ref()
                .expect("benchmark scope-scan root exists");
            let scanned = super::scoped_range::scan_scoped_range_scope(&read, &root.tree, &prefix)
                .await
                .expect("scan benchmark production scope");
            match scanned.coverage {
                Some(marker) => {
                    digest.update(&[1]);
                    digest.update(&marker.row_count.to_be_bytes());
                    digest.update(&marker.part_count.to_be_bytes());
                }
                None => {
                    digest.update(&[0]);
                }
            }
            digest.update(&(scanned.parts.len() as u64).to_be_bytes());
            for part in scanned.parts {
                for bytes in [&part.first_key, &part.last_key, &part.payload.bytes] {
                    digest.update(&(bytes.len() as u64).to_be_bytes());
                    digest.update(bytes);
                }
                digest.update(&part.row_count.to_be_bytes());
                digest.update(&part.payload.version.to_be_bytes());
            }
        }
        *digest.finalize().as_bytes()
    }

    async fn read_point_by_replay(&self, read: &(impl StorageAdapterRead + ?Sized)) -> usize {
        let point_cache = super::storage::CommitDeltaPointReadCache::default();
        for commit_id in self.commits.iter().rev() {
            let Some(state) = super::storage::load_point_replay_commit_state(read, *commit_id)
                .await
                .expect("load benchmark replay authority")
            else {
                continue;
            };
            let values = super::storage::load_commit_delta_values_encoded_from_replay_manifest(
                read,
                &state,
                std::slice::from_ref(&self.encoded_key),
                &point_cache,
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
    _parent_commit_id: Option<CommitId>,
    replay_depth: u16,
    mutations: super::types::CommitStateMutationInventory,
    touched_scope_filter: super::types::CommitStateTouchedScopeFilter,
    current_state_scoped_ranges: Option<Box<super::types::CurrentStateScopedRangeRoot>>,
) -> CommitStateManifest {
    CommitStateManifest {
        commit_id,
        change_account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
        replay_debt: CommitStateReplayDebt {
            depth: replay_depth,
            rows: u64::from(mutations.member_count),
            bytes: u64::from(mutations.member_count),
        },
        mutations,
        touched_scope_filter,
        current_state_scoped_ranges,
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
    pub live_rows: usize,
    pub shared_large_payload: Option<&'a str>,
}

impl Default for BenchPackedHistoryOptions<'_> {
    fn default() -> Self {
        Self {
            shape: BenchPackedHistoryShape::UniqueInserts,
            payload: BenchPackedHistoryPayload::None,
            live_rows: 10_000,
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
    validate_packed_history_shape(changes, commit_width, options.shape, options.live_rows);
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
    live_rows: usize,
) {
    let required_generations = match shape {
        BenchPackedHistoryShape::UniqueInserts => return,
        BenchPackedHistoryShape::RepeatedUpdates => 2,
        BenchPackedHistoryShape::DeleteReinsert => 3,
    };
    assert!(
        live_rows >= commit_width,
        "repeated packed-history shapes require at least one live row per commit row"
    );
    let required_changes = live_rows
        .checked_mul(required_generations)
        .expect("packed-history generation size should not overflow");
    assert!(
        changes >= required_changes,
        "{shape:?} requires at least {required_generations} complete generations \
         ({required_changes} changes for {live_rows} live rows)"
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
    row_pk: RowPk,
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
        let row_pk = match options.shape {
            BenchPackedHistoryShape::UniqueInserts => {
                format!("packed-history-row-{commit_index:012}-{row_index:012}")
            }
            BenchPackedHistoryShape::RepeatedUpdates | BenchPackedHistoryShape::DeleteReinsert => {
                format!(
                    "packed-history-row-{:012}",
                    change_index % options.live_rows
                )
            }
        };
        let deleted = matches!(options.shape, BenchPackedHistoryShape::DeleteReinsert)
            && (change_index / options.live_rows) % 2 == 1;
        Self {
            change_id: packed_history_change_id(commit_index, row_index),
            commit_id: packed_history_commit_id(commit_index),
            row_pk: RowPk::single(row_pk),
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
                row_pk: &self.row_pk,
                change_id: self.change_id,
                commit_id: self.commit_id,
                deleted: self.deleted,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
            snapshot,
            typed_snapshot: None,
            typed_payload: None,
            metadata: JsonSlotRef::None,
            origin_key: None,
            base_coordinate: None,
            authored: true,
        }
    }
}

fn packed_history_change_id(commit_index: usize, row_index: usize) -> ChangeId {
    if packed_history_addressable_ids() {
        return super::storage::change_id_from_packed_address(
            packed_history_commit_id(commit_index),
            u32::try_from(row_index)
                .expect("packed-history row index fits u32")
                .checked_add(1)
                .expect("packed-history packed address fits u32"),
        );
    }
    ChangeId::new(packed_history_uuid(commit_index, row_index, 0x68))
}

fn packed_history_commit_id(commit_index: usize) -> CommitId {
    let mut bytes = *packed_history_uuid(commit_index, 0, 0x43).as_bytes();
    if packed_history_addressable_ids() {
        bytes[12..].copy_from_slice(&0_u32.to_be_bytes());
    }
    CommitId::new(uuid::Uuid::from_bytes(bytes))
}

fn packed_history_addressable_ids() -> bool {
    std::env::var("LIX_PACKED_HISTORY_ID_SHAPE").is_ok_and(|shape| shape == "addressable")
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
            live_rows: 10,
            shared_large_payload: matches!(payload, BenchPackedHistoryPayload::SharedLarge)
                .then_some(r#"{"payload":"large"}"#),
        }
    }

    #[test]
    fn repeated_updates_reuse_row_identity() {
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

        assert_eq!(first.row_pk, second.row_pk);
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

        assert_eq!(insert.row_pk, delete.row_pk);
        assert_eq!(delete.row_pk, reinsert.row_pk);
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
    row_pk: RowPk,
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
            row_pk: RowPk::single(row.row_pk),
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
            row_pk: &self.row_pk,
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
        row_pk: RowPk::single(row.row_pk.clone()),
        file_id: row.file_id.clone(),
    }
}
use std::time::Instant;
