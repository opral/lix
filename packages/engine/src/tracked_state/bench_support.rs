use crate::changelog::{ChangeId, CommitId};
use crate::entity_pk::EntityPk;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageReadOptions, StorageWriteOptions,
    StorageWriteSetStats,
};
use crate::tracked_state::{
    TrackedStateCommitDeltaRef, TrackedStateContext, TrackedStateDeltaRef, TrackedStateFilter,
    TrackedStateKey, TrackedStateReadColumns, TrackedStateScanRequest,
};

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

    let commits = changes / commit_width;
    let commits_per_storage_batch = (storage_batch_changes / commit_width).max(1);
    let mut staged_puts = 0;
    let mut written_bytes = 0;
    for commit_batch_start in (0..commits).step_by(commits_per_storage_batch) {
        let commit_batch_end = (commit_batch_start + commits_per_storage_batch).min(commits);
        let mut writes = storage.new_write_set();
        for commit_index in commit_batch_start..commit_batch_end {
            let owned = (0..commit_width)
                .map(|row_index| PackedHistoryDelta::new(commit_index, row_index))
                .collect::<Vec<_>>();
            let deltas = owned
                .iter()
                .map(PackedHistoryDelta::as_commit_ref)
                .collect::<Vec<_>>();
            super::storage::stage_commit_deltas(&mut writes, &deltas)
                .expect("stage packed-history commit delta");
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
    super::storage::scan_change_records_from_commit_deltas(&read)
        .await
        .expect("scan packed history")
        .len()
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
    created_at: crate::common::LixTimestamp,
    updated_at: crate::common::LixTimestamp,
}

impl PackedHistoryDelta {
    fn new(commit_index: usize, row_index: usize) -> Self {
        Self {
            change_id: ChangeId::for_test_label(&format!(
                "packed-history-change-{commit_index}-{row_index}"
            )),
            commit_id: CommitId::for_test_label(&format!("packed-history-commit-{commit_index}")),
            entity_pk: EntityPk::single(format!(
                "packed-history-entity-{commit_index}-{row_index}"
            )),
            schema_key: "packed_history".to_string(),
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
        TrackedStateCommitDeltaRef {
            delta: TrackedStateDeltaRef {
                schema_key: &self.schema_key,
                file_id: None,
                entity_pk: &self.entity_pk,
                change_id: self.change_id,
                commit_id: self.commit_id,
                deleted: false,
                created_at: self.created_at,
                updated_at: self.updated_at,
            },
            snapshot: crate::json_store::JsonSlotRef::None,
            metadata: crate::json_store::JsonSlotRef::None,
            origin_key: None,
        }
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
        _writes: &mut crate::storage_adapter::StorageWriteSet,
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
