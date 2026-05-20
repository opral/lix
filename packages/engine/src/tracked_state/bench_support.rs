use crate::changelog::{Change, ChangeLocator, SegmentObjectLocation};
use crate::common::NullableKeyFilter;
use crate::entity_identity::EntityIdentity;
use crate::json_store::{JsonStoreContext, JsonWritePlacementRef, NormalizedJsonRef};
use crate::storage::{
    StorageBackend, StorageBackendRead, StorageBackendReadOf, StorageContext, StorageRead,
    StorageReadOptions, StorageReadScope, StorageWriteOptions, StorageWriteSetStats,
};
use crate::tracked_state::{
    TrackedStateContext, TrackedStateDeltaRef, TrackedStateFilter, TrackedStateProjection,
    TrackedStateRowRequest, TrackedStateScanRequest,
};

#[derive(Clone)]
pub struct BenchTrackedRow {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub entity_id: String,
    pub value: Vec<u8>,
    pub updated_value: Vec<u8>,
}

pub struct BenchTrackedFixture<B: StorageBackend> {
    storage: StorageContext<B>,
    context: TrackedStateContext,
    rows: Vec<BenchTrackedRow>,
    current_commit_id: Option<String>,
    next_commit_index: usize,
}

#[derive(Clone, Debug)]
pub struct BenchWriteAccounting {
    pub logical_rows: usize,
    pub staged_puts: u64,
    pub staged_deletes: u64,
    pub touched_spaces: u64,
    pub backend_calls: u64,
    pub put_batches: u64,
    pub delete_batches: u64,
    pub written_bytes: u64,
}

#[derive(Clone, Debug)]
pub struct BenchLayoutAccounting {
    pub space_id: u32,
    pub space: &'static str,
    pub rows: u64,
    pub key_bytes: u64,
    pub value_bytes: u64,
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
            backend_calls: self.stats.backend_calls,
            put_batches: self.stats.put_batches,
            delete_batches: self.stats.delete_batches,
            written_bytes: self.stats.written_bytes,
        }
    }
}

struct BenchRead<R> {
    inner: StorageReadScope<R>,
}

impl<R> BenchRead<R> {
    fn new(inner: StorageReadScope<R>) -> Self {
        Self { inner }
    }
}

// The storage bench runs these fixtures on one thread. This wrapper lets the
// tracked-state internals use their existing `Send + Sync` reader bound with
// test backends such as SQLite whose read handle is not `Sync`.
unsafe impl<R: Send> Send for BenchRead<R> {}
unsafe impl<R: Send> Sync for BenchRead<R> {}

impl<R> StorageRead for BenchRead<R>
where
    R: StorageBackendRead,
{
    type BackendRead = R;

    fn backend_read(&self) -> &Self::BackendRead {
        self.inner.backend_read()
    }
}

impl<B> BenchTrackedFixture<B>
where
    B: StorageBackend,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    pub fn new(storage: StorageContext<B>, rows: Vec<BenchTrackedRow>) -> Self {
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
        let read = BenchRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .expect("begin tracked-state read"),
        );
        let mut reader = self.context.reader(read);
        let rows = reader
            .scan_rows_at_commit(
                self.current_commit_id(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter::default(),
                    projection: TrackedStateProjection::default(),
                    limit: None,
                },
            )
            .await
            .expect("scan tracked-state rows");
        assert_eq!(rows.len(), self.rows.len());
        rows.len()
    }

    pub async fn read_all_by_pk(&self) -> usize {
        let requests = self
            .rows
            .iter()
            .map(row_request)
            .collect::<Result<Vec<_>, _>>()
            .expect("build tracked-state row requests");
        self.read_by_pk(&requests).await
    }

    pub async fn read_one_by_pk(&self) -> usize {
        let request =
            row_request(&self.rows[self.rows.len() / 2]).expect("build tracked-state row request");
        self.read_by_pk(&[request]).await
    }

    async fn read_by_pk(&self, requests: &[TrackedStateRowRequest]) -> usize {
        let read = BenchRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .expect("begin tracked-state read"),
        );
        let mut reader = self.context.reader(read);
        let rows = reader
            .load_rows_at_commit(self.current_commit_id(), requests)
            .await
            .expect("load tracked-state rows");
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
            let read = BenchRead::new(
                self.storage
                    .begin_read(StorageReadOptions::default())
                    .expect("begin tracked-state write read"),
            );
            let mut writer = self.context.writer(&read, &mut writes);
            writer
                .stage_projection_root(&commit_id, parent_commit_id.as_deref(), deltas)
                .await
                .expect("stage tracked-state projection root");
        }
        let (_commit, stats) = self
            .storage
            .commit_write_set(writes, StorageWriteOptions::default())
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

    pub fn layout_accounting(&self) -> Vec<BenchLayoutAccounting> {
        let read = BenchRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .expect("begin tracked-state layout accounting read"),
        );
        crate::storage_bench::layout_accounting(&read)
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
    change: Change,
    locator: ChangeLocator,
    created_at: String,
    updated_at: String,
}

impl OwnedDelta {
    fn new(
        row: BenchTrackedRow,
        commit_id: &str,
        index: usize,
        deleted: bool,
        writes: &mut crate::storage::StorageWriteSet,
    ) -> Self {
        let snapshot_ref = if deleted {
            None
        } else {
            let json = std::str::from_utf8(&row.value)
                .expect("tracked-state bench row payload should be UTF-8 JSON");
            let mut json_writer = JsonStoreContext::new().writer();
            let refs = json_writer
                .stage_batch(
                    writes,
                    JsonWritePlacementRef::OutOfBand,
                    [NormalizedJsonRef::new(json)],
                )
                .expect("stage tracked-state bench JSON payload");
            Some(refs[0])
        };
        let change_id = format!("tracked-crud-change-{commit_id}-{index}");
        let change = Change {
            id: change_id.clone(),
            authored_commit_id: Some(commit_id.to_string()),
            entity_id: EntityIdentity::single(row.entity_id),
            schema_key: row.schema_key,
            file_id: row.file_id,
            snapshot_ref,
            metadata_ref: None,
            created_at: "2026-05-19T00:00:00.000Z".to_string(),
        };
        let locator = ChangeLocator {
            change_id,
            commit_id: commit_id.to_string(),
            location: SegmentObjectLocation {
                segment_id: format!("tracked-crud-segment-{commit_id}"),
                offset: 0,
                len: 0,
                checksum: "bench".to_string(),
            },
        };
        Self {
            change,
            locator,
            created_at: "2026-05-19T00:00:00.000Z".to_string(),
            updated_at: "2026-05-19T00:00:00.000Z".to_string(),
        }
    }

    fn as_ref(&self) -> TrackedStateDeltaRef<'_> {
        TrackedStateDeltaRef {
            change: self.change.as_ref(),
            locator: self.locator.as_ref(),
            created_at: &self.created_at,
            updated_at: &self.updated_at,
        }
    }
}

fn row_request(row: &BenchTrackedRow) -> Result<TrackedStateRowRequest, crate::LixError> {
    Ok(TrackedStateRowRequest {
        schema_key: row.schema_key.clone(),
        entity_id: EntityIdentity::single(row.entity_id.clone()),
        file_id: NullableKeyFilter::from_nullable(row.file_id.clone()),
    })
}
