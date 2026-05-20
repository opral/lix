use std::sync::Arc;

use bytes::Bytes;
use serde_json::{json, Value as JsonValue};

use crate::binary_cas::BinaryCasContext;
use crate::catalog::CatalogContext;
use crate::entity_identity::EntityIdentity;
use crate::live_state::{
    LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateRowRequest,
    LiveStateScanRequest,
};
use crate::session::SessionMode;
use crate::storage::{
    ScanPlan, StorageBackend, StorageBackendReadOf, StorageContext, StorageCoreProjection,
    StoragePrefix, StorageRead, StorageReadOptions, StorageReadScope, StorageScanOptions,
    StorageSpace, StorageWriteOptions, StorageWriteSet, StorageWriteSetStats,
};
use crate::tracked_state::TrackedStateContext;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::untracked_state::UntrackedStateContext;
use crate::version::VersionContext;
use crate::{BackendRead, NullableKeyFilter, GLOBAL_VERSION_ID};

const SCHEMA_FIXTURE_COMMIT_ID: &str = "tracked-crud-schema-fixture";
const TIMESTAMP: &str = "2026-05-19T00:00:00.000Z";
const BENCH_VERSION_ID: &str = "tracked-crud-version";

#[derive(Clone)]
pub struct BenchTransactionRow {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub entity_id: String,
    pub value: JsonValue,
    pub updated_value: JsonValue,
}

pub struct BenchTransactionFixture<B: StorageBackend> {
    storage: StorageContext<B>,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    version_ctx: Arc<VersionContext>,
    catalog_context: Arc<CatalogContext>,
    rows: Vec<BenchTransactionRow>,
}

struct BenchRead<R> {
    inner: StorageReadScope<R>,
}

impl<R> BenchRead<R> {
    fn new(inner: StorageReadScope<R>) -> Self {
        Self { inner }
    }
}

unsafe impl<R: Send> Send for BenchRead<R> {}
unsafe impl<R: Send> Sync for BenchRead<R> {}

impl<R> StorageRead for BenchRead<R>
where
    R: BackendRead,
{
    type BackendRead = R;

    fn backend_read(&self) -> &Self::BackendRead {
        self.inner.backend_read()
    }
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

impl<B> BenchTransactionFixture<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Clone + Send + Sync + 'static,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn new(storage: StorageContext<B>, rows: Vec<BenchTransactionRow>) -> Self {
        let tracked_state = Arc::new(TrackedStateContext::new());
        let live_state = Arc::new(LiveStateContext::new(
            tracked_state.as_ref().clone(),
            UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        ));
        let version_ctx = Arc::new(VersionContext::new(Arc::new(UntrackedStateContext::new())));
        seed_visible_schema_rows(storage.clone(), tracked_state.as_ref()).await;
        Self {
            storage,
            live_state,
            tracked_state,
            binary_cas: Arc::new(BinaryCasContext::new()),
            version_ctx,
            catalog_context: Arc::new(CatalogContext::new()),
            rows,
        }
    }

    pub async fn seed(&mut self) -> usize {
        self.insert_all().await
    }

    pub async fn insert_all(&mut self) -> usize {
        self.insert_all_accounting().await.logical_rows
    }

    pub async fn insert_all_accounting(&mut self) -> BenchWriteAccounting {
        let rows = self
            .rows
            .iter()
            .map(|row| transaction_row(row, &row.value))
            .collect();
        self.commit_rows(rows).await
    }

    pub async fn update_all(&mut self) -> usize {
        self.update_all_accounting().await.logical_rows
    }

    pub async fn update_all_accounting(&mut self) -> BenchWriteAccounting {
        let rows = self
            .rows
            .iter()
            .map(|row| transaction_row(row, &row.updated_value))
            .collect();
        self.commit_rows(rows).await
    }

    pub async fn update_one_by_pk(&mut self) -> usize {
        self.update_one_by_pk_accounting().await.logical_rows
    }

    pub async fn update_one_by_pk_accounting(&mut self) -> BenchWriteAccounting {
        let row = &self.rows[self.rows.len() / 2];
        self.commit_rows(vec![transaction_row(row, &row.updated_value)])
            .await
    }

    pub async fn delete_all(&mut self) -> usize {
        self.delete_all_accounting().await.logical_rows
    }

    pub async fn delete_all_accounting(&mut self) -> BenchWriteAccounting {
        let rows = self.rows.iter().map(transaction_delete_row).collect();
        self.commit_rows(rows).await
    }

    pub async fn delete_one_by_pk(&mut self) -> usize {
        self.delete_one_by_pk_accounting().await.logical_rows
    }

    pub async fn delete_one_by_pk_accounting(&mut self) -> BenchWriteAccounting {
        let row = &self.rows[self.rows.len() / 2];
        self.commit_rows(vec![transaction_delete_row(row)]).await
    }

    pub async fn read_all(&self) -> usize {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("begin transaction bench read");
        let rows = self
            .live_state
            .reader(&read)
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["json_pointer".to_string()],
                    version_ids: vec![BENCH_VERSION_ID.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones: false,
                    ..LiveStateFilter::default()
                },
                projection: LiveStateProjection::default(),
                limit: None,
            })
            .await
            .expect("scan transaction bench rows");
        assert_eq!(rows.len(), self.rows.len());
        rows.len()
    }

    pub async fn read_all_by_pk(&self) -> usize {
        let mut count = 0;
        for row in &self.rows {
            count += self.read_one(row).await;
        }
        count
    }

    pub async fn read_one_by_pk(&self) -> usize {
        self.read_one(&self.rows[self.rows.len() / 2]).await
    }

    async fn read_one(&self, row: &BenchTransactionRow) -> usize {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("begin transaction bench read");
        let row = self
            .live_state
            .reader(&read)
            .load_row(&LiveStateRowRequest {
                schema_key: "json_pointer".to_string(),
                version_id: BENCH_VERSION_ID.to_string(),
                entity_id: EntityIdentity::single(row.entity_id.clone()),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("load transaction bench row");
        assert!(row.is_some());
        1
    }

    async fn commit_rows(&mut self, rows: Vec<TransactionWriteRow>) -> BenchWriteAccounting {
        let logical_rows = rows.len();
        let opened = super::open_transaction(
            &SessionMode::Pinned {
                version_id: BENCH_VERSION_ID.to_string(),
            },
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.version_ctx),
            Arc::clone(&self.catalog_context),
        )
        .await
        .expect("open transaction bench transaction");
        let mut transaction = opened.transaction;
        transaction
            .stage_rows(rows)
            .await
            .expect("stage transaction bench rows");
        let outcome = transaction
            .commit(&opened.runtime_functions)
            .await
            .expect("commit transaction bench rows");
        write_accounting(logical_rows, outcome.storage_stats)
    }

    pub fn layout_accounting(&self) -> Vec<BenchLayoutAccounting> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .expect("begin transaction layout accounting read");
        native_storage_spaces()
            .iter()
            .map(|space| scan_layout_space(&read, *space))
            .collect()
    }
}

fn write_accounting(logical_rows: usize, stats: StorageWriteSetStats) -> BenchWriteAccounting {
    BenchWriteAccounting {
        logical_rows,
        staged_puts: stats.staged_puts,
        staged_deletes: stats.staged_deletes,
        touched_spaces: stats.touched_spaces,
        backend_calls: stats.backend_calls,
        put_batches: stats.put_batches,
        delete_batches: stats.delete_batches,
        written_bytes: stats.written_bytes,
    }
}

fn transaction_row(row: &BenchTransactionRow, value: &JsonValue) -> TransactionWriteRow {
    TransactionWriteRow {
        entity_id: Some(EntityIdentity::single(row.entity_id.clone())),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot: Some(TransactionJson::from_value_unchecked(value.clone())),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        version_id: BENCH_VERSION_ID.to_string(),
    }
}

fn transaction_delete_row(row: &BenchTransactionRow) -> TransactionWriteRow {
    let mut out = transaction_row(row, &row.value);
    out.snapshot = None;
    out
}

async fn seed_visible_schema_rows<B>(
    storage: StorageContext<B>,
    tracked_state: &TrackedStateContext,
) where
    B: StorageBackend + Clone,
    for<'a> StorageBackendReadOf<'a, B>: Send,
{
    let mut writes = StorageWriteSet::new();
    let mut schemas = crate::schema::seed_schema_definitions()
        .into_iter()
        .cloned()
        .collect::<Vec<_>>();
    schemas.push(json_pointer_schema());
    let rows = schemas
        .iter()
        .map(|schema| {
            let key = crate::schema::schema_key_from_definition(schema)
                .expect("seed schema key should derive");
            let snapshot_content = json!({ "value": schema }).to_string();
            crate::tracked_state::MaterializedTrackedStateRow {
                entity_id: crate::schema::registered_schema_entity_id(&key.schema_key)
                    .expect("registered schema identity should derive"),
                schema_key: "lix_registered_schema".to_string(),
                file_id: None,
                snapshot_content: Some(snapshot_content),
                metadata: None,
                deleted: false,
                created_at: TIMESTAMP.to_string(),
                updated_at: TIMESTAMP.to_string(),
                change_id: format!("schema-fixture-{}", key.schema_key),
                commit_id: SCHEMA_FIXTURE_COMMIT_ID.to_string(),
            }
        })
        .collect::<Vec<_>>();
    let global_version_ref_row = crate::transaction::prepare_version_ref_row(
        GLOBAL_VERSION_ID,
        SCHEMA_FIXTURE_COMMIT_ID,
        TIMESTAMP,
    )
    .expect("schema fixture version ref should stage");
    let bench_version_ref_row = crate::transaction::prepare_version_ref_row(
        BENCH_VERSION_ID,
        SCHEMA_FIXTURE_COMMIT_ID,
        TIMESTAMP,
    )
    .expect("bench fixture version ref should stage");
    let mut read = BenchRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .expect("schema fixture read should open"),
    );
    crate::test_support::stage_tracked_root_from_materialized(
        &mut read,
        &mut writes,
        tracked_state,
        SCHEMA_FIXTURE_COMMIT_ID,
        None,
        &rows,
    )
    .await
    .expect("schema fixture rows should stage");
    UntrackedStateContext::new()
        .writer(&mut writes)
        .stage_rows([
            global_version_ref_row.row.as_ref(),
            bench_version_ref_row.row.as_ref(),
        ])
        .expect("schema fixture version ref should stage");
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .expect("schema fixture transaction should commit");
}

fn json_pointer_schema() -> JsonValue {
    json!({
        "x-lix-key": "json_pointer",
        "x-lix-primary-key": ["/path"],
        "type": "object",
        "properties": {
            "path": { "type": "string" },
            "value": true
        },
        "required": ["path", "value"],
        "additionalProperties": false
    })
}

fn native_storage_spaces() -> &'static [StorageSpace] {
    &[
        crate::untracked_state::storage::UNTRACKED_STATE_ROW_SPACE,
        crate::json_store::store::JSON_SPACE,
        crate::tracked_state::TRACKED_STATE_CHUNK_SPACE,
        crate::tracked_state::TRACKED_STATE_BY_FILE_ROOT_SPACE,
        crate::tracked_state::TRACKED_STATE_PROJECTION_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_SPACE,
        crate::binary_cas::kv::BINARY_CAS_MANIFEST_CHUNK_SPACE,
        crate::binary_cas::kv::BINARY_CAS_CHUNK_SPACE,
        crate::changelog::SEGMENT_SPACE,
        crate::changelog::COMMIT_VISIBILITY_SPACE,
        crate::changelog::BY_COMMIT_INDEX_SPACE,
        crate::changelog::BY_CHANGE_INDEX_SPACE,
        crate::changelog::BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
        crate::changelog::VISIBLE_CHANGE_PROOF_SPACE,
    ]
}

fn scan_layout_space<R>(read: &R, space: StorageSpace) -> BenchLayoutAccounting
where
    R: crate::storage::StorageRead,
{
    let result = ScanPlan::prefix(
        space,
        StoragePrefix {
            bytes: Bytes::new(),
        },
    )
    .collect(
        read,
        StorageScanOptions {
            projection: StorageCoreProjection::FullValue,
            limit_rows: 1_000_000,
            ..StorageScanOptions::default()
        },
    )
    .expect("scan transaction layout space");

    BenchLayoutAccounting {
        space_id: space.id.0,
        space: space.name,
        rows: result.value.entries.len() as u64,
        key_bytes: result
            .value
            .entries
            .iter()
            .map(|entry| entry.key.0.len() as u64 + 4)
            .sum(),
        value_bytes: result
            .value
            .entries
            .iter()
            .map(|entry| match &entry.value {
                crate::backend::ProjectedValue::KeyOnly => 0,
                crate::backend::ProjectedValue::FullValue(value) => value.len() as u64,
            })
            .sum(),
    }
}
