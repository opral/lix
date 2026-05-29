use std::sync::Arc;

use serde_json::{Value as JsonValue, json};

use crate::binary_cas::BinaryCasContext;
use crate::branch::BranchContext;
use crate::catalog::CatalogContext;
use crate::changelog::{ChangeId, CommitId};
use crate::entity_pk::EntityPk;
use crate::live_state::{
    LiveStateContext, LiveStateFilter, LiveStateProjection, LiveStateRowRequest,
    LiveStateScanRequest,
};
use crate::session::SessionMode;
use crate::storage::{
    SharedStorageRead, StorageBackend, StorageBackendReadOf, StorageContext, StorageReadOptions,
    StorageWriteSet, StorageWriteSetStats,
};
use crate::tracked_state::TrackedStateContext;
use crate::transaction::types::{TransactionJson, TransactionWriteRow};
use crate::untracked_state::UntrackedStateContext;
use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-00000000b001";
const TIMESTAMP: &str = "2026-05-19T00:00:00.000Z";
const BENCH_BRANCH_ID: &str = "tracked-crud-branch";

#[derive(Clone, Debug)]
pub struct BenchTransactionRow {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub entity_pk: String,
    pub value: JsonValue,
    pub updated_value: JsonValue,
}

#[expect(missing_debug_implementations)]
pub struct BenchTransactionFixture<B: StorageBackend> {
    storage: StorageContext<B>,
    live_state: Arc<LiveStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    branch_ctx: Arc<BranchContext>,
    catalog_context: Arc<CatalogContext>,
    rows: Vec<BenchTransactionRow>,
}

#[derive(Clone, Copy, Debug)]
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

#[derive(Clone, Copy, Debug)]
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
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn new(storage: StorageContext<B>, rows: Vec<BenchTransactionRow>) -> Self {
        let tracked_state = Arc::new(TrackedStateContext::new());
        let live_state = Arc::new(LiveStateContext::new(
            tracked_state.as_ref().clone(),
            UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(),
        ));
        let branch_ctx = Arc::new(BranchContext::new(Arc::new(UntrackedStateContext::new())));
        seed_visible_schema_rows(storage.clone(), tracked_state.as_ref()).await;
        Self {
            storage,
            live_state,
            tracked_state,
            binary_cas: Arc::new(BinaryCasContext::new()),
            branch_ctx,
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
        let read = SharedStorageRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .expect("begin transaction bench read"),
        );
        let rows = self
            .live_state
            .reader(read)
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["json_pointer".to_string()],
                    branch_ids: vec![BENCH_BRANCH_ID.to_string()],
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

    pub async fn read_many_by_pk(&self, count: usize) -> usize {
        let count = count.min(self.rows.len());
        let mut found = 0;
        for row in &self.rows[..count] {
            found += self.read_one(row).await;
        }
        found
    }

    pub async fn read_one_by_pk(&self) -> usize {
        self.read_one(&self.rows[self.rows.len() / 2]).await
    }

    async fn read_one(&self, row: &BenchTransactionRow) -> usize {
        let read = SharedStorageRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .expect("begin transaction bench read"),
        );
        let row = self
            .live_state
            .reader(read)
            .load_row(&LiveStateRowRequest {
                schema_key: "json_pointer".to_string(),
                branch_id: BENCH_BRANCH_ID.to_string(),
                entity_pk: EntityPk::single(row.entity_pk.clone()),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("load transaction bench row");
        assert!(row.is_some());
        1
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    async fn commit_rows(&mut self, rows: Vec<TransactionWriteRow>) -> BenchWriteAccounting {
        let logical_rows = rows.len();
        let opened = super::open_transaction(
            &SessionMode::Pinned {
                branch_id: BENCH_BRANCH_ID.to_string(),
            },
            self.storage.clone(),
            Arc::clone(&self.live_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            Arc::clone(&self.branch_ctx),
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
        entity_pk: Some(EntityPk::single(row.entity_pk.clone())),
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
        branch_id: BENCH_BRANCH_ID.to_string(),
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
                entity_pk: crate::schema::registered_schema_entity_pk(&key.schema_key)
                    .expect("registered schema identity should derive"),
                schema_key: "lix_registered_schema".to_string(),
                file_id: None,
                snapshot_content: Some(snapshot_content),
                metadata: None,
                deleted: false,
                created_at: TIMESTAMP.to_string(),
                updated_at: TIMESTAMP.to_string(),
                change_id: ChangeId::for_test_label(&format!("schema-fixture-{}", key.schema_key)),
                commit_id: CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID),
            }
        })
        .collect::<Vec<_>>();
    let global_branch_ref_row = crate::transaction::prepare_branch_ref_row(
        GLOBAL_BRANCH_ID,
        &CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID),
        TIMESTAMP,
    )
    .expect("schema fixture branch ref should stage");
    let bench_branch_ref_row = crate::transaction::prepare_branch_ref_row(
        BENCH_BRANCH_ID,
        &CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID),
        TIMESTAMP,
    )
    .expect("bench fixture branch ref should stage");
    let mut read = SharedStorageRead::new(
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
            global_branch_ref_row.row.as_ref(),
            bench_branch_ref_row.row.as_ref(),
        ])
        .expect("schema fixture branch ref should stage");
    crate::storage_bench::commit_write_set_for_bench(&storage, writes)
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
