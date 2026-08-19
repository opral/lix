use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{Value as JsonValue, json};

use crate::binary_cas::BinaryCasContext;
use crate::branch::{
    BranchContext, BranchHeadControl, BranchHeadControlContext, stage_branch_head_control,
};
use crate::catalog::CatalogContext;
use crate::changelog::{
    ChangeId, ChangeRecord, ChangelogAppend, ChangelogContext, ChangelogWriter, CommitId,
};
use crate::common::LixTimestamp;
use crate::row_pk::RowPk;
use crate::hot_state::{
    CurrentStateDeltaRef, HotStateContext, HotStateFilter, HotStateProjection, HotStateRowRequest,
    HotStateScanRequest, TrackedHeadContext, WorkingDiffIndexCoverage,
};
use crate::session::SessionBranch;
use crate::storage_adapter::Storage;
use crate::storage_adapter::{
    SharedStorageAdapterRead, StorageAdapter, StorageReadOptions, StorageWriteSet,
    StorageWriteSetStats,
};
use crate::tracked_state::TrackedStateContext;
use crate::transaction_types::{RawWriteBatch, TransactionJson, TransactionWriteRow};
use crate::{GLOBAL_BRANCH_ID, NullableKeyFilter};

const SCHEMA_FIXTURE_COMMIT_ID: &str = "01920000-0000-7000-8000-00000000b001";
const TIMESTAMP: &str = "2026-05-19T00:00:00.000Z";
const BENCH_BRANCH_ID: &str = "01920000-0000-7000-8000-0000000000a1";

#[derive(Clone, Debug)]
pub struct BenchTransactionRow {
    pub schema_key: String,
    pub file_id: Option<String>,
    pub row_pk: String,
    pub value: Arc<JsonValue>,
    pub updated_value: Arc<JsonValue>,
}

#[expect(missing_debug_implementations)]
pub struct BenchTransactionFixture<StorageImpl: Storage> {
    storage: StorageAdapter<StorageImpl>,
    hot_state: Arc<HotStateContext>,
    tracked_state: Arc<TrackedStateContext>,
    binary_cas: Arc<BinaryCasContext>,
    branch_ctx: Arc<BranchContext>,
    catalog_context: Arc<CatalogContext>,
    rows: Vec<BenchTransactionRow>,
    delete_one_offset: usize,
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

impl<StorageImpl> BenchTransactionFixture<StorageImpl>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    /// Like [`Self::new`], but enables the engine's deterministic functions
    /// before any transaction runs, so ids and timestamps are
    /// sequence-derived and two fixtures produce byte-identical storage.
    pub async fn new_deterministic(
        storage: StorageAdapter<StorageImpl>,
        rows: Vec<BenchTransactionRow>,
    ) -> Self {
        let fixture = Self::new(storage.clone(), rows).await;
        seed_deterministic_mode(storage).await;
        fixture
    }

    pub async fn new(storage: StorageAdapter<StorageImpl>, rows: Vec<BenchTransactionRow>) -> Self {
        let tracked_state = Arc::new(TrackedStateContext::new());
        let hot_state = Arc::new(HotStateContext::new(
            tracked_state.as_ref().clone(),
            crate::commit_graph::CommitGraphContext::new(),
        ));
        let branch_ctx = Arc::new(BranchContext::new());
        seed_visible_schema_rows(storage.clone(), tracked_state.as_ref()).await;
        Self {
            storage,
            hot_state,
            tracked_state,
            binary_cas: Arc::new(BinaryCasContext::new()),
            branch_ctx,
            catalog_context: Arc::new(CatalogContext::new()),
            rows,
            delete_one_offset: 0,
        }
    }

    pub async fn seed(&mut self) -> usize {
        self.insert_all().await
    }

    pub async fn insert_all(&mut self) -> usize {
        self.insert_all_accounting().await.logical_rows
    }

    pub async fn insert_all_accounting(&mut self) -> BenchWriteAccounting {
        let mut rows = RawWriteBatch::with_capacity(self.rows.len());
        for row in &self.rows {
            rows.push(transaction_row(row, &row.value));
        }
        self.commit_rows(rows).await
    }

    pub async fn update_all(&mut self) -> usize {
        self.update_all_accounting().await.logical_rows
    }

    pub async fn update_all_accounting(&mut self) -> BenchWriteAccounting {
        let mut rows = RawWriteBatch::with_capacity(self.rows.len());
        for row in &self.rows {
            rows.push(transaction_row(row, &row.updated_value));
        }
        self.commit_rows(rows).await
    }

    pub async fn update_one_by_pk(&mut self) -> usize {
        self.update_one_by_pk_accounting().await.logical_rows
    }

    pub async fn update_one_by_pk_accounting(&mut self) -> BenchWriteAccounting {
        let row = &self.rows[self.rows.len() / 2];
        let mut rows = RawWriteBatch::with_capacity(1);
        rows.push(transaction_row(row, &row.updated_value));
        self.commit_rows(rows).await
    }

    pub async fn delete_all(&mut self) -> usize {
        self.delete_all_accounting().await.logical_rows
    }

    pub async fn delete_all_accounting(&mut self) -> BenchWriteAccounting {
        let mut rows = RawWriteBatch::with_capacity(self.rows.len());
        for row in &self.rows {
            rows.push(transaction_delete_row(row));
        }
        self.commit_rows(rows).await
    }

    pub async fn delete_one_by_pk(&mut self) -> usize {
        self.delete_one_by_pk_accounting().await.logical_rows
    }

    pub async fn delete_one_by_pk_accounting(&mut self) -> BenchWriteAccounting {
        let row_index = (self.rows.len() / 2 + self.delete_one_offset) % self.rows.len();
        self.delete_one_offset += 1;
        let row = &self.rows[row_index];
        let mut rows = RawWriteBatch::with_capacity(1);
        rows.push(transaction_delete_row(row));
        self.commit_rows(rows).await
    }

    pub async fn read_all(&self) -> usize {
        let count = self.scan_count().await;
        assert_eq!(count, self.rows.len());
        count
    }

    async fn scan_count(&self) -> usize {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin transaction bench read"),
        );
        let rows = self
            .hot_state
            .reader(read)
            .scan_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["json_pointer".to_string()],
                    branch_ids: vec![BENCH_BRANCH_ID.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones: false,
                    ..HotStateFilter::default()
                },
                projection: HotStateProjection::default(),
                limit: None,
            })
            .await
            .expect("scan transaction bench rows");
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

    /// Returns every visible row's identity and snapshot content, sorted by
    /// identity. Unlike the timed read helpers this materializes contents,
    /// so equivalence tests can compare logical state across storage implementations.
    pub async fn read_all_contents(&self) -> Vec<(String, String)> {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin transaction bench read"),
        );
        let rows = self
            .hot_state
            .reader(read)
            .scan_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["json_pointer".to_string()],
                    branch_ids: vec![BENCH_BRANCH_ID.to_string()],
                    file_ids: vec![NullableKeyFilter::Null],
                    include_tombstones: false,
                    ..HotStateFilter::default()
                },
                projection: HotStateProjection::default(),
                limit: None,
            })
            .await
            .expect("scan transaction bench rows");
        let mut contents = rows
            .iter()
            .map(|row| {
                let row_pk = row
                    .row_pk()
                    .as_json_array_text()
                    .expect("bench row pk should render");
                (
                    row_pk,
                    row.snapshot_content()
                        .map(ToString::to_string)
                        .unwrap_or_default(),
                )
            })
            .collect::<Vec<_>>();
        contents.sort();
        contents
    }

    async fn read_one(&self, row: &BenchTransactionRow) -> usize {
        let read = SharedStorageAdapterRead::new(
            self.storage
                .begin_read(StorageReadOptions::default())
                .await
                .expect("begin transaction bench read"),
        );
        let row = self
            .hot_state
            .reader(read)
            .load_row(&HotStateRowRequest {
                schema_key: "json_pointer".to_string(),
                branch_id: BENCH_BRANCH_ID.to_string(),
                row_pk: RowPk::single(row.row_pk.clone()),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("load transaction bench row");
        assert!(row.is_some());
        1
    }

    #[expect(clippy::needless_pass_by_ref_mut)]
    async fn commit_rows(&mut self, rows: RawWriteBatch) -> BenchWriteAccounting {
        let logical_rows = rows.len();
        let opened = super::open_transaction(
            &SessionBranch::new(BENCH_BRANCH_ID.to_string()),
            crate::ANONYMOUS_ACCOUNT_ID.to_string(),
            self.storage.clone(),
            Arc::clone(&self.hot_state),
            Arc::clone(&self.tracked_state),
            Arc::clone(&self.binary_cas),
            crate::plugin::runtime::PluginRuntimeHost::new(Arc::new(crate::plugin::runtime::UnsupportedWasmRuntime)),
            Arc::clone(&self.branch_ctx),
            Arc::clone(&self.catalog_context),
            Arc::new(crate::sql2::SqlPlanningCache::default()),
            crate::sql2::SessionFileViews::default(),
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

    /// Per-row inventory of one space, for byte-exact layout comparison.
    pub async fn space_inventory(&self, space_name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin transaction inventory read");
        crate::storage_bench::space_inventory(&read, space_name).await
    }

    pub async fn layout_accounting(&self) -> Vec<BenchLayoutAccounting> {
        let read = self
            .storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("begin transaction layout accounting read");
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
}

async fn seed_deterministic_mode<StorageImpl>(storage: StorageAdapter<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
    for<'storage> StorageImpl::Read<'storage>: Send,
{
    let snapshot_content = serde_json::to_string(&json!({
        "key": crate::functions::DETERMINISTIC_MODE_KEY,
        "value": { "enabled": true },
    }))
    .expect("deterministic mode snapshot should serialize");
    let timestamp = LixTimestamp::expect_parse("created_at", "1970-01-01T00:00:00.000Z");
    let row_pk = RowPk::single(crate::functions::DETERMINISTIC_MODE_KEY);
    let read = SharedStorageAdapterRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("deterministic mode read should open"),
    );
    let mut writes = storage.new_write_set();
    let control = BranchHeadControlContext::new()
        .reader(&read)
        .load(GLOBAL_BRANCH_ID)
        .await
        .expect("global branch control should load")
        .expect("global branch control should exist");
    let snapshot = crate::json_store::JsonSlot::from_json(&snapshot_content);
    let mut working_diff_coverage = WorkingDiffIndexCoverage::default();
    TrackedHeadContext::new()
        .writer(&read, &mut writes)
        .stage_current_state_with_working_diff(
            GLOBAL_BRANCH_ID,
            Some(control.tracked_generation),
            control.head_commit_id,
            &[CurrentStateDeltaRef {
                schema_key: "lix_key_value",
                file_id: None,
                row_pk: &row_pk,
                change_id: None,
                commit_id: None,
                untracked: true,
                deleted: false,
                created_at: timestamp,
                updated_at: timestamp,
                snapshot: snapshot.as_ref_slot(),
                typed_snapshot: None,
                metadata: crate::json_store::JsonSlotRef::None,
                columnar_base_coordinate: None,
            }],
            &BTreeSet::new(),
            None,
            None,
            &mut working_diff_coverage,
        )
        .await
        .expect("deterministic mode current row should stage");
    crate::storage_bench::commit_write_set_for_bench(&storage, writes)
        .await
        .expect("deterministic mode row should commit");
}

fn write_accounting(logical_rows: usize, stats: StorageWriteSetStats) -> BenchWriteAccounting {
    BenchWriteAccounting {
        logical_rows,
        staged_puts: stats.staged_puts,
        staged_deletes: stats.staged_deletes,
        touched_spaces: stats.touched_spaces,
        storage_calls: stats.storage_calls,
        put_batches: stats.put_batches,
        delete_batches: stats.delete_batches,
        written_bytes: stats.written_bytes,
    }
}

fn transaction_row(row: &BenchTransactionRow, value: &Arc<JsonValue>) -> TransactionWriteRow {
    TransactionWriteRow {
        row_pk: Some(RowPk::single(row.row_pk.clone())),
        schema_key: row.schema_key.as_str().into(),
        file_id: row.file_id.as_deref().map(Into::into),
        snapshot: Some(TransactionJson::from_shared_value_unchecked(Arc::clone(
            value,
        ))),
        metadata: None,
        origin: None,
        created_at: None,
        updated_at: None,
        global: false,
        change_id: None,
        commit_id: None,
        untracked: false,
        branch_id: BENCH_BRANCH_ID.into(),
    }
}

fn transaction_delete_row(row: &BenchTransactionRow) -> TransactionWriteRow {
    let mut out = transaction_row(row, &row.value);
    out.snapshot = None;
    out
}

async fn seed_visible_schema_rows<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
    tracked_state: &TrackedStateContext,
) where
    StorageImpl: Storage + Clone,
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
            let snapshot_content = json!({
                "schema_key": key.schema_key.clone(),
                "value": schema,
            })
            .to_string();
            crate::tracked_state::MaterializedTrackedStateRow {
                row_pk: crate::schema::registered_schema_row_pk(&key.schema_key)
                    .expect("registered schema identity should derive"),
                schema_key: "lix_registered_schema".to_string(),
                file_id: None,
                snapshot_content: Some(snapshot_content.into()),
                typed_snapshot: None,
                metadata: None,
                deleted: false,
                created_at: TIMESTAMP.to_string(),
                updated_at: TIMESTAMP.to_string(),
                change_id: ChangeId::for_test_label(&format!("schema-fixture-{}", key.schema_key)),
                commit_id: CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID),
            }
        })
        .collect::<Vec<_>>();
    let mut read = SharedStorageAdapterRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .await
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
    // Production initialization records this revision with the schema root.
    // Keep the low-level benchmark fixture on the normal transaction-open
    // cache path rather than measuring a legacy no-revision fallback.
    crate::catalog::stage_catalog_revision(&mut writes);
    crate::storage_bench::commit_write_set_for_bench(&storage, writes)
        .await
        .expect("schema fixture tracked rows should commit");

    drop(read);
    let mut read = SharedStorageAdapterRead::new(
        storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("schema fixture current read should open"),
    );
    let timestamp = LixTimestamp::expect_parse("timestamp", TIMESTAMP);
    let commit_id = CommitId::for_test_label(SCHEMA_FIXTURE_COMMIT_ID);
    let branch_refs = [GLOBAL_BRANCH_ID, BENCH_BRANCH_ID].map(|branch_id| {
        let row_pk =
            RowPk::uuid_from_canonical(branch_id).expect("benchmark branch ID is canonical");
        let snapshot = json!({"id": branch_id, "commit_id": commit_id}).to_string();
        let change_id = ChangeId::for_test_label(&format!("bench-branch-ref-{branch_id}"));
        (branch_id, row_pk, snapshot, change_id)
    });
    let mut writes = StorageWriteSet::new();
    ChangelogContext::new()
        .writer(&mut read, &mut writes)
        .stage_append(ChangelogAppend {
            changes: branch_refs
                .iter()
                .map(|(_, row_pk, snapshot, change_id)| ChangeRecord {
                    format_version: 2,
                    change_id: *change_id,
                    account_id: crate::ANONYMOUS_ACCOUNT_ID.to_string(),
                    row_pk: row_pk.clone(),
                    schema_key: crate::branch::BRANCH_REF_SCHEMA_KEY.to_string(),
                    file_id: None,
                    snapshot: crate::json_store::JsonSlot::from_json(snapshot),
                    typed_payload: None,
                    metadata: crate::json_store::JsonSlot::None,
                    created_at: timestamp,
                    origin_key: None,
                })
                .collect(),
            ..ChangelogAppend::default()
        })
        .await
        .expect("schema fixture branch-ref changes should stage");
    // Match repository initialization: the immutable schema root and each
    // visible branch control get a complete grouped current-state generation
    // before the timed fixture begins. Otherwise the first timed tracked
    // write would bootstrap from history instead of exercising the normal
    // path.
    let tracked_head = TrackedHeadContext::new();
    let absence_guards = BTreeSet::new();
    for (branch_id, _, _, _) in &branch_refs {
        #[cfg(test)]
        {
            let mut coverage = WorkingDiffIndexCoverage::default();
            tracked_head
                .writer(&read, &mut writes)
                .stage_current_state_with_working_diff(
                    branch_id,
                    None,
                    commit_id,
                    &[],
                    &absence_guards,
                    Some(rows.clone()),
                    None,
                    &mut coverage,
                )
                .await
                .expect("schema fixture tracked head should stage");
        }
        #[cfg(not(test))]
        tracked_head
            .writer(&read, &mut writes)
            .stage_commit(
                branch_id,
                None,
                commit_id,
                &[],
                &absence_guards,
                Some(rows.clone()),
            )
            .await
            .expect("schema fixture tracked head should stage");
    }
    // Branch controls are the authoritative publication fence. The
    // branch-ref ledger changes above remain part of the public fixture, but
    // branch refs are synthesized from controls rather than duplicated in
    // current-state rows.
    for (branch_id, _, _, change_id) in &branch_refs {
        let mut control = BranchHeadControl {
            head_commit_id: commit_id,
            tracked_generation: commit_id,
            current_state_revision: 0,
            working_diff_checkpoint_commit_id: None,
            created_at: timestamp,
            updated_at: timestamp,
            ref_change_id: *change_id,
            schema_presence_bloom: [0; 4],
        };
        control.note_schemas(rows.iter().map(|row| row.schema_key.as_str()));
        stage_branch_head_control(&mut writes, branch_id, control)
            .expect("schema fixture branch control should stage");
    }
    // A branch ref can change the registered-schema catalog reachable from a
    // branch, so it rotates the same cache revision in production commits.
    crate::catalog::stage_catalog_revision(&mut writes);
    crate::storage_bench::commit_write_set_for_bench(&storage, writes)
        .await
        .expect("schema fixture grouped current state should commit");
}

fn json_pointer_schema() -> JsonValue {
    json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "json_pointer",
        "columns": [
            { "name": "path", "type": "text", "nullable": false },
            { "name": "value", "type": "jsonb", "nullable": false }
        ],
        "primary_key": ["path"]
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::Memory;

    const BULK_ROWS: usize = 1_000;

    fn bulk_rows(count: usize) -> Vec<BenchTransactionRow> {
        (0..count)
            .map(|index| BenchTransactionRow {
                schema_key: "json_pointer".to_owned(),
                file_id: None,
                row_pk: format!("/bulk/{index:04}"),
                value: Arc::new(json!({
                    "path": format!("/bulk/{index:04}"),
                    "value": format!("before-{index:04}"),
                })),
                updated_value: Arc::new(json!({
                    "path": format!("/bulk/{index:04}"),
                    "value": format!("after-{index:04}"),
                })),
            })
            .collect()
    }

    #[tokio::test]
    async fn bulk_current_state_uses_compact_certificates_without_changing_point_writes() {
        let mut fixture =
            BenchTransactionFixture::new(StorageAdapter::new(Memory::new()), bulk_rows(BULK_ROWS))
                .await;

        let inserted = fixture.insert_all_accounting().await;
        assert_eq!(inserted.logical_rows, BULK_ROWS);
        assert!(inserted.staged_puts < 30, "{inserted:?}");
        assert_eq!(fixture.read_all().await, BULK_ROWS);

        let point_update = fixture.update_one_by_pk_accounting().await;
        assert_eq!(point_update.logical_rows, 1);
        // The point write keeps the compact current-state certificate,
        // publishes its authenticated branch control, and rotates the two
        // mandatory publication epochs — binary-CAS and json_store. It touches
        // eight spaces, not eleven: those epochs and the tracked mutation fence
        // are all keys in the one revision space, the retirement candidates a
        // sweep needs are derived from the commit graph instead of being
        // published into a reachability delta row plus its queue control, and
        // the commit-derived change id is computed from the commit id instead
        // of being mirrored into a reverse-index space.
        //
        // The json_store epoch is the tenth staged put and is why this is 10
        // rather than 9. It costs no extra space, batch, or storage call — the
        // revision space is already in this write set and its one-byte keys are
        // adjacent — and it is what lets the payload sweep reclaim superseded
        // out-of-band JSON at all: those rows are content addressed, so a
        // publisher can resolve onto a row an earlier sweep plan marked dead.
        assert_eq!(point_update.staged_puts, 10, "{point_update:?}");
        assert_eq!(point_update.touched_spaces, 8, "{point_update:?}");
        assert_eq!(point_update.put_batches, 8, "{point_update:?}");

        // A sparse overlay deliberately invalidates the complete-generation
        // digest, so use a fresh fixture to exercise exact bulk replacement
        // and deletion certificates.
        let mut bulk_fixture =
            BenchTransactionFixture::new(StorageAdapter::new(Memory::new()), bulk_rows(BULK_ROWS))
                .await;
        bulk_fixture.insert_all().await;
        let updated = bulk_fixture.update_all_accounting().await;
        assert_eq!(updated.logical_rows, BULK_ROWS);
        assert!(updated.staged_puts < 30, "{updated:?}");
        assert_eq!(bulk_fixture.read_all().await, BULK_ROWS);

        let deleted = bulk_fixture.delete_all_accounting().await;
        assert_eq!(deleted.logical_rows, BULK_ROWS);
        assert!(deleted.staged_puts < 40, "{deleted:?}");
        assert_eq!(bulk_fixture.scan_count().await, 0);
    }
}
