use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvGetRequest, BackendKvKeyPage,
    BackendKvScanRequest, BackendKvValueBatch, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, Engine, LixError,
};
use serde_json::Value as JsonValue;

#[allow(dead_code)]
#[path = "support/simulation_test/engine/kv_backend.rs"]
mod kv_backend;

use kv_backend::{InMemoryKvBackend, KvMap};

const JSON_POINTER_SCHEMA_JSON: &str =
    include_str!("../../plugin-json-v2/schema/json_pointer.json");
const PNPM_LOCK_JSON: &str = include_str!("../benches/fixtures/pnpm-lock.fixture.json");
const ROW_COUNT: usize = 100;

#[derive(Debug, Clone)]
struct PointerRow {
    path: String,
    value_json: String,
}

#[derive(Clone, Default)]
struct PhysicalTraceBackend {
    inner: InMemoryKvBackend,
    events: Arc<Mutex<Vec<WriteBatchTrace>>>,
}

impl PhysicalTraceBackend {
    fn reset_trace(&self) {
        self.events
            .lock()
            .expect("physical trace events lock")
            .clear();
    }

    fn trace(&self) -> Vec<WriteBatchTrace> {
        self.events
            .lock()
            .expect("physical trace events lock")
            .clone()
    }

    fn snapshot(&self) -> KvMap {
        self.inner.snapshot()
    }
}

#[derive(Debug, Clone, Default)]
struct WriteBatchTrace {
    groups: Vec<WriteGroupTrace>,
}

#[derive(Debug, Clone)]
struct WriteGroupTrace {
    namespace: String,
    puts: usize,
    deletes: usize,
    put_key_bytes: usize,
    put_value_bytes: usize,
    delete_key_bytes: usize,
}

#[derive(Debug, Clone, Default)]
struct WriteTraceTotal {
    batches: usize,
    puts: usize,
    deletes: usize,
    put_key_bytes: usize,
    put_value_bytes: usize,
    delete_key_bytes: usize,
}

impl WriteBatchTrace {
    fn from_batch(batch: &BackendKvWriteBatch) -> Self {
        let mut groups = Vec::new();
        for group in &batch.groups {
            let mut trace = WriteGroupTrace {
                namespace: group.namespace().to_string(),
                puts: group.put_count(),
                deletes: group.delete_count(),
                put_key_bytes: 0,
                put_value_bytes: 0,
                delete_key_bytes: 0,
            };
            for index in 0..group.put_count() {
                if let Some(key) = group.put_key(index) {
                    trace.put_key_bytes += key.len();
                }
                if let Some(value) = group.put_value(index) {
                    trace.put_value_bytes += value.len();
                }
            }
            for index in 0..group.delete_count() {
                if let Some(key) = group.delete_key(index) {
                    trace.delete_key_bytes += key.len();
                }
            }
            groups.push(trace);
        }
        Self { groups }
    }
}

#[derive(Debug, Clone, Default)]
struct PhysicalDiff {
    added_entries: usize,
    updated_entries: usize,
    removed_entries: usize,
    added_bytes: usize,
    updated_before_bytes: usize,
    updated_after_bytes: usize,
    removed_bytes: usize,
}

impl PhysicalDiff {
    fn touched_entries(&self) -> usize {
        self.added_entries + self.updated_entries + self.removed_entries
    }

    fn after_bytes(&self) -> usize {
        self.added_bytes + self.updated_after_bytes
    }

    fn net_bytes(&self) -> isize {
        (self.added_bytes + self.updated_after_bytes) as isize
            - (self.removed_bytes + self.updated_before_bytes) as isize
    }
}

fn diff_by_namespace(before: &KvMap, after: &KvMap) -> BTreeMap<String, PhysicalDiff> {
    let mut result = BTreeMap::<String, PhysicalDiff>::new();
    for (key, after_value) in after {
        let entry = result.entry(key.0.clone()).or_default();
        match before.get(key) {
            None => {
                entry.added_entries += 1;
                entry.added_bytes += key.1.len() + after_value.len();
            }
            Some(before_value) if before_value != after_value => {
                entry.updated_entries += 1;
                entry.updated_before_bytes += key.1.len() + before_value.len();
                entry.updated_after_bytes += key.1.len() + after_value.len();
            }
            Some(_) => {}
        }
    }
    for (key, before_value) in before {
        if !after.contains_key(key) {
            let entry = result.entry(key.0.clone()).or_default();
            entry.removed_entries += 1;
            entry.removed_bytes += key.1.len() + before_value.len();
        }
    }
    result
        .into_iter()
        .filter(|(_, diff)| diff.touched_entries() > 0)
        .collect()
}

fn namespace_totals(snapshot: &KvMap) -> BTreeMap<String, (usize, usize)> {
    let mut result = BTreeMap::<String, (usize, usize)>::new();
    for (key, value) in snapshot {
        let entry = result.entry(key.0.clone()).or_default();
        entry.0 += 1;
        entry.1 += key.1.len() + value.len();
    }
    result
}

#[tokio::test]
#[ignore = "prints physical backend layout for 100 tracked json_pointer inserts, updates, and deletes"]
async fn log11_physical_tracked() {
    let fixture_rows = fixture_rows();
    let insert_rows = &fixture_rows[..ROW_COUNT];
    let update_rows = &fixture_rows[ROW_COUNT..ROW_COUNT * 2];

    let (backend, session) = setup_session().await;

    backend.reset_trace();
    let before_inserts = backend.snapshot();
    insert_json_pointer_rows(&session, insert_rows).await;
    log_phase(
        "insert_100",
        &before_inserts,
        &backend.snapshot(),
        &backend.trace(),
    );

    backend.reset_trace();
    let before_updates = backend.snapshot();
    update_json_pointer_rows_by_pk(&session, insert_rows, update_rows).await;
    log_phase(
        "update_100_by_pk",
        &before_updates,
        &backend.snapshot(),
        &backend.trace(),
    );

    backend.reset_trace();
    let before_deletes = backend.snapshot();
    delete_json_pointer_rows_by_pk(&session, insert_rows).await;
    let after_deletes = backend.snapshot();
    log_phase(
        "delete_100_by_pk",
        &before_deletes,
        &after_deletes,
        &backend.trace(),
    );

    println!("LOG11_FINAL_LAYOUT scenario=per_pk");
    for (namespace, (entries, bytes)) in namespace_totals(&after_deletes) {
        println!("LOG11_NAMESPACE namespace={namespace} entries={entries} key_value_bytes={bytes}");
    }

    let (batch_backend, batch_session) = setup_session().await;
    insert_json_pointer_rows(&batch_session, insert_rows).await;

    batch_backend.reset_trace();
    let before_batched_updates = batch_backend.snapshot();
    update_json_pointer_rows_by_pk_batch(&batch_session, insert_rows, update_rows).await;
    log_phase(
        "update_100_by_pk_batch",
        &before_batched_updates,
        &batch_backend.snapshot(),
        &batch_backend.trace(),
    );

    batch_backend.reset_trace();
    let before_batched_deletes = batch_backend.snapshot();
    delete_json_pointer_rows_by_pk_batch(&batch_session, insert_rows).await;
    let after_batched_deletes = batch_backend.snapshot();
    log_phase(
        "delete_100_by_pk_batch",
        &before_batched_deletes,
        &after_batched_deletes,
        &batch_backend.trace(),
    );

    println!("LOG11_FINAL_LAYOUT scenario=batched");
    for (namespace, (entries, bytes)) in namespace_totals(&after_batched_deletes) {
        println!("LOG11_NAMESPACE namespace={namespace} entries={entries} key_value_bytes={bytes}");
    }
}

async fn setup_session() -> (PhysicalTraceBackend, lix_engine::SessionContext) {
    let backend = PhysicalTraceBackend::default();
    let receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("engine should initialize");
    let engine = Engine::new(Box::new(backend.clone()))
        .await
        .expect("initialized engine should open");
    let session = engine
        .open_session(receipt.main_version_id)
        .await
        .expect("main session should open");
    register_json_pointer_schema(&session).await;
    (backend, session)
}

async fn register_json_pointer_schema(session: &lix_engine::SessionContext) {
    let sql = format!(
        "INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
         VALUES (lix_json('{}'), false, false)",
        sql_string(JSON_POINTER_SCHEMA_JSON)
    );
    let affected = session
        .execute(&sql, &[])
        .await
        .expect("register json_pointer schema")
        .rows_affected();
    assert_eq!(affected, 1);
}

async fn insert_json_pointer_rows(session: &lix_engine::SessionContext, rows: &[PointerRow]) {
    let sql = insert_sql(rows);
    let affected = session
        .execute(&sql, &[])
        .await
        .expect("100 tracked json_pointer inserts should succeed")
        .rows_affected();
    assert_eq!(affected as usize, rows.len());
}

async fn update_json_pointer_rows_by_pk(
    session: &lix_engine::SessionContext,
    rows: &[PointerRow],
    update_rows: &[PointerRow],
) {
    assert_eq!(rows.len(), update_rows.len());
    for (row, update_row) in rows.iter().zip(update_rows) {
        let affected = session
            .execute(&update_sql(row, update_row), &[])
            .await
            .expect("tracked json_pointer update by PK should succeed")
            .rows_affected();
        assert_eq!(affected, 1);
    }
}

async fn delete_json_pointer_rows_by_pk(session: &lix_engine::SessionContext, rows: &[PointerRow]) {
    for row in rows {
        let affected = session
            .execute(&delete_sql(row), &[])
            .await
            .expect("tracked json_pointer delete by PK should succeed")
            .rows_affected();
        assert_eq!(affected, 1);
    }
}

async fn update_json_pointer_rows_by_pk_batch(
    session: &lix_engine::SessionContext,
    rows: &[PointerRow],
    update_rows: &[PointerRow],
) {
    assert_eq!(rows.len(), update_rows.len());
    let affected = session
        .execute(&update_batch_sql(rows, update_rows), &[])
        .await
        .expect("tracked json_pointer batched update by PK should succeed")
        .rows_affected();
    assert_eq!(affected as usize, rows.len());
}

async fn delete_json_pointer_rows_by_pk_batch(
    session: &lix_engine::SessionContext,
    rows: &[PointerRow],
) {
    let affected = session
        .execute(&delete_batch_sql(rows), &[])
        .await
        .expect("tracked json_pointer batched delete by PK should succeed")
        .rows_affected();
    assert_eq!(affected as usize, rows.len());
}

fn insert_sql(rows: &[PointerRow]) -> String {
    let values = rows
        .iter()
        .map(|row| {
            format!(
                "('{}', lix_json('{}'))",
                sql_string(row.path.as_str()),
                sql_string(row.value_json.as_str())
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO json_pointer (path, value) VALUES {values}")
}

fn update_sql(row: &PointerRow, update_row: &PointerRow) -> String {
    format!(
        "UPDATE json_pointer SET value = lix_json('{}') WHERE path = '{}'",
        sql_string(update_row.value_json.as_str()),
        sql_string(row.path.as_str())
    )
}

fn update_batch_sql(rows: &[PointerRow], update_rows: &[PointerRow]) -> String {
    let cases = rows
        .iter()
        .zip(update_rows)
        .map(|(row, update_row)| {
            format!(
                "WHEN '{}' THEN lix_json('{}')",
                sql_string(row.path.as_str()),
                sql_string(update_row.value_json.as_str())
            )
        })
        .collect::<Vec<_>>()
        .join(" ");
    let paths = sql_in_list(rows);
    format!("UPDATE json_pointer SET value = CASE path {cases} END WHERE path IN ({paths})")
}

fn delete_sql(row: &PointerRow) -> String {
    format!(
        "DELETE FROM json_pointer WHERE path = '{}'",
        sql_string(row.path.as_str())
    )
}

fn delete_batch_sql(rows: &[PointerRow]) -> String {
    format!(
        "DELETE FROM json_pointer WHERE path IN ({})",
        sql_in_list(rows)
    )
}

fn sql_in_list(rows: &[PointerRow]) -> String {
    rows.iter()
        .map(|row| format!("'{}'", sql_string(row.path.as_str())))
        .collect::<Vec<_>>()
        .join(", ")
}

fn fixture_rows() -> Vec<PointerRow> {
    let root: JsonValue = serde_json::from_str(PNPM_LOCK_JSON).expect("pnpm lock JSON fixture");
    let mut rows = Vec::new();
    flatten_json("", &root, &mut rows);
    rows.retain(|row| !row.path.is_empty());
    assert!(
        rows.len() >= ROW_COUNT * 2,
        "pnpm lock fixture should have at least {} pointer rows, got {}",
        ROW_COUNT * 2,
        rows.len()
    );
    rows
}

fn flatten_json(path: &str, value: &JsonValue, rows: &mut Vec<PointerRow>) {
    rows.push(PointerRow {
        path: path.to_string(),
        value_json: value.to_string(),
    });

    match value {
        JsonValue::Array(items) => {
            for (index, item) in items.iter().enumerate() {
                flatten_json(&format!("{path}/{index}"), item, rows);
            }
        }
        JsonValue::Object(map) => {
            for (key, child) in map {
                flatten_json(
                    &format!("{path}/{}", escape_pointer_token(key)),
                    child,
                    rows,
                );
            }
        }
        JsonValue::Null | JsonValue::Bool(_) | JsonValue::Number(_) | JsonValue::String(_) => {}
    }
}

fn escape_pointer_token(token: &str) -> String {
    token.replace('~', "~0").replace('/', "~1")
}

fn sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn log_phase(phase: &str, before: &KvMap, after: &KvMap, trace: &[WriteBatchTrace]) {
    println!(
        "LOG11_PHASE phase={phase} before_entries={} after_entries={}",
        before.len(),
        after.len()
    );
    for (namespace, diff) in diff_by_namespace(before, after) {
        println!(
            "LOG11_DIFF phase={phase} namespace={namespace} added={} updated={} removed={} touched={} net_key_value_bytes={} changed_after_key_value_bytes={}",
            diff.added_entries,
            diff.updated_entries,
            diff.removed_entries,
            diff.touched_entries(),
            diff.net_bytes(),
            diff.after_bytes()
        );
    }
    for (namespace, total) in trace_totals_by_namespace(trace) {
        println!(
            "LOG11_WRITE_TRACE phase={phase} namespace={namespace} batches={} puts={} deletes={} put_key_bytes={} put_value_bytes={} delete_key_bytes={}",
            total.batches,
            total.puts,
            total.deletes,
            total.put_key_bytes,
            total.put_value_bytes,
            total.delete_key_bytes
        );
    }
}

fn trace_totals_by_namespace(trace: &[WriteBatchTrace]) -> BTreeMap<String, WriteTraceTotal> {
    let mut totals = BTreeMap::<String, WriteTraceTotal>::new();
    for batch in trace {
        for group in &batch.groups {
            let total = totals.entry(group.namespace.clone()).or_default();
            total.batches += 1;
            total.puts += group.puts;
            total.deletes += group.deletes;
            total.put_key_bytes += group.put_key_bytes;
            total.put_value_bytes += group.put_value_bytes;
            total.delete_key_bytes += group.delete_key_bytes;
        }
    }
    totals
}

#[async_trait]
impl Backend for PhysicalTraceBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        self.inner.begin_read_transaction().await
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        Ok(Box::new(PhysicalTraceWriteTransaction {
            inner: self.inner.begin_write_transaction().await?,
            events: Arc::clone(&self.events),
        }))
    }
}

struct PhysicalTraceWriteTransaction {
    inner: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
    events: Arc<Mutex<Vec<WriteBatchTrace>>>,
}

#[async_trait]
impl BackendReadTransaction for PhysicalTraceWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        self.inner.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        self.inner.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        self.inner.scan_keys(request).await
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        self.inner.scan_values(request).await
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        self.inner.scan_entries(request).await
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.inner.rollback().await
    }
}

#[async_trait]
impl BackendWriteTransaction for PhysicalTraceWriteTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        self.events
            .lock()
            .expect("physical trace events lock")
            .push(WriteBatchTrace::from_batch(&batch));
        self.inner.write_kv_batch(batch).await
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        self.inner.commit().await
    }
}
