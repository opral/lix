use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvGetRequest, BackendKvKeyPage,
    BackendKvScanRequest, BackendKvValueBatch, BackendKvValuePage, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, CreateVersionOptions,
    Engine, LixError, SessionContext, Value,
};

#[allow(dead_code)]
#[path = "support/simulation_test/engine/kv_backend.rs"]
mod kv_backend;

use kv_backend::{InMemoryKvBackend, KvMap};

#[derive(Debug, Clone, Default)]
struct AmplificationCounts {
    begin_read_transactions: usize,
    begin_write_transactions: usize,
    commits: usize,
    rollbacks: usize,
    write_kv_batch_calls: usize,
    puts: usize,
    deletes: usize,
    write_bytes: usize,
    get_values_calls: usize,
    get_values_keys: usize,
    exists_many_calls: usize,
    exists_many_keys: usize,
    scan_keys_calls: usize,
    scan_keys_rows: usize,
    scan_values_calls: usize,
    scan_values_rows: usize,
    scan_entries_calls: usize,
    scan_entries_rows: usize,
    puts_by_namespace: BTreeMap<String, usize>,
    deletes_by_namespace: BTreeMap<String, usize>,
    bytes_by_namespace: BTreeMap<String, usize>,
}

impl AmplificationCounts {
    fn record_write_batch(&mut self, batch: &BackendKvWriteBatch) {
        self.write_kv_batch_calls += 1;
        for group in &batch.groups {
            let namespace = group.namespace().to_string();
            for index in 0..group.put_count() {
                let Some(key) = group.put_key(index) else {
                    continue;
                };
                let Some(value) = group.put_value(index) else {
                    continue;
                };
                self.puts += 1;
                self.write_bytes += key.len() + value.len();
                *self.puts_by_namespace.entry(namespace.clone()).or_default() += 1;
                *self
                    .bytes_by_namespace
                    .entry(namespace.clone())
                    .or_default() += key.len() + value.len();
            }
            for index in 0..group.delete_count() {
                let Some(key) = group.delete_key(index) else {
                    continue;
                };
                self.deletes += 1;
                self.write_bytes += key.len();
                *self
                    .deletes_by_namespace
                    .entry(namespace.clone())
                    .or_default() += 1;
                *self
                    .bytes_by_namespace
                    .entry(namespace.clone())
                    .or_default() += key.len();
            }
        }
    }

    fn read_calls(&self) -> usize {
        self.get_values_calls
            + self.exists_many_calls
            + self.scan_keys_calls
            + self.scan_values_calls
            + self.scan_entries_calls
    }

    fn read_items(&self) -> usize {
        self.get_values_keys
            + self.exists_many_keys
            + self.scan_keys_rows
            + self.scan_values_rows
            + self.scan_entries_rows
    }

    fn write_mutations(&self) -> usize {
        self.puts + self.deletes
    }

    fn puts_in(&self, namespace: &str) -> usize {
        self.puts_by_namespace.get(namespace).copied().unwrap_or(0)
    }

    fn deletes_in(&self, namespace: &str) -> usize {
        self.deletes_by_namespace
            .get(namespace)
            .copied()
            .unwrap_or(0)
    }

    fn bytes_in(&self, namespace: &str) -> usize {
        self.bytes_by_namespace.get(namespace).copied().unwrap_or(0)
    }
}

#[derive(Clone, Default)]
struct CountingBackend {
    inner: InMemoryKvBackend,
    counts: Arc<Mutex<AmplificationCounts>>,
}

impl CountingBackend {
    fn reset_counts(&self) {
        *self.counts.lock().expect("amplification counts lock") = AmplificationCounts::default();
    }

    fn counts(&self) -> AmplificationCounts {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .clone()
    }

    fn snapshot(&self) -> KvMap {
        self.inner.snapshot()
    }
}

#[derive(Debug, Clone, Default)]
struct StorageAmplification {
    before_entries: usize,
    after_entries: usize,
    before_key_value_bytes: usize,
    after_key_value_bytes: usize,
    before_namespace_key_value_bytes: usize,
    after_namespace_key_value_bytes: usize,
    added_entries: usize,
    updated_entries: usize,
    removed_entries: usize,
    added_key_value_bytes: usize,
    updated_before_key_value_bytes: usize,
    updated_after_key_value_bytes: usize,
    removed_key_value_bytes: usize,
    added_namespace_key_value_bytes: usize,
    updated_before_namespace_key_value_bytes: usize,
    updated_after_namespace_key_value_bytes: usize,
    removed_namespace_key_value_bytes: usize,
    by_namespace: BTreeMap<String, StorageNamespaceAmplification>,
}

#[derive(Debug, Clone, Default)]
struct StorageNamespaceAmplification {
    added_entries: usize,
    updated_entries: usize,
    removed_entries: usize,
    added_key_value_bytes: usize,
    updated_before_key_value_bytes: usize,
    updated_after_key_value_bytes: usize,
    removed_key_value_bytes: usize,
    added_namespace_key_value_bytes: usize,
    updated_before_namespace_key_value_bytes: usize,
    updated_after_namespace_key_value_bytes: usize,
    removed_namespace_key_value_bytes: usize,
}

impl StorageAmplification {
    fn from_snapshots(before: &KvMap, after: &KvMap) -> Self {
        let mut result = Self {
            before_entries: before.len(),
            after_entries: after.len(),
            before_key_value_bytes: snapshot_key_value_bytes(before),
            after_key_value_bytes: snapshot_key_value_bytes(after),
            before_namespace_key_value_bytes: snapshot_namespace_key_value_bytes(before),
            after_namespace_key_value_bytes: snapshot_namespace_key_value_bytes(after),
            ..Self::default()
        };

        for (key, after_value) in after {
            match before.get(key) {
                None => {
                    result.added_entries += 1;
                    result.added_key_value_bytes += key_value_bytes(key, after_value);
                    result.added_namespace_key_value_bytes +=
                        namespace_key_value_bytes(key, after_value);
                    let namespace = result.by_namespace.entry(key.0.clone()).or_default();
                    namespace.added_entries += 1;
                    namespace.added_key_value_bytes += key_value_bytes(key, after_value);
                    namespace.added_namespace_key_value_bytes +=
                        namespace_key_value_bytes(key, after_value);
                }
                Some(before_value) if before_value != after_value => {
                    result.updated_entries += 1;
                    result.updated_before_key_value_bytes += key_value_bytes(key, before_value);
                    result.updated_after_key_value_bytes += key_value_bytes(key, after_value);
                    result.updated_before_namespace_key_value_bytes +=
                        namespace_key_value_bytes(key, before_value);
                    result.updated_after_namespace_key_value_bytes +=
                        namespace_key_value_bytes(key, after_value);
                    let namespace = result.by_namespace.entry(key.0.clone()).or_default();
                    namespace.updated_entries += 1;
                    namespace.updated_before_key_value_bytes += key_value_bytes(key, before_value);
                    namespace.updated_after_key_value_bytes += key_value_bytes(key, after_value);
                    namespace.updated_before_namespace_key_value_bytes +=
                        namespace_key_value_bytes(key, before_value);
                    namespace.updated_after_namespace_key_value_bytes +=
                        namespace_key_value_bytes(key, after_value);
                }
                Some(_) => {}
            }
        }

        for (key, before_value) in before {
            if !after.contains_key(key) {
                result.removed_entries += 1;
                result.removed_key_value_bytes += key_value_bytes(key, before_value);
                result.removed_namespace_key_value_bytes +=
                    namespace_key_value_bytes(key, before_value);
                let namespace = result.by_namespace.entry(key.0.clone()).or_default();
                namespace.removed_entries += 1;
                namespace.removed_key_value_bytes += key_value_bytes(key, before_value);
                namespace.removed_namespace_key_value_bytes +=
                    namespace_key_value_bytes(key, before_value);
            }
        }

        result
    }

    fn touched_entries(&self) -> usize {
        self.added_entries + self.updated_entries + self.removed_entries
    }

    fn changed_after_key_value_bytes(&self) -> usize {
        self.added_key_value_bytes + self.updated_after_key_value_bytes
    }

    fn changed_after_namespace_key_value_bytes(&self) -> usize {
        self.added_namespace_key_value_bytes + self.updated_after_namespace_key_value_bytes
    }

    fn net_key_value_bytes_delta(&self) -> isize {
        self.after_key_value_bytes as isize - self.before_key_value_bytes as isize
    }

    fn net_namespace_key_value_bytes_delta(&self) -> isize {
        self.after_namespace_key_value_bytes as isize
            - self.before_namespace_key_value_bytes as isize
    }
}

impl StorageNamespaceAmplification {
    fn touched_entries(&self) -> usize {
        self.added_entries + self.updated_entries + self.removed_entries
    }

    fn changed_after_key_value_bytes(&self) -> usize {
        self.added_key_value_bytes + self.updated_after_key_value_bytes
    }

    fn changed_after_namespace_key_value_bytes(&self) -> usize {
        self.added_namespace_key_value_bytes + self.updated_after_namespace_key_value_bytes
    }

    fn net_key_value_bytes_delta(&self) -> isize {
        (self.added_key_value_bytes + self.updated_after_key_value_bytes) as isize
            - (self.removed_key_value_bytes + self.updated_before_key_value_bytes) as isize
    }

    fn net_namespace_key_value_bytes_delta(&self) -> isize {
        (self.added_namespace_key_value_bytes + self.updated_after_namespace_key_value_bytes)
            as isize
            - (self.removed_namespace_key_value_bytes
                + self.updated_before_namespace_key_value_bytes) as isize
    }
}

fn storage_totals_for(
    storage: &StorageAmplification,
    namespaces: &[&str],
) -> StorageNamespaceAmplification {
    let mut totals = StorageNamespaceAmplification::default();
    for namespace in namespaces {
        let Some(item) = storage.by_namespace.get(*namespace) else {
            continue;
        };
        totals.added_entries += item.added_entries;
        totals.updated_entries += item.updated_entries;
        totals.removed_entries += item.removed_entries;
        totals.added_key_value_bytes += item.added_key_value_bytes;
        totals.updated_before_key_value_bytes += item.updated_before_key_value_bytes;
        totals.updated_after_key_value_bytes += item.updated_after_key_value_bytes;
        totals.removed_key_value_bytes += item.removed_key_value_bytes;
        totals.added_namespace_key_value_bytes += item.added_namespace_key_value_bytes;
        totals.updated_before_namespace_key_value_bytes +=
            item.updated_before_namespace_key_value_bytes;
        totals.updated_after_namespace_key_value_bytes +=
            item.updated_after_namespace_key_value_bytes;
        totals.removed_namespace_key_value_bytes += item.removed_namespace_key_value_bytes;
    }
    totals
}

fn print_storage_class_row(
    rows: usize,
    category: &str,
    namespaces: &[&str],
    totals: &StorageNamespaceAmplification,
) {
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category={category} namespaces={} \
         added_entries={} updated_entries={} removed_entries={} touched_entries={} \
         net_key_value_bytes_delta={} changed_after_key_value_bytes={} \
         net_namespace_key_value_bytes_delta={} changed_after_namespace_key_value_bytes={} \
         touched_entries_per_row={:.3} net_key_value_bytes_delta_per_row={:.1} \
         changed_after_key_value_bytes_per_row={:.1} \
         net_namespace_key_value_bytes_delta_per_row={:.1} \
         changed_after_namespace_key_value_bytes_per_row={:.1}",
        namespaces.join(","),
        totals.added_entries,
        totals.updated_entries,
        totals.removed_entries,
        totals.touched_entries(),
        totals.net_key_value_bytes_delta(),
        totals.changed_after_key_value_bytes(),
        totals.net_namespace_key_value_bytes_delta(),
        totals.changed_after_namespace_key_value_bytes(),
        totals.touched_entries() as f64 / rows as f64,
        totals.net_key_value_bytes_delta() as f64 / rows as f64,
        totals.changed_after_key_value_bytes() as f64 / rows as f64,
        totals.net_namespace_key_value_bytes_delta() as f64 / rows as f64,
        totals.changed_after_namespace_key_value_bytes() as f64 / rows as f64,
    );
}

#[derive(Debug, Clone)]
struct AmplificationRun {
    counts: AmplificationCounts,
    storage: StorageAmplification,
}

fn snapshot_key_value_bytes(snapshot: &KvMap) -> usize {
    snapshot
        .iter()
        .map(|(key, value)| key_value_bytes(key, value))
        .sum()
}

fn snapshot_namespace_key_value_bytes(snapshot: &KvMap) -> usize {
    snapshot
        .iter()
        .map(|(key, value)| namespace_key_value_bytes(key, value))
        .sum()
}

fn key_value_bytes(key: &(String, Vec<u8>), value: &[u8]) -> usize {
    key.1.len() + value.len()
}

fn namespace_key_value_bytes(key: &(String, Vec<u8>), value: &[u8]) -> usize {
    key.0.len() + key.1.len() + value.len()
}

async fn setup_counting_engine() -> (CountingBackend, Engine, String) {
    let backend = CountingBackend::default();
    let receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("engine should initialize");
    backend.reset_counts();

    let engine = Engine::new(Box::new(backend.clone()))
        .await
        .expect("initialized engine should open");
    backend.reset_counts();

    (backend, engine, receipt.main_version_id)
}

async fn open_main_session(engine: &Engine, main_version_id: &str) -> SessionContext {
    engine
        .open_session(main_version_id.to_string())
        .await
        .expect("main session should open")
}

async fn create_branch(engine: &Engine, main: &SessionContext, id: &str) -> SessionContext {
    let receipt = main
        .create_version(CreateVersionOptions {
            id: Some(id.to_string()),
            name: format!("Amplification {id}"),
            from_commit_id: None,
        })
        .await
        .expect("branch version should be created");
    engine
        .open_session(receipt.id)
        .await
        .expect("branch session should open")
}

fn start_measurement(backend: &CountingBackend) -> KvMap {
    backend.reset_counts();
    backend.snapshot()
}

fn finish_measurement(backend: &CountingBackend, before: KvMap) -> AmplificationRun {
    let after = backend.snapshot();
    AmplificationRun {
        counts: backend.counts(),
        storage: StorageAmplification::from_snapshots(&before, &after),
    }
}

#[async_trait]
impl Backend for CountingBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .begin_read_transactions += 1;
        Ok(Box::new(CountingReadTransaction {
            inner: self.inner.begin_read_transaction().await?,
            counts: Arc::clone(&self.counts),
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .begin_write_transactions += 1;
        Ok(Box::new(CountingWriteTransaction {
            inner: self.inner.begin_write_transaction().await?,
            counts: Arc::clone(&self.counts),
        }))
    }
}

struct CountingReadTransaction {
    inner: Box<dyn BackendReadTransaction + Send + Sync + 'static>,
    counts: Arc<Mutex<AmplificationCounts>>,
}

#[async_trait]
impl BackendReadTransaction for CountingReadTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        record_get_values(&self.counts, &request);
        self.inner.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        record_exists_many(&self.counts, &request);
        self.inner.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let result = self.inner.scan_keys(request).await?;
        let mut counts = self.counts.lock().expect("amplification counts lock");
        counts.scan_keys_calls += 1;
        counts.scan_keys_rows += result.keys.len();
        Ok(result)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let result = self.inner.scan_values(request).await?;
        let mut counts = self.counts.lock().expect("amplification counts lock");
        counts.scan_values_calls += 1;
        counts.scan_values_rows += result.values.len();
        Ok(result)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let result = self.inner.scan_entries(request).await?;
        let mut counts = self.counts.lock().expect("amplification counts lock");
        counts.scan_entries_calls += 1;
        counts.scan_entries_rows += result.keys.len();
        Ok(result)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .rollbacks += 1;
        self.inner.rollback().await
    }
}

struct CountingWriteTransaction {
    inner: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
    counts: Arc<Mutex<AmplificationCounts>>,
}

#[async_trait]
impl BackendReadTransaction for CountingWriteTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        record_get_values(&self.counts, &request);
        self.inner.get_values(request).await
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        record_exists_many(&self.counts, &request);
        self.inner.exists_many(request).await
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let result = self.inner.scan_keys(request).await?;
        let mut counts = self.counts.lock().expect("amplification counts lock");
        counts.scan_keys_calls += 1;
        counts.scan_keys_rows += result.keys.len();
        Ok(result)
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        let result = self.inner.scan_values(request).await?;
        let mut counts = self.counts.lock().expect("amplification counts lock");
        counts.scan_values_calls += 1;
        counts.scan_values_rows += result.values.len();
        Ok(result)
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        let result = self.inner.scan_entries(request).await?;
        let mut counts = self.counts.lock().expect("amplification counts lock");
        counts.scan_entries_calls += 1;
        counts.scan_entries_rows += result.keys.len();
        Ok(result)
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .rollbacks += 1;
        self.inner.rollback().await
    }
}

#[async_trait]
impl BackendWriteTransaction for CountingWriteTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .record_write_batch(&batch);
        self.inner.write_kv_batch(batch).await
    }

    async fn commit(self: Box<Self>) -> Result<(), LixError> {
        self.counts
            .lock()
            .expect("amplification counts lock")
            .commits += 1;
        self.inner.commit().await
    }
}

fn record_get_values(counts: &Mutex<AmplificationCounts>, request: &BackendKvGetRequest) {
    let mut counts = counts.lock().expect("amplification counts lock");
    counts.get_values_calls += 1;
    counts.get_values_keys += request
        .groups
        .iter()
        .map(|group| group.keys.len())
        .sum::<usize>();
}

fn record_exists_many(counts: &Mutex<AmplificationCounts>, request: &BackendKvGetRequest) {
    let mut counts = counts.lock().expect("amplification counts lock");
    counts.exists_many_calls += 1;
    counts.exists_many_keys += request
        .groups
        .iter()
        .map(|group| group.keys.len())
        .sum::<usize>();
}

fn insert_sql(rows: usize, value_bytes: usize) -> String {
    let values = (0..rows)
        .map(|index| {
            format!(
                "('amplification-key-{index:08}', '{}')",
                "v".repeat(value_bytes)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO lix_key_value (key, value) VALUES {values}")
}

fn update_key_value_sql(rows: usize) -> String {
    let keys = (0..rows)
        .map(|index| format!("'amplification-key-{index:08}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("UPDATE lix_key_value SET value = 'branch-updated' WHERE key IN ({keys})")
}

fn insert_lix_file_descriptor_sql(rows: usize) -> String {
    let values = (0..rows)
        .map(|index| format!("('amplification-file-{index:08}', NULL, 'file-{index:08}.bin')"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("INSERT INTO lix_file (id, directory_id, name) VALUES {values}")
}

fn update_lix_file_hidden_sql(rows: usize) -> String {
    let ids = (0..rows)
        .map(|index| format!("'amplification-file-{index:08}'"))
        .collect::<Vec<_>>()
        .join(", ");
    format!("UPDATE lix_file SET hidden = true WHERE id IN ({ids})")
}

async fn run_insert(rows: usize, value_bytes: usize) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let session = open_main_session(&engine, &main_version_id).await;
    let storage_before = start_measurement(&backend);

    session
        .execute(&insert_sql(rows, value_bytes), &[])
        .await
        .expect("lix_key_value insert should succeed");

    finish_measurement(&backend, storage_before)
}

async fn run_lix_file_insert_data(file_bytes: usize) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let session = open_main_session(&engine, &main_version_id).await;
    let storage_before = start_measurement(&backend);

    let params = [Value::Blob(synthetic_file_bytes(file_bytes))];
    session
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('amplification-video-file', '/video.bin', $1)",
            &params,
        )
        .await
        .expect("lix_file data insert should succeed");

    finish_measurement(&backend, storage_before)
}

async fn run_branch_from_head_only() -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    let before = start_measurement(&backend);
    let _branch = create_branch(&engine, &main, "amplification-branch-only").await;
    finish_measurement(&backend, before)
}

async fn run_key_value_branch_insert() -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    let branch = create_branch(&engine, &main, "amplification-kv-insert").await;
    let before = start_measurement(&backend);
    branch
        .execute(
            "INSERT INTO lix_key_value (key, value) \
             VALUES ('branch-insert-key', 'branch-value')",
            &[],
        )
        .await
        .expect("branch key-value insert should succeed");
    finish_measurement(&backend, before)
}

async fn run_key_value_branch_update(base_rows: usize, update_rows: usize) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    main.execute(&insert_sql(base_rows, 8), &[])
        .await
        .expect("base key-values should insert");
    let branch = create_branch(
        &engine,
        &main,
        &format!("amplification-kv-update-{update_rows}"),
    )
    .await;
    let before = start_measurement(&backend);
    branch
        .execute(&update_key_value_sql(update_rows), &[])
        .await
        .expect("branch key-value update should succeed");
    finish_measurement(&backend, before)
}

async fn run_lix_file_branch_insert(file_bytes: usize) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    let branch = create_branch(&engine, &main, "amplification-file-insert").await;
    let before = start_measurement(&backend);
    let params = [Value::Blob(synthetic_file_bytes(file_bytes))];
    branch
        .execute(
            "INSERT INTO lix_file (id, path, data) \
             VALUES ('branch-file', '/branch-file.bin', $1)",
            &params,
        )
        .await
        .expect("branch lix_file insert should succeed");
    finish_measurement(&backend, before)
}

async fn run_lix_file_branch_update_data(base_rows: usize, file_bytes: usize) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    main.execute(&insert_lix_file_descriptor_sql(base_rows), &[])
        .await
        .expect("base lix_file descriptors should insert");
    let branch = create_branch(&engine, &main, "amplification-file-update-data").await;
    let before = start_measurement(&backend);
    let params = [Value::Blob(synthetic_file_bytes(file_bytes))];
    branch
        .execute(
            "UPDATE lix_file SET data = $1 \
             WHERE id = 'amplification-file-00000000'",
            &params,
        )
        .await
        .expect("branch lix_file data update should succeed");
    finish_measurement(&backend, before)
}

async fn run_lix_file_branch_rename(base_rows: usize) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    main.execute(&insert_lix_file_descriptor_sql(base_rows), &[])
        .await
        .expect("base lix_file descriptors should insert");
    let branch = create_branch(&engine, &main, "amplification-file-rename").await;
    let before = start_measurement(&backend);
    branch
        .execute(
            "UPDATE lix_file SET path = '/file-00000000-renamed.bin' \
             WHERE id = 'amplification-file-00000000'",
            &[],
        )
        .await
        .expect("branch lix_file rename should succeed");
    finish_measurement(&backend, before)
}

async fn run_lix_file_branch_update_hidden(
    base_rows: usize,
    update_rows: usize,
) -> AmplificationRun {
    let (backend, engine, main_version_id) = setup_counting_engine().await;
    let main = open_main_session(&engine, &main_version_id).await;
    main.execute(&insert_lix_file_descriptor_sql(base_rows), &[])
        .await
        .expect("base lix_file descriptors should insert");
    let branch = create_branch(&engine, &main, "amplification-file-update-hidden").await;
    let before = start_measurement(&backend);
    branch
        .execute(&update_lix_file_hidden_sql(update_rows), &[])
        .await
        .expect("branch lix_file hidden update should succeed");
    finish_measurement(&backend, before)
}

fn synthetic_file_bytes(size: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; size];
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    for (index, byte) in bytes.iter_mut().enumerate() {
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        state = state.wrapping_add(index as u64);
        *byte = (state.wrapping_mul(0x2545_f491_4f6c_dd1d) >> 56) as u8;
    }
    bytes
}

fn stress_file_bytes_from_env() -> usize {
    std::env::var("LIX_FILE_STRESS_BYTES")
        .ok()
        .and_then(|value| parse_size_bytes(&value))
        .unwrap_or(100 * 1024 * 1024)
}

fn parse_size_bytes(value: &str) -> Option<usize> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lowercase = trimmed.to_ascii_lowercase();
    let (number, multiplier) = if let Some(number) = lowercase.strip_suffix("gib") {
        (number, 1024usize * 1024 * 1024)
    } else if let Some(number) = lowercase.strip_suffix("gb") {
        (number, 1000usize * 1000 * 1000)
    } else if let Some(number) = lowercase.strip_suffix("mib") {
        (number, 1024usize * 1024)
    } else if let Some(number) = lowercase.strip_suffix("mb") {
        (number, 1000usize * 1000)
    } else if let Some(number) = lowercase.strip_suffix("kib") {
        (number, 1024usize)
    } else if let Some(number) = lowercase.strip_suffix("kb") {
        (number, 1000usize)
    } else {
        (trimmed, 1usize)
    };
    number.trim().parse::<usize>().ok()?.checked_mul(multiplier)
}

fn print_amplification_row(rows: usize, value_bytes: usize, run: &AmplificationRun) {
    let counts = &run.counts;
    print_category_rows(rows, value_bytes, run);
    println!(
        "AMPLIFICATION rows={rows} value_bytes={value_bytes} read_calls={} read_items={} \
         get_values_calls={} get_values_keys={} exists_many_calls={} exists_many_keys={} \
         scan_calls={} scan_rows={} write_batches={} puts={} deletes={} write_mutations={} \
         write_bytes={} read_calls_per_row={:.3} read_items_per_row={:.3} \
         write_mutations_per_row={:.3} write_bytes_per_row={:.1}",
        counts.read_calls(),
        counts.read_items(),
        counts.get_values_calls,
        counts.get_values_keys,
        counts.exists_many_calls,
        counts.exists_many_keys,
        counts.scan_keys_calls + counts.scan_values_calls + counts.scan_entries_calls,
        counts.scan_keys_rows + counts.scan_values_rows + counts.scan_entries_rows,
        counts.write_kv_batch_calls,
        counts.puts,
        counts.deletes,
        counts.write_mutations(),
        counts.write_bytes,
        counts.read_calls() as f64 / rows as f64,
        counts.read_items() as f64 / rows as f64,
        counts.write_mutations() as f64 / rows as f64,
        counts.write_bytes as f64 / rows as f64,
    );

    for namespace in counts
        .puts_by_namespace
        .keys()
        .chain(counts.deletes_by_namespace.keys())
        .chain(counts.bytes_by_namespace.keys())
        .collect::<std::collections::BTreeSet<_>>()
    {
        println!(
            "AMPLIFICATION_NAMESPACE rows={rows} namespace={} puts={} deletes={} bytes={}",
            namespace,
            counts
                .puts_by_namespace
                .get(namespace)
                .copied()
                .unwrap_or(0),
            counts
                .deletes_by_namespace
                .get(namespace)
                .copied()
                .unwrap_or(0),
            counts
                .bytes_by_namespace
                .get(namespace)
                .copied()
                .unwrap_or(0),
        );
    }
}

fn print_category_rows(rows: usize, value_bytes: usize, run: &AmplificationRun) {
    let counts = &run.counts;
    let storage = &run.storage;
    let canonical_changelog_row_namespaces = ["changelog.change", "changelog.change_pack"];
    let canonical_commit_pack_namespaces =
        ["commit_record", "change_record_pack", "change_ref_pack"];
    let canonical_storage_namespaces = [
        "changelog.change",
        "changelog.change_pack",
        "commit_record",
        "change_record_pack",
        "change_ref_pack",
    ];
    let index_storage_namespaces = [
        "tracked_state.tree.chunk",
        "tracked_state.tree.root",
        "tracked_state.tree.root.by_file",
        "tracked_state.delta_pack",
        "change_id_index",
    ];
    let payload_storage_namespaces = [
        "json_store.json",
        "json_store.json_chunk",
        "binary_cas.manifest",
        "binary_cas.manifest_chunk",
        "binary_cas.chunk",
    ];
    let sidecar_storage_namespaces = ["untracked_state.row"];
    let canonical_storage = storage_totals_for(storage, &canonical_storage_namespaces);
    let canonical_changelog_row_storage =
        storage_totals_for(storage, &canonical_changelog_row_namespaces);
    let canonical_commit_pack_storage =
        storage_totals_for(storage, &canonical_commit_pack_namespaces);
    let index_storage = storage_totals_for(storage, &index_storage_namespaces);
    let payload_storage = storage_totals_for(storage, &payload_storage_namespaces);
    let sidecar_storage = storage_totals_for(storage, &sidecar_storage_namespaces);
    let index_puts = counts.puts_in("tracked_state.tree.chunk")
        + counts.puts_in("tracked_state.tree.root")
        + counts.puts_in("tracked_state.tree.root.by_file")
        + counts.puts_in("tracked_state.delta_pack")
        + counts.puts_in("change_id_index");
    let index_bytes = counts.bytes_in("tracked_state.tree.chunk")
        + counts.bytes_in("tracked_state.tree.root")
        + counts.bytes_in("tracked_state.tree.root.by_file")
        + counts.bytes_in("tracked_state.delta_pack")
        + counts.bytes_in("change_id_index");
    let payload_puts: usize = payload_storage_namespaces
        .iter()
        .map(|namespace| counts.puts_in(namespace))
        .sum();
    let payload_bytes: usize = payload_storage_namespaces
        .iter()
        .map(|namespace| counts.bytes_in(namespace))
        .sum();
    let logical_value_bytes = rows.saturating_mul(value_bytes);
    let scan_calls = counts.scan_keys_calls + counts.scan_values_calls + counts.scan_entries_calls;
    let scan_rows = counts.scan_keys_rows + counts.scan_values_rows + counts.scan_entries_rows;
    let changelog_encoded_objects = counts.puts_in("changelog.change")
        + counts.puts_in("commit_record")
        + counts.puts_in("change_record_pack")
        + counts.puts_in("change_ref_pack");
    let tracked_encoded_objects = index_puts;
    let sidecar_encoded_objects = counts.puts_in("untracked_state.row");

    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=row logical_rows={rows} \
         physical_put_rows={} physical_delete_rows={} physical_row_mutations={} \
         row_mutations_per_logical_row={:.3}",
        counts.puts,
        counts.deletes,
        counts.write_mutations(),
        counts.write_mutations() as f64 / rows as f64,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=write write_transactions={} commits={} \
         write_batches={} puts={} deletes={} write_mutations={} write_bytes={} \
         write_mutations_per_row={:.3} write_bytes_per_row={:.1}",
        counts.begin_write_transactions,
        counts.commits,
        counts.write_kv_batch_calls,
        counts.puts,
        counts.deletes,
        counts.write_mutations(),
        counts.write_bytes,
        counts.write_mutations() as f64 / rows as f64,
        counts.write_bytes as f64 / rows as f64,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=storage before_entries={} after_entries={} \
         added_entries={} updated_entries={} removed_entries={} touched_entries={} \
         before_key_value_bytes={} after_key_value_bytes={} net_key_value_bytes_delta={} \
         changed_after_key_value_bytes={} before_namespace_key_value_bytes={} \
         after_namespace_key_value_bytes={} net_namespace_key_value_bytes_delta={} \
         changed_after_namespace_key_value_bytes={} touched_entries_per_row={:.3} \
         net_key_value_bytes_delta_per_row={:.1} changed_after_key_value_bytes_per_row={:.1} \
         net_namespace_key_value_bytes_delta_per_row={:.1} \
         changed_after_namespace_key_value_bytes_per_row={:.1}",
        storage.before_entries,
        storage.after_entries,
        storage.added_entries,
        storage.updated_entries,
        storage.removed_entries,
        storage.touched_entries(),
        storage.before_key_value_bytes,
        storage.after_key_value_bytes,
        storage.net_key_value_bytes_delta(),
        storage.changed_after_key_value_bytes(),
        storage.before_namespace_key_value_bytes,
        storage.after_namespace_key_value_bytes,
        storage.net_namespace_key_value_bytes_delta(),
        storage.changed_after_namespace_key_value_bytes(),
        storage.touched_entries() as f64 / rows as f64,
        storage.net_key_value_bytes_delta() as f64 / rows as f64,
        storage.changed_after_key_value_bytes() as f64 / rows as f64,
        storage.net_namespace_key_value_bytes_delta() as f64 / rows as f64,
        storage.changed_after_namespace_key_value_bytes() as f64 / rows as f64,
    );
    print_storage_class_row(
        rows,
        "storage_canonical",
        &canonical_storage_namespaces,
        &canonical_storage,
    );
    print_storage_class_row(
        rows,
        "storage_canonical_changelog_rows",
        &canonical_changelog_row_namespaces,
        &canonical_changelog_row_storage,
    );
    print_storage_class_row(
        rows,
        "storage_canonical_commit_packs",
        &canonical_commit_pack_namespaces,
        &canonical_commit_pack_storage,
    );
    print_storage_class_row(
        rows,
        "storage_index",
        &index_storage_namespaces,
        &index_storage,
    );
    print_storage_class_row(
        rows,
        "storage_payload",
        &payload_storage_namespaces,
        &payload_storage,
    );
    print_storage_class_row(
        rows,
        "storage_sidecar",
        &sidecar_storage_namespaces,
        &sidecar_storage,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=read read_transactions={} rollbacks={} \
         read_calls={} read_items={} get_values_calls={} get_values_keys={} \
         exists_many_calls={} exists_many_keys={} scan_calls={} scan_rows={} \
         read_calls_per_row={:.3} read_items_per_row={:.3}",
        counts.begin_read_transactions,
        counts.rollbacks,
        counts.read_calls(),
        counts.read_items(),
        counts.get_values_calls,
        counts.get_values_keys,
        counts.exists_many_calls,
        counts.exists_many_keys,
        scan_calls,
        scan_rows,
        counts.read_calls() as f64 / rows as f64,
        counts.read_items() as f64 / rows as f64,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=serialization proxy_encoded_put_objects={} \
         proxy_changelog_objects={} proxy_json_objects={} proxy_tracked_index_objects={} \
         proxy_sidecar_objects={} proxy_encoded_objects_per_row={:.3}",
        counts.puts,
        changelog_encoded_objects,
        payload_puts,
        tracked_encoded_objects,
        sidecar_encoded_objects,
        counts.puts as f64 / rows as f64,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=index index_puts={} index_deletes={} \
         index_mutations={} index_bytes={} tracked_chunk_puts={} tracked_root_puts={} \
         tracked_by_file_root_puts={} index_mutations_per_row={:.3} index_bytes_per_row={:.1}",
        index_puts,
        0,
        index_puts,
        index_bytes,
        counts.puts_in("tracked_state.tree.chunk"),
        counts.puts_in("tracked_state.tree.root"),
        counts.puts_in("tracked_state.tree.root.by_file"),
        index_puts as f64 / rows as f64,
        index_bytes as f64 / rows as f64,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=payload logical_value_bytes={} \
         external_payload_puts={} external_payload_bytes={} external_payload_puts_per_row={:.3} \
         external_payload_bytes_per_row={:.1} external_payload_bytes_per_logical_value_byte={:.3}",
        logical_value_bytes,
        payload_puts,
        payload_bytes,
        payload_puts as f64 / rows as f64,
        payload_bytes as f64 / rows as f64,
        payload_bytes as f64 / logical_value_bytes.max(1) as f64,
    );
    println!(
        "AMPLIFICATION_CATEGORY rows={rows} category=sidecar_overlay untracked_puts={} \
         untracked_deletes={} untracked_bytes={} untracked_mutations_per_row={:.3}",
        counts.puts_in("untracked_state.row"),
        counts.deletes_in("untracked_state.row"),
        counts.bytes_in("untracked_state.row"),
        (counts.puts_in("untracked_state.row") + counts.deletes_in("untracked_state.row")) as f64
            / rows as f64,
    );

    for (namespace, namespace_storage) in &storage.by_namespace {
        println!(
            "AMPLIFICATION_STORAGE_NAMESPACE rows={rows} namespace={} added_entries={} \
             updated_entries={} removed_entries={} touched_entries={} net_key_value_bytes_delta={} \
             changed_after_key_value_bytes={} net_namespace_key_value_bytes_delta={} \
             changed_after_namespace_key_value_bytes={}",
            namespace,
            namespace_storage.added_entries,
            namespace_storage.updated_entries,
            namespace_storage.removed_entries,
            namespace_storage.touched_entries(),
            namespace_storage.net_key_value_bytes_delta(),
            namespace_storage.changed_after_key_value_bytes(),
            namespace_storage.net_namespace_key_value_bytes_delta(),
            namespace_storage.changed_after_namespace_key_value_bytes(),
        );
    }
}

fn print_amplification_case(
    name: &str,
    base_rows: usize,
    logical_rows: usize,
    value_bytes: usize,
    run: &AmplificationRun,
) {
    println!(
        "AMPLIFICATION_CASE name={name} base_rows={base_rows} logical_rows={logical_rows} value_bytes={value_bytes}",
    );
    print_amplification_row(logical_rows, value_bytes, run);
}

#[tokio::test]
#[ignore = "prints read/write amplification north-star metrics for lix_key_value inserts"]
async fn lix_key_value_insert_amplification_north_star() {
    let value_bytes = 8;
    for rows in [1usize, 100, 1_000] {
        let run = run_insert(rows, value_bytes).await;
        print_amplification_row(rows, value_bytes, &run);
    }
}

#[tokio::test]
#[ignore = "stress test for large lix_file.data inserts; defaults to 100MiB"]
async fn lix_file_data_stress_amplification() {
    let file_bytes = stress_file_bytes_from_env();
    println!(
        "AMPLIFICATION_FILE_STRESS logical_files=1 logical_file_bytes={} env=LIX_FILE_STRESS_BYTES",
        file_bytes
    );
    let run = run_lix_file_insert_data(file_bytes).await;
    print_amplification_row(1, file_bytes, &run);
}

#[tokio::test]
#[ignore = "prints branching amplification canaries for lix_key_value"]
async fn lix_key_value_branching_amplification_canaries() {
    let branch_only = run_branch_from_head_only().await;
    print_amplification_case("kv_branch_from_head_only", 0, 1, 0, &branch_only);

    let branch_insert = run_key_value_branch_insert().await;
    print_amplification_case("kv_branch_then_insert_1", 0, 1, 12, &branch_insert);

    let update_one = run_key_value_branch_update(1_000, 1).await;
    print_amplification_case("kv_branch_from_1000_update_1", 1_000, 1, 14, &update_one);

    let update_hundred = run_key_value_branch_update(1_000, 100).await;
    print_amplification_case(
        "kv_branch_from_1000_update_100",
        1_000,
        100,
        14,
        &update_hundred,
    );
}

#[tokio::test]
#[ignore = "prints branching amplification canaries for lix_file"]
async fn lix_file_branching_amplification_canaries() {
    let file_bytes = 1024;

    let branch_insert = run_lix_file_branch_insert(file_bytes).await;
    print_amplification_case(
        "file_branch_then_insert_1k_data",
        0,
        1,
        file_bytes,
        &branch_insert,
    );

    let update_data = run_lix_file_branch_update_data(1_000, file_bytes).await;
    print_amplification_case(
        "file_branch_from_1000_update_data_1",
        1_000,
        1,
        file_bytes,
        &update_data,
    );

    let rename = run_lix_file_branch_rename(1_000).await;
    print_amplification_case("file_branch_from_1000_rename_1", 1_000, 1, 0, &rename);

    let update_hidden = run_lix_file_branch_update_hidden(1_000, 100).await;
    print_amplification_case(
        "file_branch_from_1000_update_hidden_100",
        1_000,
        100,
        0,
        &update_hidden,
    );
}
