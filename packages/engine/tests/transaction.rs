use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvGetRequest, BackendKvGetResult, BackendKvGetResultGroup, BackendKvPair,
    BackendKvScanRange, BackendKvScanRequest, BackendKvScanResult, BackendKvWriteBatch,
    BackendKvWriteStats, BackendReadTransaction, BackendWriteTransaction, Engine, LixError,
};

type KvKey = (String, Vec<u8>);
type KvMap = BTreeMap<KvKey, Vec<u8>>;

#[tokio::test]
async fn read_sql_rolls_back_read_transaction_when_pre_plan_setup_fails() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("backend should initialize");
    let engine = Engine::new(Box::new(backend.clone()))
        .await
        .expect("initialized backend should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "UPDATE lix_key_value SET value = 'missing-version' \
             WHERE key = 'lix_workspace_version_id'",
            &[],
        )
        .await
        .expect("test should corrupt workspace selector");

    let before = backend.stats();
    let error = session
        .execute("SELECT 1", &[])
        .await
        .expect_err("missing active version should fail read pre-plan");
    assert!(
        error.message.contains("missing-version"),
        "unexpected error: {error:?}"
    );

    let delta = backend.stats().delta_since(&before);
    assert_eq!(delta.read_opened, 1, "read SQL should open one read tx");
    assert_eq!(
        delta.read_rolled_back, 1,
        "read SQL pre-plan errors must roll back the opened read tx"
    );
}

#[tokio::test]
async fn write_transaction_open_rolls_back_when_active_version_resolution_fails() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("backend should initialize");
    let engine = Engine::new(Box::new(backend.clone()))
        .await
        .expect("initialized backend should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "UPDATE lix_key_value SET value = 'missing-version' \
             WHERE key = 'lix_workspace_version_id'",
            &[],
        )
        .await
        .expect("test should corrupt workspace selector");

    let before = backend.stats();
    let error = session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('after-corrupt-selector', 'value')",
            &[],
        )
        .await
        .expect_err("missing active version should fail write open");
    assert_eq!(error.code, "LIX_VERSION_NOT_FOUND");

    let delta = backend.stats().delta_since(&before);
    assert_eq!(delta.write_opened, 1, "write path should open one write tx");
    assert_eq!(
        delta.write_rolled_back, 1,
        "write open errors must roll back the opened write tx"
    );
    assert_eq!(
        delta.write_committed, 0,
        "failed write open must not commit"
    );
}

#[tokio::test]
async fn rebuild_tracked_state_rolls_back_read_and_write_transactions_on_failure() {
    let backend = RecordingBackend::new();
    let receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("backend should initialize");
    let engine = Engine::new(Box::new(backend.clone()))
        .await
        .expect("initialized backend should create an engine");

    backend.fail_scan_namespace("changelog.change");
    let before = backend.stats();
    let error = engine
        .rebuild_tracked_state_for_version(&receipt.main_version_id)
        .await
        .expect_err("forced changelog scan failure should fail rebuild");
    assert!(
        error.message.contains("forced scan failure"),
        "unexpected error: {error:?}"
    );

    let delta = backend.stats().delta_since(&before);
    assert_eq!(
        delta.read_opened, delta.read_rolled_back,
        "every read tx opened during failed rebuild must be rolled back"
    );
    assert_eq!(delta.write_opened, 1, "rebuild should open one write tx");
    assert_eq!(
        delta.write_rolled_back, 1,
        "failed rebuild must roll back the opened write tx"
    );
    assert_eq!(delta.write_committed, 0, "failed rebuild must not commit");
}

#[derive(Clone, Default)]
struct RecordingBackend {
    data: Arc<Mutex<KvMap>>,
    stats: Arc<TransactionStats>,
    fail_scan_namespace: Arc<Mutex<Option<String>>>,
}

impl RecordingBackend {
    fn new() -> Self {
        Self::default()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.stats.snapshot()
    }

    fn fail_scan_namespace(&self, namespace: &str) {
        *self
            .fail_scan_namespace
            .lock()
            .expect("fail namespace lock should not poison") = Some(namespace.to_string());
    }
}

#[async_trait]
impl Backend for RecordingBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        self.stats.read_opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(RecordingTransaction {
            data: Arc::clone(&self.data),
            pending: BTreeMap::new(),
            stats: Arc::clone(&self.stats),
            fail_scan_namespace: Arc::clone(&self.fail_scan_namespace),
            mode: RecordingTransactionMode::Read,
        }))
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        self.stats.write_opened.fetch_add(1, Ordering::SeqCst);
        Ok(Box::new(RecordingTransaction {
            data: Arc::clone(&self.data),
            pending: BTreeMap::new(),
            stats: Arc::clone(&self.stats),
            fail_scan_namespace: Arc::clone(&self.fail_scan_namespace),
            mode: RecordingTransactionMode::Write,
        }))
    }
}

struct RecordingTransaction {
    data: Arc<Mutex<KvMap>>,
    pending: BTreeMap<KvKey, Option<Vec<u8>>>,
    stats: Arc<TransactionStats>,
    fail_scan_namespace: Arc<Mutex<Option<String>>>,
    mode: RecordingTransactionMode,
}

#[derive(Clone, Copy)]
enum RecordingTransactionMode {
    Read,
    Write,
}

#[async_trait]
impl BackendReadTransaction for RecordingTransaction {
    async fn get_kv_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvGetResult, LixError> {
        let data = self.data.lock().expect("recording backend lock poisoned");
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let mut values = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let identity = (group.namespace.clone(), key);
                values.push(
                    self.pending
                        .get(&identity)
                        .cloned()
                        .unwrap_or_else(|| data.get(&identity).cloned()),
                );
            }
            groups.push(BackendKvGetResultGroup {
                namespace: group.namespace,
                values,
            });
        }
        Ok(BackendKvGetResult { groups })
    }

    async fn scan_kv(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvScanResult, LixError> {
        if self
            .fail_scan_namespace
            .lock()
            .expect("fail namespace lock should not poison")
            .as_deref()
            == Some(request.namespace.as_str())
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("forced scan failure for namespace {}", request.namespace),
            ));
        }

        let mut visible = self
            .data
            .lock()
            .expect("recording backend lock poisoned")
            .clone();
        for (key, value) in &self.pending {
            match value {
                Some(value) => {
                    visible.insert(key.clone(), value.clone());
                }
                None => {
                    visible.remove(key);
                }
            }
        }

        let scan_limit = request
            .limit
            .checked_add(1 + usize::from(request.after.is_some()))
            .unwrap_or(request.limit);
        let mut rows = scan_map(
            &visible,
            &request.namespace,
            &request.range,
            Some(scan_limit),
        )
        .into_iter()
        .filter(|row| {
            request
                .after
                .as_deref()
                .is_none_or(|after| row.key.as_slice() > after)
        })
        .collect::<Vec<_>>();
        let has_more = rows.len() > request.limit;
        rows.truncate(request.limit);
        let resume_after = has_more
            .then(|| rows.last().map(|row| row.key.clone()))
            .flatten();
        Ok(BackendKvScanResult { rows, resume_after })
    }

    async fn rollback(self: Box<Self>) -> Result<(), LixError> {
        match self.mode {
            RecordingTransactionMode::Read => {
                self.stats.read_rolled_back.fetch_add(1, Ordering::SeqCst);
            }
            RecordingTransactionMode::Write => {
                self.stats.write_rolled_back.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }
}

#[async_trait]
impl BackendWriteTransaction for RecordingTransaction {
    async fn write_kv_batch(
        &mut self,
        batch: BackendKvWriteBatch,
    ) -> Result<BackendKvWriteStats, LixError> {
        let mut stats = BackendKvWriteStats::default();
        for group in batch.groups {
            for put in group.puts {
                stats.puts += 1;
                stats.bytes_written += put.key.len() + put.value.len();
                self.pending
                    .insert((group.namespace.clone(), put.key), Some(put.value));
            }
            for key in group.deletes {
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.pending.insert((group.namespace.clone(), key), None);
            }
        }
        Ok(stats)
    }

    async fn commit(mut self: Box<Self>) -> Result<(), LixError> {
        self.stats.write_committed.fetch_add(1, Ordering::SeqCst);
        let mut guard = self.data.lock().expect("recording backend lock poisoned");
        for (key, value) in std::mem::take(&mut self.pending) {
            match value {
                Some(value) => {
                    guard.insert(key, value);
                }
                None => {
                    guard.remove(&key);
                }
            }
        }
        Ok(())
    }
}

fn scan_map(
    map: &KvMap,
    namespace: &str,
    range: &BackendKvScanRange,
    limit: Option<usize>,
) -> Vec<BackendKvPair> {
    let mut pairs = map
        .iter()
        .filter_map(|((entry_namespace, key), value)| {
            if entry_namespace != namespace || !key_in_range(key, range) {
                return None;
            }
            Some(BackendKvPair::new(key.clone(), value.clone()))
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.key.cmp(&right.key));
    if let Some(limit) = limit {
        pairs.truncate(limit);
    }
    pairs
}

fn key_in_range(key: &[u8], range: &BackendKvScanRange) -> bool {
    match range {
        BackendKvScanRange::Prefix(prefix) => key.starts_with(prefix),
        BackendKvScanRange::Range { start, end } => key >= start.as_slice() && key < end.as_slice(),
    }
}

#[derive(Default)]
struct TransactionStats {
    read_opened: AtomicUsize,
    read_rolled_back: AtomicUsize,
    write_opened: AtomicUsize,
    write_committed: AtomicUsize,
    write_rolled_back: AtomicUsize,
}

impl TransactionStats {
    fn snapshot(&self) -> TransactionStatsSnapshot {
        TransactionStatsSnapshot {
            read_opened: self.read_opened.load(Ordering::SeqCst),
            read_rolled_back: self.read_rolled_back.load(Ordering::SeqCst),
            write_opened: self.write_opened.load(Ordering::SeqCst),
            write_committed: self.write_committed.load(Ordering::SeqCst),
            write_rolled_back: self.write_rolled_back.load(Ordering::SeqCst),
        }
    }
}

#[derive(Clone, Copy)]
struct TransactionStatsSnapshot {
    read_opened: usize,
    read_rolled_back: usize,
    write_opened: usize,
    write_committed: usize,
    write_rolled_back: usize,
}

impl TransactionStatsSnapshot {
    fn delta_since(self, before: &Self) -> Self {
        Self {
            read_opened: self.read_opened - before.read_opened,
            read_rolled_back: self.read_rolled_back - before.read_rolled_back,
            write_opened: self.write_opened - before.write_opened,
            write_committed: self.write_committed - before.write_committed,
            write_rolled_back: self.write_rolled_back - before.write_rolled_back,
        }
    }
}
