use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use async_trait::async_trait;
use lix_engine::{
    Backend, BackendKvEntryPage, BackendKvExistsBatch, BackendKvExistsGroup, BackendKvGetRequest,
    BackendKvKeyPage, BackendKvScanRange, BackendKvScanRequest, BackendKvValueBatch,
    BackendKvValueGroup, BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteStats,
    BackendReadTransaction, BackendWriteTransaction, BytePageBuilder, Engine, LixError,
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

    backend.fail_read_namespace("commit_store.commit");
    let before = backend.stats();
    let error = engine
        .rebuild_tracked_state_for_version(&receipt.main_version_id)
        .await
        .expect_err("forced commit-store read failure should fail rebuild");
    assert!(
        error.message.contains("forced read failure"),
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

#[tokio::test]
async fn begin_transaction_cannot_race_with_opening_session_write() {
    let backend = BlockingBeginWriteBackend::new();
    let gate = backend.gate();
    let _receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("backend should initialize");
    let engine = Engine::new(Box::new(backend))
        .await
        .expect("initialized backend should create an engine");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    gate.block_next_write();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            writer_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ('racing-session-write', 'value')",
                    &[],
                )
                .await
        })
    });

    gate.wait_until_blocked();
    let error = match session.begin_transaction().await {
        Ok(_) => panic!("explicit transaction should not race past a session write reservation"),
        Err(error) => error,
    };
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    gate.release();
    writer
        .join()
        .expect("writer thread should not panic")
        .expect("session write should complete after release");

    let result = session
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'racing-session-write'",
            &[],
        )
        .await
        .expect("session write should be committed");
    assert_eq!(result.len(), 1);
}

#[derive(Clone, Default)]
struct RecordingBackend {
    data: Arc<Mutex<KvMap>>,
    stats: Arc<TransactionStats>,
    fail_read_namespace: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct BlockingBeginWriteBackend {
    inner: RecordingBackend,
    gate: BlockingBeginWriteGate,
}

impl BlockingBeginWriteBackend {
    fn new() -> Self {
        Self {
            inner: RecordingBackend::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }
}

#[async_trait]
impl Backend for BlockingBeginWriteBackend {
    async fn begin_read_transaction(
        &self,
    ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
        self.inner.begin_read_transaction().await
    }

    async fn begin_write_transaction(
        &self,
    ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
        self.gate.maybe_block();
        self.inner.begin_write_transaction().await
    }
}

#[derive(Clone)]
struct BlockingBeginWriteGate {
    state: Arc<(Mutex<BlockingBeginWriteState>, Condvar)>,
}

impl BlockingBeginWriteGate {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(BlockingBeginWriteState::default()),
                Condvar::new(),
            )),
        }
    }

    fn block_next_write(&self) {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        state.block_next = true;
        state.blocked = false;
        state.released = false;
    }

    fn maybe_block(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        if !state.block_next {
            return;
        }
        state.block_next = false;
        state.blocked = true;
        condvar.notify_all();
        while !state.released {
            state = condvar
                .wait(state)
                .expect("blocking gate lock should be available after wait");
        }
    }

    fn wait_until_blocked(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        while !state.blocked {
            state = condvar
                .wait(state)
                .expect("blocking gate lock should be available after wait");
        }
    }

    fn release(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        state.released = true;
        condvar.notify_all();
    }
}

#[derive(Default)]
struct BlockingBeginWriteState {
    block_next: bool,
    blocked: bool,
    released: bool,
}

impl RecordingBackend {
    fn new() -> Self {
        Self::default()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.stats.snapshot()
    }

    fn fail_read_namespace(&self, namespace: &str) {
        *self
            .fail_read_namespace
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
            fail_read_namespace: Arc::clone(&self.fail_read_namespace),
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
            fail_read_namespace: Arc::clone(&self.fail_read_namespace),
            mode: RecordingTransactionMode::Write,
        }))
    }
}

struct RecordingTransaction {
    data: Arc<Mutex<KvMap>>,
    pending: BTreeMap<KvKey, Option<Vec<u8>>>,
    stats: Arc<TransactionStats>,
    fail_read_namespace: Arc<Mutex<Option<String>>>,
    mode: RecordingTransactionMode,
}

#[derive(Clone, Copy)]
enum RecordingTransactionMode {
    Read,
    Write,
}

#[async_trait]
impl BackendReadTransaction for RecordingTransaction {
    async fn get_values(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvValueBatch, LixError> {
        self.fail_if_get_namespace_matches(&request)?;
        let data = self.data.lock().expect("recording backend lock poisoned");
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut values = BytePageBuilder::with_capacity(group.keys.len(), 0);
            let mut present = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let identity = (namespace.clone(), key.clone());
                let value = self
                    .pending
                    .get(&identity)
                    .cloned()
                    .unwrap_or_else(|| data.get(&identity).cloned());
                if let Some(value) = value {
                    values.push(value);
                    present.push(true);
                } else {
                    values.push([]);
                    present.push(false);
                }
            }
            groups.push(BackendKvValueGroup::new(
                namespace,
                values.finish(),
                present,
            ));
        }
        Ok(BackendKvValueBatch { groups })
    }

    async fn exists_many(
        &mut self,
        request: BackendKvGetRequest,
    ) -> Result<BackendKvExistsBatch, LixError> {
        self.fail_if_get_namespace_matches(&request)?;
        let data = self.data.lock().expect("recording backend lock poisoned");
        let mut groups = Vec::with_capacity(request.groups.len());
        for group in request.groups {
            let namespace = group.namespace.clone();
            let mut exists = Vec::with_capacity(group.keys.len());
            for key in group.keys {
                let identity = (namespace.clone(), key.clone());
                exists.push(
                    self.pending
                        .get(&identity)
                        .map(|value| value.is_some())
                        .unwrap_or_else(|| data.contains_key(&identity)),
                );
            }
            groups.push(BackendKvExistsGroup { namespace, exists });
        }
        Ok(BackendKvExistsBatch { groups })
    }

    async fn scan_keys(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvKeyPage, LixError> {
        let entries = self.scan_visible_entries(request)?;
        Ok(BackendKvKeyPage {
            keys: entries.keys,
            resume_after: entries.resume_after,
        })
    }

    async fn scan_values(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvValuePage, LixError> {
        self.fail_if_scan_namespace_matches(&request)?;
        let entries = self.scan_visible_entries(request)?;
        Ok(BackendKvValuePage {
            values: entries.values,
            resume_after: entries.resume_after,
        })
    }

    async fn scan_entries(
        &mut self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
        self.fail_if_scan_namespace_matches(&request)?;
        self.scan_visible_entries(request)
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
            let namespace = group.namespace().to_string();
            for index in 0..group.put_count() {
                let key = group.put_key(index).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "backend write batch missing put key")
                })?;
                let value = group.put_value(index).ok_or_else(|| {
                    LixError::new("LIX_ERROR_UNKNOWN", "backend write batch missing put value")
                })?;
                stats.puts += 1;
                stats.bytes_written += key.len() + value.len();
                self.pending
                    .insert((namespace.clone(), key.to_vec()), Some(value.to_vec()));
            }
            for index in 0..group.delete_count() {
                let key = group.delete_key(index).ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        "backend write batch missing delete key",
                    )
                })?;
                stats.deletes += 1;
                stats.bytes_written += key.len();
                self.pending.insert((namespace.clone(), key.to_vec()), None);
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

impl RecordingTransaction {
    fn fail_if_get_namespace_matches(&self, request: &BackendKvGetRequest) -> Result<(), LixError> {
        for group in &request.groups {
            self.fail_if_namespace_matches(&group.namespace)?;
        }
        Ok(())
    }

    fn fail_if_scan_namespace_matches(
        &self,
        request: &BackendKvScanRequest,
    ) -> Result<(), LixError> {
        self.fail_if_namespace_matches(&request.namespace)
    }

    fn fail_if_namespace_matches(&self, namespace: &str) -> Result<(), LixError> {
        if self
            .fail_read_namespace
            .lock()
            .expect("fail namespace lock should not poison")
            .as_deref()
            == Some(namespace)
        {
            return Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("forced read failure for namespace {namespace}"),
            ));
        }
        Ok(())
    }

    fn scan_visible_entries(
        &self,
        request: BackendKvScanRequest,
    ) -> Result<BackendKvEntryPage, LixError> {
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
        Ok(scan_map(&visible, &request))
    }
}

fn scan_map(map: &KvMap, request: &BackendKvScanRequest) -> BackendKvEntryPage {
    let mut pairs = map
        .iter()
        .filter_map(|((entry_namespace, key), value)| {
            if entry_namespace != &request.namespace || !key_in_range(key, &request.range) {
                return None;
            }
            if request
                .after
                .as_deref()
                .is_some_and(|after| key.as_slice() <= after)
            {
                return None;
            }
            Some((key.clone(), value.clone()))
        })
        .collect::<Vec<_>>();
    pairs.sort_by(|left, right| left.0.cmp(&right.0));
    let has_more = pairs.len() > request.limit;
    pairs.truncate(request.limit);
    let resume_after = has_more
        .then(|| pairs.last().map(|(key, _)| key.clone()))
        .flatten();
    let mut keys = BytePageBuilder::with_capacity(pairs.len(), 0);
    let mut values = BytePageBuilder::with_capacity(pairs.len(), 0);
    for (key, value) in pairs {
        keys.push(key);
        values.push(value);
    }
    BackendKvEntryPage {
        keys: keys.finish(),
        values: values.finish(),
        resume_after,
    }
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
