use std::ops::Bound;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;

use lix_engine::backend::{
    Backend, BackendCapabilities, BackendError, BackendRead, BackendWrite, CommitResult,
    GetOptions, InMemoryBackend, InMemoryRead, InMemoryWrite, Key, KeyRange, PointVisitor,
    PutBatch, ReadOptions, ScanOptions, WriteOptions,
};
use lix_engine::Engine;

#[tokio::test]
async fn read_sql_does_not_open_write_when_pre_plan_setup_fails() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend.clone())
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
        delta.write_opened, 0,
        "failed read SQL must not open writes"
    );
}

#[tokio::test]
async fn write_setup_failure_does_not_open_backend_write() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend.clone())
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
    assert_eq!(
        delta.write_opened, 0,
        "write setup failure should not open a backend write"
    );
    assert_eq!(
        delta.write_committed, 0,
        "failed write setup must not commit"
    );
}

#[tokio::test]
async fn rebuild_tracked_state_does_not_commit_on_read_failure() {
    let backend = RecordingBackend::new();
    let receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend.clone())
        .await
        .expect("initialized backend should create an engine");

    backend.fail_read_namespace("changelog.commit_visibility");
    let before = backend.stats();
    let error = engine
        .rebuild_tracked_state_for_version(&receipt.main_version_id)
        .await
        .expect_err("forced changelog read failure should fail rebuild");
    assert!(
        error.message.contains("forced read failure"),
        "unexpected error: {error:?}"
    );

    let delta = backend.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 0,
        "failed rebuild should not open a backend write"
    );
    assert_eq!(delta.write_committed, 0, "failed rebuild must not commit");
}

#[tokio::test]
async fn write_segment_failure_does_not_commit_backend_write() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend.clone())
        .await
        .expect("initialized backend should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    backend.fail_write_namespace("changelog.segment");
    let before = backend.stats();
    let error = session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('segment-write-failure', 'value')",
            &[],
        )
        .await
        .expect_err("forced segment write failure should fail transaction commit");
    assert!(
        error.message.contains("forced write failure"),
        "unexpected error: {error:?}"
    );

    let delta = backend.stats().delta_since(&before);
    assert_eq!(delta.write_opened, 1, "write should open a backend write");
    assert_eq!(
        delta.write_committed, 0,
        "failed changelog segment write must not commit"
    );
}

#[tokio::test]
async fn active_transaction_blocks_session_read_and_allows_transaction_read() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend)
        .await
        .expect("initialized backend should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
             VALUES ('lix_deterministic_mode', \
             lix_json('{\"enabled\":true}'), true, true)",
            &[],
        )
        .await
        .expect("deterministic mode insert should succeed");

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");

    let error = session
        .execute("SELECT lix_uuid_v7()", &[])
        .await
        .expect_err("session read should be blocked while transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let result = tx
        .execute("SELECT lix_uuid_v7()", &[])
        .await
        .expect("deterministic transaction read should succeed");
    assert_eq!(
        result
            .rows()
            .first()
            .expect("read should return a row")
            .get::<String>("lix_uuid_v7()")
            .expect("uuid should be returned as text"),
        "01920000-0000-7000-8000-000000000000",
    );

    tx.rollback()
        .await
        .expect("transaction rollback should succeed");
}

#[tokio::test]
async fn transaction_read_can_query_history_surfaces() {
    let backend = RecordingBackend::new();
    let _receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend)
        .await
        .expect("initialized backend should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('history-visible-in-tx', 'value')",
            &[],
        )
        .await
        .expect("seed write should succeed");

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    let result = tx
        .execute(
            "SELECT entity_id FROM lix_state_history \
             WHERE start_commit_id = lix_active_version_commit_id() \
             AND schema_key = 'lix_key_value'",
            &[],
        )
        .await
        .expect("transaction read should register history surfaces");

    assert!(
        !result.rows().is_empty(),
        "transaction history read should see committed history rows"
    );

    tx.rollback()
        .await
        .expect("transaction rollback should succeed");
}

#[tokio::test]
async fn begin_transaction_cannot_race_with_opening_session_write() {
    let backend = BlockingBeginWriteBackend::new();
    let gate = backend.gate();
    let _receipt = Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend)
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
    inner: InMemoryBackend,
    stats: Arc<TransactionStats>,
    fail_read_namespace: Arc<Mutex<Option<String>>>,
    fail_write_namespace: Arc<Mutex<Option<String>>>,
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

impl Backend for BlockingBeginWriteBackend {
    type Read<'a>
        = <RecordingBackend as Backend>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = <RecordingBackend as Backend>::Write<'a>
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.inner.begin_read(opts)
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.gate.maybe_block();
        self.inner.begin_write(opts)
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

    fn fail_write_namespace(&self, namespace: &str) {
        *self
            .fail_write_namespace
            .lock()
            .expect("fail namespace lock should not poison") = Some(namespace.to_string());
    }
}

impl Backend for RecordingBackend {
    type Read<'a>
        = RecordingRead
    where
        Self: 'a;

    type Write<'a>
        = RecordingWrite
    where
        Self: 'a;

    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.stats.read_opened.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingRead {
            inner: self.inner.begin_read(opts)?,
            stats: Arc::clone(&self.stats),
            fail_read_namespace: Arc::clone(&self.fail_read_namespace),
        })
    }

    fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.stats.write_opened.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingWrite {
            inner: self.inner.begin_write(opts)?,
            stats: Arc::clone(&self.stats),
            fail_write_namespace: Arc::clone(&self.fail_write_namespace),
        })
    }
}

#[derive(Clone)]
struct RecordingRead {
    inner: InMemoryRead,
    stats: Arc<TransactionStats>,
    fail_read_namespace: Arc<Mutex<Option<String>>>,
}

struct RecordingWrite {
    inner: InMemoryWrite,
    stats: Arc<TransactionStats>,
    fail_write_namespace: Arc<Mutex<Option<String>>>,
}

impl BackendRead for RecordingRead {
    type RangeScan<'cursor> = <InMemoryRead as BackendRead>::RangeScan<'cursor>;

    fn visit_keys<V>(
        &self,
        keys: &[Key],
        opts: GetOptions<'_>,
        visitor: &mut V,
    ) -> Result<(), BackendError>
    where
        V: PointVisitor + ?Sized,
    {
        self.fail_if_keys_match(keys)?;
        self.inner.visit_keys(keys, opts, visitor)
    }

    fn with_range_scan<T, F>(
        &self,
        range: KeyRange,
        opts: ScanOptions<'_>,
        f: F,
    ) -> Result<T, BackendError>
    where
        F: FnOnce(&mut Self::RangeScan<'_>) -> Result<T, BackendError>,
    {
        self.fail_if_range_matches(&range)?;
        self.inner.with_range_scan(range, opts, f)
    }

    fn close(self) -> Result<(), BackendError> {
        self.stats.read_rolled_back.fetch_add(1, Ordering::SeqCst);
        self.inner.close()
    }
}

impl BackendWrite for RecordingWrite {
    fn put_many(&mut self, entries: PutBatch) -> Result<(), BackendError> {
        self.fail_if_entries_match(&entries)?;
        self.inner.put_many(entries)
    }

    fn delete_many(&mut self, keys: &[Key]) -> Result<(), BackendError> {
        self.inner.delete_many(keys)
    }

    fn delete_range(&mut self, range: KeyRange) -> Result<(), BackendError> {
        self.inner.delete_range(range)
    }

    fn commit(self) -> Result<CommitResult, BackendError> {
        self.stats.write_committed.fetch_add(1, Ordering::SeqCst);
        self.inner.commit()
    }

    fn rollback(self) -> Result<(), BackendError> {
        self.stats.write_rolled_back.fetch_add(1, Ordering::SeqCst);
        self.inner.rollback()
    }
}

impl RecordingWrite {
    fn fail_if_entries_match(&self, entries: &PutBatch) -> Result<(), BackendError> {
        if let Some(namespace) = self.fail_write_namespace() {
            if let Some(prefix) = namespace_prefix(&namespace) {
                if entries
                    .entries
                    .iter()
                    .any(|entry| key_has_space_prefix(&entry.key, &prefix))
                {
                    return Err(forced_write_failure(&namespace));
                }
            }
        }
        Ok(())
    }

    fn fail_write_namespace(&self) -> Option<String> {
        self.fail_write_namespace
            .lock()
            .expect("fail namespace lock should not poison")
            .clone()
    }
}

impl RecordingRead {
    fn fail_if_keys_match(&self, keys: &[Key]) -> Result<(), BackendError> {
        if let Some(namespace) = self.fail_read_namespace() {
            if let Some(prefix) = namespace_prefix(&namespace) {
                if keys.iter().any(|key| key_has_space_prefix(key, &prefix)) {
                    return Err(forced_read_failure(&namespace));
                }
            }
        }
        Ok(())
    }

    fn fail_if_range_matches(&self, range: &KeyRange) -> Result<(), BackendError> {
        if let Some(namespace) = self.fail_read_namespace() {
            if let Some(prefix) = namespace_prefix(&namespace) {
                if range_may_include_space_prefix(range, &prefix) {
                    return Err(forced_read_failure(&namespace));
                }
            }
        }
        Ok(())
    }

    fn fail_read_namespace(&self) -> Option<String> {
        self.fail_read_namespace
            .lock()
            .expect("fail namespace lock should not poison")
            .clone()
    }
}

fn namespace_prefix(namespace: &str) -> Option<[u8; 4]> {
    match namespace {
        "changelog.commit_visibility" => {
            Some(lix_engine::changelog::COMMIT_VISIBILITY_SPACE.physical_prefix())
        }
        "changelog.segment" => Some(lix_engine::changelog::SEGMENT_SPACE.physical_prefix()),
        _ => None,
    }
}

fn key_has_space_prefix(key: &Key, prefix: &[u8; 4]) -> bool {
    key.0.starts_with(prefix)
}

fn range_may_include_space_prefix(range: &KeyRange, prefix: &[u8; 4]) -> bool {
    let lower_allows = match &range.lower {
        Bound::Unbounded => true,
        Bound::Included(key) => key.0.as_ref() <= prefix.as_slice(),
        Bound::Excluded(key) => key.0.as_ref() < prefix.as_slice(),
    };
    let upper_allows = match &range.upper {
        Bound::Unbounded => true,
        Bound::Included(key) => key.0.as_ref() >= prefix.as_slice(),
        Bound::Excluded(key) => key.0.as_ref() > prefix.as_slice(),
    };
    lower_allows && upper_allows
}

fn forced_read_failure(namespace: &str) -> BackendError {
    BackendError::Io(format!("forced read failure for namespace {namespace}"))
}

fn forced_write_failure(namespace: &str) -> BackendError {
    BackendError::Io(format!("forced write failure for namespace {namespace}"))
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
