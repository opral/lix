use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use lix_engine::Engine;
use lix_engine::storage::{
    CommitResult, GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead, MemoryWrite,
    PutBatch, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage, StorageError, StorageRead,
    StorageWrite, WriteOptions,
};

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(2);

fn wait_until(description: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        thread::yield_now();
    }
}

fn join_thread<T>(handle: thread::JoinHandle<T>, description: &str) -> T {
    wait_until(description, || handle.is_finished());
    handle
        .join()
        .unwrap_or_else(|_| panic!("{description} panicked"))
}

#[tokio::test]
async fn read_sql_does_not_open_write_when_pre_plan_setup_fails() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "UPDATE lix_key_value SET value = 'missing-branch' \
             WHERE key = 'lix_workspace_branch_id'",
            &[],
        )
        .await
        .expect("test should corrupt workspace selector");

    let before = storage.stats();
    let error = session
        .execute("SELECT 1", &[])
        .await
        .expect_err("missing active branch should fail read pre-plan");
    assert!(
        error.message.contains("missing-branch"),
        "unexpected error: {error:?}"
    );

    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.read_opened, 1, "read SQL should open one read tx");
    assert_eq!(
        delta.write_opened, 0,
        "failed read SQL must not open writes"
    );
}

#[tokio::test]
async fn write_setup_failure_does_not_open_storage_write() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session
        .execute(
            "UPDATE lix_key_value SET value = 'missing-branch' \
             WHERE key = 'lix_workspace_branch_id'",
            &[],
        )
        .await
        .expect("test should corrupt workspace selector");

    let before = storage.stats();
    let error = session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('after-corrupt-selector', 'value')",
            &[],
        )
        .await
        .expect_err("missing active branch should fail write open");
    assert_eq!(error.code, "LIX_BRANCH_NOT_FOUND");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 0,
        "write setup failure should not open a storage write"
    );
    assert_eq!(
        delta.write_committed, 0,
        "failed write setup must not commit"
    );
}

#[tokio::test]
async fn rebuild_tracked_state_does_not_commit_on_read_failure() {
    let storage = RecordingStorage::new();
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");

    storage.fail_read_namespace("changelog.commit");
    let before = storage.stats();
    let error = engine
        .rebuild_tracked_state_for_branch(&receipt.main_branch_id)
        .await
        .expect_err("forced changelog read failure should fail rebuild");
    assert!(
        error.message.contains("forced read failure"),
        "unexpected error: {error:?}"
    );

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 0,
        "failed rebuild should not open a storage write"
    );
    assert_eq!(delta.write_committed, 0, "failed rebuild must not commit");
}

#[tokio::test]
async fn write_changelog_commit_failure_does_not_commit_storage_write() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    storage.fail_write_namespace("changelog.commit");
    let before = storage.stats();
    let error = session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('changelog-commit-write-failure', 'value')",
            &[],
        )
        .await
        .expect_err("forced changelog commit write failure should fail transaction commit");
    assert!(
        error.message.contains("forced write failure"),
        "unexpected error: {error:?}"
    );

    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.write_opened, 1, "write should open a storage write");
    assert_eq!(
        delta.write_committed, 0,
        "failed changelog commit write must not commit"
    );
}

#[tokio::test]
async fn active_transaction_blocks_session_read_and_allows_transaction_read() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
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
    tokio::time::timeout(TEST_WAIT_TIMEOUT, session.close())
        .await
        .expect("timed out closing after active transaction rejection")
        .expect("session close should succeed after rollback");
}

#[tokio::test]
async fn transaction_read_can_query_history_surfaces() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
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
            "SELECT entity_pk FROM lix_state_history \
             WHERE start_commit_id = lix_active_branch_commit_id() \
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
async fn close_rejects_idle_explicit_transaction_without_dropping_it() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('closed-session-tx', 'value')",
        &[],
    )
    .await
    .expect("staging before close should succeed");

    let close_error = session
        .close()
        .await
        .expect_err("close should reject an idle explicit transaction");
    assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");

    let result = tx
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'closed-session-tx'",
            &[],
        )
        .await
        .expect("rejected close should leave the transaction usable");
    assert_eq!(result.len(), 1);

    tx.rollback()
        .await
        .expect("transaction rollback should succeed after rejected close");

    let reopened = engine
        .open_workspace_session()
        .await
        .expect("new session should open after closing previous session");
    let result = reopened
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'closed-session-tx'",
            &[],
        )
        .await
        .expect("read through reopened session should succeed");
    assert_eq!(
        result.len(),
        0,
        "rolled-back transaction rows must not commit"
    );
}

#[tokio::test]
async fn closed_session_still_allows_active_transaction_rollback() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    let tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    let close_error = session
        .close()
        .await
        .expect_err("close should reject an idle explicit transaction");
    assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");

    tx.rollback()
        .await
        .expect("rollback should remain available after rejected close");
    session
        .close()
        .await
        .expect("session close should succeed after rollback");
}

#[tokio::test]
async fn closed_session_active_branch_id_does_not_open_storage_read() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");

    session.close().await.expect("session close should succeed");
    let before = storage.stats();
    let error = session
        .active_branch_id()
        .await
        .expect_err("active_branch_id should reject a closed session");
    assert_eq!(error.code, lix_engine::LixError::CODE_CLOSED);

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.read_opened, 0,
        "closed active_branch_id must reject before storage IO"
    );
}

#[tokio::test]
async fn close_during_transaction_open_rejects_opened_transaction() {
    let storage = BlockingBeginReadStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    gate.block_next_write();
    let opener_session = Arc::clone(&session);
    let opener = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { opener_session.begin_transaction().await })
    });

    gate.wait_until_blocked();
    let closer_session = Arc::clone(&session);
    let closer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { closer_session.close().await })
    });
    thread::sleep(Duration::from_millis(20));
    assert!(
        !closer.is_finished(),
        "close should wait for blocked transaction open to unwind"
    );

    gate.release();
    let Err(open_error) = join_thread(opener, "blocked transaction opener") else {
        panic!("transaction open that loses the close race should fail");
    };
    assert_eq!(open_error.code, lix_engine::LixError::CODE_CLOSED);
    join_thread(closer, "close after blocked transaction opener")
        .expect("session close should succeed");
}

#[tokio::test]
async fn close_during_transaction_commit_waits_after_commit_boundary() {
    let storage = BlockingBeginReadStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('close-during-commit', 'value')",
        &[],
    )
    .await
    .expect("staging before close should succeed");

    gate.block_next_write();
    let before = storage.stats();
    let committer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { tx.commit().await })
    });

    gate.wait_until_blocked();
    let close_session = Arc::clone(&session);
    let closer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { close_session.close().await })
    });
    assert!(
        !closer.is_finished(),
        "close should wait for the blocked commit boundary to exit"
    );
    gate.release();
    join_thread(committer, "committer waiting after commit boundary")
        .expect("commit past the boundary should finish before close");
    join_thread(closer, "close while commit waits after commit boundary")
        .expect("session close should succeed after commit exits");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 1,
        "commit preparation should open a storage write"
    );
    assert_eq!(
        delta.write_committed, 1,
        "commit past the boundary should commit storage writes"
    );
    assert_eq!(
        delta.write_rolled_back, 0,
        "commit past the boundary should not roll back storage writes"
    );
}

#[tokio::test]
async fn close_waits_for_transaction_blocked_in_storage_commit() {
    let storage = BlockingCommitStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('blocked-storage-commit', 'value')",
        &[],
    )
    .await
    .expect("staging before blocked commit should succeed");

    gate.block_next_write();
    let before = storage.stats();
    let committer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { tx.commit().await })
    });

    gate.wait_until_blocked();
    let closer = spawn_close_waiter(Arc::clone(&session));
    wait_until("close to wait on blocked storage commit", || {
        !closer.is_finished()
    });
    assert!(
        !closer.is_finished(),
        "close should wait for storage commit to unblock"
    );

    gate.release();
    join_thread(committer, "committer blocked in storage commit")
        .expect("commit at storage commit boundary should finish");
    join_thread(closer, "close after storage commit unblocks")
        .expect("session close should succeed after storage commit exits");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.write_opened, 1, "commit should open a storage write");
    assert_eq!(
        delta.write_committed, 1,
        "blocked storage commit should eventually commit"
    );
}

fn spawn_close_waiter<StorageImpl>(
    session: Arc<lix_engine::SessionContext<StorageImpl>>,
) -> thread::JoinHandle<Result<(), lix_engine::LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { session.close().await })
    })
}

#[tokio::test]
async fn begin_transaction_cannot_race_with_opening_session_write() {
    let storage = BlockingBeginWriteStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
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
    let Err(error) = session.begin_transaction().await else {
        panic!("explicit transaction should not race past a session write reservation");
    };
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    gate.release();
    join_thread(writer, "session writer racing transaction open")
        .expect("session write should complete after release");

    let result = session
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'racing-session-write'",
            &[],
        )
        .await
        .expect("session write should be committed");
    assert_eq!(result.len(), 1);
    tokio::time::timeout(TEST_WAIT_TIMEOUT, session.close())
        .await
        .expect("timed out closing after transaction reservation rejection")
        .expect("session close should succeed after reservation rejection");
}

#[tokio::test]
async fn session_read_waits_for_automatic_write_instead_of_rejecting() {
    let storage = BlockingBeginWriteStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
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
                    "INSERT INTO lix_key_value (key, value) VALUES ('read-after-automatic-write', 'value')",
                    &[],
                )
                .await
        })
    });
    gate.wait_until_blocked();

    let reader_session = Arc::clone(&session);
    let reader = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            reader_session
                .execute(
                    "SELECT key FROM lix_key_value WHERE key = 'read-after-automatic-write'",
                    &[],
                )
                .await
        })
    });

    gate.release();
    join_thread(writer, "blocked automatic writer")
        .expect("automatic write should finish after release");
    let result = join_thread(reader, "reader waiting for automatic write")
        .expect("session read should wait behind automatic write");
    assert_eq!(result.len(), 1);
}

#[derive(Clone, Default)]
struct RecordingStorage {
    inner: Memory,
    stats: Arc<TransactionStats>,
    fail_read_namespace: Arc<Mutex<Option<String>>>,
    fail_write_namespace: Arc<Mutex<Option<String>>>,
}

#[derive(Clone)]
struct BlockingBeginWriteStorage {
    inner: RecordingStorage,
    gate: BlockingBeginWriteGate,
}

#[derive(Clone)]
struct BlockingBeginReadStorage {
    inner: RecordingStorage,
    gate: BlockingBeginWriteGate,
}

#[derive(Clone)]
struct BlockingCommitStorage {
    inner: RecordingStorage,
    gate: BlockingBeginWriteGate,
}

impl BlockingBeginWriteStorage {
    fn new() -> Self {
        Self {
            inner: RecordingStorage::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }
}

impl BlockingBeginReadStorage {
    fn new() -> Self {
        Self {
            inner: RecordingStorage::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.inner.stats()
    }
}

impl BlockingCommitStorage {
    fn new() -> Self {
        Self {
            inner: RecordingStorage::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.inner.stats()
    }
}

impl Storage for BlockingBeginWriteStorage {
    type Read<'a>
        = <RecordingStorage as Storage>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = <RecordingStorage as Storage>::Write<'a>
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.gate.maybe_block();
        self.inner.begin_write(opts).await
    }
}

impl Storage for BlockingBeginReadStorage {
    type Read<'a>
        = <RecordingStorage as Storage>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = <RecordingStorage as Storage>::Write<'a>
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.gate.maybe_block();
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(opts).await
    }
}

impl Storage for BlockingCommitStorage {
    type Read<'a>
        = <RecordingStorage as Storage>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = BlockingCommitWrite
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(BlockingCommitWrite {
            inner: self.inner.begin_write(opts).await?,
            gate: self.gate.clone(),
        })
    }
}

struct BlockingCommitWrite {
    inner: RecordingWrite,
    gate: BlockingBeginWriteGate,
}

impl StorageWrite for BlockingCommitWrite {
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), StorageError> {
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.gate.maybe_block();
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
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
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for blocking gate release"
            );
            let (next_state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .expect("blocking gate lock should be available after wait");
            state = next_state;
            assert!(
                !wait_result.timed_out() || state.released,
                "timed out waiting for blocking gate release"
            );
        }
    }

    fn wait_until_blocked(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        while !state.blocked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for blocking gate");
            let (next_state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .expect("blocking gate lock should be available after wait");
            state = next_state;
            assert!(
                !wait_result.timed_out() || state.blocked,
                "timed out waiting for blocking gate"
            );
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

impl RecordingStorage {
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

impl Storage for RecordingStorage {
    type Read<'a>
        = RecordingRead
    where
        Self: 'a;

    type Write<'a>
        = RecordingWrite
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.stats.read_opened.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingRead {
            inner: self.inner.begin_read(opts).await?,
            fail_read_namespace: Arc::clone(&self.fail_read_namespace),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats.write_opened.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingWrite {
            inner: self.inner.begin_write(opts).await?,
            stats: Arc::clone(&self.stats),
            fail_write_namespace: Arc::clone(&self.fail_write_namespace),
        })
    }
}

#[derive(Clone)]
struct RecordingRead {
    inner: MemoryRead,
    fail_read_namespace: Arc<Mutex<Option<String>>>,
}

struct RecordingWrite {
    inner: MemoryWrite,
    stats: Arc<TransactionStats>,
    fail_write_namespace: Arc<Mutex<Option<String>>>,
}

impl StorageRead for RecordingRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.fail_if_space_matches(space)?;
        self.inner.get_many(space, keys, opts).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.fail_if_space_matches(space)?;
        self.inner.scan(space, range, opts).await
    }
}

impl StorageWrite for RecordingWrite {
    async fn put_many(&mut self, space: SpaceId, entries: PutBatch) -> Result<(), StorageError> {
        self.fail_if_space_matches(space)?;
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(&mut self, space: SpaceId, keys: &[Key]) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(&mut self, space: SpaceId, range: KeyRange) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.stats.write_committed.fetch_add(1, Ordering::SeqCst);
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.stats.write_rolled_back.fetch_add(1, Ordering::SeqCst);
        self.inner.rollback().await
    }
}

impl RecordingWrite {
    fn fail_if_space_matches(&self, space: SpaceId) -> Result<(), StorageError> {
        if let Some(namespace) = self.fail_write_namespace() {
            if let Some(failing) = namespace_space(&namespace) {
                if space == failing {
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
    fn fail_if_space_matches(&self, space: SpaceId) -> Result<(), StorageError> {
        if let Some(namespace) = self.fail_read_namespace() {
            if let Some(failing) = namespace_space(&namespace) {
                if space == failing {
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

fn namespace_space(namespace: &str) -> Option<SpaceId> {
    match namespace {
        "changelog.commit" => Some(SpaceId(0x0006_0001)),
        "changelog.change" => Some(SpaceId(0x0006_0002)),
        "changelog.commit_change_ref_chunk" => Some(SpaceId(0x0006_0003)),
        _ => None,
    }
}

fn forced_read_failure(namespace: &str) -> StorageError {
    StorageError::Io(format!("forced read failure for namespace {namespace}"))
}

fn forced_write_failure(namespace: &str) -> StorageError {
    StorageError::Io(format!("forced write failure for namespace {namespace}"))
}

#[derive(Default)]
struct TransactionStats {
    read_opened: AtomicUsize,
    write_opened: AtomicUsize,
    write_committed: AtomicUsize,
    write_rolled_back: AtomicUsize,
}

impl TransactionStats {
    fn snapshot(&self) -> TransactionStatsSnapshot {
        TransactionStatsSnapshot {
            read_opened: self.read_opened.load(Ordering::SeqCst),
            write_opened: self.write_opened.load(Ordering::SeqCst),
            write_committed: self.write_committed.load(Ordering::SeqCst),
            write_rolled_back: self.write_rolled_back.load(Ordering::SeqCst),
        }
    }
}

#[derive(Clone, Copy)]
struct TransactionStatsSnapshot {
    read_opened: usize,
    write_opened: usize,
    write_committed: usize,
    write_rolled_back: usize,
}

impl TransactionStatsSnapshot {
    fn delta_since(self, before: &Self) -> Self {
        Self {
            read_opened: self.read_opened - before.read_opened,
            write_opened: self.write_opened - before.write_opened,
            write_committed: self.write_committed - before.write_committed,
            write_rolled_back: self.write_rolled_back - before.write_rolled_back,
        }
    }
}
