use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use lix_engine::telemetry::{
    TelemetrySink, TelemetrySpanEnd, TelemetrySpanEnterGuard, TelemetrySpanHandle,
    TelemetrySpanKind, TelemetrySpanStart, TelemetryValue,
};
use lix_engine::{
    Engine, EngineOptions, ExecuteBatchStatement, GetManyResult, GetOptions, Key, KeyRange, Memory,
    MemoryRead, MemoryWrite, ReadOptions, ScanChunk, ScanOptions, SpaceId, Storage, StorageError,
    StorageRead, WriteOptions,
};
use serde_json::json;

const TEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
struct BlockingSnapshotStorage {
    inner: Memory,
    gate: ReadGate,
}

struct BlockingSnapshotRead {
    inner: MemoryRead,
    gate: ReadGate,
    block_after_arm: bool,
}

#[derive(Clone)]
struct ReadGate {
    state: Arc<(Mutex<ReadGateState>, Condvar)>,
}

struct ReadGateState {
    phase: ReadGatePhase,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
enum ReadGatePhase {
    #[default]
    Idle,
    TargetNextRead,
    Captured,
    Armed,
    Blocked,
    Released,
}

impl Default for ReadGateState {
    fn default() -> Self {
        Self {
            phase: ReadGatePhase::Idle,
        }
    }
}

#[derive(Clone)]
struct SecondBatchStatementGate {
    gate: ReadGate,
}

struct NoopTelemetrySpan;

impl BlockingSnapshotStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            gate: ReadGate::new(),
        }
    }

    fn gate(&self) -> ReadGate {
        self.gate.clone()
    }
}

impl Storage for BlockingSnapshotStorage {
    type Read<'a>
        = BlockingSnapshotRead
    where
        Self: 'a;

    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        // Memory captures its snapshot here, before a selected read handle can
        // be paused by the second statement's first storage operation.
        let inner = self.inner.begin_read(options).await?;
        Ok(BlockingSnapshotRead {
            inner,
            gate: self.gate.clone(),
            block_after_arm: self.gate.take_target_next_read(),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

impl StorageRead for BlockingSnapshotRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        options: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.gate.maybe_block(self.block_after_arm);
        self.inner.get_many(space, keys, options).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.gate.maybe_block(self.block_after_arm);
        self.inner.scan(space, range, options).await
    }
}

impl ReadGate {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(ReadGateState::default()), Condvar::new())),
        }
    }

    fn target_next_read(&self) {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("read gate lock should not poison");
        assert!(
            matches!(state.phase, ReadGatePhase::Idle | ReadGatePhase::Released),
            "a read is already targeted"
        );
        state.phase = ReadGatePhase::TargetNextRead;
    }

    fn take_target_next_read(&self) -> bool {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("read gate lock should not poison");
        if state.phase != ReadGatePhase::TargetNextRead {
            return false;
        }
        state.phase = ReadGatePhase::Captured;
        true
    }

    fn arm(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("read gate lock should not poison");
        assert_eq!(state.phase, ReadGatePhase::Captured);
        state.phase = ReadGatePhase::Armed;
        condvar.notify_all();
    }

    fn maybe_block(&self, block_after_arm: bool) {
        if !block_after_arm {
            return;
        }
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("read gate lock should not poison");
        if state.phase != ReadGatePhase::Armed {
            return;
        }
        state.phase = ReadGatePhase::Blocked;
        condvar.notify_all();
        let deadline = Instant::now() + TEST_TIMEOUT;
        while state.phase != ReadGatePhase::Released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for the blocked batch read to be released"
            );
            let (next_state, wait) = condvar
                .wait_timeout(state, remaining)
                .expect("read gate lock should not poison after wait");
            state = next_state;
            assert!(
                !wait.timed_out() || state.phase == ReadGatePhase::Released,
                "timed out waiting for the blocked batch read to be released"
            );
        }
    }

    fn wait_until_blocked(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("read gate lock should not poison");
        let deadline = Instant::now() + TEST_TIMEOUT;
        while state.phase != ReadGatePhase::Blocked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                state.phase = ReadGatePhase::Released;
                condvar.notify_all();
                panic!("timed out waiting for the second batch statement to read");
            }
            let (next_state, wait) = condvar
                .wait_timeout(state, remaining)
                .expect("read gate lock should not poison after wait");
            state = next_state;
            if wait.timed_out() && state.phase != ReadGatePhase::Blocked {
                state.phase = ReadGatePhase::Released;
                condvar.notify_all();
                panic!("timed out waiting for the second batch statement to read");
            }
        }
    }

    fn release(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("read gate lock should not poison");
        state.phase = ReadGatePhase::Released;
        condvar.notify_all();
    }
}

impl TelemetrySink for SecondBatchStatementGate {
    fn enabled(&self, kind: TelemetrySpanKind) -> bool {
        kind == TelemetrySpanKind::SqlQuery
    }

    fn start_span(&self, start: TelemetrySpanStart) -> Box<dyn TelemetrySpanHandle> {
        let is_second_batch_statement = start.attributes.iter().any(|attribute| {
            attribute.key == "lix.batch.index" && matches!(&attribute.value, TelemetryValue::U64(1))
        });
        if is_second_batch_statement {
            self.gate.arm();
        }
        Box::new(NoopTelemetrySpan)
    }
}

impl TelemetrySpanHandle for NoopTelemetrySpan {
    fn enter(&self) -> Box<dyn TelemetrySpanEnterGuard + '_> {
        Box::new(())
    }

    fn finish(self: Box<Self>, _end: TelemetrySpanEnd) {}
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_read_batch_keeps_one_snapshot_across_concurrent_commit() {
    let storage = BlockingSnapshotStorage::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let writer_engine = Engine::new(storage.clone())
        .await
        .expect("writer engine should open");
    let writer = writer_engine
        .open_workspace_session()
        .await
        .expect("writer workspace session should open");
    let branch_id = writer
        .active_branch_id()
        .await
        .expect("writer branch should resolve");
    writer
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('batch-snapshot', 'before')",
            &[],
        )
        .await
        .expect("initial value should commit");

    let before_head = writer_engine
        .load_branch_head_commit_id(&branch_id)
        .await
        .expect("main head should load")
        .expect("main should have a head");
    let before_revision = writer
        .storage_mutation_revision()
        .await
        .expect("initial storage revision should load");

    let gate = storage.gate();
    let telemetry = Arc::new(SecondBatchStatementGate { gate: gate.clone() });
    let reader_engine = Engine::new_with_options(
        storage.clone(),
        EngineOptions::new().with_telemetry(telemetry),
    )
    .await
    .expect("reader engine should open");
    let reader = Arc::new(
        reader_engine
            .open_workspace_session()
            .await
            .expect("reader session should open"),
    );
    let statements = vec![
        ExecuteBatchStatement {
            sql: "SELECT value FROM lix_key_value WHERE key = 'batch-snapshot'".to_string(),
            params: Vec::new(),
        },
        ExecuteBatchStatement {
            sql: "SELECT value FROM lix_key_value WHERE key = 'batch-snapshot'".to_string(),
            params: Vec::new(),
        },
        ExecuteBatchStatement {
            sql: "SELECT lix_active_branch_commit_id() AS commit_id".to_string(),
            params: Vec::new(),
        },
    ];

    gate.target_next_read();
    let batch_reader = Arc::clone(&reader);
    let batch_branch_id = branch_id.clone();
    let batch_thread = thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("batch runtime should build")
            .block_on(batch_reader.execute_read_batch(&batch_branch_id, &statements))
    });

    // Statement zero has completed when statement one emits its start span.
    // The next storage call on the already-open batch read is paused here.
    gate.wait_until_blocked();
    let update_result = writer
        .execute(
            "UPDATE lix_key_value SET value = 'after' WHERE key = 'batch-snapshot'",
            &[],
        )
        .await;
    let after_head_result = writer_engine.load_branch_head_commit_id(&branch_id).await;
    let after_revision_result = writer.storage_mutation_revision().await;
    gate.release();

    update_result.expect("concurrent value should commit");
    let after_head = after_head_result
        .expect("updated main head should load")
        .expect("updated main should have a head");
    let after_revision =
        after_revision_result.expect("updated storage revision should load after commit");
    assert_ne!(
        after_head, before_head,
        "the concurrent write must advance main"
    );
    assert_ne!(
        after_revision, before_revision,
        "the concurrent write must advance the storage revision"
    );

    let batch = join_thread(batch_thread, "coherent read batch")
        .expect("coherent read batch should finish");
    assert_eq!(batch.branch_id, branch_id);
    assert_eq!(batch.branch_commit_id, before_head);
    assert_eq!(batch.storage_mutation_revision, before_revision);
    assert_eq!(batch.results.len(), 3);
    for result in &batch.results[..2] {
        assert_eq!(result.len(), 1);
        assert_eq!(
            result.rows()[0]
                .get::<serde_json::Value>("value")
                .expect("value should be JSON"),
            json!("before")
        );
    }
    assert_eq!(
        batch.results[2].rows()[0]
            .get::<String>("commit_id")
            .expect("commit ID should be text"),
        batch.branch_commit_id
    );
}

#[tokio::test]
async fn explicit_read_batch_does_not_depend_on_or_repair_workspace_selector() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let workspace = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open while its selector is valid");
    let branch_id = workspace
        .active_branch_id()
        .await
        .expect("workspace branch should resolve");
    workspace
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('explicit-batch-row', 'main')",
            &[],
        )
        .await
        .expect("explicit branch row should commit");
    workspace
        .execute(
            "UPDATE lix_key_value SET value = 'missing-branch' \
             WHERE key = 'lix_workspace_branch_id'",
            &[],
        )
        .await
        .expect("workspace selector should be corrupted for the test");

    let before_error = workspace
        .active_branch_id()
        .await
        .expect_err("corrupt workspace selector should not resolve");
    assert!(before_error.message.contains("missing-branch"));

    let batch = workspace
        .execute_read_batch(
            &branch_id,
            &[ExecuteBatchStatement {
                sql: "SELECT value FROM lix_key_value WHERE key = 'explicit-batch-row'".to_string(),
                params: Vec::new(),
            }],
        )
        .await
        .expect("explicit branch batch must ignore the workspace selector");
    assert_eq!(batch.branch_id, branch_id);
    assert_eq!(
        batch.results[0].rows()[0]
            .get::<serde_json::Value>("value")
            .expect("value should be JSON"),
        json!("main")
    );

    let after_error = workspace
        .active_branch_id()
        .await
        .expect_err("explicit branch batch must not repair the workspace selector");
    assert!(after_error.message.contains("missing-branch"));
}

fn join_thread<T>(handle: thread::JoinHandle<T>, description: &str) -> T {
    let deadline = Instant::now() + TEST_TIMEOUT;
    while !handle.is_finished() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        thread::yield_now();
    }
    handle
        .join()
        .unwrap_or_else(|_| panic!("{description} panicked"))
}
