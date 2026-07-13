mod support;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use lix_engine::{
    Backend, BackendError, BackendRead, Engine, GetManyResult, GetOptions, InMemoryBackend,
    InMemoryRead, InMemoryWrite, Key, KeyRange, ObserveEvent, ReadOptions, ScanChunk, ScanOptions,
    SessionContext, SpaceId, Value, WriteOptions,
};
use serde_json::json;
use support::simulation_test::engine::{SimSession, Simulation};

const NEXT_TIMEOUT: Duration = Duration::from_secs(1);
const NO_EVENT_TIMEOUT: Duration = Duration::from_millis(250);
const KEY_VALUE_SQL: &str = "SELECT key, value FROM lix_key_value WHERE key = $1 ORDER BY key";

async fn open_workspace_session(sim: &Simulation, engine: &Engine) -> (SessionContext, SimSession) {
    let raw_session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let session = sim.wrap_session(raw_session.clone(), engine);
    (raw_session, session)
}

fn observe_key(session: &SessionContext, key: &str) -> lix_engine::ObserveEvents {
    let params = [Value::Text(key.to_string())];
    session
        .observe(KEY_VALUE_SQL, &params)
        .expect("observe should open")
}

async fn next_event<B>(events: &mut lix_engine::ObserveEvents<B>, label: &str) -> ObserveEvent
where
    B: Backend + Clone + Send + Sync + 'static,
{
    tokio::time::timeout(NEXT_TIMEOUT, events.next())
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for observe event: {label}"))
        .unwrap_or_else(|error| panic!("observe next failed for {label}: {error:?}"))
        .unwrap_or_else(|| panic!("observe closed before event: {label}"))
}

async fn expect_no_event<B>(events: &mut lix_engine::ObserveEvents<B>, label: &str)
where
    B: Backend + Clone + Send + Sync + 'static,
{
    match tokio::time::timeout(NO_EVENT_TIMEOUT, events.next()).await {
        Err(_) => {}
        Ok(Ok(Some(event))) => panic!("unexpected observe event for {label}: {event:?}"),
        Ok(Ok(None)) => panic!("observe closed unexpectedly while waiting for no event: {label}"),
        Ok(Err(error)) => panic!("observe errored while waiting for no event {label}: {error:?}"),
    }
}

fn assert_key_value_row(event: &ObserveEvent, key: &str, value: &str) {
    assert_eq!(event.rows.columns(), &["key", "value"]);
    assert_eq!(event.rows.len(), 1);
    assert_eq!(
        event.rows.rows()[0].values(),
        &[Value::Text(key.to_string()), Value::Json(json!(value)),]
    );
}

simulation_test!(observe_next_returns_initial_snapshot, |sim| async move {
    let engine = sim.boot_engine().await;
    let (raw_session, session) = open_workspace_session(&sim, &engine).await;
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('observe-initial', 'v0')",
            &[],
        )
        .await
        .expect("seed insert should succeed");

    let mut events = raw_session
        .observe(KEY_VALUE_SQL, &[Value::Text("observe-initial".to_string())])
        .expect("observe should open");
    let initial = next_event(&mut events, "initial snapshot").await;

    assert_eq!(initial.sequence, 0);
    assert_key_value_row(&initial, "observe-initial", "v0");
});

#[tokio::test]
async fn observe_initial_next_waits_without_rejecting_same_session_write() {
    let backend = BlockingBeginReadBackend::new();
    let gate = backend.gate();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );
    let mut warmup = session
        .observe("SELECT 1", &[])
        .expect("warmup observe should open");
    let _ = next_event(&mut warmup, "warmup snapshot").await;
    let params = [Value::Text("observe-blocked-initial-write".to_string())];
    let mut events = session
        .observe(KEY_VALUE_SQL, &params)
        .expect("observe should open");

    gate.block_next_read();
    let observer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { events.next().await })
    });
    gate.wait_until_blocked();

    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            writer_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) \
                     VALUES ('observe-blocked-initial-write', 'v0')",
                    &[],
                )
                .await
        })
    });

    gate.release();
    let initial = observer
        .join()
        .expect("observer thread should not panic")
        .expect("observe next should not fail")
        .expect("observe next should emit initial snapshot");
    assert_eq!(initial.sequence, 0);
    writer
        .join()
        .expect("writer thread should not panic")
        .expect("write should succeed after observe initial read finishes");
}

#[tokio::test]
async fn observe_registration_allows_automatic_write_instead_of_rejecting() {
    let backend = BlockingBeginWriteBackend::new();
    let gate = backend.gate();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );

    gate.block_next();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            writer_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) \
                     VALUES ('observe-registration-after-automatic-write', 'v0')",
                    &[],
                )
                .await
        })
    });
    gate.wait_until_blocked();

    let params = [Value::Text(
        "observe-registration-after-automatic-write".to_string(),
    )];
    let mut events = session
        .observe(KEY_VALUE_SQL, &params)
        .expect("observe registration should not reject an automatic transaction");

    gate.release();
    writer
        .join()
        .expect("writer thread should not panic")
        .expect("automatic write should finish after release");
    let initial = next_event(&mut events, "observe after automatic write").await;
    assert_key_value_row(&initial, "observe-registration-after-automatic-write", "v0");
}

#[tokio::test]
async fn observe_initial_next_waits_for_automatic_write_instead_of_rejecting() {
    let backend = BlockingBeginWriteBackend::new();
    let gate = backend.gate();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend).await.expect("engine should open");
    let session = Arc::new(
        engine
            .open_workspace_session()
            .await
            .expect("workspace session should open"),
    );
    let params = [Value::Text("observe-after-automatic-write".to_string())];
    let mut events = session
        .observe(KEY_VALUE_SQL, &params)
        .expect("observe should open");

    gate.block_next();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            writer_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) \
                     VALUES ('observe-after-automatic-write', 'v0')",
                    &[],
                )
                .await
        })
    });
    gate.wait_until_blocked();

    let observer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { events.next().await })
    });

    gate.release();
    writer
        .join()
        .expect("writer thread should not panic")
        .expect("automatic write should finish after release");
    let initial = observer
        .join()
        .expect("observer thread should not panic")
        .expect("observe next should not fail")
        .expect("observe next should emit initial snapshot");
    assert_key_value_row(&initial, "observe-after-automatic-write", "v0");
}

simulation_test!(
    observe_emits_after_committed_mutation_changes_result,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-update");

        let initial = next_event(&mut events, "initial empty snapshot").await;
        assert_eq!(initial.sequence, 0);
        assert!(initial.rows.is_empty());

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-update', 'v0')",
                &[],
            )
            .await
            .expect("insert should commit");

        let update = next_event(&mut events, "insert update").await;
        assert_eq!(update.sequence, 1);
        assert!(
            update.mutation_sequence > initial.mutation_sequence,
            "committed mutation should advance observe mutation sequence"
        );
        assert_key_value_row(&update, "observe-update", "v0");
    }
);

simulation_test!(
    observe_sees_mutation_from_another_session,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (observer_raw_session, _) = open_workspace_session(&sim, &engine).await;
        let (_, writer_session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&observer_raw_session, "observe-cross-session");

        let initial = next_event(&mut events, "initial empty snapshot").await;
        assert!(initial.rows.is_empty());

        writer_session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-cross-session', 'v0')",
                &[],
            )
            .await
            .expect("cross-session insert should commit");

        let update = next_event(&mut events, "cross-session update").await;
        assert_eq!(update.sequence, 1);
        assert!(
            update.mutation_sequence > initial.mutation_sequence,
            "cross-session committed mutation should advance observe mutation sequence"
        );
        assert_key_value_row(&update, "observe-cross-session", "v0");
    }
);

simulation_test!(
    observe_does_not_emit_for_read_only_execute,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-read-only");
        let _initial = next_event(&mut events, "initial snapshot").await;

        session
            .execute("SELECT key FROM lix_key_value ORDER BY key", &[])
            .await
            .expect("read should succeed");

        expect_no_event(&mut events, "read-only execute").await;
    }
);

simulation_test!(
    observe_does_not_emit_when_unrelated_mutation_leaves_rows_unchanged,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-target");
        let _initial = next_event(&mut events, "initial snapshot").await;

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-other', 'v0')",
                &[],
            )
            .await
            .expect("unrelated insert should commit");

        expect_no_event(&mut events, "unchanged rows after unrelated mutation").await;
    }
);

simulation_test!(observe_does_not_emit_after_failed_write, |sim| async move {
    let engine = sim.boot_engine().await;
    let (raw_session, session) = open_workspace_session(&sim, &engine).await;
    let mut events = observe_key(&raw_session, "observe-failed-write");
    let initial = next_event(&mut events, "initial empty snapshot").await;
    assert!(initial.rows.is_empty());

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('observe-failed-write', 'v0')",
            &[],
        )
        .await
        .expect("seed insert should commit");
    let seeded = next_event(&mut events, "seed insert").await;
    assert_key_value_row(&seeded, "observe-failed-write", "v0");

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('observe-failed-write', 'v1')",
            &[],
        )
        .await
        .expect_err("duplicate key insert should fail");

    expect_no_event(&mut events, "failed duplicate insert").await;
});

simulation_test!(
    observe_emits_only_after_explicit_transaction_commit,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-transaction");
        let _initial = next_event(&mut events, "initial snapshot").await;

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-transaction', 'v0')",
                &[],
            )
            .await
            .expect("transaction write should stage");

        expect_no_event(&mut events, "staged transaction write before commit").await;

        transaction
            .commit()
            .await
            .expect("transaction should commit");
        let update = next_event(&mut events, "transaction commit").await;
        assert_eq!(update.sequence, 1);
        assert_key_value_row(&update, "observe-transaction", "v0");
    }
);

simulation_test!(
    observe_does_not_emit_after_explicit_transaction_rollback,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-rollback");
        let _initial = next_event(&mut events, "initial snapshot").await;

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("transaction should open");
        transaction
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-rollback', 'v0')",
                &[],
            )
            .await
            .expect("transaction write should stage");
        transaction
            .rollback()
            .await
            .expect("transaction should roll back");

        expect_no_event(&mut events, "transaction rollback").await;

        let result = session
            .execute(
                "SELECT key, value FROM lix_key_value WHERE key = 'observe-rollback'",
                &[],
            )
            .await
            .expect("post-rollback read should succeed");
        assert!(
            result.is_empty(),
            "rolled-back transaction should not leave visible rows"
        );
    }
);

simulation_test!(
    observe_coalesces_multiple_writes_before_next_into_latest_snapshot,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-coalesce");
        let initial = next_event(&mut events, "initial snapshot").await;
        assert!(initial.rows.is_empty());

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-coalesce', 'v0')",
                &[],
            )
            .await
            .expect("insert should commit");
        session
            .execute(
                "UPDATE lix_key_value SET value = 'v1' WHERE key = 'observe-coalesce'",
                &[],
            )
            .await
            .expect("update should commit");

        let update = next_event(&mut events, "coalesced update").await;
        assert_eq!(update.sequence, 1);
        assert!(
            update.mutation_sequence > initial.mutation_sequence,
            "coalesced mutations should advance observe mutation sequence"
        );
        assert_key_value_row(&update, "observe-coalesce", "v1");
        expect_no_event(&mut events, "coalesced write should not queue stale event").await;
    }
);

simulation_test!(
    observe_multiple_observers_receive_updates_independently,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        let mut events_a = observe_key(&raw_session, "observe-multi");
        let mut events_b = observe_key(&raw_session, "observe-multi");
        let initial_a = next_event(&mut events_a, "first initial snapshot").await;
        let initial_b = next_event(&mut events_b, "second initial snapshot").await;
        assert_eq!(initial_a.sequence, 0);
        assert_eq!(initial_b.sequence, 0);
        assert!(initial_a.rows.is_empty());
        assert!(initial_b.rows.is_empty());

        session
            .execute(
                "INSERT INTO lix_key_value (key, value) VALUES ('observe-multi', 'v0')",
                &[],
            )
            .await
            .expect("insert should commit");

        let update_a = next_event(&mut events_a, "first observer update").await;
        let update_b = next_event(&mut events_b, "second observer update").await;
        assert_eq!(update_a.sequence, 1);
        assert_eq!(update_b.sequence, 1);
        assert_key_value_row(&update_a, "observe-multi", "v0");
        assert_key_value_row(&update_b, "observe-multi", "v0");
    }
);

#[tokio::test]
async fn observe_identical_queries_share_one_evaluation_per_generation() {
    let backend = CountingReadBackend::new();
    Engine::initialize(backend.clone())
        .await
        .expect("backend should initialize");
    let engine = Engine::new(backend.clone())
        .await
        .expect("engine should open");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    let params = [Value::Text("observe-singleflight".to_string())];
    let mut first = session
        .observe(KEY_VALUE_SQL, &params)
        .expect("first observe should open");
    let mut second = session
        .observe(KEY_VALUE_SQL, &params)
        .expect("second observe should open");
    let _first_initial = next_event(&mut first, "first initial snapshot").await;
    let _second_initial = next_event(&mut second, "second initial snapshot").await;

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('observe-singleflight', 'v0')",
            &[],
        )
        .await
        .expect("insert should commit");
    backend.reset_read_count();

    let (first_update, second_update) = tokio::join!(
        next_event(&mut first, "first update"),
        next_event(&mut second, "second update"),
    );

    assert_key_value_row(&first_update, "observe-singleflight", "v0");
    assert_key_value_row(&second_update, "observe-singleflight", "v0");
    assert_eq!(
        backend.read_count(),
        1,
        "identical observers should share the same query evaluation for one invalidation generation"
    );
}

simulation_test!(
    observe_rejects_durable_runtime_functions,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, _) = open_workspace_session(&sim, &engine).await;

        match raw_session.observe("SELECT lix_uuid_v7()", &[]) {
            Ok(_) => panic!("observe should reject durable runtime functions"),
            Err(error) => {
                assert_eq!(error.code, lix_engine::LixError::CODE_INVALID_PARAM);
                assert!(
                    error.message.contains("durable runtime functions"),
                    "unexpected observe error: {error:?}"
                );
            }
        }
    }
);

simulation_test!(
    observe_next_rejects_active_transaction_even_when_shared_cache_is_warm,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (blocked_raw_session, _) = open_workspace_session(&sim, &engine).await;
        let (cache_raw_session, _) = open_workspace_session(&sim, &engine).await;
        let (_, writer_session) = open_workspace_session(&sim, &engine).await;
        let params = [Value::Text("observe-active-transaction-cache".to_string())];
        let mut blocked_events = blocked_raw_session
            .observe(KEY_VALUE_SQL, &params)
            .expect("blocked observer should open");
        let mut cache_events = cache_raw_session
            .observe(KEY_VALUE_SQL, &params)
            .expect("cache observer should open");
        let blocked_initial = next_event(&mut blocked_events, "blocked observer initial").await;
        let cache_initial = next_event(&mut cache_events, "cache observer initial").await;
        assert!(blocked_initial.rows.is_empty());
        assert!(cache_initial.rows.is_empty());

        writer_session
            .execute(
                "INSERT INTO lix_key_value (key, value) \
                 VALUES ('observe-active-transaction-cache', 'v0')",
                &[],
            )
            .await
            .expect("writer insert should commit");

        let cache_update = next_event(&mut cache_events, "cache observer update").await;
        assert_key_value_row(&cache_update, "observe-active-transaction-cache", "v0");

        let transaction = blocked_raw_session
            .begin_transaction()
            .await
            .expect("transaction should open on blocked observer session");

        match tokio::time::timeout(NEXT_TIMEOUT, blocked_events.next())
            .await
            .expect("blocked observer next should resolve")
        {
            Ok(result) => {
                panic!("observe next should reject active transaction, got {result:?}")
            }
            Err(error) => assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE"),
        }

        transaction
            .rollback()
            .await
            .expect("transaction should roll back");
    }
);

simulation_test!(observe_close_makes_next_return_none, |sim| async move {
    let engine = sim.boot_engine().await;
    let (raw_session, _) = open_workspace_session(&sim, &engine).await;
    let mut events = observe_key(&raw_session, "observe-close");
    let _initial = next_event(&mut events, "initial snapshot").await;

    events.close();

    let closed = tokio::time::timeout(NEXT_TIMEOUT, events.next())
        .await
        .expect("closed observe next should resolve")
        .expect("closed observe next should not error");
    assert!(closed.is_none());
});

simulation_test!(observe_rejects_closed_session, |sim| async move {
    let engine = sim.boot_engine().await;
    let (raw_session, _) = open_workspace_session(&sim, &engine).await;

    raw_session
        .close()
        .await
        .expect("session close should succeed");

    let params = [Value::Text("observe-closed-session".to_string())];
    match raw_session.observe(KEY_VALUE_SQL, &params) {
        Ok(_) => panic!("observe should reject a closed session"),
        Err(error) => assert_eq!(error.code, lix_engine::LixError::CODE_CLOSED),
    }
});

simulation_test!(observe_rejects_active_transaction, |sim| async move {
    let engine = sim.boot_engine().await;
    let (raw_session, _) = open_workspace_session(&sim, &engine).await;
    let transaction = raw_session
        .begin_transaction()
        .await
        .expect("transaction should open");

    let params = [Value::Text("observe-active-transaction".to_string())];
    match raw_session.observe(KEY_VALUE_SQL, &params) {
        Ok(_) => panic!("observe should reject a session with an active transaction"),
        Err(error) => assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE"),
    }

    transaction
        .rollback()
        .await
        .expect("transaction should roll back");
});

simulation_test!(
    observe_pending_next_returns_none_when_session_closes,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, _) = open_workspace_session(&sim, &engine).await;
        let mut events = observe_key(&raw_session, "observe-session-close");
        let _initial = next_event(&mut events, "initial snapshot").await;

        let mut pending_next = Box::pin(events.next());
        tokio::select! {
            () = tokio::time::sleep(NO_EVENT_TIMEOUT) => {}
            result = pending_next.as_mut() => {
                panic!("observe next resolved before session close: {result:?}");
            }
        }

        raw_session
            .close()
            .await
            .expect("session close should succeed");

        let closed = tokio::time::timeout(NEXT_TIMEOUT, pending_next)
            .await
            .expect("pending observe next should wake after session close")
            .expect("pending observe next should not error after session close");
        assert!(closed.is_none());
    }
);

#[derive(Clone)]
struct CountingReadBackend {
    inner: InMemoryBackend,
    read_count: Arc<AtomicUsize>,
}

#[derive(Clone)]
struct BlockingBeginReadBackend {
    inner: InMemoryBackend,
    gate: BlockingReadGate,
}

#[derive(Clone)]
struct BlockingBeginWriteBackend {
    inner: InMemoryBackend,
    gate: BlockingReadGate,
}

impl BlockingBeginReadBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            gate: BlockingReadGate::new(),
        }
    }

    fn gate(&self) -> BlockingReadGate {
        self.gate.clone()
    }
}

impl BlockingBeginWriteBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            gate: BlockingReadGate::new(),
        }
    }

    fn gate(&self) -> BlockingReadGate {
        self.gate.clone()
    }
}

impl Backend for BlockingBeginReadBackend {
    type Read<'a>
        = InMemoryRead
    where
        Self: 'a;

    type Write<'a>
        = InMemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.gate.maybe_block();
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.inner.begin_write(opts).await
    }
}

impl Backend for BlockingBeginWriteBackend {
    type Read<'a>
        = InMemoryRead
    where
        Self: 'a;

    type Write<'a>
        = InMemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.gate.maybe_block();
        self.inner.begin_write(opts).await
    }
}

#[derive(Clone)]
struct BlockingReadGate {
    state: Arc<(Mutex<BlockingReadState>, Condvar)>,
}

impl BlockingReadGate {
    fn new() -> Self {
        Self {
            state: Arc::new((Mutex::new(BlockingReadState::default()), Condvar::new())),
        }
    }

    fn block_next_read(&self) {
        self.block_next();
    }

    fn block_next(&self) {
        let (lock, _) = &*self.state;
        let mut state = lock
            .lock()
            .expect("blocking read gate lock should not poison");
        state.block_next = true;
        state.blocked = false;
        state.released = false;
    }

    fn maybe_block(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock
            .lock()
            .expect("blocking read gate lock should not poison");
        if !state.block_next {
            return;
        }
        state.block_next = false;
        state.blocked = true;
        condvar.notify_all();
        let deadline = Instant::now() + NEXT_TIMEOUT;
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for blocking read gate release"
            );
            let (next_state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .expect("blocking read gate lock should not poison after wait");
            state = next_state;
            assert!(
                !wait_result.timed_out() || state.released,
                "timed out waiting for blocking read gate release"
            );
        }
    }

    fn wait_until_blocked(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock
            .lock()
            .expect("blocking read gate lock should not poison");
        let deadline = Instant::now() + NEXT_TIMEOUT;
        while !state.blocked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for blocking read gate"
            );
            let (next_state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .expect("blocking read gate lock should not poison after wait");
            state = next_state;
            assert!(
                !wait_result.timed_out() || state.blocked,
                "timed out waiting for blocking read gate"
            );
        }
    }

    fn release(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock
            .lock()
            .expect("blocking read gate lock should not poison");
        state.released = true;
        condvar.notify_all();
    }
}

#[derive(Default)]
struct BlockingReadState {
    block_next: bool,
    blocked: bool,
    released: bool,
}

struct CountingRead {
    inner: InMemoryRead,
    read_count: Arc<AtomicUsize>,
    counted: AtomicBool,
}

impl CountingReadBackend {
    fn new() -> Self {
        Self {
            inner: InMemoryBackend::new(),
            read_count: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn reset_read_count(&self) {
        self.read_count.store(0, Ordering::SeqCst);
    }

    fn read_count(&self) -> usize {
        self.read_count.load(Ordering::SeqCst)
    }
}

impl Backend for CountingReadBackend {
    type Read<'a>
        = CountingRead
    where
        Self: 'a;

    type Write<'a>
        = InMemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, BackendError> {
        Ok(CountingRead {
            inner: self.inner.begin_read(opts).await?,
            read_count: Arc::clone(&self.read_count),
            counted: AtomicBool::new(false),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, BackendError> {
        self.inner.begin_write(opts).await
    }
}

impl BackendRead for CountingRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        opts: GetOptions,
    ) -> Result<GetManyResult, BackendError> {
        self.count_user_read(space);
        self.inner.get_many(space, keys, opts).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        opts: ScanOptions,
    ) -> Result<ScanChunk, BackendError> {
        self.count_user_read(space);
        self.inner.scan(space, range, opts).await
    }
}

impl CountingRead {
    fn count_user_read(&self, space: SpaceId) {
        if space != SpaceId(0x0007_0001) && !self.counted.swap(true, Ordering::SeqCst) {
            self.read_count.fetch_add(1, Ordering::SeqCst);
        }
    }
}

simulation_test!(
    observe_emits_after_durable_runtime_function_read_changes_storage,
    options = support::simulation_test::engine::SimulationOptions {
        deterministic: false,
    },
    |sim| async move {
        let engine = sim.boot_engine().await;
        let (raw_session, session) = open_workspace_session(&sim, &engine).await;
        session
            .execute(
                "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
                 VALUES ('lix_deterministic_mode', \
                 lix_json('{\"enabled\":true}'), true, true)",
                &[],
            )
            .await
            .expect("deterministic mode insert should succeed");

        let mut events = observe_key(&raw_session, "lix_deterministic_sequence_number");
        let initial = next_event(&mut events, "initial deterministic sequence snapshot").await;
        assert!(initial.rows.is_empty());

        session
            .execute("SELECT lix_uuid_v7()", &[])
            .await
            .expect("deterministic uuid read should succeed");

        let update = next_event(&mut events, "deterministic sequence update").await;
        assert_eq!(update.sequence, 1);
        assert_eq!(update.rows.columns(), &["key", "value"]);
        assert_eq!(update.rows.len(), 1);
        assert_eq!(
            update.rows.rows()[0].values(),
            &[
                Value::Text("lix_deterministic_sequence_number".to_string()),
                Value::Json(json!(0)),
            ]
        );
    }
);
