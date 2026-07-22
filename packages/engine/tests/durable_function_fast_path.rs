use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lix_engine::{
    Engine, GetManyResult, GetOptions, Key, KeyRange, Memory, MemoryRead, MemoryWrite, ReadOptions,
    ScanChunk, ScanOptions, SpaceId, Storage, StorageError, StorageRead, Value, WriteOptions,
};

#[derive(Clone, Default)]
struct CountingStorage {
    inner: Memory,
    counters: Arc<StorageCounters>,
}

struct CountingRead {
    inner: MemoryRead,
    counters: Arc<StorageCounters>,
}

#[derive(Default)]
struct StorageCounters {
    begin_reads: AtomicU64,
    get_many_calls: AtomicU64,
    scan_calls: AtomicU64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct CounterSnapshot {
    begin_reads: u64,
    get_many_calls: u64,
    scan_calls: u64,
}

impl CountingStorage {
    fn snapshot(&self) -> CounterSnapshot {
        CounterSnapshot {
            begin_reads: self.counters.begin_reads.load(Ordering::Relaxed),
            get_many_calls: self.counters.get_many_calls.load(Ordering::Relaxed),
            scan_calls: self.counters.scan_calls.load(Ordering::Relaxed),
        }
    }
}

impl Storage for CountingStorage {
    type Read<'a>
        = CountingRead
    where
        Self: 'a;
    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, options: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.counters.begin_reads.fetch_add(1, Ordering::Relaxed);
        Ok(CountingRead {
            inner: self.inner.begin_read(options).await?,
            counters: Arc::clone(&self.counters),
        })
    }

    async fn begin_write(&self, options: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(options).await
    }
}

impl StorageRead for CountingRead {
    async fn get_many(
        &self,
        space: SpaceId,
        keys: &[Key],
        options: GetOptions,
    ) -> Result<GetManyResult, StorageError> {
        self.counters.get_many_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.get_many(space, keys, options).await
    }

    async fn scan(
        &self,
        space: SpaceId,
        range: KeyRange,
        options: ScanOptions,
    ) -> Result<ScanChunk, StorageError> {
        self.counters.scan_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.scan(space, range, options).await
    }
}

impl CounterSnapshot {
    fn delta_since(self, before: Self) -> Self {
        Self {
            begin_reads: self.begin_reads - before.begin_reads,
            get_many_calls: self.get_many_calls - before.get_many_calls,
            scan_calls: self.scan_calls - before.scan_calls,
        }
    }
}

async fn open_session() -> (CountingStorage, lix_engine::SessionContext<CountingStorage>) {
    let storage = CountingStorage::default();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create engine");
    let session = engine
        .open_workspace_session()
        .await
        .expect("workspace session should open");
    (storage, session)
}

#[tokio::test]
async fn pure_read_skips_durable_function_state_storage_work() {
    let (storage, session) = open_session().await;

    // Warm durable state first because persisting it invalidates session cache
    // generations, then warm the pure-query catalog state used for comparison.
    session.execute("SELECT lix_uuid_v7()", &[]).await.unwrap();
    session.execute("SELECT 1 AS value", &[]).await.unwrap();

    let before_pure = storage.snapshot();
    let pure = session.execute("SELECT 1 AS value", &[]).await.unwrap();
    let pure_reads = storage.snapshot().delta_since(before_pure);
    assert_eq!(pure.rows()[0].get::<i64>("value").unwrap(), 1);

    let before_durable = storage.snapshot();
    session.execute("SELECT lix_uuid_v7()", &[]).await.unwrap();
    let durable_reads = storage.snapshot().delta_since(before_durable);

    assert_eq!(
        durable_reads.begin_reads,
        pure_reads.begin_reads + 1,
        "durable setup should own exactly one additional read snapshot: \
         pure={pure_reads:?}, durable={durable_reads:?}"
    );
    assert!(
        durable_reads.get_many_calls > pure_reads.get_many_calls,
        "only the durable-function statement should load durable mode state: \
         pure={pure_reads:?}, durable={durable_reads:?}"
    );
    assert_eq!(durable_reads.scan_calls, pure_reads.scan_calls);
}

#[tokio::test]
async fn pure_reads_do_not_advance_and_durable_reads_still_persist_deterministic_runtime_state() {
    let (_storage, session) = open_session().await;
    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
             VALUES ('lix_deterministic_mode', lix_json('{\"enabled\":true}'), true, true)",
            &[],
        )
        .await
        .expect("deterministic mode should enable");

    for value in 0..10 {
        let result = session
            .execute("SELECT $1 AS value", &[Value::Integer(value)])
            .await
            .expect("pure read should execute in deterministic mode");
        assert_eq!(result.rows()[0].get::<i64>("value").unwrap(), value);
    }

    let first_uuid = session
        .execute("SELECT lix_uuid_v7() AS value", &[])
        .await
        .expect("durable function should execute")
        .rows()[0]
        .get::<String>("value")
        .expect("uuid should be text");
    assert_eq!(first_uuid, "01920000-0000-7000-8000-000000000000");

    let first_timestamp = session
        .execute("SELECT lix_timestamp() AS value", &[])
        .await
        .expect("second durable function should execute")
        .rows()[0]
        .get::<String>("value")
        .expect("timestamp should be text");
    assert_eq!(first_timestamp, "1970-01-01T00:00:00.001Z");

    let next_uuid = session
        .execute("SELECT lix_uuid_v7() AS value", &[])
        .await
        .expect("persisted durable sequence should continue")
        .rows()[0]
        .get::<String>("value")
        .expect("uuid should be text");
    assert_eq!(next_uuid, "01920000-0000-7000-8000-000000000002");
}
