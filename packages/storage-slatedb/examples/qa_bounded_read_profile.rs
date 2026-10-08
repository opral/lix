//! Isolated scheduling profile for byte-bounded point reads against an
//! artificial-latency ObjectStore. This measures adapter behavior and remote
//! ObjectStore calls; it does not claim an S3 deployment latency.

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::stream::BoxStream;
use lix::storage::{
    GetManyRequest, Key, PutBatch, PutEntry, ReadBudget, ReadOptions, SpaceId, Storage,
    StorageRead, StorageSpace, StorageWrite, StoredValue, WriteOptions,
};
use lix_storage_slatedb::{
    SlateDB, SlateDBCacheOptions, SlateDBIoCounters, SlateDBObjectStoreOptions,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
};
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

const SPACE: StorageSpace = StorageSpace::mutable(SpaceId(0x7fff_0021), "qa.bounded_read");
const VALUE_BYTES: usize = 256;
const VALUE_BUDGET: ReadBudget = ReadBudget {
    max_result_bytes: 1024 * 1024,
    max_single_value_bytes: VALUE_BYTES,
};

#[derive(Clone, Debug)]
struct DelayedStore {
    inner: Arc<InMemory>,
    delay: Duration,
    measuring: Arc<AtomicBool>,
    get_calls: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
}

impl DelayedStore {
    fn new(delay: Duration) -> Self {
        Self {
            inner: Arc::new(InMemory::new()),
            delay,
            measuring: Arc::new(AtomicBool::new(false)),
            get_calls: Arc::new(AtomicUsize::new(0)),
            active: Arc::new(AtomicUsize::new(0)),
            max_active: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn reset(&self) {
        self.get_calls.store(0, Ordering::Relaxed);
        self.max_active.store(0, Ordering::Relaxed);
        self.measuring.store(true, Ordering::Release);
    }

    fn stop(&self) {
        self.measuring.store(false, Ordering::Release);
    }

    async fn delay_get(&self) -> bool {
        if !self.measuring.load(Ordering::Acquire) {
            return false;
        }
        self.get_calls.fetch_add(1, Ordering::Relaxed);
        let active = self.active.fetch_add(1, Ordering::AcqRel) + 1;
        self.max_active.fetch_max(active, Ordering::Relaxed);
        tokio::time::sleep(self.delay).await;
        true
    }

    async fn wait_idle(&self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.active.load(Ordering::Acquire) != 0 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("SlateDB setup GETs should drain before profiling");
    }

    fn finish_get(&self) {
        self.active.fetch_sub(1, Ordering::AcqRel);
    }
}

impl fmt::Display for DelayedStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("delayed in-memory object store")
    }
}

#[async_trait]
impl ObjectStore for DelayedStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        options: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, options).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        options: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, options).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        let measured = self.delay_get().await;
        let result = self.inner.get_opts(location, options).await;
        if measured {
            self.finish_get();
        }
        result
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[std::ops::Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let measured = self.delay_get().await;
        let result = self.inner.get_ranges(location, ranges).await;
        if measured {
            self.finish_get();
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        options: RenameOptions,
    ) -> object_store::Result<()> {
        self.inner.rename_opts(from, to, options).await
    }
}

#[derive(Clone, Copy)]
enum Operation {
    Ordinary,
    Bounded,
    BoundedPrefix,
}

impl Operation {
    fn name(self) -> &'static str {
        match self {
            Self::Ordinary => "get_many",
            Self::Bounded => "get_many_bounded",
            Self::BoundedPrefix => "get_many_bounded_prefix",
        }
    }
}

fn options(cache_root: std::path::PathBuf) -> SlateDBObjectStoreOptions {
    SlateDBObjectStoreOptions {
        cache: Some(SlateDBCacheOptions {
            root_folder: cache_root,
            // SlateDB requires a positive configured cache size. Two bytes
            // split to a one-byte SlateDB cache, too small to retain a block.
            max_disk_cache_bytes: 2,
            block_cache_bytes: 0,
            metadata_cache_bytes: 0,
            max_open_file_handles: 1000,
        }),
    }
}

async fn seed(storage: &SlateDB, item_count: usize) {
    let mut write = storage
        .begin_write(WriteOptions::default())
        .await
        .expect("begin seed write");
    let entries = (0..item_count)
        .map(|index| PutEntry {
            key: Key(Bytes::copy_from_slice(&(index as u64).to_be_bytes())),
            value: StoredValue {
                bytes: Bytes::from(vec![(index % 251) as u8; VALUE_BYTES]),
            },
        })
        .collect();
    write
        .put_many(SPACE, PutBatch { entries })
        .await
        .expect("write seed rows");
    write.commit().await.expect("commit seed rows");
    storage.flush().await.expect("flush seed rows");
}

fn check_values(values: &[Option<lix::storage::ProjectedValue>], item_count: usize) {
    assert_eq!(values.len(), item_count);
    for (index, value) in values.iter().enumerate() {
        let expected = vec![(index % 251) as u8; VALUE_BYTES];
        assert_eq!(
            value,
            &Some(lix::storage::ProjectedValue::FullValue(Bytes::from(
                expected
            ))),
            "result order/value mismatch at slot {index}"
        );
    }
}

async fn profile(operation: Operation, item_count: usize, delay_ms: u64) -> serde_json::Value {
    let backend = Arc::new(DelayedStore::new(Duration::from_millis(delay_ms)));
    let cache_dir = tempfile::tempdir().expect("create isolated empty cache");
    let db_path = format!("qa-bounded-profile-{}", std::process::id());
    let seed_storage = SlateDB::open_object_store_with_options(
        db_path.clone(),
        backend.clone(),
        options(cache_dir.path().join("seed")),
    )
    .expect("open seed SlateDB");
    seed(&seed_storage, item_count).await;
    drop(seed_storage);

    let counters = SlateDBIoCounters::default();
    let storage = SlateDB::open_object_store_with_options_and_io_counters(
        db_path,
        backend.clone(),
        options(cache_dir.path().join("measure")),
        counters.clone(),
    )
    .expect("open measured SlateDB");
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .expect("begin measured read");
    let keys = (0..item_count)
        .map(|index| Key(Bytes::copy_from_slice(&(index as u64).to_be_bytes())))
        .collect::<Vec<_>>();
    let requests = [GetManyRequest {
        space: SPACE,
        keys: &keys,
        opts: Default::default(),
    }];

    backend.wait_idle().await;
    backend.reset();
    let io_before = counters.snapshot();
    let started = Instant::now();
    let slots = match operation {
        Operation::Ordinary => {
            read.get_many(&requests)
                .await
                .expect("ordinary get_many")
                .values
        }
        Operation::Bounded => {
            read.get_many_bounded(&requests, VALUE_BUDGET)
                .await
                .expect("bounded get_many")
                .values
        }
        Operation::BoundedPrefix => {
            read.get_many_bounded_prefix(&requests, 0, item_count, VALUE_BUDGET)
                .await
                .expect("bounded prefix")
                .values
        }
    };
    let elapsed_ms = started.elapsed().as_secs_f64() * 1000.0;
    backend.stop();
    backend.wait_idle().await;
    check_values(&slots, item_count);
    let io = counters.snapshot().saturating_sub(io_before);
    let result = serde_json::json!({
        "operation": operation.name(),
        "items": item_count,
        "value_bytes": item_count * VALUE_BYTES,
        "synthetic_get_rtt_ms": delay_ms,
        "elapsed_ms": elapsed_ms,
        "object_store_get_calls": backend.get_calls.load(Ordering::Relaxed),
        "max_active_get_calls": backend.max_active.load(Ordering::Relaxed),
        "slatedb_read_objects": io.read_objects,
        "slatedb_read_bytes": io.read_bytes,
        "slatedb_main_get_requests": io.main.read_requests,
        "slatedb_reader_get_requests": io.reader.read_requests,
        "ordered_values_verified": true,
    });

    drop(read);
    drop(storage);
    result
}

#[tokio::main]
async fn main() {
    let item_counts = [16usize, 64, 128];
    let delays = [10u64, 50];
    let operations = [
        Operation::Ordinary,
        Operation::Bounded,
        Operation::BoundedPrefix,
    ];
    for delay_ms in delays {
        for item_count in item_counts {
            for operation in operations {
                let result = profile(operation, item_count, delay_ms).await;
                println!("{}", serde_json::to_string(&result).unwrap());
            }
        }
    }
}
