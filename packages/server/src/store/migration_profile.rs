//! Server-only migration profiling fixtures.
//!
//! This deliberately stages the repository marker in the server crate. It
//! exercises the production server opener without changing the SDK migration
//! implementation or depending on SDK-private test helpers.

use super::*;
use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::{self, BoxStream};
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result as ObjectStoreResult,
    path::Path as ObjectPath,
};
use std::fmt;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

const PROFILE_ID: &str = "01936f4e-7b6c-7c3d-8f9a-123456789abc";
const PROFILE_ROWS: usize = 128;
const EPOCH: lix_sdk::storage::StorageSpace = lix_sdk::storage::StorageSpace::mutable(
    lix_sdk::storage::SpaceId(0x0009_0001),
    "repository.epoch.v1",
);

#[derive(Debug, Default)]
struct CallCounters {
    get: AtomicU64,
    get_nanos: AtomicU64,
    get_ranges: AtomicU64,
    get_ranges_nanos: AtomicU64,
    put: AtomicU64,
    put_nanos: AtomicU64,
    copy: AtomicU64,
    copy_nanos: AtomicU64,
    list: AtomicU64,
    list_nanos: AtomicU64,
    delete: AtomicU64,
}

impl CallCounters {
    fn reset(&self) {
        for value in [
            &self.get,
            &self.get_nanos,
            &self.get_ranges,
            &self.get_ranges_nanos,
            &self.put,
            &self.put_nanos,
            &self.copy,
            &self.copy_nanos,
            &self.list,
            &self.list_nanos,
            &self.delete,
        ] {
            value.store(0, Ordering::Relaxed);
        }
    }

    fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "get": self.get.load(Ordering::Relaxed),
            "getMs": nanos_to_millis(self.get_nanos.load(Ordering::Relaxed)),
            "getRanges": self.get_ranges.load(Ordering::Relaxed),
            "getRangesMs": nanos_to_millis(self.get_ranges_nanos.load(Ordering::Relaxed)),
            "put": self.put.load(Ordering::Relaxed),
            "putMs": nanos_to_millis(self.put_nanos.load(Ordering::Relaxed)),
            "copy": self.copy.load(Ordering::Relaxed),
            "copyMs": nanos_to_millis(self.copy_nanos.load(Ordering::Relaxed)),
            "list": self.list.load(Ordering::Relaxed),
            "listMs": nanos_to_millis(self.list_nanos.load(Ordering::Relaxed)),
            "delete": self.delete.load(Ordering::Relaxed),
        })
    }
}

fn nanos_to_millis(nanos: u64) -> u64 {
    nanos / 1_000_000
}

#[derive(Debug)]
struct CountingStore {
    inner: Arc<dyn ObjectStore>,
    calls: Arc<CallCounters>,
}

impl fmt::Display for CountingStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "migration-profile {}", self.inner)
    }
}

#[async_trait]
impl ObjectStore for CountingStore {
    async fn put_opts(
        &self,
        path: &ObjectPath,
        payload: PutPayload,
        options: PutOptions,
    ) -> ObjectStoreResult<PutResult> {
        self.calls.put.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.inner.put_opts(path, payload, options).await;
        self.calls
            .put_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    async fn put_multipart_opts(
        &self,
        path: &ObjectPath,
        options: PutMultipartOptions,
    ) -> ObjectStoreResult<Box<dyn MultipartUpload>> {
        self.calls.put.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.inner.put_multipart_opts(path, options).await;
        self.calls
            .put_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    async fn get_opts(
        &self,
        path: &ObjectPath,
        options: GetOptions,
    ) -> ObjectStoreResult<GetResult> {
        self.calls.get.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.inner.get_opts(path, options).await;
        self.calls
            .get_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    async fn get_ranges(
        &self,
        path: &ObjectPath,
        ranges: &[Range<u64>],
    ) -> ObjectStoreResult<Vec<Bytes>> {
        self.calls.get_ranges.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.inner.get_ranges(path, ranges).await;
        self.calls
            .get_ranges_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    fn delete_stream(
        &self,
        paths: BoxStream<'static, ObjectStoreResult<ObjectPath>>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectPath>> {
        self.calls.delete.fetch_add(1, Ordering::Relaxed);
        self.inner.delete_stream(paths)
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, ObjectStoreResult<ObjectMeta>> {
        self.calls.list.fetch_add(1, Ordering::Relaxed);
        let inner = Arc::clone(&self.inner);
        let calls = Arc::clone(&self.calls);
        let prefix = prefix.cloned();
        Box::pin(
            stream::once(async move {
                let started = Instant::now();
                let mut listed = inner.list(prefix.as_ref());
                let mut values = Vec::new();
                while let Some(value) = listed.next().await {
                    values.push(value);
                }
                calls
                    .list_nanos
                    .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
                values
            })
            .flat_map(stream::iter),
        )
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> ObjectStoreResult<ListResult> {
        self.calls.list.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        self.calls
            .list_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }

    async fn copy_opts(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
        options: CopyOptions,
    ) -> ObjectStoreResult<()> {
        self.calls.copy.fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();
        let result = self.inner.copy_opts(from, to, options).await;
        self.calls
            .copy_nanos
            .fetch_add(started.elapsed().as_nanos() as u64, Ordering::Relaxed);
        result
    }
}

async fn point(
    storage: &SlateDB,
    space: lix_sdk::storage::StorageSpace,
    key: &'static [u8],
) -> Bytes {
    use lix_sdk::storage::{
        GetManyRequest, Key, ProjectedValue, Storage, StorageRead, StorageSession,
    };
    let storage = StorageSession::acquire(storage.clone()).await.unwrap();
    let read = storage.begin_read(Default::default()).await.unwrap();
    let mut values = read
        .get_many(&[GetManyRequest {
            space,
            keys: &[Key(Bytes::from_static(key))],
            opts: Default::default(),
        }])
        .await
        .unwrap()
        .values;
    match values.pop().unwrap().unwrap() {
        ProjectedValue::FullValue(value) => value,
        _ => panic!("profile fixture requires a full value"),
    }
}

async fn stage_v81(manager: &LixRuntimeManager, physical: &str) {
    use lix_sdk::storage::{
        Key, PutBatch, PutEntry, SpaceId, Storage, StorageSession, StorageWrite, StoredValue,
    };
    let storage = manager
        .open_storage(physical, SlateDBIoCounters::default())
        .unwrap();
    let seed = lix_sdk::open_lix()
        .with_storage(storage.clone())
        .await
        .unwrap();
    let schema = serde_json::json!({
        "$schema": "https://lix.dev/schema-v1.json",
        "key": "migration_profile_index",
        "columns": [
            {"name": "id", "type": "text", "nullable": false},
            {"name": "label", "type": "text", "nullable": false}
        ],
        "primary_key": ["id"],
        "unique": [["label"]]
    });
    seed.execute(
        "INSERT INTO lix_registered_schema(value) VALUES ($1)",
        &[lix_sdk::Value::Jsonb(schema.into())],
    )
    .await
    .unwrap();
    let values = (0..PROFILE_ROWS)
        .map(|index| format!("('id-{index}', 'label-{index}')"))
        .collect::<Vec<_>>()
        .join(",");
    seed.execute(
        &format!("INSERT INTO migration_profile_index(id,label) VALUES {values}"),
        &[],
    )
    .await
    .unwrap();
    seed.close().await.unwrap();
    drop(seed);
    let authority = lix_sdk::open_lix()
        .with_storage(storage.clone())
        .serve()
        .with_lix_id(PROFILE_ID)
        .await
        .unwrap();
    authority.close().await.unwrap();
    drop(authority);

    let storage = manager
        .open_storage(physical, SlateDBIoCounters::default())
        .unwrap();
    let original = point(&storage, EPOCH, b"active").await;
    let mut parts = std::str::from_utf8(&original)
        .unwrap()
        .split('|')
        .map(str::to_owned)
        .collect::<Vec<_>>();
    assert_eq!(parts[1], "active");
    let prefix = match parts[2].as_str() {
        "a" => 0x4000_0000,
        "b" => 0x8000_0000,
        other => panic!("unexpected fixture bank {other}"),
    };
    parts[4] = "81".to_owned();
    let pointer = Bytes::from(parts.join("|"));
    let mut write = StorageSession::acquire(storage)
        .await
        .unwrap()
        .begin_write(Default::default())
        .await
        .unwrap();
    write
        .put_many(
            EPOCH,
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"active")),
                    value: StoredValue { bytes: pointer },
                }],
            },
        )
        .await
        .unwrap();
    write
        .put_many(
            lix_sdk::storage::StorageSpace::mutable(
                SpaceId(prefix | 0x0004_0011),
                "repository.protocol.v1",
            ),
            PutBatch {
                entries: vec![PutEntry {
                    key: Key(Bytes::from_static(b"current")),
                    value: StoredValue {
                        bytes: Bytes::from_static(b"tracked-default-branch.v81"),
                    },
                }],
            },
        )
        .await
        .unwrap();
    write.commit().await.unwrap();
}

async fn write_profile_catalog(manager: &LixRuntimeManager, physical: &str) {
    let (objects, prefix) = manager.catalog_store();
    objects
        .put(
            &ObjectPath::from(format!("{prefix}.lix-repositories/{PROFILE_ID}.json")),
            serde_json::to_vec(&RepositoryRecord {
                state: "live".to_owned(),
                fingerprint: Some("migration-profile".to_owned()),
                storage_id: physical.to_owned(),
                retired: Vec::new(),
                admission: Some(AuthorityAdmission {
                    storage_epoch: 81,
                    protocol_epoch: lix_sdk::SYNC_PROTOCOL_VERSION - 1,
                }),
            })
            .unwrap()
            .into(),
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "explicit migration profiling probe"]
async fn profile_server_v81_to_v82_migration_backend_calls() {
    let mut manager = LixRuntimeManager::new_in_memory(4);
    let inner = match &manager.backend {
        StorageBackend::Memory { object_store } => Arc::clone(object_store),
        StorageBackend::S3 { .. } => unreachable!("profile uses in-memory object storage"),
    };
    let calls = Arc::new(CallCounters::default());
    Arc::get_mut(&mut manager).unwrap().backend = StorageBackend::Memory {
        object_store: Arc::new(CountingStore {
            inner,
            calls: Arc::clone(&calls),
        }),
    };
    let physical = uuid::Uuid::new_v4().to_string();
    stage_v81(&manager, &physical).await;
    write_profile_catalog(&manager, &physical).await;
    calls.reset();

    let started = Instant::now();
    let service = loop {
        match manager.get(PROFILE_ID).await {
            Ok(service) => break service,
            Err(LixRuntimeError::Migrating { .. }) => {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            Err(error) => panic!("profile migration failed: {error:?}"),
        }
    };
    let elapsed_ms = started.elapsed().as_millis();
    eprintln!(
        "migration_profile baseline={}",
        serde_json::json!({"elapsedMs": elapsed_ms, "backend": calls.json()})
    );

    service.close().await.unwrap();
    manager.shutdown().await.unwrap();
}
