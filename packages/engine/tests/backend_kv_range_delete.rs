#![cfg(feature = "storage-benches")]

use lix_engine::{
    Backend, BackendKvGetGroup, BackendKvGetRequest, BackendKvScanRange, BackendKvWriteBatch,
    BackendKvWriteGroup,
};

#[path = "../benches/storage/rocksdb_backend.rs"]
mod rocksdb_backend;
#[path = "../benches/storage/sqlite_backend.rs"]
mod sqlite_backend;

use rocksdb_backend::RocksDbBenchBackend;
use sqlite_backend::SqliteBenchBackend;

#[tokio::test]
async fn sqlite_range_delete_preserves_order_and_namespace_bounds() {
    let backend = SqliteBenchBackend::tempfile().expect("create sqlite backend");
    range_delete_preserves_order_and_namespace_bounds(&backend).await;
    prefix_ff_range_delete_is_bounded(&backend).await;
}

#[tokio::test]
async fn rocksdb_range_delete_preserves_order_and_namespace_bounds() {
    let backend = RocksDbBenchBackend::new().expect("create rocksdb backend");
    range_delete_preserves_order_and_namespace_bounds(&backend).await;
    prefix_ff_range_delete_is_bounded(&backend).await;
}

async fn range_delete_preserves_order_and_namespace_bounds(backend: &dyn Backend) {
    write_ops(
        backend,
        "ns",
        [
            WriteOp::Put(b"a".as_slice(), b"old-a".as_slice()),
            WriteOp::Put(b"survivor".as_slice(), b"old-survivor".as_slice()),
            WriteOp::Put(&[0xFF], b"old-ff".as_slice()),
        ],
    )
    .await;
    write_ops(
        backend,
        "other",
        [WriteOp::Put(&[0xFF], b"other-ff".as_slice())],
    )
    .await;

    write_ops(
        backend,
        "ns",
        [
            WriteOp::Put(b"survivor".as_slice(), b"before-range".as_slice()),
            WriteOp::DeleteRange(BackendKvScanRange::prefix(Vec::new())),
            WriteOp::Put(b"survivor".as_slice(), b"after-range".as_slice()),
            WriteOp::Put(&[0xFF], b"after-ff".as_slice()),
        ],
    )
    .await;

    assert_eq!(get(backend, "ns", b"a").await, None);
    assert_eq!(
        get(backend, "ns", b"survivor").await,
        Some(b"after-range".to_vec())
    );
    assert_eq!(
        get(backend, "ns", &[0xFF]).await,
        Some(b"after-ff".to_vec())
    );
    assert_eq!(
        get(backend, "other", &[0xFF]).await,
        Some(b"other-ff".to_vec())
    );
}

async fn prefix_ff_range_delete_is_bounded(backend: &dyn Backend) {
    write_ops(
        backend,
        "ff",
        [
            WriteOp::Put(&[0xFE], b"before".as_slice()),
            WriteOp::Put(&[0xFF], b"prefix-root".as_slice()),
            WriteOp::Put(&[0xFF, 0x00], b"prefix-child".as_slice()),
        ],
    )
    .await;
    write_ops(
        backend,
        "other_ff",
        [WriteOp::Put(&[0xFF, 0x00], b"other".as_slice())],
    )
    .await;

    write_ops(
        backend,
        "ff",
        [WriteOp::DeleteRange(BackendKvScanRange::prefix([0xFF]))],
    )
    .await;

    assert_eq!(get(backend, "ff", &[0xFE]).await, Some(b"before".to_vec()));
    assert_eq!(get(backend, "ff", &[0xFF]).await, None);
    assert_eq!(get(backend, "ff", &[0xFF, 0x00]).await, None);
    assert_eq!(
        get(backend, "other_ff", &[0xFF, 0x00]).await,
        Some(b"other".to_vec())
    );
}

enum WriteOp<'a> {
    Put(&'a [u8], &'a [u8]),
    DeleteRange(BackendKvScanRange),
}

async fn write_ops<'a>(
    backend: &dyn Backend,
    namespace: &str,
    ops: impl IntoIterator<Item = WriteOp<'a>>,
) {
    let mut group = BackendKvWriteGroup::new(namespace);
    for op in ops {
        match op {
            WriteOp::Put(key, value) => group.put(key, value),
            WriteOp::DeleteRange(range) => group.delete_range(range),
        }
    }
    let mut transaction = backend
        .begin_write_transaction()
        .await
        .expect("write transaction opens");
    transaction
        .write_kv_batch(BackendKvWriteBatch {
            groups: vec![group],
        })
        .await
        .expect("write succeeds");
    transaction.commit().await.expect("commit succeeds");
}

async fn get(backend: &dyn Backend, namespace: &str, key: &[u8]) -> Option<Vec<u8>> {
    let mut transaction = backend
        .begin_read_transaction()
        .await
        .expect("read transaction opens");
    let result = transaction
        .get_values(BackendKvGetRequest {
            groups: vec![BackendKvGetGroup {
                namespace: namespace.to_string(),
                keys: vec![key.to_vec()],
            }],
        })
        .await
        .expect("get succeeds");
    transaction.rollback().await.expect("rollback succeeds");
    result
        .groups
        .into_iter()
        .next()
        .and_then(|group| group.value(0).flatten().map(<[u8]>::to_vec))
}
