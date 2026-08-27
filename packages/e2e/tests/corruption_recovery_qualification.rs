use async_trait::async_trait;
use bytes::Bytes;
use lix::storage::{Storage, StorageSession, StorageSpace, ValueSemantics};
use lix::storage_adapter::{
    StorageAdapter, StorageKey, StorageReadOptions, StorageValue, StorageWriteOptions,
};
use lix::storage_bench::{
    MergeBaseBenchScenario, layout_space_catalog, merge_base_for_bench, prepare_merge_for_bench,
    read_binary_cas_for_bench, seed_merge_base_fixture_for_bench, space_inventory,
    write_binary_cas_for_bench,
};
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use std::path::Path;

// Must track `BRANCH_HEAD_CONTROL_NAMESPACE` in `lix::branch::control`, which
// is bumped whenever the branch-control record layout changes. Unlike the
// predicate in the server-protocol gate this cannot match on a prefix: it names
// the space it reconstructs. A stale value here does not fail loudly at the
// mismatch, it makes the inventory come back empty.
const BRANCH_CONTROL_SPACE: &str = "branch.head_control.v11";
const COMMIT_MANIFEST_SPACE: &str = "tracked_state.commit_state_manifest.v7";
const TREE_CHUNK_SPACE: &str = "tracked_state.tree_chunk";
const BINARY_CHUNK_SPACE: &str = "binary_cas.chunk";

#[async_trait]
trait DurableBackend: Storage + Clone + Send + Sync + 'static {
    fn open(path: &Path) -> Self;
    async fn flush_all(&self);
    async fn corrupt_binary_chunk(
        &self,
        path: &Path,
        storage: &StorageAdapter<StorageSession<Self>>,
        target_key: &[u8],
        target_value: &[u8],
        offset: usize,
    ) where
        Self: Sized;
}

#[async_trait]
impl DurableBackend for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB corruption fixture")
    }

    async fn flush_all(&self) {
        self.flush().expect("flush RocksDB corruption fixture");
    }

    async fn corrupt_binary_chunk(
        &self,
        _path: &Path,
        storage: &StorageAdapter<StorageSession<Self>>,
        target_key: &[u8],
        _target_value: &[u8],
        offset: usize,
    ) {
        replace_immutable_value_with_corruption(storage, BINARY_CHUNK_SPACE, target_key, offset)
            .await;
        self.flush().expect("flush corrupt RocksDB binary chunk");
    }
}

#[async_trait]
impl DurableBackend for SlateDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open SlateDB corruption fixture")
    }

    async fn flush_all(&self) {
        self.flush_memtable_for_diagnostics()
            .await
            .expect("flush SlateDB corruption fixture");
    }

    async fn corrupt_binary_chunk(
        &self,
        path: &Path,
        _storage: &StorageAdapter<StorageSession<Self>>,
        _target_key: &[u8],
        target_value: &[u8],
        offset: usize,
    ) {
        corrupt_slatedb_immutable_sidecar(path, target_value, offset);
    }
}

#[tokio::test]
async fn rocksdb_cold_reopen_corruption_qualification() {
    qualify_backend::<RocksDB>().await;
}

#[tokio::test]
async fn slatedb_cold_reopen_corruption_qualification() {
    qualify_backend::<SlateDB>().await;
}

async fn qualify_backend<B: DurableBackend>() {
    qualify_healthy_reopen_undo_diff_and_branch_control::<B>().await;
    qualify_10k_history_manifest::<B>().await;
    qualify_tracked_tree_chunk::<B>().await;
    qualify_binary_payload_chunk::<B>().await;
}

async fn qualify_10k_history_manifest<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create 10K-history corruption fixture");
    let path = directory.path();
    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("open 10K-history corruption fixture");
    let storage = lix.storage_adapter();
    let fixture = seed_merge_base_fixture_for_bench(
        &storage,
        10_000,
        MergeBaseBenchScenario::AncestorDescendant,
    )
    .await
    .expect("seed 10K production commit graph");
    lix.close().await.expect("close seeded 10K fixture");
    drop(lix);
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("cold reopen 10K-history fixture");
    let storage = lix.storage_adapter();
    assert_eq!(
        merge_base_for_bench(&storage, &fixture.left_head, &fixture.right_head)
            .await
            .expect("10K merge base should survive cold reopen"),
        fixture.left_head
    );
    let prepared = prepare_merge_for_bench(&storage, &fixture.left_head, &fixture.right_head)
        .await
        .expect("10K empty-root diff should survive cold reopen");
    assert_eq!((prepared.target_entries, prepared.source_entries), (0, 0));

    let target_key = uuid::Uuid::parse_str(&fixture.right_head)
        .expect("benchmark head is a UUID")
        .as_bytes()
        .to_vec();
    lix.close().await.expect("close healthy 10K reopen");
    drop(lix);
    replace_immutable_value_with_corruption(&storage, COMMIT_MANIFEST_SPACE, &target_key, 0).await;
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let error = match open_lix().with_storage(database.clone()).await {
        Ok(lix) => {
            let storage = lix.storage_adapter();
            let result =
                prepare_merge_for_bench(&storage, &fixture.left_head, &fixture.right_head).await;
            let _ = lix.close().await;
            drop(lix);
            result.expect_err("corrupt commit manifest must fail closed")
        }
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("commit_state_manifest"),
        "unexpected manifest corruption error: {error}"
    );
}

async fn qualify_healthy_reopen_undo_diff_and_branch_control<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create healthy corruption fixture");
    let path = directory.path();
    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("open healthy workspace");
    lix.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('corruption-probe', 'before')",
        &[],
    )
    .await
    .expect("insert healthy probe");
    lix.create_checkpoint().await.expect("create checkpoint");
    let checkpoint_head = active_head(&lix).await;
    lix.execute(
        "UPDATE lix_key_value SET value = 'after' WHERE key = 'corruption-probe'",
        &[],
    )
    .await
    .expect("update healthy probe");
    let updated_head = active_head(&lix).await;
    lix.close().await.expect("close seeded workspace");
    drop(lix);
    database.flush_all().await;
    drop(database);

    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("cold reopen healthy workspace");
    assert_probe_value(&lix, "after").await;
    let diff = lix
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff('lix_key_value', $1, $2)",
            &[Value::Text(checkpoint_head), Value::Text(updated_head)],
        )
        .await
        .expect("healthy diff should survive reopen");
    assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);
    lix.undo()
        .await
        .expect("healthy undo should survive reopen");
    assert_probe_value(&lix, "before").await;
    lix.redo()
        .await
        .expect("healthy redo should survive reopen");
    assert_probe_value(&lix, "after").await;
    let storage = lix.storage_adapter();
    lix.close().await.expect("close healthy reopened workspace");
    drop(lix);
    database.flush_all().await;
    corrupt_every_mutable_value(&storage, BRANCH_CONTROL_SPACE, 4).await;
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let error = match open_lix().with_storage(database.clone()).await {
        Ok(lix) => {
            let result = lix.execute("SELECT * FROM lix_key_value", &[]).await;
            let _ = lix.close().await;
            drop(lix);
            result.expect_err("corrupt branch control must fail on first read")
        }
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("authentication digest mismatch"),
        "unexpected branch-control corruption error: {error}"
    );
    drop(database);
}

async fn qualify_tracked_tree_chunk<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create tracked-tree corruption fixture");
    let path = directory.path();
    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("open tracked-tree fixture");
    for index in 0..8 {
        lix.execute(
            &format!(
                "INSERT INTO lix_key_value (key, value) VALUES ('tree-{index}', 'value-{index}')"
            ),
            &[],
        )
        .await
        .expect("insert tracked-tree fixture row");
    }
    lix.create_checkpoint()
        .await
        .expect("checkpoint tracked-tree fixture");
    let checkpoint_head = active_head(&lix).await;
    lix.execute(
        "UPDATE lix_key_value SET value = 'updated' WHERE key = 'tree-7'",
        &[],
    )
    .await
    .expect("update tracked-tree fixture row");
    lix.create_checkpoint()
        .await
        .expect("checkpoint updated tracked-tree fixture");
    let updated_head = active_head(&lix).await;
    lix.close().await.expect("close tracked-tree fixture");
    drop(lix);
    database.flush_all().await;
    drop(database);

    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("cold reopen healthy tracked tree");
    let healthy = lix
        .execute(
            "SELECT key, value FROM lix_key_value WHERE key LIKE 'tree-%' ORDER BY key",
            &[],
        )
        .await
        .expect("healthy tracked tree should survive cold reopen");
    assert_eq!(healthy.rows().len(), 8, "healthy tracked tree row count");
    let healthy_diff = lix
        .execute(
            "SELECT 'lix_key_value' AS schema_key, diff_type \
             FROM lix_diff('lix_key_value', $1, $2)",
            &[
                Value::Text(checkpoint_head.clone()),
                Value::Text(updated_head.clone()),
            ],
        )
        .await
        .expect("healthy tracked-tree diff should survive cold reopen");
    assert_eq!(
        healthy_diff.rows().len(),
        1,
        "healthy tracked-tree diff row count"
    );
    let storage = lix.storage_adapter();
    lix.close()
        .await
        .expect("close healthy tracked-tree reopen");
    drop(lix);
    corrupt_every_mutable_value(&storage, TREE_CHUNK_SPACE, 0).await;
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let error = match open_lix().with_storage(database.clone()).await {
        Ok(lix) => {
            let result = lix
                .execute(
                    "SELECT 'lix_key_value' AS schema_key, diff_type \
                     FROM lix_diff('lix_key_value', $1, $2)",
                    &[Value::Text(checkpoint_head), Value::Text(updated_head)],
                )
                .await;
            let _ = lix.close().await;
            drop(lix);
            result.expect_err("corrupt tracked tree chunk must fail on historical diff")
        }
        Err(error) => error,
    };
    assert!(
        error
            .to_string()
            .contains("tracked-state chunk hash mismatch"),
        "unexpected tree corruption error: {error}"
    );
    drop(database);
}

async fn qualify_binary_payload_chunk<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create binary-payload corruption fixture");
    let path = directory.path();
    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("open binary-payload fixture");
    let storage = lix.storage_adapter();
    let payload = vec![b'x'; 2 * 1024 * 1024];
    let hash = write_binary_cas_for_bench(&storage, &payload)
        .await
        .expect("seed binary payload");
    lix.close().await.expect("close binary-payload fixture");
    drop(lix);
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let lix = open_lix()
        .with_storage(database.clone())
        .await
        .expect("cold reopen binary-payload fixture");
    let storage = lix.storage_adapter();
    assert_eq!(
        read_binary_cas_for_bench(&storage, &hash)
            .await
            .expect("healthy binary payload should survive cold reopen"),
        Some(payload)
    );
    let chunks = inventory(&storage, BINARY_CHUNK_SPACE).await;
    let target = chunks.first().expect("binary payload should own a chunk");
    lix.close()
        .await
        .expect("close healthy binary-payload reopen");
    drop(lix);
    database
        .corrupt_binary_chunk(path, &storage, &target.0, &target.1, 0)
        .await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let error = match open_lix().with_storage(database.clone()).await {
        Ok(lix) => {
            let storage = lix.storage_adapter();
            let result = read_binary_cas_for_bench(&storage, &hash).await;
            let _ = lix.close().await;
            drop(lix);
            result.expect_err("corrupt file/plugin payload chunk must fail closed")
        }
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("chunk") || error.to_string().contains("hash"),
        "unexpected binary payload corruption error: {error}"
    );
    drop(database);
}

async fn active_head<B: Storage + Clone + Send + Sync + 'static>(lix: &Lix<B>) -> String {
    lix.execute("SELECT lix_active_branch_commit_id() AS commit_id", &[])
        .await
        .expect("load active head")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("active head is text")
}

async fn assert_probe_value<B: Storage + Clone + Send + Sync + 'static>(lix: &Lix<B>, value: &str) {
    let result = lix
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'corruption-probe'",
            &[],
        )
        .await
        .expect("read corruption probe");
    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!(value)
    );
}

async fn inventory<B: Storage + Clone>(
    storage: &StorageAdapter<B>,
    name: &str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open corruption inventory read");
    space_inventory(&read, name).await
}

/// The registered space for one name, value semantics included.
///
/// The semantics used to be a parameter of this helper, which made the caller
/// a second authority for a fact the engine registry already owns. Both
/// adapters place data by that declaration, so a caller that named the wrong
/// one corrupted a different physical location than the one under test.
fn storage_space(name: &str) -> StorageSpace {
    let (id, _) = layout_space_catalog()
        .into_iter()
        .find(|(_, candidate)| *candidate == name)
        .unwrap_or_else(|| panic!("unknown storage space '{name}'"));
    lix::storage_bench::storage_space_by_id(id)
}

async fn corrupt_every_mutable_value<B: Storage + Clone>(
    storage: &StorageAdapter<B>,
    name: &str,
    offset: usize,
) {
    let entries = inventory(storage, name).await;
    assert!(!entries.is_empty(), "corruption target '{name}' is empty");
    let mut writes = storage.new_write_set();
    let space = storage_space(name);
    assert_eq!(
        space.value_semantics,
        ValueSemantics::Mutable,
        "{name} is not a mutable space; overwriting it in place is not how it is published"
    );
    for (key, mut value) in entries {
        assert!(offset < value.len(), "corruption offset for '{name}'");
        value[offset] ^= 1;
        writes.put(
            space,
            StorageKey(Bytes::from(key)),
            StorageValue {
                bytes: Bytes::from(value),
            },
        );
    }
    storage
        .commit_write_set(writes, StorageWriteOptions::default())
        .await
        .expect("commit mutable corruption fixture");
}

async fn replace_immutable_value_with_corruption<B: Storage + Clone>(
    storage: &StorageAdapter<B>,
    name: &str,
    target_key: &[u8],
    offset: usize,
) {
    let entries = inventory(storage, name).await;
    let (_, mut value) = entries
        .into_iter()
        .find(|(key, _)| key == target_key)
        .unwrap_or_else(|| panic!("corruption target key is absent from '{name}'"));
    assert!(offset < value.len(), "corruption offset for '{name}'");
    value[offset] ^= 1;
    let space = storage_space(name);
    assert_eq!(
        space.value_semantics,
        ValueSemantics::Immutable,
        "{name} is not an immutable space; the delete-then-republish dance is unnecessary"
    );
    let key = StorageKey(Bytes::copy_from_slice(target_key));
    let mut deletion = storage.new_write_set();
    deletion.delete(space, key.clone());
    storage
        .commit_write_set(deletion, StorageWriteOptions::default())
        .await
        .expect("delete immutable corruption target");
    let mut replacement = storage.new_write_set();
    replacement.put(
        space,
        key,
        StorageValue {
            bytes: Bytes::from(value),
        },
    );
    storage
        .commit_write_set(replacement, StorageWriteOptions::default())
        .await
        .expect("replace immutable corruption target");
}

fn corrupt_slatedb_immutable_sidecar(path: &Path, target_value: &[u8], offset: usize) {
    const IMMUTABLE_VALUE_MAGIC: &[u8; 8] = b"LIXIVS1\0";
    const IMMUTABLE_VALUE_HEADER_BYTES: usize = 16;

    let directory = path.join("db").join("lix-immutable-value-segment-v1");
    let mut matches = 0usize;
    for entry in std::fs::read_dir(&directory).expect("list SlateDB immutable sidecars") {
        let path = entry.expect("read SlateDB immutable sidecar entry").path();
        if !path.is_file() {
            continue;
        }
        let mut bytes = std::fs::read(&path).expect("read SlateDB immutable sidecar");
        let mut cursor = 0usize;
        let mut changed = false;
        while cursor < bytes.len() {
            let header_end = cursor
                .checked_add(IMMUTABLE_VALUE_HEADER_BYTES)
                .expect("SlateDB immutable sidecar header offset");
            assert!(
                header_end <= bytes.len()
                    && &bytes[cursor..cursor + IMMUTABLE_VALUE_MAGIC.len()]
                        == IMMUTABLE_VALUE_MAGIC,
                "invalid SlateDB immutable sidecar envelope"
            );
            let value_len = usize::try_from(u64::from_le_bytes(
                bytes[cursor + IMMUTABLE_VALUE_MAGIC.len()..header_end]
                    .try_into()
                    .expect("fixed SlateDB immutable sidecar length"),
            ))
            .expect("SlateDB immutable sidecar value length");
            let value_end = header_end
                .checked_add(value_len)
                .expect("SlateDB immutable sidecar value offset");
            assert!(
                value_end <= bytes.len(),
                "truncated SlateDB immutable sidecar envelope"
            );
            if &bytes[header_end..value_end] == target_value {
                assert!(offset < value_len, "SlateDB immutable corruption offset");
                bytes[header_end + offset] ^= 1;
                matches += 1;
                changed = true;
            }
            cursor = value_end;
        }
        if changed {
            std::fs::write(&path, bytes).expect("write corrupt SlateDB immutable sidecar");
            std::fs::File::open(&path)
                .expect("open corrupt SlateDB immutable sidecar")
                .sync_all()
                .expect("sync corrupt SlateDB immutable sidecar");
        }
    }
    assert_eq!(matches, 1, "exactly one SlateDB immutable payload target");
}
