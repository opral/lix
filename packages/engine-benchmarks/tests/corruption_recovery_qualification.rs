use async_trait::async_trait;
use bytes::Bytes;
use lix::storage::{Storage, StorageSpace, ValueSemantics};
use lix::storage_adapter::{
    StorageAdapter, StorageKey, StorageReadOptions, StorageValue, StorageWriteOptions,
};
use lix::storage_bench::{
    MergeBaseBenchScenario, layout_space_catalog, load_seeded_branch_plugin_checkpoint_for_bench,
    merge_base_for_bench, prepare_merge_for_bench, read_binary_cas_for_bench,
    seed_branch_plugin_checkpoints_for_bench, seed_merge_base_fixture_for_bench, space_inventory,
    write_binary_cas_for_bench,
};
use lix::{Lix, Value, open_lix};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use std::path::Path;

const BRANCH_ID: &str = "01920000-0000-7000-8000-000000000001";
const CHECKPOINT_FILE_ID: &str = "01920000-0000-7000-8000-000010000000";
const BRANCH_CONTROL_SPACE: &str = "branch.head_control.v10";
const PLUGIN_CHECKPOINT_SPACE: &str = "plugin.current_checkpoint.v2";
const COMMIT_MANIFEST_SPACE: &str = "tracked_state.commit_state_manifest.v7";
const TREE_CHUNK_SPACE: &str = "tracked_state.tree_chunk";
const BINARY_CHUNK_SPACE: &str = "binary_cas.chunk";

#[async_trait]
trait DurableBackend: Storage + Clone + Send + Sync + 'static {
    fn open(path: &Path) -> Self;
    async fn flush_all(&self);
}

#[async_trait]
impl DurableBackend for RocksDB {
    fn open(path: &Path) -> Self {
        Self::open(path).expect("open RocksDB corruption fixture")
    }

    async fn flush_all(&self) {
        self.flush().expect("flush RocksDB corruption fixture");
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
    qualify_10k_history_manifest_and_checkpoint::<B>().await;
    qualify_tracked_tree_chunk::<B>().await;
    qualify_binary_payload_chunk::<B>().await;
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
            "SELECT COUNT(*) AS entries FROM lix_diff($1, $2) WHERE schema_key = 'lix_key_value'",
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
    lix.close().await.expect("close healthy reopened workspace");
    database.flush_all().await;
    drop(database);

    let database = B::open(path);
    corrupt_every_mutable_value(&database, BRANCH_CONTROL_SPACE, 4).await;
    database.flush_all().await;
    drop(database);

    let database = B::open(path);
    let error = match open_lix().with_storage(database.clone()).await {
        Ok(lix) => {
            let result = lix.execute("SELECT * FROM lix_key_value", &[]).await;
            let _ = lix.close().await;
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

async fn qualify_10k_history_manifest_and_checkpoint<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create 10K-history corruption fixture");
    let path = directory.path();
    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
    let fixture = seed_merge_base_fixture_for_bench(
        &storage,
        10_000,
        MergeBaseBenchScenario::AncestorDescendant,
    )
    .await
    .expect("seed 10K production commit graph");
    seed_branch_plugin_checkpoints_for_bench(&storage, BRANCH_ID, 1, 1)
        .await
        .expect("seed authenticated checkpoint");
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
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
    assert_eq!(
        load_seeded_branch_plugin_checkpoint_for_bench(&storage, BRANCH_ID, CHECKPOINT_FILE_ID)
            .await
            .expect("healthy checkpoint should load"),
        Some((
            b"checkpoint-runtime".to_vec(),
            b"checkpoint-authority".to_vec()
        ))
    );
    corrupt_every_mutable_value(&database, PLUGIN_CHECKPOINT_SPACE, 92).await;
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
    let error =
        load_seeded_branch_plugin_checkpoint_for_bench(&storage, BRANCH_ID, CHECKPOINT_FILE_ID)
            .await
            .expect_err("corrupt checkpoint must not become a cache miss");
    assert!(
        error.to_string().contains("authentication digest mismatch"),
        "unexpected checkpoint corruption error: {error}"
    );

    let target_key = uuid::Uuid::parse_str(&fixture.right_head)
        .expect("benchmark head is a UUID")
        .as_bytes()
        .to_vec();
    replace_immutable_value_with_corruption(&database, COMMIT_MANIFEST_SPACE, &target_key, 0).await;
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
    let error = prepare_merge_for_bench(&storage, &fixture.left_head, &fixture.right_head)
        .await
        .expect_err("corrupt commit manifest must fail closed");
    assert!(
        error.to_string().contains("commit_state_manifest"),
        "unexpected manifest corruption error: {error}"
    );
    drop(storage);
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
    lix.close().await.expect("close tracked-tree fixture");
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
    lix.close()
        .await
        .expect("close healthy tracked-tree reopen");
    corrupt_every_mutable_value(&database, TREE_CHUNK_SPACE, 0).await;
    database.flush_all().await;
    drop(database);

    let database = B::open(path);
    let error = match open_lix().with_storage(database.clone()).await {
        Ok(lix) => {
            let result = lix
                .execute(
                    "SELECT key, value FROM lix_key_value WHERE key LIKE 'tree-%' ORDER BY key",
                    &[],
                )
                .await;
            let _ = lix.close().await;
            result.expect_err("corrupt tracked tree chunk must fail on first read")
        }
        Err(error) => error,
    };
    assert!(
        error.to_string().contains("digest") || error.to_string().contains("tree"),
        "unexpected tree corruption error: {error}"
    );
    drop(database);
}

async fn qualify_binary_payload_chunk<B: DurableBackend>() {
    let directory = tempfile::tempdir().expect("create binary-payload corruption fixture");
    let path = directory.path();
    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
    let payload = vec![b'x'; 2 * 1024 * 1024];
    let hash = write_binary_cas_for_bench(&storage, &payload)
        .await
        .expect("seed binary payload");
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
    assert_eq!(
        read_binary_cas_for_bench(&storage, &hash)
            .await
            .expect("healthy binary payload should survive cold reopen"),
        Some(payload)
    );
    let chunks = inventory(&database, BINARY_CHUNK_SPACE).await;
    let target = chunks.first().expect("binary payload should own a chunk");
    replace_immutable_value_with_corruption(&database, BINARY_CHUNK_SPACE, &target.0, 0).await;
    database.flush_all().await;
    drop(storage);
    drop(database);

    let database = B::open(path);
    let storage = StorageAdapter::new(database.clone());
    let error = read_binary_cas_for_bench(&storage, &hash)
        .await
        .expect_err("corrupt file/plugin payload chunk must fail closed");
    assert!(
        error.to_string().contains("chunk") || error.to_string().contains("hash"),
        "unexpected binary payload corruption error: {error}"
    );
    drop(storage);
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

async fn inventory<B: Storage + Clone>(database: &B, name: &str) -> Vec<(Vec<u8>, Vec<u8>)> {
    let storage = StorageAdapter::new(database.clone());
    let read = storage
        .begin_read(StorageReadOptions::default())
        .await
        .expect("open corruption inventory read");
    space_inventory(&read, name).await
}

fn storage_space(name: &str, semantics: ValueSemantics) -> StorageSpace {
    let (id, canonical_name) = layout_space_catalog()
        .into_iter()
        .find(|(_, candidate)| *candidate == name)
        .unwrap_or_else(|| panic!("unknown storage space '{name}'"));
    match semantics {
        ValueSemantics::Mutable => StorageSpace::mutable(lix::storage::SpaceId(id), canonical_name),
        ValueSemantics::Immutable => {
            StorageSpace::immutable(lix::storage::SpaceId(id), canonical_name)
        }
    }
}

async fn corrupt_every_mutable_value<B: Storage + Clone>(database: &B, name: &str, offset: usize) {
    let entries = inventory(database, name).await;
    assert!(!entries.is_empty(), "corruption target '{name}' is empty");
    let storage = StorageAdapter::new(database.clone());
    let mut writes = storage.new_write_set();
    let space = storage_space(name, ValueSemantics::Mutable);
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
    database: &B,
    name: &str,
    target_key: &[u8],
    offset: usize,
) {
    let entries = inventory(database, name).await;
    let (_, mut value) = entries
        .into_iter()
        .find(|(key, _)| key == target_key)
        .unwrap_or_else(|| panic!("corruption target key is absent from '{name}'"));
    assert!(offset < value.len(), "corruption offset for '{name}'");
    value[offset] ^= 1;
    let storage = StorageAdapter::new(database.clone());
    let space = storage_space(name, ValueSemantics::Immutable);
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
