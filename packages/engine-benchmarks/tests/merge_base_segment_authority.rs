use lix::storage_adapter::{Memory, Storage, StorageAdapter, StorageReadOptions};
use lix::storage_bench::{
    MergeBaseBenchScenario, delete_commit_state_authority_for_bench, merge_base_for_bench,
    seed_merge_base_fixture_for_bench, space_inventory,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const COMMIT_SPACE: &str = "changelog.commit";
const MANIFEST_SPACE: &str = "tracked_state.commit_state_manifest.v7";

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bounded_segment_missing_interior_authority_fails_closed_on_public_adapters() {
    assert_missing_interior_authority_fails_closed("memory", &StorageAdapter::new(Memory::new()))
        .await;

    let rocks_directory = tempfile::tempdir().expect("create RocksDB test directory");
    let rocks = RocksDB::open(rocks_directory.path()).expect("open RocksDB test storage");
    assert_missing_interior_authority_fails_closed("rocksdb", &StorageAdapter::new(rocks)).await;

    let slate_directory = tempfile::tempdir().expect("create SlateDB test directory");
    let slate = SlateDB::open(slate_directory.path()).expect("open SlateDB test storage");
    assert_missing_interior_authority_fails_closed("slatedb", &StorageAdapter::new(slate)).await;
}

async fn assert_missing_interior_authority_fails_closed<S>(
    backend: &str,
    storage: &StorageAdapter<S>,
) where
    S: Storage,
{
    let fixture = seed_merge_base_fixture_for_bench(storage, 9, MergeBaseBenchScenario::DeepFork)
        .await
        .unwrap_or_else(|error| panic!("seed {backend} deep fork: {error}"));
    let expected = fixture
        .expected_base
        .as_ref()
        .expect("deep fork has a base");
    assert_eq!(
        merge_base_for_bench(storage, &fixture.left_head, &fixture.right_head)
            .await
            .unwrap_or_else(|error| panic!("resolve healthy {backend} merge base: {error}")),
        *expected,
    );

    let interior = fixture
        .segment_interior_commit_id
        .as_deref()
        .expect("deep fork exposes an elided interior member");
    let before = storage
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap_or_else(|error| panic!("open {backend} before snapshot: {error}"));
    let commit_rows_before = space_inventory(&before, COMMIT_SPACE).await;
    let authority_rows_before = space_inventory(&before, MANIFEST_SPACE).await;
    drop(before);
    assert_eq!(commit_rows_before.len(), fixture.commits);
    assert_eq!(authority_rows_before.len(), fixture.commits);

    delete_commit_state_authority_for_bench(storage, interior)
        .await
        .unwrap_or_else(|error| panic!("delete {backend} interior authority: {error}"));

    let after = storage
        .begin_read(StorageReadOptions::default())
        .await
        .unwrap_or_else(|error| panic!("open {backend} after snapshot: {error}"));
    assert_eq!(
        space_inventory(&after, COMMIT_SPACE).await,
        commit_rows_before
    );
    assert_eq!(
        space_inventory(&after, MANIFEST_SPACE).await.len(),
        authority_rows_before.len() - 1,
    );
    drop(after);

    let error = merge_base_for_bench(storage, &fixture.left_head, &fixture.right_head)
        .await
        .expect_err("bounded segment must reject a missing interior authority");
    assert!(
        error.message.contains("missing its commit-state authority"),
        "{backend}: {error}"
    );
    assert!(error.message.contains(interior), "{backend}: {error}");
}
