#[path = "../../../lix/tests/adapter_deterministic_sequence_corruption.rs"]
mod deterministic_sequence_corruption;
mod file_sql;
mod native_file_read;
mod native_file_upsert;
#[path = "../../../lix/tests/support/registered_native_singleton.rs"]
mod registered_native_singleton;
mod rocksdb_specific;
#[path = "../../../lix/tests/adapter_undo_redo_checkpoint.rs"]
mod undo_redo_checkpoint;

use lix::open_lix;
use lix::storage::conformance::run_storage_conformance;
use lix_storage_rocksdb::{RocksDB, RocksDBFactory};

#[tokio::test]
async fn rocksdb_passes_storage_conformance() {
    let factory = RocksDBFactory::new();

    run_storage_conformance(&factory).await.assert_no_failures();
}

#[test]
fn rocksdb_exposes_database_path_and_flushes() {
    let temp_dir = tempfile::tempdir().expect("create rocksdb storage temp dir");
    let path = temp_dir.path().join("storage.rocksdb");

    let storage = RocksDB::open(&path).expect("open rocksdb storage");
    storage.flush().expect("flush rocksdb storage");

    assert_eq!(storage.path(), path.as_path());
}

#[tokio::test]
async fn checkpointed_state_survives_undo_redo_and_cold_reopen_on_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let path = temp_dir.path().join("undo-redo.rocksdb");
    let storage = RocksDB::open(&path).expect("open RocksDB storage");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("open repository");
    let branch_id = undo_redo_checkpoint::stage_checkpointed_a_and_undo_b(&lix).await;
    drop(lix);
    storage.flush().expect("flush undo state");
    drop(storage);

    let storage = RocksDB::open(&path).expect("reopen RocksDB after undo");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("reopen repository after undo");
    undo_redo_checkpoint::assert_cold_undo_then_redo(&lix, branch_id.clone()).await;
    drop(lix);
    storage.flush().expect("flush redo state");
    drop(storage);

    let storage = RocksDB::open(&path).expect("reopen RocksDB after redo");
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("reopen repository after redo");
    undo_redo_checkpoint::assert_cold_redo(&lix, branch_id).await;
}

#[tokio::test]
async fn deterministic_sequence_member_corruption_fails_closed_on_rocksdb() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");

    let initial_path = temp_dir.path().join("sequence-initial.rocksdb");
    let storage = RocksDB::open(&initial_path).expect("open initial RocksDB storage");
    deterministic_sequence_corruption::initialize_with_deterministic_mode(storage.clone()).await;
    storage.flush().expect("flush initial deterministic mode");
    drop(storage);
    let storage = RocksDB::open(&initial_path).expect("reopen initial RocksDB storage");
    deterministic_sequence_corruption::assert_next_uuid(storage, "000000000000").await;

    let corrupt_path = temp_dir.path().join("sequence-corrupt.rocksdb");
    let storage = RocksDB::open(&corrupt_path).expect("open corruption RocksDB storage");
    deterministic_sequence_corruption::initialize_with_deterministic_mode(storage.clone()).await;
    deterministic_sequence_corruption::assert_next_uuid(storage.clone(), "000000000000").await;
    storage.flush().expect("flush published sequence member");
    drop(storage);

    let storage = RocksDB::open(&corrupt_path).expect("reopen published sequence storage");
    deterministic_sequence_corruption::replace_selected_sequence_member_with_unrelated(&storage)
        .await;
    storage
        .flush()
        .expect("flush same-count sequence member substitution");
    drop(storage);

    let storage = RocksDB::open(&corrupt_path).expect("reopen corrupt sequence storage");
    deterministic_sequence_corruption::assert_missing_sequence_member_fails_closed(storage).await;
}

#[tokio::test]
async fn registered_native_singleton_survives_rocksdb_cold_reopen() {
    let temp_dir = tempfile::tempdir().expect("create RocksDB temp directory");
    let path = temp_dir.path().join("registered-native-singleton.rocksdb");
    let storage = RocksDB::open(&path).expect("open RocksDB storage");
    registered_native_singleton::stage_and_assert_registered_singleton(storage.clone()).await;
    storage.flush().expect("flush registered singleton");
    drop(storage);

    let reopened = RocksDB::open(&path).expect("reopen RocksDB storage");
    registered_native_singleton::assert_reopened_registered_singleton(reopened).await;
}
