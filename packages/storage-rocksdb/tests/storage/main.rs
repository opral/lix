mod file_sql;
mod native_file_read;
mod native_file_upsert;
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
