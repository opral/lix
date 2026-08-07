use lix::{Lix, open_lix};
use lix_cli::db::{block_on, init_lix_at, open_lix_at};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;
use std::fs;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

fn assert_rocksdb_owner(_: &Lix<RocksDB>) {}

fn assert_slatedb_owner(_: &Lix<SlateDB>) {}

fn temporary_directory(label: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time must follow the Unix epoch")
        .as_nanos();
    let path = std::env::temp_dir().join(format!(
        "forktree-stage2-cli-route-{label}-{}-{nanos}",
        std::process::id()
    ));
    fs::create_dir_all(&path).expect("temporary directory must be created");
    path
}

#[test]
fn normal_cli_init_open_is_rocksdb_and_cold_reopens() {
    let directory = temporary_directory("rocks");
    let path = directory.join("normal-cli.lix");

    assert!(init_lix_at(&path).expect("normal CLI init must open RocksDB"));
    let first = open_lix_at(&path).expect("normal CLI open must reopen initialized RocksDB");
    assert_rocksdb_owner(&first);
    block_on(first.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('stage2-cli-rocks', lix_json('\"present\"'))",
        &[],
    ))
    .expect("RocksDB marker write must commit");
    block_on(first.close()).expect("first RocksDB-backed Lix must close");
    drop(first);

    let reopened = open_lix_at(&path).expect("normal CLI RocksDB must cold reopen");
    assert_rocksdb_owner(&reopened);
    let result = block_on(reopened.execute(
        "SELECT key FROM lix_key_value WHERE key = 'stage2-cli-rocks'",
        &[],
    ))
    .expect("RocksDB marker read must succeed");
    assert_eq!(result.len(), 1, "cold reopen must retain RocksDB data");
    block_on(reopened.close()).expect("reopened RocksDB-backed Lix must close");
    drop(reopened);
    fs::remove_dir_all(directory).expect("RocksDB test directory must be removable");
}

#[test]
fn explicit_slatedb_selection_cold_reopens() {
    let directory = temporary_directory("slate");
    let path = directory.join("explicit-slate.lix");

    let first_storage = SlateDB::open(&path).expect("explicit SlateDB must open");
    let first = block_on(open_lix().with_storage(first_storage))
        .expect("Lix must initialize over explicit SlateDB");
    assert_slatedb_owner(&first);
    block_on(first.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('stage2-cli-slate', lix_json('\"present\"'))",
        &[],
    ))
    .expect("SlateDB marker write must commit");
    block_on(first.close()).expect("first SlateDB-backed Lix must close");
    drop(first);

    let reopened_storage = SlateDB::open(&path).expect("explicit SlateDB must cold reopen");
    let reopened = block_on(open_lix().with_storage(reopened_storage))
        .expect("Lix must reopen over explicit SlateDB");
    assert_slatedb_owner(&reopened);
    let result = block_on(reopened.execute(
        "SELECT key FROM lix_key_value WHERE key = 'stage2-cli-slate'",
        &[],
    ))
    .expect("SlateDB marker read must succeed");
    assert_eq!(result.len(), 1, "cold reopen must retain SlateDB data");
    block_on(reopened.close()).expect("reopened SlateDB-backed Lix must close");
    drop(reopened);
    fs::remove_dir_all(directory).expect("SlateDB test directory must be removable");
}
