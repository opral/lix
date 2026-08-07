use lix::storage::Memory;
use lix::storage_bench::{
    run_forktree_application_oracle, verify_forktree_application_oracle_reopen,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const CASES: &[&str] = &[
    "state_catalog",
    "upload_gc",
    "shared_final",
    "retained_races",
    "corruption",
];

async fn run_memory() {
    for case in CASES {
        let storage = Memory::new();
        let result = run_forktree_application_oracle(&storage, case)
            .await
            .unwrap_or_else(|error| panic!("Memory {case}: {error}"));
        eprintln!("backend=memory case={case} phase=run result={result}");
        let result = verify_forktree_application_oracle_reopen(&storage, case)
            .await
            .unwrap_or_else(|error| panic!("Memory reopen {case}: {error}"));
        eprintln!("backend=memory case={case} phase=reopen result={result}");
    }
}

#[tokio::test]
async fn forktree_stage1_application_memory() {
    run_memory().await;
}

#[tokio::test]
async fn forktree_stage1_application_rocksdb() {
    for case in CASES {
        let root = tempfile::tempdir().expect("RocksDB application-oracle directory");
        let path = root.path().join(case);
        let storage = RocksDB::open(&path).expect("open RocksDB application oracle");
        let result = run_forktree_application_oracle(&storage, case)
            .await
            .unwrap_or_else(|error| panic!("RocksDB {case}: {error}"));
        eprintln!("backend=rocksdb case={case} phase=run result={result}");
        drop(storage);
        let reopened = RocksDB::open(&path).expect("reopen RocksDB application oracle");
        let result = verify_forktree_application_oracle_reopen(&reopened, case)
            .await
            .unwrap_or_else(|error| panic!("RocksDB reopen {case}: {error}"));
        eprintln!("backend=rocksdb case={case} phase=reopen result={result}");
    }
}

#[tokio::test]
async fn forktree_stage1_application_slatedb() {
    for case in CASES {
        let root = tempfile::tempdir().expect("SlateDB application-oracle directory");
        let path = root.path().join(case);
        let storage = SlateDB::open(&path).expect("open SlateDB application oracle");
        let result = run_forktree_application_oracle(&storage, case)
            .await
            .unwrap_or_else(|error| panic!("SlateDB {case}: {error}"));
        eprintln!("backend=slatedb case={case} phase=run result={result}");
        drop(storage);
        let reopened = SlateDB::open(&path).expect("reopen SlateDB application oracle");
        let result = verify_forktree_application_oracle_reopen(&reopened, case)
            .await
            .unwrap_or_else(|error| panic!("SlateDB reopen {case}: {error}"));
        eprintln!("backend=slatedb case={case} phase=reopen result={result}");
    }
}
