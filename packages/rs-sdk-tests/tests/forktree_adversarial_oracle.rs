use lix::storage_bench::{
    run_forktree_adversarial_oracle, verify_forktree_adversarial_oracle_reopen,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const FOCUSED_CASES: &[&str] = &[
    "corrupt_chunk",
    "wrong_domain",
    "edge_mismatch",
    "shared_identity",
    "upload_lifecycle",
    "epoch_races",
    "checkpoint_retention",
    "branch_merge",
];

#[tokio::test]
async fn forktree_adversarial_rocksdb() {
    for case in FOCUSED_CASES {
        let root = tempfile::tempdir().expect("RocksDB oracle directory");
        let path = root.path().join(case);
        let storage = RocksDB::open(&path).expect("open RocksDB oracle");
        let result = run_forktree_adversarial_oracle(&storage, case)
            .await
            .unwrap_or_else(|error| panic!("RocksDB {case}: {error}"));
        eprintln!("backend=rocksdb case={case} phase=run result={result}");
        drop(storage);
        let reopened = RocksDB::open(&path).expect("reopen RocksDB oracle");
        let result = verify_forktree_adversarial_oracle_reopen(&reopened, case)
            .await
            .unwrap_or_else(|error| panic!("RocksDB reopen {case}: {error}"));
        eprintln!("backend=rocksdb case={case} phase=reopen result={result}");
    }
}

#[tokio::test]
async fn forktree_adversarial_slatedb() {
    for case in FOCUSED_CASES {
        let root = tempfile::tempdir().expect("SlateDB oracle directory");
        let path = root.path().join(case);
        let storage = SlateDB::open(&path).expect("open SlateDB oracle");
        let result = run_forktree_adversarial_oracle(&storage, case)
            .await
            .unwrap_or_else(|error| panic!("SlateDB {case}: {error}"));
        eprintln!("backend=slatedb case={case} phase=run result={result}");
        drop(storage);
        let reopened = SlateDB::open(&path).expect("reopen SlateDB oracle");
        let result = verify_forktree_adversarial_oracle_reopen(&reopened, case)
            .await
            .unwrap_or_else(|error| panic!("SlateDB reopen {case}: {error}"));
        eprintln!("backend=slatedb case={case} phase=reopen result={result}");
    }
}
