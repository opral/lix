use lix::Value;
use lix::open_lix;
use lix::storage::Storage;
use lix::storage_bench::{
    collect_repository_gc_for_bench, read_binary_cas_for_bench, write_binary_cas_for_bench,
};
use lix_storage_rocksdb::RocksDB;
use lix_storage_slatedb::SlateDB;

const V1: &[u8] = b"durable-history-file-v1";
// Different length and below the flat-delta threshold: v2 has an independent
// full manifest, so releasing root A can reclaim v1 without a serving-layout
// dependency from the current blob.
const V2: &[u8] = &[0xa5; 256];

struct HistoryFixture {
    branch_id: String,
    root_a: String,
    root_b: String,
    v1_hash: String,
    v2_hash: String,
}

#[tokio::test]
async fn rocksdb_history_blob_survives_until_final_root_release() {
    let temp = tempfile::tempdir().expect("create RocksDB history fixture");
    let path = temp.path().join("database");
    let fixture =
        prepare_history(RocksDB::open(&path).expect("open RocksDB history preparation")).await;
    verify_history_and_release(
        RocksDB::open(&path).expect("cold reopen RocksDB history fixture"),
        fixture,
    )
    .await;
    verify_final_state(RocksDB::open(&path).expect("final reopen RocksDB history fixture")).await;
}

#[tokio::test]
async fn slatedb_history_blob_survives_until_final_root_release() {
    let temp = tempfile::tempdir().expect("create SlateDB history fixture");
    let path = temp.path().join("database");
    let fixture =
        prepare_history(SlateDB::open(&path).expect("open SlateDB history preparation")).await;
    verify_history_and_release(
        SlateDB::open(&path).expect("cold reopen SlateDB history fixture"),
        fixture,
    )
    .await;
    verify_final_state(SlateDB::open(&path).expect("final reopen SlateDB history fixture")).await;
}

#[tokio::test]
async fn rocksdb_current_untracked_blob_survives_sweep_and_cold_reopen() {
    let temp = tempfile::tempdir().expect("create RocksDB untracked fixture");
    let path = temp.path().join("database");
    prepare_current_untracked(RocksDB::open(&path).expect("open RocksDB untracked preparation"))
        .await;
    verify_current_untracked(RocksDB::open(&path).expect("cold reopen RocksDB untracked fixture"))
        .await;
}

#[tokio::test]
async fn slatedb_current_untracked_blob_survives_sweep_and_cold_reopen() {
    let temp = tempfile::tempdir().expect("create SlateDB untracked fixture");
    let path = temp.path().join("database");
    prepare_current_untracked(SlateDB::open(&path).expect("open SlateDB untracked preparation"))
        .await;
    verify_current_untracked(SlateDB::open(&path).expect("cold reopen SlateDB untracked fixture"))
        .await;
}

#[tokio::test]
async fn rocksdb_shared_blob_survives_replace_rollback_delete_gc_and_reopen() {
    let temp = tempfile::tempdir().expect("create RocksDB shared-blob fixture");
    shared_blob_replacement_lifecycle(&temp.path().join("database"), |path| RocksDB::open(path))
        .await;
}

#[tokio::test]
async fn slatedb_shared_blob_survives_replace_rollback_delete_gc_and_reopen() {
    let temp = tempfile::tempdir().expect("create SlateDB shared-blob fixture");
    shared_blob_replacement_lifecycle(&temp.path().join("database"), |path| SlateDB::open(path))
        .await;
}

async fn shared_blob_replacement_lifecycle<S, O>(path: &std::path::Path, open: O)
where
    S: Storage + Clone + Send + Sync + 'static,
    O: Fn(&std::path::Path) -> Result<S, lix::storage::StorageError>,
{
    const OLD: &[u8] = b"shared-old-content";
    const NEW: &[u8] = b"replacement-content";
    const ROLLED_BACK: &[u8] = b"rolled-back-content";
    let storage = open(path).expect("open shared-blob fixture");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("initialize shared-blob repository");
    let session = lix
        .open_another_session()
        .await
        .expect("open shared-blob session");
    let adapter = lix.storage_adapter();
    let branch = session
        .create_branch(lix::CreateBranchOptions {
            id: Some("01990000-0000-7000-8000-00000000000c".to_owned()),
            name: "shared-blob-disposable".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("create shared-blob disposable branch");
    let branch_id = branch.id;
    session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: branch_id.clone(),
        })
        .await
        .expect("switch shared-blob session branch");
    for path in ["/shared-a.bin", "/shared-b.bin"] {
        session
            .execute(
                "INSERT INTO lix_file (path, content) VALUES ($1, $2)",
                &[
                    Value::Text(path.to_owned()),
                    Value::Blob(OLD.to_vec().into()),
                ],
            )
            .await
            .expect("insert shared blob owner");
    }

    let mut rollback = session
        .begin_transaction()
        .await
        .expect("begin shared replacement rollback");
    rollback
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/shared-a.bin'",
            &[Value::Blob(ROLLED_BACK.to_vec().into())],
        )
        .await
        .expect("stage shared replacement rollback");
    rollback
        .rollback()
        .await
        .expect("rollback shared replacement");
    assert_eq!(
        read_file_at(&session, "/shared-a.bin").await,
        Some(OLD.to_vec())
    );
    assert_eq!(
        read_file_at(&session, "/shared-b.bin").await,
        Some(OLD.to_vec())
    );
    let rolled_back_hash = blake3::hash(ROLLED_BACK).to_hex().to_string();
    collect_repository_gc_for_bench(&adapter)
        .await
        .expect("collect rolled-back replacement");
    assert!(
        read_binary_cas_for_bench(&adapter, &rolled_back_hash)
            .await
            .expect("rolled-back CAS lookup should succeed")
            .is_none(),
        "rolled-back replacement must not become a current or historical owner",
    );

    session
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/shared-a.bin'",
            &[Value::Blob(NEW.to_vec().into())],
        )
        .await
        .expect("commit one shared owner replacement");
    assert_eq!(
        read_file_at(&session, "/shared-a.bin").await,
        Some(NEW.to_vec())
    );
    session
        .execute("DELETE FROM lix_file WHERE path = '/shared-a.bin'", &[])
        .await
        .expect("delete replaced shared owner");
    collect_repository_gc_for_bench(&adapter)
        .await
        .expect("collect while second shared owner remains");
    assert_eq!(
        read_file_at(&session, "/shared-b.bin").await,
        Some(OLD.to_vec())
    );
    let old_hash = blake3::hash(OLD).to_hex().to_string();
    let new_hash = blake3::hash(NEW).to_hex().to_string();
    assert_eq!(
        read_binary_cas_for_bench(&adapter, &old_hash)
            .await
            .expect("shared old CAS lookup should succeed")
            .as_deref(),
        Some(OLD),
    );
    assert_eq!(
        read_binary_cas_for_bench(&adapter, &new_hash)
            .await
            .expect("historical replacement CAS lookup should succeed")
            .as_deref(),
        Some(NEW),
        "deleted replacement must remain while branch history still reaches it",
    );
    drop(session);
    drop(adapter);
    drop(lix);
    drop(storage);

    let reopened_storage = open(path).expect("cold reopen shared-blob fixture");
    let reopened = open_lix()
        .with_storage(reopened_storage.clone())
        .await
        .expect("open shared-blob repository after reopen");
    let reopened_session = reopened
        .open_another_session()
        .await
        .expect("open shared-blob session after reopen");
    reopened_session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: branch_id.clone(),
        })
        .await
        .expect("switch reopened shared-blob branch");
    assert_eq!(
        read_file_at(&reopened_session, "/shared-b.bin").await,
        Some(OLD.to_vec())
    );
    let reopened_adapter = reopened.storage_adapter();
    assert_eq!(
        read_binary_cas_for_bench(&reopened_adapter, &old_hash)
            .await
            .expect("reopened old CAS lookup should succeed")
            .as_deref(),
        Some(OLD),
    );
    assert_eq!(
        read_binary_cas_for_bench(&reopened_adapter, &new_hash)
            .await
            .expect("reopened replacement CAS lookup should succeed")
            .as_deref(),
        Some(NEW),
    );
    reopened_session
        .execute("DELETE FROM lix_file WHERE path = '/shared-b.bin'", &[])
        .await
        .expect("delete final shared owner");
    assert_eq!(read_file_at(&reopened_session, "/shared-a.bin").await, None);
    assert_eq!(read_file_at(&reopened_session, "/shared-b.bin").await, None);
    drop(reopened_session);

    let main = reopened
        .open_another_session()
        .await
        .expect("open shared-blob main session");
    main.execute(
        "DELETE FROM lix_branch WHERE id = $1",
        &[Value::Text(branch_id)],
    )
    .await
    .expect("release shared-blob branch history");
    drop(main);

    let sweep = collect_repository_gc_for_bench(&reopened_adapter)
        .await
        .expect("collect after final owner and history release");
    assert_ne!(
        sweep.swept_commits, 0,
        "shared history release must sweep commits"
    );
    assert!(
        read_binary_cas_for_bench(&reopened_adapter, &old_hash)
            .await
            .expect("released old CAS lookup should succeed")
            .is_none(),
        "old shared content must reclaim after every current and historical owner is released",
    );
    assert!(
        read_binary_cas_for_bench(&reopened_adapter, &new_hash)
            .await
            .expect("released replacement CAS lookup should succeed")
            .is_none(),
        "replacement content must reclaim after its branch history is released",
    );
}

async fn read_file_at<S>(session: &lix::Lix<S>, path: &str) -> Option<Vec<u8>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT content FROM lix_file WHERE path = $1",
            &[Value::Text(path.to_owned())],
        )
        .await
        .expect("read shared blob owner")
        .rows()
        .first()
        .map(|row| row.get::<Vec<u8>>("content").expect("blob content"))
}

async fn prepare_current_untracked<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("untracked repository should initialize");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("untracked repository should open");
    let session = lix
        .open_another_session()
        .await
        .expect("untracked session should open");
    session
        .execute(
            "INSERT INTO lix_file (path, content, lixcol_untracked) \
             VALUES ('/current-untracked.bin', $1, true)",
            &[Value::Blob(b"durable-current-untracked".to_vec().into())],
        )
        .await
        .expect("current untracked file should publish");
    let adapter = lix.storage_adapter();
    let orphan_hash = write_binary_cas_for_bench(&adapter, b"durable-untracked-orphan")
        .await
        .expect("untracked unrelated orphan should stage");
    collect_repository_gc_for_bench(&adapter)
        .await
        .expect("untracked preserving sweep should commit");
    assert!(
        read_binary_cas_for_bench(&adapter, &orphan_hash)
            .await
            .expect("untracked orphan CAS lookup should succeed")
            .is_none(),
        "unrelated CAS garbage must reclaim while the current untracked root survives",
    );
    drop(session);
    drop(lix);
    drop(adapter);
    drop(storage);
}

async fn verify_current_untracked<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("untracked repository should cold reopen");
    let session = lix
        .open_another_session()
        .await
        .expect("cold untracked session should open");
    let content = session
        .execute(
            "SELECT content FROM lix_file WHERE path = '/current-untracked.bin'",
            &[],
        )
        .await
        .expect("cold current untracked file should read");
    assert_eq!(
        content.rows()[0].get::<Vec<u8>>("content").unwrap(),
        b"durable-current-untracked",
    );
}

async fn prepare_history<S>(storage: S) -> HistoryFixture
where
    S: Storage + Clone + Send + Sync + 'static,
{
    open_lix()
        .with_storage(storage.clone())
        .await
        .expect("history repository should initialize");
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("history repository should open");
    let session = lix
        .open_another_session()
        .await
        .expect("history preparation session should open");
    let branch = session
        .create_branch(lix::CreateBranchOptions {
            id: Some("01990000-0000-7000-8000-00000000000b".to_owned()),
            name: "cas-history-disposable".to_owned(),
            from_commit_id: None,
        })
        .await
        .expect("history disposable branch should create");
    let branch_id = branch.id;
    let branch_session = lix
        .open_another_session()
        .await
        .expect("history disposable branch should open");
    branch_session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (branch_id.clone()).to_string(),
        })
        .await
        .expect("switch session branch");
    branch_session
        .execute(
            "INSERT INTO lix_file (path, content) VALUES ('/history.bin', $1)",
            &[Value::Blob(V1.to_vec().into())],
        )
        .await
        .expect("history v1 should publish");
    let root_a = branch_commit(&branch_session, &branch_id).await;
    branch_session
        .execute(
            "UPDATE lix_file SET content = $1 WHERE path = '/history.bin'",
            &[Value::Blob(V2.to_vec().into())],
        )
        .await
        .expect("history v2 should publish");
    let root_b = branch_commit(&branch_session, &branch_id).await;
    let adapter = lix.storage_adapter();
    collect_repository_gc_for_bench(&adapter)
        .await
        .expect("history-preserving sweep should commit");
    let v1_hash = blake3::hash(V1).to_hex().to_string();
    let v2_hash = blake3::hash(V2).to_hex().to_string();
    assert_eq!(
        read_binary_cas_for_bench(&adapter, &v1_hash)
            .await
            .expect("v1 retained CAS read should succeed")
            .as_deref(),
        Some(V1),
    );
    drop(branch_session);
    drop(session);
    drop(lix);
    drop(adapter);
    drop(storage);
    HistoryFixture {
        branch_id,
        root_a,
        root_b,
        v1_hash,
        v2_hash,
    }
}

async fn verify_history_and_release<S>(storage: S, fixture: HistoryFixture)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage.clone())
        .await
        .expect("history repository should cold reopen");
    let session = lix
        .open_another_session()
        .await
        .expect("history disposable branch should cold reopen");
    session
        .switch_branch(lix::SwitchBranchOptions {
            branch_id: (fixture.branch_id.clone()).to_string(),
        })
        .await
        .expect("switch session branch");
    let diff = session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_diff('lix_file', $1, $2)",
            &[
                Value::Text(fixture.root_a.clone()),
                Value::Text(fixture.root_b.clone()),
            ],
        )
        .await
        .expect("retained historical blob diff should remain readable");
    assert_eq!(diff.rows()[0].get::<i64>("entries").unwrap(), 1);
    session
        .undo()
        .await
        .expect("retained historical blob should support undo");
    assert_eq!(read_current_file(&session).await, V1);
    session
        .redo()
        .await
        .expect("retained historical blob should support redo");
    assert_eq!(read_current_file(&session).await, V2);
    drop(session);

    let main = lix
        .open_another_session()
        .await
        .expect("history main session should reopen");
    main.execute(
        "DELETE FROM lix_branch WHERE id = $1",
        &[Value::Text(fixture.branch_id.clone())],
    )
    .await
    .expect("history disposable branch should delete");
    drop(main);

    let adapter = lix.storage_adapter();
    let sweep = collect_repository_gc_for_bench(&adapter)
        .await
        .expect("post-deletion sweep should consume B -> None");
    assert!(
        sweep.swept_commits != 0,
        "post-deletion sweep must consume the disposable branch frontier",
    );
    assert!(
        read_binary_cas_for_bench(&adapter, &fixture.v1_hash)
            .await
            .expect("released v1 CAS lookup should succeed")
            .is_none(),
        "v1 must reclaim after branch deletion releases every root for A",
    );
    assert!(
        read_binary_cas_for_bench(&adapter, &fixture.v2_hash)
            .await
            .expect("released v2 CAS lookup should succeed")
            .is_none(),
        "v2 must reclaim with its deleted branch",
    );
    drop(lix);
    drop(adapter);
    drop(storage);
}

async fn verify_final_state<S>(storage: S)
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let lix = open_lix()
        .with_storage(storage)
        .await
        .expect("final history repository should reopen");
    let session = lix
        .open_another_session()
        .await
        .expect("final history session should open");
    let branches = session
        .execute(
            "SELECT COUNT(*) AS entries FROM lix_branch WHERE name = 'cas-history-disposable'",
            &[],
        )
        .await
        .expect("retired branch absence should read");
    assert_eq!(branches.rows()[0].get::<i64>("entries").unwrap(), 0);
}

async fn branch_commit<S>(session: &lix::Lix<S>, branch_id: &str) -> String
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT commit_id FROM lix_branch WHERE id = $1",
            &[Value::Text(branch_id.to_owned())],
        )
        .await
        .expect("branch commit should load")
        .rows()[0]
        .get::<String>("commit_id")
        .expect("branch commit should exist")
}

async fn read_current_file<S>(session: &lix::Lix<S>) -> Vec<u8>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    session
        .execute(
            "SELECT content FROM lix_file WHERE path = '/history.bin'",
            &[],
        )
        .await
        .expect("current history file should read")
        .rows()[0]
        .get::<Vec<u8>>("content")
        .expect("current history content should exist")
}
