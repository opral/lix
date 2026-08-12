use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use lix::CreateBranchOptions;
use lix::integration::{Engine, SessionContext};
use lix::storage::{
    BeginScanOptions, CommitResult, GetManyRequest, GetManyResult, Key, KeyRange, Memory,
    MemoryRead, MemoryWrite, PutBatch, ReadOptions, ScanCursor, SpaceId, Storage, StorageError,
    StorageRead, StorageWrite, WriteOptions,
};

const TEST_WAIT_TIMEOUT: Duration = Duration::from_secs(2);
const UNTRACKED_RACE_BRANCH_ID: &str = "01930000-0000-7000-8000-000000000018";

async fn setup_untracked_race_branch<StorageImpl>(engine: &Engine<StorageImpl>)
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let setup = engine
        .open_session()
        .await
        .expect("setup session should open");
    setup
        .execute(
            r#"INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
               VALUES (lix_json('{"x-lix-key":"untracked_race_parent","x-lix-primary-key":["/id"],"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}'), false, false)"#,
            &[],
        )
        .await
        .expect("parent schema should register");
    setup
        .execute(
            r#"INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
               VALUES (lix_json('{"x-lix-key":"untracked_race_child","x-lix-primary-key":["/id"],"x-lix-foreign-keys":[{"properties":["/parent_id"],"references":{"schemaKey":"untracked_race_parent","properties":["/id"]}}],"type":"object","properties":{"id":{"type":"string"},"parent_id":{"type":"string"}},"required":["id","parent_id"],"additionalProperties":false}'), false, false)"#,
            &[],
        )
        .await
        .expect("child schema should register");
    setup
        .create_branch(CreateBranchOptions {
            id: Some(UNTRACKED_RACE_BRANCH_ID.to_string()),
            name: "Untracked race".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("test branch should be created");
}

#[tokio::test]
async fn stale_transaction_composes_disjoint_semantic_writes() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let stale_session = engine
        .open_session()
        .await
        .expect("stale session should open");
    let winner_session = engine
        .open_session()
        .await
        .expect("winner session should open");

    let mut stale = stale_session
        .begin_transaction()
        .await
        .expect("stale transaction should begin");
    stale
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('stale-key', 'stale')",
            &[],
        )
        .await
        .expect("stale transaction should stage its write");

    winner_session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('winner-key', 'winner')",
            &[],
        )
        .await
        .expect("newer transaction should commit");

    stale
        .commit()
        .await
        .expect("disjoint stale transaction should compose onto the current head");

    let result = winner_session
        .execute(
            "SELECT key FROM lix_key_value \
             WHERE key IN ('stale-key', 'winner-key') ORDER BY key",
            &[],
        )
        .await
        .expect("committed state should remain readable");
    assert_eq!(result.rows().len(), 2);
    assert_eq!(result.rows()[0].get::<String>("key").unwrap(), "stale-key");
    assert_eq!(result.rows()[1].get::<String>("key").unwrap(), "winner-key");
}

/// Media ingest and an unrelated project-file save are independent writers:
/// they touch disjoint files, disjoint payload rows, and disjoint branch state.
/// Neither may be rejected because the other committed first.
///
/// This is the shape the ignored `slatedb_movie_workspace_interference`
/// qualification exercises with a 1 TiB corpus, reduced to the one interleaving
/// that matters. Resumable upload parts commit straight to storage instead of
/// through the collaboration write gate, so an ingest part lands squarely inside
/// an ordinary commit's precondition window: between the save's commit-time
/// snapshot and the save's storage write. The gate below parks the save exactly
/// there rather than leaving the interleaving to the scheduler.
#[tokio::test]
async fn committed_media_ingest_part_does_not_invalidate_a_concurrent_project_save() {
    let storage = InterferingStorage::new();
    let gate = storage.gate();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let ingest = engine
        .open_session()
        .await
        .expect("media ingest session should open");
    let saver = engine
        .open_session()
        .await
        .expect("project-save session should open");
    saver
        .upsert_file_content(
            "/project/edit.json".to_owned(),
            br#"{"timelineRevision":0}"#.to_vec().into(),
        )
        .await
        .expect("seed project file should save");

    gate.arm();
    let save_future = async {
        saver
            .upsert_file_content(
                "/project/edit.json".to_owned(),
                br#"{"timelineRevision":1}"#.to_vec().into(),
            )
            .await
    };
    let ingest_future = async {
        // Only start once the save is parked on its storage write, so this
        // publisher is guaranteed to commit inside the save's precondition
        // window. One part of a two-part upload: the upload does not finalize,
        // so this path never needs the collaboration write gate the save holds.
        gate.wait_until_a_write_is_parked().await;
        let progress = ingest
            .upsert_file_content_part(
                "concurrent-ingest".to_owned(),
                "/media/import.mov".to_owned(),
                0,
                2 * lix::FILE_UPLOAD_PART_BYTES as u64,
                vec![0x5a; lix::FILE_UPLOAD_PART_BYTES].into(),
            )
            .await;
        gate.release_parked_write();
        progress
    };
    let (save_result, ingest_result) =
        tokio::time::timeout(TEST_WAIT_TIMEOUT * 15, async move {
            tokio::join!(save_future, ingest_future)
        })
        .await
        .expect("interfering writers should not deadlock");

    ingest_result.expect("media ingest part should commit");
    save_result
        .expect("a committed media ingest part must not invalidate a concurrent project save");

    let saved = saver
        .read_file_content("/project/edit.json".to_owned(), None)
        .await
        .expect("saved project file should read")
        .expect("saved project file should exist");
    assert_eq!(saved.content().as_ref(), br#"{"timelineRevision":1}"#);
}

#[tokio::test]
async fn stale_transaction_reports_overlapping_ordinary_insert_atomically() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let stale_session = engine
        .open_session()
        .await
        .expect("stale session should open");
    let winner_session = engine
        .open_session()
        .await
        .expect("winner session should open");

    let mut stale = stale_session
        .begin_transaction()
        .await
        .expect("stale transaction should begin");
    stale
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('same-key', 'stale')",
            &[],
        )
        .await
        .expect("stale transaction should stage its write");
    winner_session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('same-key', 'winner')",
            &[],
        )
        .await
        .expect("winner transaction should commit");

    let error = stale
        .commit()
        .await
        .expect_err("overlapping ordinary rows must remain conservative");
    assert_eq!(error.code, "LIX_ERROR_UNIQUE");
    let result = winner_session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'same-key'",
            &[],
        )
        .await
        .expect("winner state should remain readable");
    assert_eq!(result.rows().len(), 1);
    assert_eq!(
        result.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("winner")
    );
}

#[tokio::test]
async fn explicit_transaction_reads_stable_snapshot_and_own_writes() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage).await.expect("engine should open");
    let transaction_session = engine.open_session().await.unwrap();
    let concurrent_session = engine.open_session().await.unwrap();
    concurrent_session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
             VALUES ('snapshot-key', 'base', false)",
            &[],
        )
        .await
        .unwrap();

    let mut transaction = transaction_session.begin_transaction().await.unwrap();
    let opening = transaction
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'snapshot-key'",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        opening.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("base")
    );
    concurrent_session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_untracked) \
             VALUES ('snapshot-key', 'concurrent', false) \
             ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            &[],
        )
        .await
        .unwrap();

    let stable = transaction
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'snapshot-key'",
            &[],
        )
        .await
        .expect("concurrent commits must not invalidate transaction reads");
    assert_eq!(
        stable.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("base")
    );
    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('own-key', 'own')",
            &[],
        )
        .await
        .unwrap();
    let own = transaction
        .execute("SELECT value FROM lix_key_value WHERE key = 'own-key'", &[])
        .await
        .unwrap();
    assert_eq!(
        own.rows()[0].get::<serde_json::Value>("value").unwrap(),
        serde_json::json!("own")
    );
    transaction.rollback().await.unwrap();
}

simulation_test!(
    explicit_transaction_collection_delete_is_visible_and_terminates,
    |sim| async move {
        let engine = sim.boot_engine().await;
        let session = sim.wrap_session(
            engine
                .open_session()
                .await
                .expect("session should open"),
            &engine,
        );
        session
            .execute(
                r#"INSERT INTO lix_registered_schema (value, lixcol_global, lixcol_untracked)
                   VALUES (lix_json('{"x-lix-key":"transaction_collection_delete","x-lix-primary-key":["/id"],"type":"object","properties":{"id":{"type":"string"}},"required":["id"],"additionalProperties":false}'), false, false)"#,
                &[],
            )
            .await
            .expect("collection schema should register");
        session
            .execute(
                "INSERT INTO transaction_collection_delete (id) VALUES ('a'), ('b')",
                &[],
            )
            .await
            .expect("collection members should seed");

        let mut transaction = session
            .begin_transaction()
            .await
            .expect("explicit transaction should begin");
        let deleted = transaction
            .execute("DELETE FROM transaction_collection_delete", &[])
            .await
            .expect("whole collection delete should terminate");
        assert_eq!(deleted.rows_affected(), 2);
        let visible = transaction
            .execute("SELECT id FROM transaction_collection_delete", &[])
            .await
            .expect("transaction should read its staged collection delete");
        assert!(visible.rows().is_empty());
        let deleted_again = transaction
            .execute("DELETE FROM transaction_collection_delete", &[])
            .await
            .expect("repeated whole collection delete should terminate");
        assert_eq!(deleted_again.rows_affected(), 0);
        transaction
            .commit()
            .await
            .expect("whole collection delete should commit");

        let committed = session
            .execute("SELECT id FROM transaction_collection_delete", &[])
            .await
            .expect("committed collection delete should remain visible");
        assert!(committed.rows().is_empty());
    }
);

#[tokio::test]
async fn deferred_active_branch_check_rejects_deleted_branch_at_commit() {
    let storage = Memory::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");

    let setup_engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create setup engine");
    let setup_session = setup_engine
        .open_session()
        .await
        .expect("setup session should open");
    setup_session
        .create_branch(CreateBranchOptions {
            id: Some("01930000-0000-7000-8000-000000000017".to_string()),
            name: "Deferred branch".to_string(),
            from_commit_id: None,
        })
        .await
        .expect("local branch should be created");

    // Separate engines share storage but not a collaboration gate, so the
    // branch can disappear after the transaction opens and before it commits.
    let branch_engine = Engine::new(storage.clone())
        .await
        .expect("branch engine should open");
    let delete_engine = Engine::new(storage)
        .await
        .expect("delete engine should open");
    let branch_session = branch_engine
        .open_session_at("01930000-0000-7000-8000-000000000017")
        .await
        .expect("local branch session should open");
    let delete_session = delete_engine
        .open_session()
        .await
        .expect("delete session should open");

    let mut transaction = branch_session
        .begin_transaction()
        .await
        .expect("local transaction should begin");
    delete_session
        .execute(
            "DELETE FROM lix_branch WHERE id = '01930000-0000-7000-8000-000000000017'",
            &[],
        )
        .await
        .expect("branch delete should commit");

    transaction
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('64656665-7272-8564-8d62-72616e636801', 'value')",
            &[],
        )
        .await
        .expect("normal active-branch write should stage without a branch-head probe");
    let error = transaction
        .commit()
        .await
        .expect_err("commit must reject a branch deleted after transaction open");
    assert_eq!(error.code, "LIX_BRANCH_NOT_FOUND");
    assert_eq!(
        delete_engine
            .load_branch_head_commit_id("01930000-0000-7000-8000-000000000017")
            .await
            .expect("branch head lookup should succeed"),
        None,
        "the failed transaction must not resurrect the deleted branch"
    );
}

/// An untracked write may validate a foreign key against tracked state. It must
/// therefore retry when that tracked state changes before its storage commit.
#[tokio::test]
async fn untracked_commit_retries_when_tracked_fk_target_changes_after_validation() {
    let storage = BlockingCommitStorage::new();
    Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");

    let setup_engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create a setup engine");
    setup_untracked_race_branch(&setup_engine).await;

    // Separate engines avoid the per-engine collaboration gate so this test
    // exercises exactly the storage-level optimistic-concurrency boundary.
    let tracked_engine = Engine::new(storage.clone())
        .await
        .expect("tracked engine should open");
    let untracked_engine = Engine::new(storage.clone())
        .await
        .expect("untracked engine should open");
    let tracked_session = tracked_engine
        .open_session_at(UNTRACKED_RACE_BRANCH_ID)
        .await
        .expect("tracked session should open");
    let untracked_session = untracked_engine
        .open_session_at(UNTRACKED_RACE_BRANCH_ID)
        .await
        .expect("untracked session should open");

    tracked_session
        .execute(
            "INSERT INTO untracked_race_parent (id) VALUES ('parent-1')",
            &[],
        )
        .await
        .expect("tracked parent should seed");

    let mut untracked_insert = untracked_session
        .begin_transaction()
        .await
        .expect("untracked transaction should begin");
    untracked_insert
        .execute(
            "INSERT INTO untracked_race_child (id, parent_id, lixcol_untracked) VALUES ('child-1', 'parent-1', true)",
            &[],
        )
        .await
        .expect("untracked child should stage");

    let gate = storage.gate();
    gate.block_next_write();
    let untracked_commit = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { untracked_insert.commit().await })
    });
    gate.wait_until_blocked();

    let tracked_delete_result = tracked_session
        .execute(
            "DELETE FROM untracked_race_parent WHERE id = 'parent-1'",
            &[],
        )
        .await;
    gate.release();
    tracked_delete_result.expect("tracked delete should commit while untracked write is blocked");
    let error = join_thread(untracked_commit, "blocked untracked foreign-key insert")
        .expect_err("untracked write validated against stale tracked state must retry");
    assert_eq!(error.code, "LIX_TRANSACTION_CONFLICT");

    let retry_error = untracked_session
        .execute(
            "INSERT INTO untracked_race_child (id, parent_id, lixcol_untracked) VALUES ('child-1', 'parent-1', true)",
            &[],
        )
        .await
        .expect_err("fresh untracked insert must observe that its tracked FK target was deleted");
    assert_eq!(retry_error.code, "LIX_ERROR_FOREIGN_KEY");
}

fn wait_until(description: &str, mut condition: impl FnMut() -> bool) {
    let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
    while !condition() {
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        thread::yield_now();
    }
}

fn join_thread<T>(handle: thread::JoinHandle<T>, description: &str) -> T {
    wait_until(description, || handle.is_finished());
    handle
        .join()
        .unwrap_or_else(|_| panic!("{description} panicked"))
}

#[tokio::test]
async fn existing_session_ignores_later_default_branch_corruption() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    session
        .execute(
            "UPDATE lix_key_value_by_branch \
             SET value = '6d697373-696e-872d-8272-616e63680000' \
             WHERE key = 'lix_default_branch_id' \
               AND lixcol_branch_id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
            &[],
        )
        .await
        .expect("test should corrupt the repository default branch");

    let before = storage.stats();
    session
        .execute("SELECT 1", &[])
        .await
        .expect("an existing session should retain its valid pinned branch");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.read_opened, 1, "read SQL should open one read tx");
    assert_eq!(delta.write_opened, 0, "read SQL must not open writes");
    engine
        .open_session()
        .await
        .err()
        .expect("a new session should reject the corrupt repository default");
}

#[tokio::test]
async fn explicit_transaction_open_uses_one_authoritative_snapshot() {
    let storage = RecordingStorage::new();
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    let before = storage.stats();
    let transaction = session
        .begin_transaction()
        .await
        .expect("transaction should open");
    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.read_opened, 1, "session open must own one snapshot");
    assert_eq!(delta.write_opened, 0, "transaction open must not write");
    transaction
        .rollback()
        .await
        .expect("transaction should roll back");

    let pinned = engine
        .open_session_at(receipt.main_branch_id)
        .await
        .expect("pinned session should open");
    let before = storage.stats();
    let pinned_transaction = pinned
        .begin_transaction()
        .await
        .expect("pinned transaction should open");
    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.read_opened, 1, "pinned open must own one snapshot");
    assert_eq!(delta.write_opened, 0, "transaction open must not write");
    pinned_transaction
        .rollback()
        .await
        .expect("pinned transaction should roll back");
}

#[tokio::test]
async fn existing_session_writes_to_its_selected_branch() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    session
        .execute(
            "UPDATE lix_key_value_by_branch \
             SET value = '6d697373-696e-872d-8272-616e63680000' \
             WHERE key = 'lix_default_branch_id' \
               AND lixcol_branch_id = 'ffffffff-ffff-7fff-bfff-ffffffffffff'",
            &[],
        )
        .await
        .expect("test should corrupt the repository default branch");

    let before = storage.stats();
    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('after-corrupt-selector', 'value')",
            &[],
        )
        .await
        .expect("an existing session should write to its valid pinned branch");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 1,
        "the pinned-session write should open one storage write"
    );
    assert_eq!(
        delta.write_committed, 1,
        "the pinned-session write should commit"
    );
    engine
        .open_session()
        .await
        .err()
        .expect("a new session should reject the corrupt repository default");
}

#[tokio::test]
async fn rebuild_tracked_state_does_not_commit_on_read_failure() {
    let storage = RecordingStorage::new();
    let receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");

    storage.fail_read_space(CHANGELOG_COMMIT_SPACE_ID);
    let before = storage.stats();
    let error = engine
        .rebuild_tracked_state_for_branch(&receipt.main_branch_id)
        .await
        .expect_err("forced changelog read failure should fail rebuild");
    assert!(
        error.message.contains("forced read failure"),
        "unexpected error: {error:?}"
    );

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 0,
        "failed rebuild should not open a storage write"
    );
    assert_eq!(delta.write_committed, 0, "failed rebuild must not commit");
}

#[tokio::test]
async fn write_changelog_commit_failure_does_not_commit_storage_write() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    storage.fail_write_space(CHANGELOG_COMMIT_SPACE_ID);
    let before = storage.stats();
    let error = session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('changelog-commit-write-failure', 'value')",
            &[],
        )
        .await
        .expect_err("forced changelog commit write failure should fail transaction commit");
    assert!(
        error.message.contains("forced write failure"),
        "unexpected error: {error:?}"
    );

    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.write_opened, 1, "write should open a storage write");
    assert_eq!(
        delta.write_committed, 0,
        "failed changelog commit write must not commit"
    );
}

#[tokio::test]
async fn active_transaction_blocks_session_read_and_allows_transaction_read() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    session
        .execute(
            "INSERT INTO lix_key_value (key, value, lixcol_global, lixcol_untracked) \
             VALUES ('lix_deterministic_mode', \
             lix_json('{\"enabled\":true}'), true, true)",
            &[],
        )
        .await
        .expect("deterministic mode insert should succeed");

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");

    let error = session
        .execute("SELECT lix_uuid_v7()", &[])
        .await
        .expect_err("session read should be blocked while transaction is active");
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    let result = tx
        .execute("SELECT lix_uuid_v7()", &[])
        .await
        .expect("deterministic transaction read should succeed");
    assert_eq!(
        result
            .rows()
            .first()
            .expect("read should return a row")
            .get::<String>("lix_uuid_v7()")
            .expect("uuid should be returned as text"),
        "01920000-0000-7000-8000-000000000000",
    );

    tx.rollback()
        .await
        .expect("transaction rollback should succeed");
    tokio::time::timeout(TEST_WAIT_TIMEOUT, session.close())
        .await
        .expect("timed out closing after active transaction rejection")
        .expect("session close should succeed after rollback");
}

#[tokio::test]
async fn transaction_read_can_query_history_surfaces() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    session
        .execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('history-visible-in-tx', 'value')",
            &[],
        )
        .await
        .expect("seed write should succeed");

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    let result = tx
        .execute("SELECT key FROM lix_key_value_history()", &[])
        .await
        .expect("transaction read should register history surfaces");

    assert!(
        !result.rows().is_empty(),
        "transaction history read should see committed history rows"
    );

    tx.rollback()
        .await
        .expect("transaction rollback should succeed");
}

#[tokio::test]
async fn close_rejects_idle_explicit_transaction_without_dropping_it() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('closed-session-tx', 'value')",
        &[],
    )
    .await
    .expect("staging before close should succeed");

    let close_error = session
        .close()
        .await
        .expect_err("close should reject an idle explicit transaction");
    assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");

    let result = tx
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'closed-session-tx'",
            &[],
        )
        .await
        .expect("rejected close should leave the transaction usable");
    assert_eq!(result.len(), 1);

    tx.rollback()
        .await
        .expect("transaction rollback should succeed after rejected close");

    let reopened = engine
        .open_session()
        .await
        .expect("new session should open after closing previous session");
    let result = reopened
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'closed-session-tx'",
            &[],
        )
        .await
        .expect("read through reopened session should succeed");
    assert_eq!(
        result.len(),
        0,
        "rolled-back transaction rows must not commit"
    );
}

#[tokio::test]
async fn closed_session_still_allows_active_transaction_rollback() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    let tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    let close_error = session
        .close()
        .await
        .expect_err("close should reject an idle explicit transaction");
    assert_eq!(close_error.code, "LIX_INVALID_TRANSACTION_STATE");

    tx.rollback()
        .await
        .expect("rollback should remain available after rejected close");
    session
        .close()
        .await
        .expect("session close should succeed after rollback");
}

#[tokio::test]
async fn closed_session_active_branch_id_does_not_open_storage_read() {
    let storage = RecordingStorage::new();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = engine
        .open_session()
        .await
        .expect("session should open");

    session.close().await.expect("session close should succeed");
    let before = storage.stats();
    let error = session
        .active_branch_id()
        .await
        .expect_err("active_branch_id should reject a closed session");
    assert_eq!(error.code, lix::LixError::CODE_CLOSED);

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.read_opened, 0,
        "closed active_branch_id must reject before storage IO"
    );
}

#[tokio::test]
async fn close_during_transaction_open_rejects_opened_transaction() {
    let storage = BlockingBeginReadStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    gate.block_next_write();
    let opener_session = Arc::clone(&session);
    let opener = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { opener_session.begin_transaction().await })
    });

    gate.wait_until_blocked();
    let closer_session = Arc::clone(&session);
    let closer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { closer_session.close().await })
    });
    thread::sleep(Duration::from_millis(20));
    assert!(
        !closer.is_finished(),
        "close should wait for blocked transaction open to unwind"
    );

    gate.release();
    let Err(open_error) = join_thread(opener, "blocked transaction opener") else {
        panic!("transaction open that loses the close race should fail");
    };
    assert_eq!(open_error.code, lix::LixError::CODE_CLOSED);
    join_thread(closer, "close after blocked transaction opener")
        .expect("session close should succeed");
}

#[tokio::test]
async fn close_during_transaction_commit_waits_after_commit_boundary() {
    let storage = BlockingBeginReadStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('close-during-commit', 'value')",
        &[],
    )
    .await
    .expect("staging before close should succeed");

    gate.block_next_write();
    let before = storage.stats();
    let committer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { tx.commit().await })
    });

    gate.wait_until_blocked();
    let close_session = Arc::clone(&session);
    let closer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { close_session.close().await })
    });
    assert!(
        !closer.is_finished(),
        "close should wait for the blocked commit boundary to exit"
    );
    gate.release();
    join_thread(committer, "committer waiting after commit boundary")
        .expect("commit past the boundary should finish before close");
    join_thread(closer, "close while commit waits after commit boundary")
        .expect("session close should succeed after commit exits");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(
        delta.write_opened, 1,
        "commit preparation should open a storage write"
    );
    assert_eq!(
        delta.write_committed, 1,
        "commit past the boundary should commit storage writes"
    );
    assert_eq!(
        delta.write_rolled_back, 0,
        "commit past the boundary should not roll back storage writes"
    );
}

#[tokio::test]
async fn close_waits_for_transaction_blocked_in_storage_commit() {
    let storage = BlockingCommitStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage.clone())
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    let mut tx = session
        .begin_transaction()
        .await
        .expect("transaction should begin");
    tx.execute(
        "INSERT INTO lix_key_value (key, value) VALUES ('blocked-storage-commit', 'value')",
        &[],
    )
    .await
    .expect("staging before blocked commit should succeed");

    gate.block_next_write();
    let before = storage.stats();
    let committer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { tx.commit().await })
    });

    gate.wait_until_blocked();
    let closer = spawn_close_waiter(Arc::clone(&session));
    wait_until("close to wait on blocked storage commit", || {
        !closer.is_finished()
    });
    assert!(
        !closer.is_finished(),
        "close should wait for storage commit to unblock"
    );

    gate.release();
    join_thread(committer, "committer blocked in storage commit")
        .expect("commit at storage commit boundary should finish");
    join_thread(closer, "close after storage commit unblocks")
        .expect("session close should succeed after storage commit exits");

    let delta = storage.stats().delta_since(&before);
    assert_eq!(delta.write_opened, 1, "commit should open a storage write");
    assert_eq!(
        delta.write_committed, 1,
        "blocked storage commit should eventually commit"
    );
}

fn spawn_close_waiter<StorageImpl>(
    session: Arc<SessionContext<StorageImpl>>,
) -> thread::JoinHandle<Result<(), lix::LixError>>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move { session.close().await })
    })
}

#[tokio::test]
async fn begin_transaction_cannot_race_with_opening_session_write() {
    let storage = BlockingBeginWriteStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    gate.block_next_write();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            writer_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ('racing-session-write', 'value')",
                    &[],
                )
                .await
        })
    });

    gate.wait_until_blocked();
    let Err(error) = session.begin_transaction().await else {
        panic!("explicit transaction should not race past a session write reservation");
    };
    assert_eq!(error.code, "LIX_INVALID_TRANSACTION_STATE");

    gate.release();
    join_thread(writer, "session writer racing transaction open")
        .expect("session write should complete after release");

    let result = session
        .execute(
            "SELECT key FROM lix_key_value WHERE key = 'racing-session-write'",
            &[],
        )
        .await
        .expect("session write should be committed");
    assert_eq!(result.len(), 1);
    tokio::time::timeout(TEST_WAIT_TIMEOUT, session.close())
        .await
        .expect("timed out closing after transaction reservation rejection")
        .expect("session close should succeed after reservation rejection");
}

#[tokio::test]
async fn session_read_waits_for_automatic_write_instead_of_rejecting() {
    let storage = BlockingBeginWriteStorage::new();
    let gate = storage.gate();
    let _receipt = Engine::initialize(storage.clone())
        .await
        .expect("storage should initialize");
    let engine = Engine::new(storage)
        .await
        .expect("initialized storage should create an engine");
    let session = Arc::new(
        engine
            .open_session()
            .await
            .expect("session should open"),
    );

    gate.block_next_write();
    let writer_session = Arc::clone(&session);
    let writer = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            writer_session
                .execute(
                    "INSERT INTO lix_key_value (key, value) VALUES ('read-after-automatic-write', 'value')",
                    &[],
                )
                .await
        })
    });
    gate.wait_until_blocked();

    let reader_session = Arc::clone(&session);
    let reader = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("test runtime should build");
        runtime.block_on(async move {
            reader_session
                .execute(
                    "SELECT key FROM lix_key_value WHERE key = 'read-after-automatic-write'",
                    &[],
                )
                .await
        })
    });

    gate.release();
    join_thread(writer, "blocked automatic writer")
        .expect("automatic write should finish after release");
    let result = join_thread(reader, "reader waiting for automatic write")
        .expect("session read should wait behind automatic write");
    assert_eq!(result.len(), 1);
}

/// In-memory storage that can park one storage write and hand control to
/// another writer.
///
/// Commit-path concurrency defects live in the window between a transaction's
/// commit-time snapshot and its storage write. Sampling that window by racing
/// two futures is unreliable — on a current-thread runtime the phase
/// relationship between them is fixed, and the interleaving that matters may
/// never occur. Parking the first write after [`InterferenceGate::arm`] and
/// resuming it only after the other writer has committed makes the interleaving
/// a property of the test rather than of the scheduler.
#[derive(Clone)]
struct InterferingStorage {
    inner: Memory,
    gate: Arc<InterferenceGate>,
}

impl InterferingStorage {
    fn new() -> Self {
        Self {
            inner: Memory::new(),
            gate: Arc::new(InterferenceGate::new()),
        }
    }

    fn gate(&self) -> Arc<InterferenceGate> {
        Arc::clone(&self.gate)
    }
}

impl Storage for InterferingStorage {
    type Read<'a>
        = MemoryRead
    where
        Self: 'a;

    type Write<'a>
        = MemoryWrite
    where
        Self: 'a;

    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.gate.park_if_armed().await;
        self.inner.begin_write(opts).await
    }
}

struct InterferenceGate {
    armed: std::sync::atomic::AtomicBool,
    parked: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}

impl InterferenceGate {
    fn new() -> Self {
        Self {
            armed: std::sync::atomic::AtomicBool::new(false),
            parked: tokio::sync::Semaphore::new(0),
            release: tokio::sync::Semaphore::new(0),
        }
    }

    fn arm(&self) {
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Parks the next storage write only. Disarming before announcing means the
    /// writer woken by `wait_until_a_write_is_parked` is never itself parked.
    async fn park_if_armed(&self) {
        if self.armed.swap(false, Ordering::SeqCst) {
            self.parked.add_permits(1);
            self.release
                .acquire()
                .await
                .expect("interference gate should stay open")
                .forget();
        }
    }

    async fn wait_until_a_write_is_parked(&self) {
        self.parked
            .acquire()
            .await
            .expect("interference gate should stay open")
            .forget();
    }

    fn release_parked_write(&self) {
        self.release.add_permits(1);
    }
}

/// The changelog commit space, identified by space id.
///
/// This fault injector used to be armed with a space *name*, resolved through a
/// hand-written `namespace_space()` match from name to `SpaceId`. That match was
/// a second authority for the engine registry and it failed **open**: an
/// unrecognised name returned `None`, the injector never fired, and the test
/// went green having injected no fault at all. A space name carries its record
/// encoding version and is expected to churn, so that was a live way to turn a
/// fault-injection test into a no-op silently.
///
/// The id is the durable identity — the first four bytes of every physical key,
/// which a routine encoding bump does not move. The declaration this pins to is
/// `lix::registered_spaces::COMMIT_SPACE`, `pub` only under `storage-benches`;
/// this test target has no `required-features`, so it builds without that
/// feature and the id is restated here rather than imported.
const CHANGELOG_COMMIT_SPACE_ID: SpaceId = SpaceId(0x0006_0001);

#[derive(Clone, Default)]
struct RecordingStorage {
    inner: Memory,
    stats: Arc<TransactionStats>,
    fail_read_space: Arc<Mutex<Option<SpaceId>>>,
    fail_write_space: Arc<Mutex<Option<SpaceId>>>,
}

#[derive(Clone)]
struct BlockingBeginWriteStorage {
    inner: RecordingStorage,
    gate: BlockingBeginWriteGate,
}

#[derive(Clone)]
struct BlockingBeginReadStorage {
    inner: RecordingStorage,
    gate: BlockingBeginWriteGate,
}

#[derive(Clone)]
struct BlockingCommitStorage {
    inner: RecordingStorage,
    gate: BlockingBeginWriteGate,
}

impl BlockingBeginWriteStorage {
    fn new() -> Self {
        Self {
            inner: RecordingStorage::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }
}

impl BlockingBeginReadStorage {
    fn new() -> Self {
        Self {
            inner: RecordingStorage::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.inner.stats()
    }
}

impl BlockingCommitStorage {
    fn new() -> Self {
        Self {
            inner: RecordingStorage::new(),
            gate: BlockingBeginWriteGate::new(),
        }
    }

    fn gate(&self) -> BlockingBeginWriteGate {
        self.gate.clone()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.inner.stats()
    }
}

impl Storage for BlockingBeginWriteStorage {
    type Read<'a>
        = <RecordingStorage as Storage>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = <RecordingStorage as Storage>::Write<'a>
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.gate.maybe_block();
        self.inner.begin_write(opts).await
    }
}

impl Storage for BlockingBeginReadStorage {
    type Read<'a>
        = <RecordingStorage as Storage>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = <RecordingStorage as Storage>::Write<'a>
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.gate.maybe_block();
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.inner.begin_write(opts).await
    }
}

impl Storage for BlockingCommitStorage {
    type Read<'a>
        = <RecordingStorage as Storage>::Read<'a>
    where
        Self: 'a;

    type Write<'a>
        = BlockingCommitWrite
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.inner.begin_read(opts).await
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        Ok(BlockingCommitWrite {
            inner: self.inner.begin_write(opts).await?,
            gate: self.gate.clone(),
        })
    }
}

struct BlockingCommitWrite {
    inner: RecordingWrite,
    gate: BlockingBeginWriteGate,
}

impl StorageWrite for BlockingCommitWrite {
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(
        &mut self,
        space: lix::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.gate.maybe_block();
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.inner.rollback().await
    }
}

#[derive(Clone)]
struct BlockingBeginWriteGate {
    state: Arc<(Mutex<BlockingBeginWriteState>, Condvar)>,
}

impl BlockingBeginWriteGate {
    fn new() -> Self {
        Self {
            state: Arc::new((
                Mutex::new(BlockingBeginWriteState::default()),
                Condvar::new(),
            )),
        }
    }

    fn block_next_write(&self) {
        let (lock, _) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        state.block_next = true;
        state.blocked = false;
        state.released = false;
    }

    fn maybe_block(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        if !state.block_next {
            return;
        }
        state.block_next = false;
        state.blocked = true;
        condvar.notify_all();
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        while !state.released {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(
                !remaining.is_zero(),
                "timed out waiting for blocking gate release"
            );
            let (next_state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .expect("blocking gate lock should be available after wait");
            state = next_state;
            assert!(
                !wait_result.timed_out() || state.released,
                "timed out waiting for blocking gate release"
            );
        }
    }

    fn wait_until_blocked(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        let deadline = Instant::now() + TEST_WAIT_TIMEOUT;
        while !state.blocked {
            let remaining = deadline.saturating_duration_since(Instant::now());
            assert!(!remaining.is_zero(), "timed out waiting for blocking gate");
            let (next_state, wait_result) = condvar
                .wait_timeout(state, remaining)
                .expect("blocking gate lock should be available after wait");
            state = next_state;
            assert!(
                !wait_result.timed_out() || state.blocked,
                "timed out waiting for blocking gate"
            );
        }
    }

    fn release(&self) {
        let (lock, condvar) = &*self.state;
        let mut state = lock.lock().expect("blocking gate lock should be available");
        state.released = true;
        condvar.notify_all();
    }
}

#[derive(Default)]
struct BlockingBeginWriteState {
    block_next: bool,
    blocked: bool,
    released: bool,
}

impl RecordingStorage {
    fn new() -> Self {
        Self::default()
    }

    fn stats(&self) -> TransactionStatsSnapshot {
        self.stats.snapshot()
    }

    fn fail_read_space(&self, space: SpaceId) {
        *self
            .fail_read_space
            .lock()
            .expect("fail space lock should not poison") = Some(space);
    }

    fn fail_write_space(&self, space: SpaceId) {
        *self
            .fail_write_space
            .lock()
            .expect("fail space lock should not poison") = Some(space);
    }
}

impl Storage for RecordingStorage {
    type Read<'a>
        = RecordingRead
    where
        Self: 'a;

    type Write<'a>
        = RecordingWrite
    where
        Self: 'a;
    async fn begin_read(&self, opts: ReadOptions) -> Result<Self::Read<'_>, StorageError> {
        self.stats.read_opened.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingRead {
            inner: self.inner.begin_read(opts).await?,
            fail_read_space: Arc::clone(&self.fail_read_space),
        })
    }

    async fn begin_write(&self, opts: WriteOptions) -> Result<Self::Write<'_>, StorageError> {
        self.stats.write_opened.fetch_add(1, Ordering::SeqCst);
        Ok(RecordingWrite {
            inner: self.inner.begin_write(opts).await?,
            stats: Arc::clone(&self.stats),
            fail_write_space: Arc::clone(&self.fail_write_space),
        })
    }
}

#[derive(Clone)]
struct RecordingRead {
    inner: MemoryRead,
    fail_read_space: Arc<Mutex<Option<SpaceId>>>,
}

struct RecordingWrite {
    inner: MemoryWrite,
    stats: Arc<TransactionStats>,
    fail_write_space: Arc<Mutex<Option<SpaceId>>>,
}

impl StorageRead for RecordingRead {
    async fn get_many(
        &self,
        requests: &[GetManyRequest<'_>],
    ) -> Result<GetManyResult, StorageError> {
        for request in requests {
            self.fail_if_space_matches(request.space)?;
        }
        self.inner.get_many(requests).await
    }

    async fn begin_scan(
        &self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
        opts: BeginScanOptions,
    ) -> Result<ScanCursor<'_>, StorageError> {
        self.fail_if_space_matches(space)?;
        self.inner.begin_scan(space, range, opts).await
    }
}

impl StorageWrite for RecordingWrite {
    async fn put_many(
        &mut self,
        space: lix::storage::StorageSpace,
        entries: PutBatch,
    ) -> Result<(), StorageError> {
        self.fail_if_space_matches(space)?;
        self.inner.put_many(space, entries).await
    }

    async fn delete_many(
        &mut self,
        space: lix::storage::StorageSpace,
        keys: &[Key],
    ) -> Result<(), StorageError> {
        self.inner.delete_many(space, keys).await
    }

    async fn delete_range(
        &mut self,
        space: lix::storage::StorageSpace,
        range: KeyRange,
    ) -> Result<(), StorageError> {
        self.inner.delete_range(space, range).await
    }

    async fn commit(self) -> Result<CommitResult, StorageError> {
        self.stats.write_committed.fetch_add(1, Ordering::SeqCst);
        self.inner.commit().await
    }

    async fn rollback(self) -> Result<(), StorageError> {
        self.stats.write_rolled_back.fetch_add(1, Ordering::SeqCst);
        self.inner.rollback().await
    }
}

impl RecordingWrite {
    fn fail_if_space_matches(&self, space: lix::storage::StorageSpace) -> Result<(), StorageError> {
        if self.fail_write_space() == Some(space.id) {
            return Err(forced_write_failure(space.name));
        }
        Ok(())
    }

    fn fail_write_space(&self) -> Option<SpaceId> {
        *self
            .fail_write_space
            .lock()
            .expect("fail space lock should not poison")
    }
}

impl RecordingRead {
    fn fail_if_space_matches(&self, space: lix::storage::StorageSpace) -> Result<(), StorageError> {
        if self.fail_read_space() == Some(space.id) {
            return Err(forced_read_failure(space.name));
        }
        Ok(())
    }

    fn fail_read_space(&self) -> Option<SpaceId> {
        *self
            .fail_read_space
            .lock()
            .expect("fail space lock should not poison")
    }
}

/// The space name comes from the `StorageSpace` that actually matched, so the
/// message names whatever the registry currently calls that space instead of
/// whatever this file last believed it was called.
fn forced_read_failure(space: &str) -> StorageError {
    StorageError::Io(format!("forced read failure for namespace {space}"))
}

fn forced_write_failure(space: &str) -> StorageError {
    StorageError::Io(format!("forced write failure for namespace {space}"))
}

#[derive(Default)]
struct TransactionStats {
    read_opened: AtomicUsize,
    write_opened: AtomicUsize,
    write_committed: AtomicUsize,
    write_rolled_back: AtomicUsize,
}

impl TransactionStats {
    fn snapshot(&self) -> TransactionStatsSnapshot {
        TransactionStatsSnapshot {
            read_opened: self.read_opened.load(Ordering::SeqCst),
            write_opened: self.write_opened.load(Ordering::SeqCst),
            write_committed: self.write_committed.load(Ordering::SeqCst),
            write_rolled_back: self.write_rolled_back.load(Ordering::SeqCst),
        }
    }
}

#[derive(Clone, Copy)]
struct TransactionStatsSnapshot {
    read_opened: usize,
    write_opened: usize,
    write_committed: usize,
    write_rolled_back: usize,
}

impl TransactionStatsSnapshot {
    fn delta_since(self, before: &Self) -> Self {
        Self {
            read_opened: self.read_opened - before.read_opened,
            write_opened: self.write_opened - before.write_opened,
            write_committed: self.write_committed - before.write_committed,
            write_rolled_back: self.write_rolled_back - before.write_rolled_back,
        }
    }
}
