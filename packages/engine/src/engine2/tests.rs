use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
use crate::engine2::changelog::ChangelogContext;
use crate::engine2::commit_graph::CommitGraphContext;
use crate::engine2::{Engine, ExecuteResult};
use crate::Value;

#[tokio::test]
async fn tracked_state_rebuild_restores_sql_reads_from_changelog() {
    let backend = UnitTestBackend::new();
    let receipt = Engine::initialize(Box::new(backend.clone()))
        .await
        .expect("backend should initialize");
    let engine = Engine::new(Box::new(backend.clone()))
        .await
        .expect("initialized backend should create an engine");
    let session = engine
        .open_session(receipt.main_version_id.clone())
        .await
        .expect("main session should open");

    let insert_result = session
        .execute(
            "INSERT INTO lix_state (\
             entity_id, schema_key, file_id, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'rebuild-key', 'lix_key_value', NULL, lix_json('{\"key\":\"rebuild-key\",\"value\":\"before-rebuild\"}'), '1', false, false\
             )",
            &[],
        )
        .await
        .expect("tracked state write should succeed");
    assert_eq!(insert_result, ExecuteResult::AffectedRows(1));
    assert_key_value_visible(&session, "\"before-rebuild\"").await;

    let head_commit_id = engine
        .version_ref()
        .reader(&backend)
        .load_head_commit_id(&receipt.main_version_id)
        .await
        .expect("version head should load")
        .expect("version head should exist");
    delete_tracked_root_for_commit(&engine, &backend, &head_commit_id).await;
    assert_key_value_missing(&session).await;

    let commit_graph = CommitGraphContext::new(ChangelogContext::new());
    let mut rebuild_transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("rebuild transaction should open");
    let rebuild_report = engine
        .tracked_state()
        .rebuild_state_at_commit(
            &commit_graph,
            &backend,
            rebuild_transaction.as_mut(),
            &head_commit_id,
        )
        .await
        .expect("tracked state should rebuild from changelog");
    assert!(
        rebuild_report.written_rows > 0,
        "rebuild should write tracked rows derived from changelog"
    );
    rebuild_transaction
        .commit()
        .await
        .expect("rebuild transaction should commit");
    assert_key_value_visible(&session, "\"before-rebuild\"").await;
}

async fn delete_tracked_root_for_commit(
    engine: &Engine,
    backend: &UnitTestBackend,
    commit_id: &str,
) {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("delete transaction should open");
    let tracked_state = engine.tracked_state();
    tracked_state
        .writer(transaction.as_mut())
        .delete_root_for_rebuild(commit_id)
        .await
        .expect("tracked root should delete");
    transaction
        .commit()
        .await
        .expect("delete transaction should commit");
}

async fn assert_key_value_visible(session: &crate::engine2::SessionContext, expected: &str) {
    let result = session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'rebuild-key'",
            &[],
        )
        .await
        .expect("key-value read should succeed");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows.rows()[0].values(),
        &[Value::Text(expected.to_string())]
    );
}

async fn assert_key_value_missing(session: &crate::engine2::SessionContext) {
    let result = session
        .execute(
            "SELECT value FROM lix_key_value WHERE key = 'rebuild-key'",
            &[],
        )
        .await
        .expect("key-value read should succeed");
    let ExecuteResult::Rows(rows) = result else {
        panic!("SELECT should return rows");
    };
    assert_eq!(rows.len(), 0);
}
