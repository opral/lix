use serde_json::Value as JsonValue;

use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
use crate::engine2::changelog::ChangelogContext;
use crate::engine2::commit_graph::CommitGraphContext;
use crate::engine2::live_state::{LiveStateContext, LiveStateRowRequest};
use crate::engine2::tracked_state::{
    TrackedStateContext, TrackedStateDeleteRequest, TrackedStateFilter,
};
use crate::engine2::untracked_state::UntrackedStateContext;
use crate::engine2::{Engine, ExecuteResult};
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixError, NullableKeyFilter, Value};

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
             entity_id, schema_key, file_id, plugin_key, snapshot_content, schema_version, global, untracked\
             ) VALUES (\
             'rebuild-key', 'lix_key_value', NULL, NULL, lix_json('{\"key\":\"rebuild-key\",\"value\":\"before-rebuild\"}'), '1', false, false\
             )",
            &[],
        )
        .await
        .expect("tracked state write should succeed");
    assert_eq!(insert_result, ExecuteResult::AffectedRows(1));
    assert_key_value_visible(&session, "\"before-rebuild\"").await;

    let head_commit_id = load_version_head(&backend, &receipt.main_version_id)
        .await
        .expect("version head should load");
    delete_tracked_rows_for_version(&engine, &backend, &receipt.main_version_id).await;
    assert_key_value_missing(&session).await;

    let commit_graph = CommitGraphContext::new(ChangelogContext::new());
    let mut rebuild_transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("rebuild transaction should open");
    let rebuild_report = engine
        .tracked_state()
        .rebuild_version_state(
            &commit_graph,
            &backend,
            rebuild_transaction.as_mut(),
            &receipt.main_version_id,
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

async fn delete_tracked_rows_for_version(
    engine: &Engine,
    backend: &UnitTestBackend,
    version_id: &str,
) {
    let mut transaction = backend
        .begin_transaction(TransactionBeginMode::Write)
        .await
        .expect("delete transaction should open");
    let tracked_state = engine.tracked_state();
    let deleted = tracked_state
        .writer(transaction.as_mut())
        .delete_rows(&TrackedStateDeleteRequest {
            filter: TrackedStateFilter {
                version_ids: vec![version_id.to_string()],
                include_tombstones: true,
                ..Default::default()
            },
        })
        .await
        .expect("tracked rows should delete");
    assert!(
        deleted > 0,
        "test should delete tracked rows for the version"
    );
    transaction
        .commit()
        .await
        .expect("delete transaction should commit");
}

async fn load_version_head(
    backend: &UnitTestBackend,
    version_id: &str,
) -> Result<String, LixError> {
    let live_state =
        LiveStateContext::new(TrackedStateContext::new(), UntrackedStateContext::new());
    let row = live_state
        .reader(backend)
        .load_row(&LiveStateRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: version_id.to_string(),
            file_id: NullableKeyFilter::Null,
        })
        .await?
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("missing version ref for version '{version_id}'"),
            )
        })?;
    let snapshot_content = row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version ref for version '{version_id}' is missing snapshot_content"),
        )
    })?;
    let snapshot = serde_json::from_str::<JsonValue>(snapshot_content).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("version ref snapshot is invalid JSON: {error}"),
        )
    })?;
    snapshot
        .get("commit_id")
        .and_then(JsonValue::as_str)
        .map(str::to_string)
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!("version ref for version '{version_id}' is missing commit_id"),
            )
        })
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
