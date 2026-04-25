use std::collections::BTreeSet;

use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::init as init_live_state;
use crate::live_state::tracked::{
    load_exact_row_with_backend, scan_rows_with_backend, ExactTrackedRowRequest, TrackedScanRequest,
};
use crate::live_state::{LiveWriteOperation, LiveWriteRow};
use crate::session::workspace::init as init_workspace;
use crate::test_support::TestSqliteBackend;
use crate::transaction::{LiveStateWriteTransaction, OverlayReadContext, TransactionDelta};
use crate::{LixError, NullableKeyFilter, Value};

async fn commit_tracked_rows(
    backend: &TestSqliteBackend,
    rows: Vec<LiveWriteRow>,
) -> Result<(), LixError> {
    let read_context = OverlayReadContext::new(backend, backend);
    let backend_txn = backend.begin_write_transaction().await?;
    let mut write_tx = LiveStateWriteTransaction::new(backend_txn, read_context);
    let schema_keys = rows
        .iter()
        .map(|row| row.schema_key.clone())
        .collect::<BTreeSet<_>>();
    for schema_key in schema_keys {
        write_tx.register_schema(schema_key)?;
    }
    write_tx.stage(TransactionDelta { writes: rows })?;
    write_tx.commit().await?;
    Ok(())
}

#[tokio::test]
async fn live_tracked_state_tombstones_hide_rows() {
    let backend = TestSqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    let tombstone_time = "2026-03-24T00:05:00Z";
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    commit_tracked_rows(
        &backend,
        vec![LiveWriteRow {
            entity_id: "edge-1".to_string(),
            schema_key: "lix_commit_edge".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            version_id: "main".to_string(),
            global: true,
            untracked: false,
            plugin_key: None,
            metadata: Some("{\"kind\":\"module-test\"}".to_string()),
            change_id: "change-1".to_string(),
            snapshot_content: Some(
                "{\"child_id\":\"child-1\",\"parent_id\":\"parent-edge-1\"}".to_string(),
            ),
            created_at: Some(timestamp.to_string()),
            updated_at: timestamp.to_string(),
            operation: LiveWriteOperation::Upsert,
        }],
    )
    .await
    .expect("initial tracked transaction should succeed");

    commit_tracked_rows(
        &backend,
        vec![LiveWriteRow {
            entity_id: "edge-1".to_string(),
            schema_key: "lix_commit_edge".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            version_id: "main".to_string(),
            global: true,
            untracked: false,
            plugin_key: None,
            metadata: Some("{\"kind\":\"module-test\"}".to_string()),
            change_id: "change-2".to_string(),
            snapshot_content: None,
            created_at: Some(tombstone_time.to_string()),
            updated_at: tombstone_time.to_string(),
            operation: LiveWriteOperation::Tombstone,
        }],
    )
    .await
    .expect("tracked tombstone transaction should succeed");

    let exact = load_exact_row_with_backend(
        &backend,
        &ExactTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: NullableKeyFilter::Null,
            untracked: false,
        },
    )
    .await
    .expect("exact tracked lookup should succeed after tombstone");
    assert!(exact.is_none());

    let scanned = scan_rows_with_backend(
        &backend,
        &TrackedScanRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            constraints: vec![ScanConstraint {
                field: ScanField::EntityId,
                operator: ScanOperator::Eq(Value::Text("edge-1".to_string())),
            }],
            required_columns: vec!["child_id".to_string()],
        },
    )
    .await
    .expect("tracked scan should succeed after tombstone");
    assert!(scanned.is_empty());
}
