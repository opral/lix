use std::collections::BTreeSet;

use crate::live_state::constraints::{Bound, ScanConstraint, ScanField, ScanOperator};
use crate::live_state::init as init_live_state;
use crate::live_state::tracked::{
    load_exact_row_with_backend, load_exact_rows_with_backend, scan_rows_with_backend,
    BatchTrackedRowRequest, ExactTrackedRowRequest, TrackedScanRequest,
};
use crate::live_state::{
    load_exact_live_row, ExactLiveRowQuery, LiveRowSource, LiveWriteOperation, LiveWriteRow,
};
use crate::session::workspace::init as init_workspace;
use crate::test_support::TestSqliteBackend;
use crate::transaction::{LiveStateWriteTransaction, OverlayReadContext, TransactionDelta};
use crate::{LixError, NullableKeyFilter, Value};

fn tracked_row(entity_id: &str, child_id: &str, change_id: &str, timestamp: &str) -> LiveWriteRow {
    LiveWriteRow {
        entity_id: entity_id.to_string(),
        schema_key: "lix_commit_edge".to_string(),
        schema_version: "1".to_string(),
        file_id: None,
        version_id: "main".to_string(),
        global: true,
        untracked: false,
        plugin_key: None,
        metadata: Some("{\"kind\":\"module-test\"}".to_string()),
        change_id: change_id.to_string(),
        snapshot_content: Some(format!(
            "{{\"child_id\":\"{child_id}\",\"parent_id\":\"parent-{entity_id}\"}}"
        )),
        created_at: Some(timestamp.to_string()),
        updated_at: timestamp.to_string(),
        operation: LiveWriteOperation::Upsert,
    }
}

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
async fn live_tracked_state_roundtrips_rows() {
    let backend = TestSqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    init_workspace(&backend)
        .await
        .expect("workspace init should succeed");
    commit_tracked_rows(
        &backend,
        vec![
            tracked_row("edge-1", "child-1", "change-1", timestamp),
            tracked_row("edge-2", "child-2", "change-2", timestamp),
        ],
    )
    .await
    .expect("tracked transaction should succeed");

    let exact = load_exact_live_row(
        &backend,
        &ExactLiveRowQuery {
            source: LiveRowSource::Tracked,
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_id: "edge-1".to_string(),
            file_id: NullableKeyFilter::Null,
            schema_version: None,
            plugin_key: NullableKeyFilter::Any,
            global: None,
            untracked: None,
            include_tombstones: false,
            include_global_overlay: true,
            include_untracked_overlay: true,
        },
    )
    .await
    .expect("exact tracked lookup should succeed")
    .expect("tracked row should exist");
    assert_eq!(exact.change_id.as_deref(), Some("change-1"));
    let snapshot: serde_json::Value = serde_json::from_str(
        exact
            .snapshot_content
            .as_deref()
            .expect("tracked live row should include snapshot_content"),
    )
    .expect("tracked live row snapshot_content should be valid JSON");
    assert_eq!(snapshot["child_id"].as_str(), Some("child-1"));

    let batch = load_exact_rows_with_backend(
        &backend,
        &BatchTrackedRowRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            entity_ids: vec!["edge-1".to_string(), "edge-2".to_string()],
            file_id: NullableKeyFilter::Null,
        },
    )
    .await
    .expect("batch tracked lookup should succeed");
    assert_eq!(batch.len(), 2);

    let scanned = scan_rows_with_backend(
        &backend,
        &TrackedScanRequest {
            schema_key: "lix_commit_edge".to_string(),
            version_id: "main".to_string(),
            constraints: vec![
                ScanConstraint {
                    field: ScanField::EntityId,
                    operator: ScanOperator::In(vec![
                        Value::Text("edge-1".to_string()),
                        Value::Text("edge-2".to_string()),
                    ]),
                },
                ScanConstraint {
                    field: ScanField::PluginKey,
                    operator: ScanOperator::Eq(Value::Null),
                },
                ScanConstraint {
                    field: ScanField::SchemaVersion,
                    operator: ScanOperator::Range {
                        lower: Some(Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                        upper: Some(Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                    },
                },
            ],
            required_columns: vec!["child_id".to_string(), "parent_id".to_string()],
        },
    )
    .await
    .expect("tracked scan should succeed");
    assert_eq!(scanned.len(), 2);
    assert_eq!(
        scanned
            .iter()
            .map(|row| row.property_text("child_id").unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["child-1".to_string(), "child-2".to_string()]
    );
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
        vec![tracked_row("edge-1", "child-1", "change-1", timestamp)],
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
