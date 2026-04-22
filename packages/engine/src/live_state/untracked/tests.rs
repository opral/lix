use crate::live_state::builtin_schema_storage_metadata;
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::init as init_live_state;
use crate::live_state::testing::local_version_head_live_row;
use crate::live_state::untracked::{
    load_exact_row_with_backend, load_exact_rows_with_backend, scan_rows_with_backend,
    BatchUntrackedRowRequest, ExactUntrackedRowRequest, UntrackedScanRequest,
};
use crate::live_state::LiveRow;
use crate::schema::LixActiveVersion;
use crate::test_support::{
    commit_untracked_rows as commit_untracked_rows_via_support, TestSqliteBackend,
};
use crate::{LixError, NullableKeyFilter, Value};

async fn commit_untracked_rows(
    backend: &TestSqliteBackend,
    rows: Vec<LiveRow>,
) -> Result<(), LixError> {
    commit_untracked_rows_via_support(backend, rows).await
}

fn active_version_helper_live_row(entity_id: &str, version_id: &str, timestamp: &str) -> LiveRow {
    let metadata = builtin_schema_storage_metadata("lix_active_version")
        .expect("lix_active_version metadata should exist");
    LiveRow {
        entity_id: entity_id.to_string(),
        schema_key: metadata.schema_key,
        schema_version: metadata.schema_version,
        file_id: None,
        version_id: crate::version::GLOBAL_VERSION_ID.to_string(),
        global: true,
        plugin_key: None,
        metadata: None,
        snapshot_content: Some(
            serde_json::to_string(&LixActiveVersion {
                id: entity_id.to_string(),
                version_id: version_id.to_string(),
            })
            .expect("lix_active_version snapshot serialization must succeed"),
        ),
        created_at: Some(timestamp.to_string()),
        updated_at: Some(timestamp.to_string()),
        change_id: Some(format!(
            "change-active-version::{entity_id}::{version_id}::{timestamp}"
        )),
        untracked: true,
    }
}

fn active_version_schema_key() -> String {
    builtin_schema_storage_metadata("lix_active_version")
        .expect("lix_active_version metadata should exist")
        .schema_key
}

fn active_version_storage_version_id() -> String {
    crate::version::GLOBAL_VERSION_ID.to_string()
}

#[tokio::test]
async fn live_untracked_state_roundtrips_helper_rows() {
    let backend = TestSqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    crate::live_state::register_schema(&backend, "lix_active_version")
        .await
        .expect("lix_active_version schema registration should succeed");
    crate::live_state::register_schema(&backend, "lix_version_ref")
        .await
        .expect("lix_version_ref schema registration should succeed");
    commit_untracked_rows(
        &backend,
        vec![
            active_version_helper_live_row("active-row", "main", timestamp),
            local_version_head_live_row("main", "commit-1", timestamp),
            local_version_head_live_row("other", "commit-2", timestamp),
        ],
    )
    .await
    .expect("helper row transaction should succeed");

    let active_version = load_exact_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: active_version_schema_key().to_string(),
            version_id: active_version_storage_version_id().to_string(),
            entity_id: "active-row".to_string(),
            file_id: NullableKeyFilter::Null,
        },
    )
    .await
    .expect("active version lookup should succeed")
    .expect("active version row should exist");
    assert_eq!(active_version.entity_id, "active-row");
    assert_eq!(
        active_version.property_text("version_id").as_deref(),
        Some("main")
    );

    let exact = load_exact_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: NullableKeyFilter::Null,
        },
    )
    .await
    .expect("exact untracked lookup should succeed")
    .expect("exact untracked row should exist");
    assert_eq!(
        exact.property_text("commit_id").as_deref(),
        Some("commit-1")
    );

    let batch = load_exact_rows_with_backend(
        &backend,
        &BatchUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_ids: vec!["main".to_string(), "other".to_string()],
            file_id: NullableKeyFilter::Null,
        },
    )
    .await
    .expect("batch untracked lookup should succeed");
    assert_eq!(batch.len(), 2);

    let scanned = scan_rows_with_backend(
        &backend,
        &UntrackedScanRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            constraints: vec![
                ScanConstraint {
                    field: ScanField::EntityId,
                    operator: ScanOperator::In(vec![
                        Value::Text("main".to_string()),
                        Value::Text("other".to_string()),
                    ]),
                },
                ScanConstraint {
                    field: ScanField::PluginKey,
                    operator: ScanOperator::Eq(Value::Null),
                },
                ScanConstraint {
                    field: ScanField::SchemaVersion,
                    operator: ScanOperator::Range {
                        lower: Some(crate::live_state::constraints::Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                        upper: Some(crate::live_state::constraints::Bound {
                            value: Value::Text("1".to_string()),
                            inclusive: true,
                        }),
                    },
                },
            ],
            required_columns: vec!["commit_id".to_string()],
        },
    )
    .await
    .expect("scan should succeed");
    assert_eq!(scanned.len(), 2);
    assert_eq!(
        scanned
            .iter()
            .map(|row| row.property_text("commit_id").unwrap_or_default())
            .collect::<Vec<_>>(),
        vec!["commit-1".to_string(), "commit-2".to_string()]
    );
}

#[tokio::test]
async fn live_untracked_state_delete_removes_rows() {
    let backend = TestSqliteBackend::new();
    let timestamp = "2026-03-24T00:00:00Z";
    init_live_state(&backend)
        .await
        .expect("live_state init should succeed");
    crate::live_state::register_schema(&backend, "lix_version_ref")
        .await
        .expect("lix_version_ref schema registration should succeed");
    commit_untracked_rows(
        &backend,
        vec![local_version_head_live_row("main", "commit-1", timestamp)],
    )
    .await
    .expect("initial version ref transaction should succeed");

    commit_untracked_rows(
        &backend,
        vec![LiveRow {
            entity_id: "main".to_string(),
            schema_key: "lix_version_ref".to_string(),
            schema_version: "1".to_string(),
            file_id: None,
            version_id: "global".to_string(),
            global: true,
            plugin_key: None,
            metadata: None,
            change_id: Some("change-delete-version-ref-main".to_string()),
            untracked: true,
            snapshot_content: None,
            created_at: None,
            updated_at: Some(timestamp.to_string()),
        }],
    )
    .await
    .expect("delete transaction should succeed");

    let version_ref = load_exact_row_with_backend(
        &backend,
        &ExactUntrackedRowRequest {
            schema_key: "lix_version_ref".to_string(),
            version_id: "global".to_string(),
            entity_id: "main".to_string(),
            file_id: NullableKeyFilter::Null,
        },
    )
    .await
    .expect("version ref lookup should succeed");
    assert!(version_ref.is_none());
}
