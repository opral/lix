use crate::backend::{KvScanRange, KvStore, KvWriter};
use crate::engine2::tracked_state::{
    TrackedStateDeleteRequest, TrackedStateRow, TrackedStateRowRequest, TrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

const TRACKED_STATE_ROW_NAMESPACE: &str = "tracked_state.row";

pub(crate) async fn scan_rows(
    store: &mut impl KvStore,
    request: &TrackedStateScanRequest,
) -> Result<Vec<TrackedStateRow>, LixError> {
    let mut rows = scan_all_rows(store).await?;
    rows.retain(|row| row_matches_scan(row, request));
    if !request.filter.include_tombstones {
        rows.retain(|row| row.snapshot_content.is_some());
    }
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

pub(crate) async fn load_row(
    store: &mut impl KvStore,
    request: &TrackedStateRowRequest,
) -> Result<Option<TrackedStateRow>, LixError> {
    let Some(identity) = TrackedStateRowIdentity::from_request(request) else {
        return Ok(None);
    };
    let Some(bytes) = store
        .kv_get(
            TRACKED_STATE_ROW_NAMESPACE,
            &encode_tracked_state_row_key(&identity),
        )
        .await?
    else {
        return Ok(None);
    };
    let row = decode_tracked_state_row(&bytes)?;
    Ok(row.snapshot_content.is_some().then_some(row))
}

pub(crate) async fn write_rows(
    writer: &mut impl KvWriter,
    rows: &[TrackedStateRow],
) -> Result<(), LixError> {
    for row in rows {
        put_tracked_state_row(writer, row).await?;
    }
    Ok(())
}

pub(crate) async fn delete_rows(
    writer: &mut impl KvWriter,
    request: &TrackedStateDeleteRequest,
) -> Result<usize, LixError> {
    let rows = scan_all_rows(writer).await?;
    let mut deleted = 0usize;
    for row in rows {
        if row_matches_filter(&row, &request.filter) {
            let identity = TrackedStateRowIdentity::from_row(&row);
            writer
                .kv_delete(
                    TRACKED_STATE_ROW_NAMESPACE,
                    &encode_tracked_state_row_key(&identity),
                )
                .await?;
            deleted += 1;
        }
    }
    Ok(deleted)
}

async fn scan_all_rows(store: &mut impl KvStore) -> Result<Vec<TrackedStateRow>, LixError> {
    store
        .kv_scan(
            TRACKED_STATE_ROW_NAMESPACE,
            KvScanRange::prefix(Vec::new()),
            None,
        )
        .await?
        .into_iter()
        .map(|pair| decode_tracked_state_row(&pair.value))
        .collect()
}

fn row_matches_scan(row: &TrackedStateRow, request: &TrackedStateScanRequest) -> bool {
    row_matches_filter(row, &request.filter)
}

fn row_matches_filter(
    row: &TrackedStateRow,
    filter: &crate::engine2::tracked_state::TrackedStateFilter,
) -> bool {
    (filter.schema_keys.is_empty() || filter.schema_keys.contains(&row.schema_key))
        && (filter.entity_ids.is_empty() || filter.entity_ids.contains(&row.entity_id))
        && (filter.version_ids.is_empty() || filter.version_ids.contains(&row.version_id))
        && nullable_matches_filters(&row.file_id, &filter.file_ids)
        && nullable_matches_filters(&row.plugin_key, &filter.plugin_keys)
}

fn nullable_matches_filters(value: &Option<String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => value.is_none(),
            NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
        })
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct TrackedStateRowIdentity {
    version_id: String,
    schema_key: String,
    entity_id: String,
    file_id: Option<String>,
}

impl TrackedStateRowIdentity {
    fn from_row(row: &TrackedStateRow) -> Self {
        Self {
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    fn from_request(request: &TrackedStateRowRequest) -> Option<Self> {
        let file_id = match &request.file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value.clone()),
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            version_id: request.version_id.clone(),
            schema_key: request.schema_key.clone(),
            entity_id: request.entity_id.clone(),
            file_id,
        })
    }
}

async fn put_tracked_state_row(
    writer: &mut impl KvWriter,
    row: &TrackedStateRow,
) -> Result<(), LixError> {
    let identity = TrackedStateRowIdentity::from_row(row);
    writer
        .kv_put(
            TRACKED_STATE_ROW_NAMESPACE,
            &encode_tracked_state_row_key(&identity),
            &encode_tracked_state_row(row)?,
        )
        .await
}

fn encode_tracked_state_row(row: &TrackedStateRow) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(row).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode tracked-state row: {error}"),
        )
    })
}

fn decode_tracked_state_row(bytes: &[u8]) -> Result<TrackedStateRow, LixError> {
    serde_json::from_slice(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode tracked-state row: {error}"),
        )
    })
}

fn encode_tracked_state_row_key(identity: &TrackedStateRowIdentity) -> Vec<u8> {
    let mut out = Vec::new();
    push_component(&mut out, &identity.version_id);
    push_component(&mut out, &identity.schema_key);
    push_component(&mut out, &identity.entity_id);
    match &identity.file_id {
        Some(file_id) => {
            out.push(1);
            push_component(&mut out, file_id);
        }
        None => out.push(0),
    }
    out
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, LixBackend, TransactionBeginMode};
    use crate::engine2::tracked_state::{
        TrackedStateDeleteRequest, TrackedStateFilter, TrackedStateScanRequest,
    };

    #[test]
    fn row_key_distinguishes_null_and_value_file_id() {
        let null_key = encode_tracked_state_row_key(&TrackedStateRowIdentity {
            version_id: "global".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: "key".to_string(),
            file_id: None,
        });
        let value_key = encode_tracked_state_row_key(&TrackedStateRowIdentity {
            version_id: "global".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: "key".to_string(),
            file_id: Some("file".to_string()),
        });

        assert_ne!(null_key, value_key);
    }

    #[tokio::test]
    async fn tracked_write_load_roundtrip() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let row = tracked_row(
            "lix_key_value",
            "global",
            "selected-tab",
            Some("{\"value\":\"a\"}"),
        );

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            write_rows(&mut writer, &[row.clone()])
                .await
                .expect("row should write");
        }
        transaction.commit().await.expect("commit should persist");

        let mut store = Arc::clone(&backend);
        let loaded = load_row(
            &mut store,
            &TrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: "selected-tab".to_string(),
                file_id: NullableKeyFilter::Null,
            },
        )
        .await
        .expect("load should succeed")
        .expect("row should exist");

        assert_eq!(loaded, row);
    }

    #[tokio::test]
    async fn scan_filters_by_schema_and_version() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let matching = tracked_row("schema_a", "version_a", "entity-a", Some("{}"));
        let wrong_schema = tracked_row("schema_b", "version_a", "entity-b", Some("{}"));
        let wrong_version = tracked_row("schema_a", "version_b", "entity-c", Some("{}"));

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            write_rows(
                &mut writer,
                &[matching.clone(), wrong_schema, wrong_version],
            )
            .await
            .expect("rows should write");
        }
        transaction.commit().await.expect("commit should persist");

        let mut store = Arc::clone(&backend);
        let rows = scan_rows(
            &mut store,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec!["schema_a".to_string()],
                    version_ids: vec!["version_a".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("scan should succeed");

        assert_eq!(rows, vec![matching]);
    }

    #[tokio::test]
    async fn tombstone_hidden_unless_include_tombstones() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let tombstone = tracked_row("lix_key_value", "global", "deleted-key", None);

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            write_rows(&mut writer, &[tombstone.clone()])
                .await
                .expect("row should write");
        }
        transaction.commit().await.expect("commit should persist");

        let mut store = Arc::clone(&backend);
        let visible_rows = scan_rows(&mut store, &TrackedStateScanRequest::default())
            .await
            .expect("scan should succeed");
        assert!(visible_rows.is_empty());

        let mut store = Arc::clone(&backend);
        let tombstones = scan_rows(
            &mut store,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    include_tombstones: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("scan should succeed");
        assert_eq!(tombstones, vec![tombstone]);

        let mut store = Arc::clone(&backend);
        let loaded = load_row(
            &mut store,
            &TrackedStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "global".to_string(),
                entity_id: "deleted-key".to_string(),
                file_id: NullableKeyFilter::Null,
            },
        )
        .await
        .expect("load should succeed");
        assert_eq!(loaded, None);
    }

    #[tokio::test]
    async fn delete_rows_removes_matching_version_rows() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let version_a_row = tracked_row("schema_a", "version_a", "entity-a", Some("{}"));
        let version_b_row = tracked_row("schema_a", "version_b", "entity-b", Some("{}"));

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        {
            let mut writer = transaction.as_mut();
            write_rows(&mut writer, &[version_a_row.clone(), version_b_row.clone()])
                .await
                .expect("rows should write");
            let deleted = delete_rows(
                &mut writer,
                &TrackedStateDeleteRequest {
                    filter: TrackedStateFilter {
                        version_ids: vec!["version_a".to_string()],
                        ..Default::default()
                    },
                },
            )
            .await
            .expect("delete should succeed");
            assert_eq!(deleted, 1);
        }
        transaction.commit().await.expect("commit should persist");

        let mut store = Arc::clone(&backend);
        let rows = scan_rows(
            &mut store,
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    version_ids: vec!["version_a".to_string(), "version_b".to_string()],
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .expect("scan should succeed");
        assert_eq!(rows, vec![version_b_row]);
    }

    #[tokio::test]
    async fn writer_delete_rows_routes_to_storage() {
        let backend: Arc<dyn LixBackend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let context = crate::engine2::tracked_state::TrackedStateContext::new();
        let row = tracked_row("schema_a", "version_a", "entity-a", Some("{}"));

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        {
            let mut writer = context.writer(transaction.as_mut());
            writer
                .write_rows(std::slice::from_ref(&row))
                .await
                .expect("row should write");
            let deleted = writer
                .delete_rows(&TrackedStateDeleteRequest {
                    filter: TrackedStateFilter {
                        version_ids: vec!["version_a".to_string()],
                        ..Default::default()
                    },
                })
                .await
                .expect("delete should succeed");
            assert_eq!(deleted, 1);
        }
        transaction.commit().await.expect("commit should persist");

        let mut store = Arc::clone(&backend);
        let rows = scan_rows(&mut store, &TrackedStateScanRequest::default())
            .await
            .expect("scan should succeed");
        assert!(rows.is_empty());
    }

    fn tracked_row(
        schema_key: &str,
        version_id: &str,
        entity_id: &str,
        snapshot_content: Option<&str>,
    ) -> TrackedStateRow {
        TrackedStateRow {
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: snapshot_content.map(str::to_string),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            change_id: format!("change-{version_id}-{entity_id}"),
            commit_id: format!("commit-{version_id}"),
            version_id: version_id.to_string(),
        }
    }
}
