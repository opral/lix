use crate::backend::{KvScanRange, KvStore, KvWriter};
use crate::engine2::untracked_state::{
    UntrackedStateIdentity, UntrackedStateRow, UntrackedStateRowRequest, UntrackedStateScanRequest,
};
use crate::{LixError, NullableKeyFilter};

const UNTRACKED_STATE_ROW_NAMESPACE: &str = "untracked_state.row";

pub(crate) async fn scan_rows(
    store: &mut impl KvStore,
    request: &UntrackedStateScanRequest,
) -> Result<Vec<UntrackedStateRow>, LixError> {
    let mut rows = scan_all_untracked_rows(store).await?;
    rows.retain(|row| row_matches_scan(row, request));
    if let Some(limit) = request.limit {
        rows.truncate(limit);
    }
    Ok(rows)
}

pub(crate) async fn load_row(
    store: &mut impl KvStore,
    request: &UntrackedStateRowRequest,
) -> Result<Option<UntrackedStateRow>, LixError> {
    let Some(identity) = identity_from_request(request) else {
        return Ok(None);
    };
    let Some(bytes) = store
        .kv_get(
            UNTRACKED_STATE_ROW_NAMESPACE,
            &encode_untracked_state_row_key(&identity),
        )
        .await?
    else {
        return Ok(None);
    };
    decode_untracked_state_row(&bytes).map(Some)
}

pub(crate) async fn write_rows(
    writer: &mut impl KvWriter,
    rows: &[UntrackedStateRow],
) -> Result<(), LixError> {
    for row in rows {
        let identity = UntrackedStateIdentity::from_row(row);
        if row.snapshot_content.is_none() {
            delete_untracked_row(writer, &identity).await?;
        } else {
            put_untracked_row(writer, row, &identity).await?;
        }
    }
    Ok(())
}

pub(crate) async fn delete_rows(
    writer: &mut impl KvWriter,
    identities: &[UntrackedStateIdentity],
) -> Result<(), LixError> {
    for identity in identities {
        delete_untracked_row(writer, identity).await?;
    }
    Ok(())
}

async fn scan_all_untracked_rows(
    store: &mut impl KvStore,
) -> Result<Vec<UntrackedStateRow>, LixError> {
    store
        .kv_scan(
            UNTRACKED_STATE_ROW_NAMESPACE,
            KvScanRange::prefix(Vec::new()),
            None,
        )
        .await?
        .into_iter()
        .map(|pair| decode_untracked_state_row(&pair.value))
        .collect()
}

fn row_matches_scan(row: &UntrackedStateRow, request: &UntrackedStateScanRequest) -> bool {
    (request.filter.schema_keys.is_empty() || request.filter.schema_keys.contains(&row.schema_key))
        && (request.filter.entity_ids.is_empty()
            || request.filter.entity_ids.contains(&row.entity_id))
        && (request.filter.version_ids.is_empty()
            || request.filter.version_ids.contains(&row.version_id))
        && nullable_matches_filters(&row.file_id, &request.filter.file_ids)
        && nullable_matches_filters(&row.plugin_key, &request.filter.plugin_keys)
}

fn nullable_matches_filters(value: &Option<String>, filters: &[NullableKeyFilter<String>]) -> bool {
    filters.is_empty()
        || filters.iter().any(|filter| match filter {
            NullableKeyFilter::Any => true,
            NullableKeyFilter::Null => value.is_none(),
            NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
        })
}

fn identity_from_request(request: &UntrackedStateRowRequest) -> Option<UntrackedStateIdentity> {
    let file_id = match &request.file_id {
        NullableKeyFilter::Null => None,
        NullableKeyFilter::Value(value) => Some(value.clone()),
        NullableKeyFilter::Any => return None,
    };
    Some(UntrackedStateIdentity {
        version_id: request.version_id.clone(),
        schema_key: request.schema_key.clone(),
        entity_id: request.entity_id.clone(),
        file_id,
    })
}

async fn put_untracked_row(
    writer: &mut impl KvWriter,
    row: &UntrackedStateRow,
    identity: &UntrackedStateIdentity,
) -> Result<(), LixError> {
    writer
        .kv_put(
            UNTRACKED_STATE_ROW_NAMESPACE,
            &encode_untracked_state_row_key(identity),
            &encode_untracked_state_row(row)?,
        )
        .await
}

async fn delete_untracked_row(
    writer: &mut impl KvWriter,
    identity: &UntrackedStateIdentity,
) -> Result<(), LixError> {
    writer
        .kv_delete(
            UNTRACKED_STATE_ROW_NAMESPACE,
            &encode_untracked_state_row_key(identity),
        )
        .await
}

fn encode_untracked_state_row(row: &UntrackedStateRow) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(row).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode untracked-state row: {error}"),
        )
    })
}

fn decode_untracked_state_row(bytes: &[u8]) -> Result<UntrackedStateRow, LixError> {
    serde_json::from_slice(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode untracked-state row: {error}"),
        )
    })
}

fn encode_untracked_state_row_key(identity: &UntrackedStateIdentity) -> Vec<u8> {
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
    use crate::engine2::untracked_state::UntrackedStateContext;

    #[tokio::test]
    async fn write_and_load_roundtrips() {
        let backend = Arc::new(UnitTestBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        context
            .writer(transaction.as_mut())
            .write_rows(std::slice::from_ref(&row))
            .await
            .expect("write should succeed");
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(Arc::clone(&backend));
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: "ui-tab".to_string(),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, Some(row));
    }

    #[tokio::test]
    async fn scan_filters_by_schema_and_version() {
        let backend = Arc::new(UnitTestBackend::new());
        let context = UntrackedStateContext::new();
        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        context
            .writer(transaction.as_mut())
            .write_rows(&[
                untracked_row("global", "lix_key_value", "global-ui"),
                untracked_row("version-a", "lix_key_value", "version-ui"),
                untracked_row("version-a", "other_schema", "other"),
            ])
            .await
            .expect("writes should succeed");
        transaction.commit().await.expect("commit should succeed");

        let rows = {
            let mut reader = context.reader(Arc::clone(&backend));
            reader
                .scan_rows(&UntrackedStateScanRequest {
                    filter: crate::engine2::untracked_state::UntrackedStateFilter {
                        schema_keys: vec!["lix_key_value".to_string()],
                        version_ids: vec!["version-a".to_string()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .await
        }
        .expect("scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "version-ui");
    }

    #[tokio::test]
    async fn delete_removes_row() {
        let backend = Arc::new(UnitTestBackend::new());
        let context = UntrackedStateContext::new();
        let row = untracked_row("global", "lix_key_value", "ui-tab");
        let identity = UntrackedStateIdentity::from_row(&row);

        let mut transaction = backend
            .begin_transaction(TransactionBeginMode::Write)
            .await
            .expect("transaction should open");
        let mut writer = context.writer(transaction.as_mut());
        writer
            .write_rows(std::slice::from_ref(&row))
            .await
            .expect("write should succeed");
        writer
            .delete_rows(&[identity])
            .await
            .expect("delete should succeed");
        transaction.commit().await.expect("commit should succeed");

        let loaded = {
            let mut reader = context.reader(Arc::clone(&backend));
            reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "lix_key_value".to_string(),
                    version_id: "global".to_string(),
                    entity_id: "ui-tab".to_string(),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("load should succeed");
        assert_eq!(loaded, None);
    }

    fn untracked_row(version_id: &str, schema_key: &str, entity_id: &str) -> UntrackedStateRow {
        UntrackedStateRow {
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            file_id: None,
            plugin_key: None,
            snapshot_content: Some(format!("{{\"key\":\"{}\",\"value\":\"value\"}}", entity_id)),
            metadata: None,
            schema_version: "1".to_string(),
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == "global",
            version_id: version_id.to_string(),
        }
    }
}
