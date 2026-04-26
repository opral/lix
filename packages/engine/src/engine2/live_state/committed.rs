use std::sync::Arc;

use async_trait::async_trait;

use crate::backend::KvScanRange;
use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{
    LiveStateContext as EngineLiveStateContext, LiveStateRowRequest, LiveStateScanRequest,
};
use crate::{LixBackend, LixBackendTransaction, LixError, NullableKeyFilter};

const LIVE_STATE_ROW_NAMESPACE: &str = "live_state.row";

/// Committed live-state view backed by the backend key/value API.
pub(crate) struct CommittedLiveStateContext {
    backend: Arc<dyn LixBackend + Send + Sync>,
}

impl CommittedLiveStateContext {
    pub(crate) fn new(backend: Arc<dyn LixBackend + Send + Sync>) -> Self {
        Self { backend }
    }
}

#[async_trait]
impl EngineLiveStateContext for CommittedLiveStateContext {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<LiveStateRow>, LixError> {
        let mut rows = scan_all_state_rows(self.backend.as_ref()).await?;
        rows.retain(|row| state_row_matches_engine_scan(row, request));
        if !request.filter.include_tombstones {
            rows.retain(|row| row.snapshot_content.is_some());
        }
        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<LiveStateRow>, LixError> {
        let Some(identity) = StateRowIdentity::from_exact_parts(
            request.untracked,
            request.version_id.clone(),
            request.schema_key.clone(),
            request.entity_id.clone(),
            request.file_id.clone(),
        ) else {
            return Ok(None);
        };
        let Some(bytes) = self
            .backend
            .kv_get(LIVE_STATE_ROW_NAMESPACE, &encode_state_row_key(&identity))
            .await?
        else {
            return Ok(None);
        };
        let row = decode_state_row(&bytes)?;
        Ok(row.snapshot_content.is_some().then_some(row))
    }
}

pub(crate) async fn write_state_rows(
    transaction: &mut dyn LixBackendTransaction,
    rows: &[LiveStateRow],
) -> Result<(), LixError> {
    for row in rows {
        put_state_row(transaction, row).await?;
    }
    Ok(())
}

async fn scan_all_state_rows(
    backend: &(dyn LixBackend + Send + Sync),
) -> Result<Vec<LiveStateRow>, LixError> {
    backend
        .kv_scan(
            LIVE_STATE_ROW_NAMESPACE,
            KvScanRange::prefix(Vec::new()),
            None,
        )
        .await?
        .into_iter()
        .map(|pair| decode_state_row(&pair.value))
        .collect()
}

fn state_row_matches_engine_scan(row: &LiveStateRow, request: &LiveStateScanRequest) -> bool {
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

/// Stable row identity for the simple key/value live-state projection.
///
/// This is intentionally the same identity used by transaction staging: one
/// visible row per version/schema/entity/file/untracked tuple.
#[derive(Debug, Clone, PartialEq, Eq)]
struct StateRowIdentity {
    untracked: bool,
    version_id: String,
    schema_key: String,
    entity_id: String,
    file_id: Option<String>,
}

impl StateRowIdentity {
    fn from_row(row: &LiveStateRow) -> Self {
        Self {
            untracked: row.untracked,
            version_id: row.version_id.clone(),
            schema_key: row.schema_key.clone(),
            entity_id: row.entity_id.clone(),
            file_id: row.file_id.clone(),
        }
    }

    fn from_exact_parts(
        untracked: bool,
        version_id: String,
        schema_key: String,
        entity_id: String,
        file_id: NullableKeyFilter<String>,
    ) -> Option<Self> {
        let file_id = match file_id {
            NullableKeyFilter::Null => None,
            NullableKeyFilter::Value(value) => Some(value),
            NullableKeyFilter::Any => return None,
        };
        Some(Self {
            untracked,
            version_id,
            schema_key,
            entity_id,
            file_id,
        })
    }
}

fn encode_state_row(row: &LiveStateRow) -> Result<Vec<u8>, LixError> {
    serde_json::to_vec(row).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to encode live-state row: {error}"),
        )
    })
}

fn decode_state_row(bytes: &[u8]) -> Result<LiveStateRow, LixError> {
    serde_json::from_slice(bytes).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("failed to decode live-state row: {error}"),
        )
    })
}

fn encode_state_row_key(identity: &StateRowIdentity) -> Vec<u8> {
    let mut out = Vec::new();
    out.push(if identity.untracked { 1 } else { 0 });
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

async fn put_state_row(
    transaction: &mut dyn LixBackendTransaction,
    row: &LiveStateRow,
) -> Result<(), LixError> {
    let identity = StateRowIdentity::from_row(row);
    transaction
        .kv_put(
            LIVE_STATE_ROW_NAMESPACE,
            &encode_state_row_key(&identity),
            &encode_state_row(row)?,
        )
        .await
}

fn push_component(out: &mut Vec<u8>, value: &str) {
    let bytes = value.as_bytes();
    out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
    out.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_key_distinguishes_null_and_value_file_id() {
        let null_key = encode_state_row_key(&StateRowIdentity {
            untracked: true,
            version_id: "global".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: "key".to_string(),
            file_id: None,
        });
        let value_key = encode_state_row_key(&StateRowIdentity {
            untracked: true,
            version_id: "global".to_string(),
            schema_key: "lix_key_value".to_string(),
            entity_id: "key".to_string(),
            file_id: Some("file".to_string()),
        });

        assert_ne!(null_key, value_key);
    }
}
