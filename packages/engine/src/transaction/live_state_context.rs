#![allow(dead_code)]

use std::collections::BTreeSet;
use std::sync::Arc;

use async_trait::async_trait;

use crate::live_state::{ExactRowRequest, LiveRow, LiveStateContext, LiveStateScanRequest};
use crate::transaction::{PendingSemanticRow, PendingWriteOverlay};
use crate::{LixError, NullableKeyFilter};

/// Transaction-visible live-state view.
///
/// SQL execution should only depend on `LiveStateContext`. The session or
/// transaction layer owns composing committed state with staged write overlays.
pub(crate) struct TransactionLiveStateContext {
    committed: Arc<dyn LiveStateContext>,
    pending_overlay: Option<PendingWriteOverlay>,
}

impl TransactionLiveStateContext {
    pub(crate) fn new(
        committed: Arc<dyn LiveStateContext>,
        pending_overlay: Option<PendingWriteOverlay>,
    ) -> Self {
        Self {
            committed,
            pending_overlay,
        }
    }
}

#[async_trait]
impl LiveStateContext for TransactionLiveStateContext {
    async fn scan(&self, request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
        let pending_rows = self
            .pending_overlay
            .as_ref()
            .map(|overlay| pending_overlay_live_rows_for_scan(overlay, request))
            .unwrap_or_default();
        let mut pending_identities = pending_rows
            .iter()
            .map(live_row_identity)
            .collect::<BTreeSet<_>>();
        let mut rows = pending_rows;

        for row in self.committed.scan(request).await? {
            if pending_identities.insert(live_row_identity(&row)) {
                rows.push(row);
            }
        }

        if let Some(limit) = request.limit {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    async fn load_exact(&self, request: &ExactRowRequest) -> Result<Option<LiveRow>, LixError> {
        if let Some(overlay) = &self.pending_overlay {
            for row in overlay.visible_all_semantic_rows() {
                if pending_row_matches_exact_request(&row, request) {
                    return Ok(Some(live_row_from_pending(row)));
                }
            }
        }

        self.committed.load_exact(request).await
    }
}

fn pending_overlay_live_rows_for_scan(
    pending_overlay: &PendingWriteOverlay,
    request: &LiveStateScanRequest,
) -> Vec<LiveRow> {
    pending_overlay
        .visible_all_semantic_rows()
        .into_iter()
        .filter(|row| pending_row_matches_scan_request(row, request))
        .map(live_row_from_pending)
        .collect()
}

fn pending_row_matches_scan_request(
    row: &PendingSemanticRow,
    request: &LiveStateScanRequest,
) -> bool {
    if row.tombstone && !request.filter.include_tombstones {
        return false;
    }
    if !request.filter.schema_keys.is_empty()
        && !request.filter.schema_keys.contains(&row.schema_key)
    {
        return false;
    }
    if !request.filter.entity_ids.is_empty() && !request.filter.entity_ids.contains(&row.entity_id)
    {
        return false;
    }
    if !request.filter.version_ids.is_empty()
        && !request.filter.version_ids.contains(&row.version_id)
    {
        return false;
    }
    nullable_key_matches_filters(&row.file_id, &request.filter.file_ids)
        && nullable_key_matches_filters(&row.plugin_key, &request.filter.plugin_keys)
}

fn pending_row_matches_exact_request(row: &PendingSemanticRow, request: &ExactRowRequest) -> bool {
    !row.tombstone
        && row.schema_key == request.schema_key
        && row.entity_id == request.entity_id
        && row.version_id == request.version_id
        && nullable_key_matches_filter(&row.file_id, &request.file_id)
}

fn nullable_key_matches_filters(
    value: &Option<String>,
    filters: &[NullableKeyFilter<String>],
) -> bool {
    filters.is_empty()
        || filters
            .iter()
            .any(|filter| nullable_key_matches_filter(value, filter))
}

fn nullable_key_matches_filter(value: &Option<String>, filter: &NullableKeyFilter<String>) -> bool {
    match filter {
        NullableKeyFilter::Any => true,
        NullableKeyFilter::Null => value.is_none(),
        NullableKeyFilter::Value(expected) => value.as_ref() == Some(expected),
    }
}

fn live_row_from_pending(row: PendingSemanticRow) -> LiveRow {
    LiveRow {
        entity_id: row.entity_id,
        file_id: row.file_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        version_id: row.version_id.clone(),
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: row.change_id,
        commit_id: None,
        global: row.version_id == crate::version::GLOBAL_VERSION_ID,
        untracked: row.untracked,
        created_at: None,
        updated_at: None,
        snapshot_content: row.snapshot_content,
    }
}

fn live_row_identity(
    row: &LiveRow,
) -> (
    bool,
    String,
    String,
    Option<String>,
    String,
    Option<String>,
    String,
) {
    (
        row.untracked,
        row.schema_key.clone(),
        row.entity_id.clone(),
        row.file_id.clone(),
        row.version_id.clone(),
        row.plugin_key.clone(),
        row.schema_version.clone(),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::json;

    use crate::live_state::{
        ExactRowRequest, LiveRow, LiveStateContext, LiveStateFilter, LiveStateProjection,
        LiveStateScanRequest,
    };
    use crate::sql::{MutationOperation, MutationRow};
    use crate::transaction::build_direct_mutation_transaction_write_delta;
    use crate::{LixError, NullableKeyFilter};

    use super::TransactionLiveStateContext;

    struct EmptyLiveStateContext;

    #[async_trait]
    impl LiveStateContext for EmptyLiveStateContext {
        async fn scan(&self, _request: &LiveStateScanRequest) -> Result<Vec<LiveRow>, LixError> {
            Ok(vec![])
        }

        async fn load_exact(
            &self,
            _request: &ExactRowRequest,
        ) -> Result<Option<LiveRow>, LixError> {
            Ok(None)
        }
    }

    fn staged_lix_key_value_overlay() -> crate::transaction::PendingWriteOverlay {
        let delta = build_direct_mutation_transaction_write_delta(
            vec![MutationRow {
                operation: MutationOperation::Insert,
                entity_id: "entity-1".to_string(),
                schema_key: "lix_key_value".to_string(),
                schema_version: "1".to_string(),
                file_id: None,
                version_id: "version-a".to_string(),
                plugin_key: None,
                snapshot_content: Some(json!({
                    "key": "hello",
                    "value": "world",
                })),
                metadata: Some("{\"source\":\"transaction\"}".to_string()),
                untracked: false,
            }],
            None,
        )
        .expect("direct mutation delta should build");
        delta
            .pending_write_overlay()
            .expect("direct mutation delta should expose pending overlay")
    }

    #[tokio::test]
    async fn scan_sees_pending_overlay_rows() {
        let live_state = TransactionLiveStateContext::new(
            Arc::new(EmptyLiveStateContext),
            Some(staged_lix_key_value_overlay()),
        );

        let rows = live_state
            .scan(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_ids: vec!["entity-1".to_string()],
                    version_ids: vec!["version-a".to_string()],
                    ..LiveStateFilter::default()
                },
                projection: LiveStateProjection::default(),
                limit: None,
            })
            .await
            .expect("transaction live-state scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, "entity-1");
        assert_eq!(rows[0].schema_key, "lix_key_value");
        assert_eq!(rows[0].schema_version, "1");
        assert_eq!(rows[0].version_id, "version-a");
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
        assert_eq!(
            rows[0].metadata.as_deref(),
            Some("{\"source\":\"transaction\"}")
        );
    }

    #[tokio::test]
    async fn load_exact_prefers_pending_overlay_rows() {
        let live_state = TransactionLiveStateContext::new(
            Arc::new(EmptyLiveStateContext),
            Some(staged_lix_key_value_overlay()),
        );

        let row = live_state
            .load_exact(&ExactRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: "version-a".to_string(),
                entity_id: "entity-1".to_string(),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("transaction live-state exact load should succeed")
            .expect("pending row should be visible");

        assert_eq!(row.entity_id, "entity-1");
        assert_eq!(row.version_id, "version-a");
        assert_eq!(
            row.snapshot_content.as_deref(),
            Some("{\"key\":\"hello\",\"value\":\"world\"}")
        );
    }
}
