use crate::live_state::MaterializedLiveStateRow;
use crate::live_state::{LiveStateReader, LiveStateScanRequest};
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::LixError;

pub(crate) async fn overlay_scan_rows(
    base: &dyn LiveStateReader,
    staged: &PreparedStateRowOverlay,
    request: &LiveStateScanRequest,
) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
    let mut candidate_request = request.clone();
    candidate_request.limit = None;
    candidate_request.filter.include_tombstones = true;
    let staged_parts = staged.scan_parts(&candidate_request)?;
    let rows = base.scan_rows(&candidate_request).await?;
    Ok(crate::live_state::resolve_overlay_rows(
        rows,
        staged_parts.rows,
        &request.filter.version_ids,
        request.filter.include_tombstones,
        request.limit,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;

    use super::*;
    use crate::catalog::SchemaPlanId;
    use crate::entity_identity::EntityIdentity;
    use crate::functions::{FunctionProvider, SharedFunctionProvider};
    use crate::live_state::{LiveStateFilter, LiveStateRowRequest};
    use crate::transaction::staging::TransactionWriteBuffer;
    use crate::transaction::types::{
        PreparedRowFacts, PreparedStateRow, PreparedTransactionWrite, TransactionWriteMode,
    };

    #[tokio::test]
    async fn overlay_applies_limit_after_staged_tombstones_hide_base_rows() {
        let base = LimitAwareBaseReader {
            rows: vec![row("a", "A"), row("b", "B")],
        };
        let staged_writes = Arc::new(TransactionWriteBuffer::new(SharedFunctionProvider::new(
            Box::new(TestFunctionProvider) as Box<dyn FunctionProvider + Send>,
        )));
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![tombstone("a")],
            })
            .expect("staged tombstone should be accepted");
        let staged = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged writes");

        let rows = overlay_scan_rows(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    version_ids: vec!["global".to_string()],
                    ..LiveStateFilter::default()
                },
                limit: Some(1),
                ..LiveStateScanRequest::default()
            },
        )
        .await
        .expect("overlay scan should succeed");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].entity_id, EntityIdentity::single("b"));
    }

    #[tokio::test]
    async fn overlay_keeps_base_tombstones_as_visibility_candidates() {
        let base = LimitAwareBaseReader {
            rows: vec![tombstone_row("a", "version-a")],
        };
        let staged_writes = Arc::new(TransactionWriteBuffer::new(SharedFunctionProvider::new(
            Box::new(TestFunctionProvider) as Box<dyn FunctionProvider + Send>,
        )));
        staged_writes
            .stage_write(PreparedTransactionWrite::Rows {
                mode: TransactionWriteMode::Replace,
                rows: vec![present("a", "staged-global")],
            })
            .expect("staged row should be accepted");
        let staged = staged_writes
            .staging_overlay()
            .expect("overlay should build from staged writes");

        let rows = overlay_scan_rows(
            &base,
            &staged,
            &LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_key_value".to_string()],
                    entity_ids: vec![EntityIdentity::single("a")],
                    version_ids: vec!["version-a".to_string()],
                    include_tombstones: false,
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            },
        )
        .await
        .expect("overlay scan should succeed");

        assert!(
            rows.is_empty(),
            "base tombstone should participate in winner selection before tombstone filtering"
        );
    }

    struct LimitAwareBaseReader {
        rows: Vec<MaterializedLiveStateRow>,
    }

    #[async_trait]
    impl LiveStateReader for LimitAwareBaseReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            let mut rows = self.rows.clone();
            if let Some(limit) = request.limit {
                rows.truncate(limit);
            }
            Ok(rows)
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    struct TestFunctionProvider;

    impl FunctionProvider for TestFunctionProvider {
        fn uuid_v7(&mut self) -> String {
            "test-uuid".to_string()
        }

        fn timestamp(&mut self) -> String {
            "test-timestamp".to_string()
        }
    }

    fn row(entity_id: &str, value: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot_content: Some(format!("{{\"key\":\"{entity_id}\",\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: "test-created-at".to_string(),
            updated_at: "test-updated-at".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: "global".to_string(),
        }
    }

    fn tombstone(entity_id: &str) -> PreparedStateRow {
        let mut row = present(entity_id, "deleted");
        row.snapshot = None;
        row
    }

    fn present(entity_id: &str, value: &str) -> PreparedStateRow {
        PreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_id: EntityIdentity::single(entity_id),
            schema_key: "lix_key_value".to_string(),
            file_id: None,
            snapshot: Some(
                crate::transaction::types::stage_json_from_value(
                    crate::transaction::types::TransactionJson::from_value_for_test(
                        serde_json::json!({ "key": entity_id, "value": value }),
                    ),
                    "test overlay row snapshot",
                )
                .expect("test snapshot should prepare"),
            ),
            metadata: None,
            origin: None,
            created_at: "test-created-at".to_string(),
            updated_at: "test-updated-at".to_string(),
            global: true,
            change_id: None,
            commit_id: None,
            untracked: true,
            version_id: "global".to_string(),
        }
    }

    fn tombstone_row(entity_id: &str, version_id: &str) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            deleted: true,
            snapshot_content: None,
            version_id: version_id.to_string(),
            global: version_id == "global",
            ..row(entity_id, "deleted")
        }
    }
}
