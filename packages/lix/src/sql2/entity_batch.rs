//! Terminal SQL projections over the canonical live-state scan.
//!
//! The entity capability does not own a storage/index read. It asks the
//! operation-owned `LiveStateReader::scan_batch` for the visible rows once and
//! projects that authenticated batch into snapshot bytes or primary keys.

use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::live_state::{LiveStateExactBatchRequest, LiveStateReader, LiveStateScanRequest};

/// Optional private capability supplied by a SQL execution context.
///
/// The production implementation is only a terminal projection: it performs
/// one canonical `LiveStateReader::scan_batch` and returns no storage handle,
/// alternate visibility result, or legacy DTO. `None` remains available for
/// contexts that do not provide this capability.
#[async_trait]
pub(crate) trait EntitySnapshotReader: Send + Sync {
    async fn scan_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError>;

    /// Returns primary keys from the same committed direct-scan proof as raw
    /// snapshots. Providers use this only when every projected SQL field is
    /// an exact primary-key component, avoiding a redundant JSON decode while
    /// leaving all relational operators to DataFusion.
    async fn scan_entity_primary_keys(
        &self,
        _request: LiveStateScanRequest,
    ) -> Result<Option<Vec<EntityPk>>, LixError> {
        Ok(None)
    }

    async fn load_exact_entity_snapshots(
        &self,
        _request: LiveStateExactBatchRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        Ok(None)
    }

    async fn load_exact_entity_primary_keys(
        &self,
        _request: LiveStateExactBatchRequest,
    ) -> Result<Option<Vec<EntityPk>>, LixError> {
        Ok(None)
    }
}

/// Terminal projection over an already operation-owned live-state reader.
/// This adapter only projects the authenticated batch; it does not acquire a
/// storage read, expose a store, or provide a second visibility authority.
pub(crate) struct CanonicalEntitySnapshotProjection {
    live_state: Arc<dyn LiveStateReader>,
}

impl CanonicalEntitySnapshotProjection {
    pub(crate) fn new(live_state: Arc<dyn LiveStateReader>) -> Self {
        Self { live_state }
    }
}

#[async_trait]
impl EntitySnapshotReader for CanonicalEntitySnapshotProjection {
    async fn scan_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        validate_terminal_projection_request(&request)?;
        Ok(Some(
            canonical_snapshot_projection(self.live_state.as_ref(), &request).await?,
        ))
    }

    async fn scan_entity_primary_keys(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<EntityPk>>, LixError> {
        validate_terminal_projection_request(&request)?;
        Ok(Some(
            canonical_primary_key_projection(self.live_state.as_ref(), &request).await?,
        ))
    }

    async fn load_exact_entity_snapshots(
        &self,
        request: LiveStateExactBatchRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        validate_exact_terminal_projection_request(&request)?;
        Ok(Some(
            canonical_exact_snapshot_projection(self.live_state.as_ref(), &request).await?,
        ))
    }

    async fn load_exact_entity_primary_keys(
        &self,
        request: LiveStateExactBatchRequest,
    ) -> Result<Option<Vec<EntityPk>>, LixError> {
        validate_exact_terminal_projection_request(&request)?;
        Ok(Some(
            canonical_exact_primary_key_projection(self.live_state.as_ref(), &request).await?,
        ))
    }
}

fn validate_terminal_projection_request(request: &LiveStateScanRequest) -> Result<(), LixError> {
    if request.filter.include_tombstones {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity terminal projection does not preserve tombstone rows",
        ));
    }
    Ok(())
}

fn validate_exact_terminal_projection_request(
    request: &LiveStateExactBatchRequest,
) -> Result<(), LixError> {
    if request.include_tombstones {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "entity terminal projection does not preserve tombstone rows",
        ));
    }
    Ok(())
}

async fn canonical_snapshot_projection<R>(
    reader: &R,
    request: &LiveStateScanRequest,
) -> Result<Vec<Option<Bytes>>, LixError>
where
    R: LiveStateReader + ?Sized,
{
    Ok(reader
        .scan_batch(request)
        .await?
        .into_identity_ordered_snapshots())
}

async fn canonical_primary_key_projection<R>(
    reader: &R,
    request: &LiveStateScanRequest,
) -> Result<Vec<EntityPk>, LixError>
where
    R: LiveStateReader + ?Sized,
{
    Ok(reader
        .scan_batch(request)
        .await?
        .into_identity_ordered_primary_keys())
}

async fn canonical_exact_snapshot_projection<R>(
    reader: &R,
    request: &LiveStateExactBatchRequest,
) -> Result<Vec<Option<Bytes>>, LixError>
where
    R: LiveStateReader + ?Sized,
{
    Ok(reader
        .load_exact_batch(request)
        .await?
        .into_present_batch()
        .into_identity_ordered_snapshots())
}

async fn canonical_exact_primary_key_projection<R>(
    reader: &R,
    request: &LiveStateExactBatchRequest,
) -> Result<Vec<EntityPk>, LixError>
where
    R: LiveStateReader + ?Sized,
{
    Ok(reader
        .load_exact_batch(request)
        .await?
        .into_present_batch()
        .into_identity_ordered_primary_keys())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::live_state::{MaterializedLiveStateBatch, MaterializedLiveStateRow};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCanonicalReader {
        rows: MaterializedLiveStateBatch,
        scans: AtomicUsize,
        exact_loads: AtomicUsize,
    }

    #[async_trait]
    impl LiveStateReader for CountingCanonicalReader {
        async fn scan_batch(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<MaterializedLiveStateBatch, LixError> {
            self.scans.fetch_add(1, Ordering::SeqCst);
            Ok(self.rows.clone())
        }

        async fn load_exact_batch(
            &self,
            _request: &LiveStateExactBatchRequest,
        ) -> Result<crate::live_state::MaterializedLiveStateExactBatch, LixError> {
            self.exact_loads.fetch_add(1, Ordering::SeqCst);
            Ok(
                crate::live_state::MaterializedLiveStateExactBatch::from_rows(
                    self.rows
                        .clone()
                        .into_rows()
                        .into_iter()
                        .map(Some)
                        .collect(),
                ),
            )
        }
    }

    fn mixed_retention_rows() -> MaterializedLiveStateBatch {
        MaterializedLiveStateBatch::from_rows(vec![
            row("b", "tracked", false),
            row("a", "untracked", true),
        ])
    }

    fn row(entity_pk: &str, value: &str, untracked: bool) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single(entity_pk),
            schema_key: "entity".to_string(),
            file_id: None,
            snapshot_content: Some(format!(r#"{{"value":"{value}"}}"#).into()),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00Z"),
            global: false,
            change_id: Some(ChangeId::for_test_label("change")),
            commit_id: Some(CommitId::for_test_label("commit")),
            untracked,
            branch_id: "branch".into(),
        }
    }

    #[tokio::test]
    async fn terminal_projections_use_one_canonical_scan_for_mixed_retention() {
        let reader = CountingCanonicalReader {
            rows: mixed_retention_rows(),
            scans: AtomicUsize::new(0),
            exact_loads: AtomicUsize::new(0),
        };
        let snapshots = canonical_snapshot_projection(&reader, &LiveStateScanRequest::default())
            .await
            .expect("canonical snapshot projection should succeed");
        assert_eq!(reader.scans.load(Ordering::SeqCst), 1);
        assert_eq!(
            snapshots,
            vec![
                Some(Bytes::from(r#"{"value":"untracked"}"#)),
                Some(Bytes::from(r#"{"value":"tracked"}"#)),
            ]
        );

        let reader = CountingCanonicalReader {
            rows: mixed_retention_rows(),
            scans: AtomicUsize::new(0),
            exact_loads: AtomicUsize::new(0),
        };
        let primary_keys =
            canonical_primary_key_projection(&reader, &LiveStateScanRequest::default())
                .await
                .expect("canonical primary-key projection should succeed");
        assert_eq!(reader.scans.load(Ordering::SeqCst), 1);
        assert_eq!(
            primary_keys,
            vec![EntityPk::single("a"), EntityPk::single("b")]
        );
    }

    #[tokio::test]
    async fn terminal_exact_projection_never_scans() {
        let reader = CountingCanonicalReader {
            rows: mixed_retention_rows(),
            scans: AtomicUsize::new(0),
            exact_loads: AtomicUsize::new(0),
        };
        let request = LiveStateExactBatchRequest {
            rows: vec![
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "entity".into(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
                    entity_pk: EntityPk::single("a"),
                    file_id: None,
                },
                crate::live_state::LiveStateExactRowRequest {
                    schema_key: "entity".into(),
                    branch_id: "01920000-0000-7000-8000-0000000000a1".into(),
                    entity_pk: EntityPk::single("b"),
                    file_id: None,
                },
            ],
            ..Default::default()
        };
        let snapshots = canonical_exact_snapshot_projection(&reader, &request)
            .await
            .expect("exact projection should succeed");
        assert_eq!(snapshots.len(), 2);
        assert_eq!(reader.scans.load(Ordering::SeqCst), 0);
        assert_eq!(reader.exact_loads.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn terminal_projection_rejects_tombstones_before_acquisition() {
        let request = LiveStateScanRequest {
            filter: crate::live_state::LiveStateFilter {
                include_tombstones: true,
                ..Default::default()
            },
            ..Default::default()
        };
        assert!(validate_terminal_projection_request(&request).is_err());
    }
}
