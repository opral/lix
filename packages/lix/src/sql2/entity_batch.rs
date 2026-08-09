//! Terminal SQL projections over the canonical live-state scan.
//!
//! The entity capability does not own a storage/index read. It asks the
//! operation-owned `LiveStateReader::scan_batch` for the visible rows once and
//! projects that authenticated batch into snapshot bytes or primary keys.

use async_trait::async_trait;
use bytes::Bytes;

use crate::LixError;
use crate::entity_pk::EntityPk;
use crate::forktree::ForkTreeReadFacade;
use crate::live_state::{LiveStateReader, LiveStateScanRequest};
use crate::storage_adapter::StorageAdapterRead;

/// Optional private capability supplied by a SQL execution context.
///
/// The production implementation is only a terminal projection: it performs
/// one canonical `LiveStateReader::scan_batch` and returns no storage handle,
/// alternate visibility result, or legacy DTO. `None` remains available for
/// contexts that do not provide this capability.
#[async_trait]
pub(crate) trait EntitySnapshotReader: Send + Sync {
    /// Projects the complete authenticated row batch for non-direct SQL
    /// shapes. This remains a terminal capability over the operation-owned
    /// ForkTree reader; it is not a second visibility path.
    async fn scan_entity_rows(
        &self,
        _request: LiveStateScanRequest,
    ) -> Result<Option<crate::live_state::MaterializedLiveStateBatch>, LixError> {
        Ok(None)
    }

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
}

pub(crate) struct CurrentEntitySnapshotReader<S> {
    forktree: ForkTreeReadFacade<S>,
}

impl<S> CurrentEntitySnapshotReader<S> {
    pub(crate) fn new(forktree: ForkTreeReadFacade<S>) -> Self {
        Self { forktree }
    }
}

/// Terminal entity projection over a derived or transaction-owned live-state
/// capability. The capability must already be operation-owned; this adapter
/// only projects its authenticated rows and never acquires storage.
pub(crate) struct LiveStateEntitySnapshotReader {
    live_state: std::sync::Arc<dyn LiveStateReader>,
}

impl LiveStateEntitySnapshotReader {
    pub(crate) fn new(live_state: std::sync::Arc<dyn LiveStateReader>) -> Self {
        Self { live_state }
    }
}

#[async_trait]
impl EntitySnapshotReader for LiveStateEntitySnapshotReader {
    async fn scan_entity_rows(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<crate::live_state::MaterializedLiveStateBatch>, LixError> {
        validate_terminal_projection_request(&request)?;
        Ok(Some(self.live_state.scan_batch(&request).await?))
    }

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
}

#[async_trait]
impl<S> EntitySnapshotReader for CurrentEntitySnapshotReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn scan_entity_rows(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<crate::live_state::MaterializedLiveStateBatch>, LixError> {
        validate_terminal_projection_request(&request)?;
        Ok(Some(self.forktree.scan_batch(&request).await?))
    }

    async fn scan_entity_snapshots(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<Option<Bytes>>>, LixError> {
        validate_terminal_projection_request(&request)?;
        Ok(Some(
            canonical_snapshot_projection(&self.forktree, &request).await?,
        ))
    }

    async fn scan_entity_primary_keys(
        &self,
        request: LiveStateScanRequest,
    ) -> Result<Option<Vec<EntityPk>>, LixError> {
        validate_terminal_projection_request(&request)?;
        Ok(Some(
            canonical_primary_key_projection(&self.forktree, &request).await?,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::{ChangeId, CommitId};
    use crate::common::LixTimestamp;
    use crate::live_state::{MaterializedLiveStateBatch, MaterializedLiveStateRow};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingCanonicalReader {
        rows: MaterializedLiveStateBatch,
        scans: AtomicUsize,
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
            _request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<crate::live_state::MaterializedLiveStateExactBatch, LixError> {
            Err(LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "test reader does not provide exact rows",
            ))
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
    async fn non_direct_terminal_projection_still_uses_one_canonical_scan() {
        let reader = Arc::new(CountingCanonicalReader {
            rows: mixed_retention_rows(),
            scans: AtomicUsize::new(0),
        });
        let entity_reader = LiveStateEntitySnapshotReader::new(reader.clone());
        let request = LiveStateScanRequest {
            filter: crate::live_state::LiveStateFilter {
                constraints: vec![crate::live_state::ScanConstraint {
                    field: crate::live_state::ScanField::EntityPk,
                    operator: crate::live_state::ScanOperator::Eq(crate::Value::Text(
                        "a".to_owned(),
                    )),
                }],
                ..Default::default()
            },
            ..Default::default()
        };
        let rows = entity_reader
            .scan_entity_rows(request)
            .await
            .expect("non-direct request should use canonical row projection")
            .expect("canonical row projection should be available");
        assert_eq!(reader.scans.load(Ordering::SeqCst), 1);
        assert_eq!(rows.len(), 2);
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
