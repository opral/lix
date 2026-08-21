//! Direct current-state snapshot serving for current SQL row scans.
//!
//! The generic live-state reader intentionally exposes fully materialized
//! engine rows. The committed current-state index has a narrower, private capability:
//! it can serve durable snapshot bytes directly. Arrow providers and native
//! public-result reads consume those same bytes, keeping visibility proof in
//! one place and leaving every unsupported shape on the established row path.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{Array, BooleanArray, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::physical_plan::Statistics;

use crate::LixError;
use crate::hot_state::{
    HotStateContext, HotStateRowFilter, HotStateScanRequest, RowColumnarShadowMaskCache,
    RowColumnarShadowMaskKey,
};
use crate::row_pk::RowPk;
use crate::storage_adapter::StorageAdapterRead;

#[derive(Clone, Debug)]
#[allow(dead_code)] // Consumed by the source-wide statistics cache in the provider layer.
pub(crate) struct RowColumnarScanLayout {
    pub(crate) id: crate::columnar_row_group::RowGroupSetId,
    pub(crate) manifest: Arc<crate::columnar_row_group::RowGroupManifest>,
    pub(crate) manifest_digest: [u8; 32],
    pub(crate) overlay: Arc<Vec<crate::hot_state::RowColumnarOverlayRow>>,
    pub(crate) branch_id: Arc<str>,
    pub(crate) head_commit_id: crate::changelog::CommitId,
    pub(crate) current_state_revision: u64,
    pub(crate) live_count: u64,
}

/// Optional private capability supplied only by committed read sessions.
///
/// Returning `None` is the normal conservative answer: generic contexts,
/// transaction-local staged state, retention-scoped reads, and unsupported
/// SQL shapes all retain the existing materialized-row implementation. A
/// successful result has one entry per live current-state member, ordered by
/// logical row primary key ascending. File-backed members sharing a
/// primary key remain separate adjacent entries; a caller that relies on a
/// stronger tie order must retain the general SQL path.
#[async_trait]
pub(crate) trait RowSnapshotReader: Send + Sync {
    /// Returns primary keys from the same committed direct-scan proof as raw
    /// snapshots. Providers use this only when every projected SQL field is
    /// an exact primary-key component, avoiding a redundant JSON decode while
    /// leaving all relational operators to DataFusion.
    async fn scan_row_primary_keys(
        &self,
        _request: HotStateScanRequest,
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        Ok(None)
    }

    /// Returns raw durable typed payloads and their authenticated storage
    /// identities for one exclusive packed collection. Protocol v69 providers
    /// can project these directly without constructing compatibility JSON or
    /// a generic materialized current-state batch.
    async fn scan_row_snapshots(
        &self,
        _request: HotStateScanRequest,
    ) -> Result<Option<crate::tracked_state::ExclusiveRowSnapshotBatch>, LixError> {
        Ok(None)
    }

    async fn plan_row_columnar_scan(
        &self,
        _request: HotStateScanRequest,
    ) -> Result<Option<Arc<RowColumnarScanLayout>>, LixError> {
        Ok(None)
    }

    async fn load_row_columnar_group(
        &self,
        _layout: Arc<RowColumnarScanLayout>,
        _group_index: usize,
        _projection: Vec<usize>,
    ) -> Result<RecordBatch, LixError> {
        Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "row snapshot reader does not support columnar groups".to_string(),
        ))
    }

    /// Returns an exact keep mask for one immutable base group. Implementors
    /// may cache it because both the row-group set and the sorted shadow
    /// identity digest are content-addressed inputs.
    async fn row_columnar_shadow_mask(
        &self,
        layout: Arc<RowColumnarScanLayout>,
        group_index: usize,
        identity_column: usize,
        shadow_identities: Arc<HashSet<String, ahash::RandomState>>,
        _shadow_identity_digest: [u8; 32],
    ) -> Result<Arc<BooleanArray>, LixError> {
        Ok(Arc::new(
            load_row_columnar_shadow_mask(
                self,
                layout,
                group_index,
                identity_column,
                shadow_identities.as_ref(),
            )
            .await?,
        ))
    }

    #[allow(dead_code)]
    async fn cached_row_columnar_statistics(
        &self,
        _layout: &RowColumnarScanLayout,
        _group_index: usize,
        _shadow_identity_digest: [u8; 32],
        _projection: &[usize],
    ) -> Result<Option<Statistics>, LixError> {
        Ok(None)
    }

    #[allow(dead_code)]
    async fn cache_row_columnar_statistics(
        &self,
        _layout: &RowColumnarScanLayout,
        _group_index: usize,
        _shadow_identity_digest: [u8; 32],
        _projection: Vec<usize>,
        _statistics: Statistics,
    ) -> Result<(), LixError> {
        Ok(())
    }

    async fn cached_row_columnar_batch(
        &self,
        _layout: &RowColumnarScanLayout,
        _group_index: usize,
        _shadow_identity_digest: [u8; 32],
        _projection: &[usize],
    ) -> Result<Option<Arc<RecordBatch>>, LixError> {
        Ok(None)
    }

    async fn cache_row_columnar_batch(
        &self,
        _layout: &RowColumnarScanLayout,
        _group_index: usize,
        _shadow_identity_digest: [u8; 32],
        _projection: Vec<usize>,
        batch: Arc<RecordBatch>,
    ) -> Result<Arc<RecordBatch>, LixError> {
        Ok(batch)
    }
}

pub(crate) struct CurrentRowSnapshotReader<S> {
    hot_state: Arc<HotStateContext>,
    store: S,
    row_columnar_shadow_masks: Arc<Mutex<RowColumnarShadowMaskCache>>,
    row_decoded_columns: crate::hot_state::RowDecodedColumnCache,
}

impl<S> CurrentRowSnapshotReader<S> {
    pub(crate) fn new(hot_state: Arc<HotStateContext>, store: S) -> Self {
        let row_columnar_shadow_masks = hot_state.row_columnar_scan_cache();
        let row_decoded_columns = hot_state.row_decoded_column_cache();
        Self {
            hot_state,
            store,
            row_columnar_shadow_masks,
            row_decoded_columns,
        }
    }
}

#[async_trait]
impl<S> RowSnapshotReader for CurrentRowSnapshotReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn scan_row_primary_keys(
        &self,
        request: HotStateScanRequest,
    ) -> Result<Option<Vec<RowPk>>, LixError> {
        if !direct_row_snapshot_request(&request) {
            return Ok(None);
        }
        self.hot_state
            .reader(self.store.clone())
            .scan_direct_row_primary_keys(&request)
            .await
    }

    async fn scan_row_snapshots(
        &self,
        request: HotStateScanRequest,
    ) -> Result<Option<crate::tracked_state::ExclusiveRowSnapshotBatch>, LixError> {
        if !direct_row_snapshot_request(&request) {
            return Ok(None);
        }
        self.hot_state
            .reader(self.store.clone())
            .scan_direct_row_snapshots(&request)
            .await
    }

    async fn plan_row_columnar_scan(
        &self,
        request: HotStateScanRequest,
    ) -> Result<Option<Arc<RowColumnarScanLayout>>, LixError> {
        if !direct_row_columnar_request(&request) {
            return Ok(None);
        }
        Ok(self
            .hot_state
            .reader(self.store.clone())
            .plan_direct_row_columnar_scan(&request)
            .await?
            .map(
                |(
                    id,
                    manifest,
                    manifest_digest,
                    overlay,
                    branch_id,
                    head_commit_id,
                    current_state_revision,
                    live_count,
                )| {
                    Arc::new(RowColumnarScanLayout {
                        id,
                        manifest,
                        manifest_digest,
                        overlay,
                        branch_id: Arc::from(branch_id),
                        head_commit_id,
                        current_state_revision,
                        live_count,
                    })
                },
            ))
    }

    async fn load_row_columnar_group(
        &self,
        layout: Arc<RowColumnarScanLayout>,
        group_index: usize,
        projection: Vec<usize>,
    ) -> Result<RecordBatch, LixError> {
        let schema =
            crate::columnar_row_group::row_group_projected_schema(&layout.manifest, &projection)?;
        let row_count = layout
            .manifest
            .groups
            .get(group_index)
            .ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("row-group index {group_index} is outside the manifest"),
                )
            })?
            .row_count as usize;
        let arrays = self
            .row_decoded_columns
            .load_projection(
                &self.store,
                layout.id,
                layout.manifest_digest,
                &layout.manifest,
                group_index,
                &projection,
            )
            .await?;
        if projection.is_empty() {
            return RecordBatch::try_new_with_options(
                schema,
                arrays,
                &datafusion::arrow::record_batch::RecordBatchOptions::new()
                    .with_row_count(Some(row_count)),
            )
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()));
        }
        RecordBatch::try_new(schema, arrays)
            .map_err(|error| LixError::new(LixError::CODE_INTERNAL_ERROR, error.to_string()))
    }

    async fn row_columnar_shadow_mask(
        &self,
        layout: Arc<RowColumnarScanLayout>,
        group_index: usize,
        identity_column: usize,
        shadow_identities: Arc<HashSet<String, ahash::RandomState>>,
        shadow_identity_digest: [u8; 32],
    ) -> Result<Arc<BooleanArray>, LixError> {
        let key = row_columnar_cache_key(&layout, group_index, shadow_identity_digest);
        if let Some(mask) = self
            .row_columnar_shadow_masks
            .lock()
            .map_err(|_| row_columnar_mask_error("row columnar shadow-mask cache poisoned"))?
            .get(&key)
        {
            return Ok(mask);
        }

        let mask = Arc::new(
            load_row_columnar_shadow_mask(
                self,
                layout,
                group_index,
                identity_column,
                shadow_identities.as_ref(),
            )
            .await?,
        );
        Ok(self
            .row_columnar_shadow_masks
            .lock()
            .map_err(|_| row_columnar_mask_error("row columnar shadow-mask cache poisoned"))?
            .insert(key, mask))
    }

    async fn cached_row_columnar_statistics(
        &self,
        layout: &RowColumnarScanLayout,
        group_index: usize,
        shadow_identity_digest: [u8; 32],
        projection: &[usize],
    ) -> Result<Option<Statistics>, LixError> {
        let key = row_columnar_cache_key(layout, group_index, shadow_identity_digest);
        Ok(self
            .row_columnar_shadow_masks
            .lock()
            .map_err(|_| row_columnar_mask_error("row columnar shadow-mask cache poisoned"))?
            .statistics(&key, projection))
    }

    async fn cache_row_columnar_statistics(
        &self,
        layout: &RowColumnarScanLayout,
        group_index: usize,
        shadow_identity_digest: [u8; 32],
        projection: Vec<usize>,
        statistics: Statistics,
    ) -> Result<(), LixError> {
        let key = row_columnar_cache_key(layout, group_index, shadow_identity_digest);
        self.row_columnar_shadow_masks
            .lock()
            .map_err(|_| row_columnar_mask_error("row columnar shadow-mask cache poisoned"))?
            .insert_statistics(&key, projection, statistics);
        Ok(())
    }

    async fn cached_row_columnar_batch(
        &self,
        layout: &RowColumnarScanLayout,
        group_index: usize,
        shadow_identity_digest: [u8; 32],
        projection: &[usize],
    ) -> Result<Option<Arc<RecordBatch>>, LixError> {
        let key = row_columnar_cache_key(layout, group_index, shadow_identity_digest);
        Ok(self
            .row_columnar_shadow_masks
            .lock()
            .map_err(|_| row_columnar_mask_error("row columnar scan cache poisoned"))?
            .batch(&key, projection))
    }

    async fn cache_row_columnar_batch(
        &self,
        layout: &RowColumnarScanLayout,
        group_index: usize,
        shadow_identity_digest: [u8; 32],
        projection: Vec<usize>,
        batch: Arc<RecordBatch>,
    ) -> Result<Arc<RecordBatch>, LixError> {
        let key = row_columnar_cache_key(layout, group_index, shadow_identity_digest);
        Ok(self
            .row_columnar_shadow_masks
            .lock()
            .map_err(|_| row_columnar_mask_error("row columnar scan cache poisoned"))?
            .insert_batch(key, projection, batch))
    }
}

fn row_columnar_cache_key(
    layout: &RowColumnarScanLayout,
    group_index: usize,
    shadow_identity_digest: [u8; 32],
) -> RowColumnarShadowMaskKey {
    RowColumnarShadowMaskKey {
        row_groups: layout.id,
        branch_id: Arc::clone(&layout.branch_id),
        head_commit_id: layout.head_commit_id,
        current_state_revision: layout.current_state_revision,
        shadow_identity_digest,
        group_index,
    }
}

async fn load_row_columnar_shadow_mask<R: RowSnapshotReader + ?Sized>(
    reader: &R,
    layout: Arc<RowColumnarScanLayout>,
    group_index: usize,
    identity_column: usize,
    shadow_identities: &HashSet<String, ahash::RandomState>,
) -> Result<BooleanArray, LixError> {
    let batch = reader
        .load_row_columnar_group(layout, group_index, vec![identity_column])
        .await?;
    let identities = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| row_columnar_mask_error("row columnar identity column is not Utf8"))?;
    if identities.null_count() != 0 {
        return Err(row_columnar_mask_error(
            "row columnar identity column contains NULL",
        ));
    }
    Ok(BooleanArray::from(
        (0..identities.len())
            .map(|index| !shadow_identities.contains(identities.value(index)))
            .collect::<Vec<_>>(),
    ))
}

fn row_columnar_mask_error(message: &str) -> LixError {
    LixError::new(LixError::CODE_INTERNAL_ERROR, message.to_owned())
}

/// The raw snapshot plane is deliberately narrower than the general
/// live-state reader. It has no identity/filter evaluator beyond exact row
/// PKs, so only a no-tombstone request without file or residual constraints
/// can use it. Both Arrow and public-result consumers add their own
/// output-shape checks above this shared serving boundary.
fn direct_row_snapshot_request(request: &HotStateScanRequest) -> bool {
    matches!(request.filter.rows, HotStateRowFilter::All)
        && !request.filter.include_tombstones
        && request.filter.untracked.is_none()
        && request.filter.file_ids.is_empty()
        && request.filter.constraints.is_empty()
}

fn direct_row_columnar_request(request: &HotStateScanRequest) -> bool {
    direct_row_snapshot_request(request)
        && request.filter.row_pks.is_empty()
        && request.filter.row_pk_lower.is_none()
        && request.filter.row_pk_upper.is_none()
        && request.limit.is_none()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cache_key_test_layout() -> RowColumnarScanLayout {
        RowColumnarScanLayout {
            id: crate::columnar_row_group::RowGroupSetId::new([41; 16]),
            manifest: Arc::new(crate::columnar_row_group::RowGroupManifest {
                namespace: "cache-key-test".to_owned(),
                metadata: std::collections::HashMap::new(),
                fields: Vec::new(),
                groups: Vec::new(),
                encoded_digest: [0; 32],
            }),
            manifest_digest: [42; 32],
            overlay: Arc::new(Vec::new()),
            branch_id: Arc::from("branch-a"),
            head_commit_id: crate::changelog::CommitId::for_test_label("cache-key-head"),
            current_state_revision: 17,
            live_count: 0,
        }
    }

    #[test]
    fn columnar_cache_key_preserves_every_layout_dimension() {
        let layout = cache_key_test_layout();
        let key = row_columnar_cache_key(&layout, 3, [43; 32]);

        assert_eq!(key.row_groups, layout.id);
        assert_eq!(key.branch_id, layout.branch_id);
        assert_eq!(key.head_commit_id, layout.head_commit_id);
        assert_eq!(key.current_state_revision, layout.current_state_revision);
        assert_eq!(key.shadow_identity_digest, [43; 32]);
        assert_eq!(key.group_index, 3);
    }

    #[test]
    fn exact_primary_key_and_limit_bypass_columnar_scan() {
        let mut request = HotStateScanRequest::default();
        assert!(direct_row_columnar_request(&request));

        request.filter.row_pks.push(RowPk::single("point-read"));
        assert!(!direct_row_columnar_request(&request));

        request.filter.row_pks.clear();
        request.limit = Some(1);
        assert!(!direct_row_columnar_request(&request));
    }
}
