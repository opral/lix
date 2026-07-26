//! Direct tracked-head serving for broad SQL entity scans.
//!
//! The generic live-state reader intentionally exposes fully materialized
//! engine rows. SQL entity projection has a narrower need: it can decode the
//! durable tracked snapshot bytes straight into Arrow. Keeping that capability
//! here prevents Arrow types from leaking into the engine's general read
//! contract and leaves every unsupported shape on the established row path.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};

use crate::live_state::{LiveStateContext, LiveStateRowFilter, LiveStateScanRequest};
use crate::sql2::catalog::EntitySurfaceSpec;
use crate::sql2::entity_projection::{
    EntityProjectionDecoder, entity_projection_error_to_datafusion_error,
};
use crate::sql2::error::lix_error_to_datafusion_error;
use crate::storage_adapter::StorageAdapterRead;

/// One entity scan that may be served directly from durable tracked-head
/// bytes. It owns every planning input because providers outlive the SQL
/// planner that constructed them.
#[derive(Clone)]
pub(crate) struct EntityBatchRequest {
    pub(crate) spec: Arc<EntitySurfaceSpec>,
    pub(crate) schema: datafusion::arrow::datatypes::SchemaRef,
    pub(crate) live_request: LiveStateScanRequest,
}

/// Optional private capability supplied only by committed read sessions.
///
/// Returning `None` is the normal conservative answer: generic contexts,
/// transaction overlays, untracked state, exact reads, and unsupported
/// SQL shapes all retain the existing materialized-row implementation.
#[async_trait]
pub(crate) trait EntityBatchReader: Send + Sync {
    async fn scan_entity_batch(&self, request: EntityBatchRequest) -> Result<Option<RecordBatch>>;
}

pub(crate) struct TrackedEntityBatchReader<S> {
    live_state: Arc<LiveStateContext>,
    store: S,
}

impl<S> TrackedEntityBatchReader<S> {
    pub(crate) fn new(live_state: Arc<LiveStateContext>, store: S) -> Self {
        Self { live_state, store }
    }
}

#[async_trait]
impl<S> EntityBatchReader for TrackedEntityBatchReader<S>
where
    S: StorageAdapterRead + Clone + Send + Sync + 'static,
{
    async fn scan_entity_batch(&self, request: EntityBatchRequest) -> Result<Option<RecordBatch>> {
        if !matches!(request.live_request.filter.rows, LiveStateRowFilter::All)
            || request.schema.fields().is_empty()
            || request.live_request.filter.include_tombstones
            || !request.live_request.filter.entity_pks.is_empty()
            || !request.live_request.filter.file_ids.is_empty()
            || !request.live_request.filter.constraints.is_empty()
            || request
                .schema
                .fields()
                .iter()
                .any(|field| field.name().starts_with("lixcol_"))
        {
            return Ok(None);
        }

        let Some(rows) = self
            .live_state
            .reader(self.store.clone())
            .scan_direct_entity_snapshots(&request.live_request)
            .await
            .map_err(lix_error_to_datafusion_error)?
        else {
            return Ok(None);
        };
        let decoder = EntityProjectionDecoder::new(
            &request.spec,
            request
                .schema
                .fields()
                .iter()
                .map(|field| field.name().as_str()),
        )
        .map_err(entity_projection_error_to_datafusion_error)?;
        let columns = decoder
            .decode_arrow_columns(rows.iter().map(Option::as_deref))
            .map_err(entity_projection_error_to_datafusion_error)?;
        RecordBatch::try_new(request.schema, columns)
            .map(Some)
            .map_err(DataFusionError::from)
    }
}
