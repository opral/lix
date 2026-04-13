use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::contracts::PendingOverlayView;
use crate::contracts::{PreparedPublicRead, ReadTimeProjectionPlan, RowIdentity};
use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadTimeProjectionRow {
    pub surface_name: String,
    pub identity: Option<RowIdentity>,
    pub values: BTreeMap<String, Value>,
}

#[async_trait(?Send)]
pub trait ReadExecutionHost {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        artifact: &ReadTimeProjectionPlan,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError>;
}

#[async_trait(?Send)]
pub trait PendingPublicReadHost {
    async fn execute_prepared_public_read_with_pending_overlay_view(
        &self,
        host: &dyn ReadExecutionHost,
        pending_overlay_view: Option<&dyn PendingOverlayView>,
        public_read: &PreparedPublicRead,
    ) -> Result<QueryResult, LixError>;
}
