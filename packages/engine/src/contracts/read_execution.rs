use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::contracts::PendingView;
use crate::contracts::{PreparedPublicReadArtifact, ReadTimeProjectionRead, RowIdentity};
use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadTimeProjectionRow {
    pub surface_name: String,
    pub identity: Option<RowIdentity>,
    pub values: BTreeMap<String, Value>,
}

#[async_trait(?Send)]
pub trait ReadExecutionBindings {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        artifact: &ReadTimeProjectionRead,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError>;
}

#[async_trait(?Send)]
pub trait PendingPublicReadExecutionBackend {
    async fn execute_prepared_public_read_with_pending_view(
        &self,
        bindings: &dyn ReadExecutionBindings,
        pending_view: Option<&dyn PendingView>,
        public_read: &PreparedPublicReadArtifact,
    ) -> Result<QueryResult, LixError>;
}
