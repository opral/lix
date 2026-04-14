use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::sql::{PreparedPublicRead, ReadTimeProjectionPlan};
use crate::transaction::PendingOverlay;
use crate::{LixBackend, LixError, QueryResult, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct ReadTimeProjectionIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadTimeProjectionRow {
    pub surface_name: String,
    pub identity: Option<ReadTimeProjectionIdentity>,
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
    async fn execute_pending_overlay_public_read(
        &self,
        host: &dyn ReadExecutionHost,
        pending_overlay: Option<&dyn PendingOverlay>,
        public_read: &PreparedPublicRead,
    ) -> Result<QueryResult, LixError>;
}

#[async_trait(?Send)]
pub trait PendingPublicReadTransaction {
    async fn require_live_state_ready(&mut self) -> Result<(), LixError>;
}
