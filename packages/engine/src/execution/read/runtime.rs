use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::sql::ReadTimeProjectionPlan;
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Default)]
pub(crate) struct ReadTimeProjectionIdentity {
    pub schema_key: String,
    pub version_id: String,
    pub entity_id: String,
    pub file_id: String,
}

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ReadTimeProjectionRow {
    pub surface_name: String,
    pub identity: Option<ReadTimeProjectionIdentity>,
    pub values: BTreeMap<String, Value>,
}

#[async_trait(?Send)]
pub(crate) trait ReadExecutionHost {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
        artifact: &ReadTimeProjectionPlan,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError>;
}

#[async_trait(?Send)]
pub(crate) trait PendingPublicReadTransaction {
    async fn require_live_state_ready(&mut self) -> Result<(), LixError>;
}
