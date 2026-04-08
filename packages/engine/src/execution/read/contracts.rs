use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::contracts::artifacts::RowIdentity;
use crate::{LixBackend, LixError, Value};

#[derive(Debug, Clone, PartialEq, Default)]
pub(crate) struct ReadTimeProjectionRow {
    pub(crate) surface_name: String,
    pub(crate) identity: Option<RowIdentity>,
    pub(crate) values: BTreeMap<String, Value>,
}

#[async_trait(?Send)]
pub(crate) trait ReadExecutionBindings {
    async fn derive_read_time_projection_rows(
        &self,
        backend: &dyn LixBackend,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError>;
}
