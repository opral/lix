use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::catalog::{CatalogReadTimeProjectionRequest, SurfaceReadFreshness};
use crate::{LixBackend, LixBackendTransaction, LixError, Value};

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
        request: &CatalogReadTimeProjectionRequest,
    ) -> Result<Vec<ReadTimeProjectionRow>, LixError>;

    async fn ensure_projection_freshness_with_backend(
        &self,
        backend: &dyn LixBackend,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError>;

    async fn ensure_projection_freshness_in_transaction(
        &self,
        transaction: &mut dyn LixBackendTransaction,
        freshness_contract: SurfaceReadFreshness,
        resolved_relations: &[String],
    ) -> Result<(), LixError>;
}
