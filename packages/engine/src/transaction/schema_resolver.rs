use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::live_state::{
    LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::schema_registry::SchemaRegistry;
use crate::transaction::live_state_overlay::overlay_scan_rows;
use crate::transaction::normalization::{
    remember_pending_registered_schema, TransactionSchemaCatalog, REGISTERED_SCHEMA_KEY,
};
use crate::transaction::staging::{PreparedStateRowOverlay, PreparedWriteValidationSet};
use crate::LixError;

pub(crate) struct TransactionSchemaResolver {
    registry: Arc<SchemaRegistry>,
    catalogs_by_version: BTreeMap<String, TransactionSchemaCatalog>,
}

impl TransactionSchemaResolver {
    pub(crate) fn new(registry: Arc<SchemaRegistry>) -> Self {
        Self {
            registry,
            catalogs_by_version: BTreeMap::new(),
        }
    }

    async fn load_catalog_for_version(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        version_id: &str,
    ) -> Result<(), LixError> {
        if !self.catalogs_by_version.contains_key(version_id) {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_schema_catalog_load();
            let reader = TransactionSchemaLiveStateReader {
                base: live_state,
                staged,
            };
            let schemas = self.registry.visible_schemas(&reader, version_id).await?;
            let catalog = TransactionSchemaCatalog::from_visible_schemas(&schemas)?;
            self.catalogs_by_version
                .insert(version_id.to_string(), catalog);
        }
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        version_id: &str,
    ) -> Result<&mut TransactionSchemaCatalog, LixError> {
        self.load_catalog_for_version(live_state, staged, version_id)
            .await?;
        Ok(self
            .catalogs_by_version
            .get_mut(version_id)
            .expect("catalog cache should contain requested version"))
    }

    pub(crate) async fn catalog_for_validation(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged_writes: &PreparedWriteValidationSet<'_>,
        version_id: &str,
    ) -> Result<TransactionSchemaCatalog, LixError> {
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_transaction_schema_catalog_load();
        let schemas = self
            .registry
            .visible_schemas(live_state, version_id)
            .await?;
        let mut catalog = TransactionSchemaCatalog::from_visible_schemas(&schemas)?;
        absorb_registered_schema_writes(&mut catalog, staged_writes)?;
        Ok(catalog)
    }

    pub(crate) fn remember_visible_schemas(
        &mut self,
        version_id: impl Into<String>,
        schemas: Vec<JsonValue>,
    ) -> Result<(), LixError> {
        let version_id = version_id.into();
        let catalog = TransactionSchemaCatalog::from_visible_schemas(&schemas)?;
        self.catalogs_by_version.insert(version_id, catalog);
        Ok(())
    }
}

fn absorb_registered_schema_writes(
    schema_catalog: &mut TransactionSchemaCatalog,
    staged_writes: &PreparedWriteValidationSet<'_>,
) -> Result<(), LixError> {
    for row in staged_writes.rows() {
        if row.schema_key() == REGISTERED_SCHEMA_KEY {
            remember_pending_registered_schema(row.snapshot_json(), schema_catalog)?;
        }
    }
    Ok(())
}

struct TransactionSchemaLiveStateReader<'a> {
    base: &'a dyn LiveStateReader,
    staged: &'a PreparedStateRowOverlay,
}

#[async_trait]
impl LiveStateReader for TransactionSchemaLiveStateReader<'_> {
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        overlay_scan_rows(self.base, self.staged, request).await
    }

    async fn load_row(
        &self,
        request: &LiveStateRowRequest,
    ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
        self.base.load_row(request).await
    }
}
