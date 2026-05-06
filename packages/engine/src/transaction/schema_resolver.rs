use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::live_state::{
    LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::schema_registry::SchemaRegistry;
use crate::transaction::live_state_overlay::overlay_scan_rows;
use crate::transaction::normalization::TransactionSchemaCatalog;
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::LixError;

pub(crate) struct TransactionSchemaResolver {
    registry: Arc<SchemaRegistry>,
    catalogs_by_version: BTreeMap<String, TransactionSchemaCatalogEntry>,
}

enum TransactionSchemaCatalogEntry {
    VisibleSchemas(Vec<JsonValue>),
    Catalog(TransactionSchemaCatalog),
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
        staged: Option<&PreparedStateRowOverlay>,
        version_id: &str,
    ) -> Result<(), LixError> {
        let needs_load = !self.catalogs_by_version.contains_key(version_id);
        if needs_load {
            let schemas = if let Some(staged) = staged {
                let reader = TransactionSchemaLiveStateReader {
                    base: live_state,
                    staged,
                };
                self.registry.visible_schemas(&reader, version_id).await?
            } else {
                self.registry
                    .visible_schemas(live_state, version_id)
                    .await?
            };
            self.catalogs_by_version.insert(
                version_id.to_string(),
                TransactionSchemaCatalogEntry::VisibleSchemas(schemas),
            );
        }

        let should_materialize = self
            .catalogs_by_version
            .get(version_id)
            .is_some_and(|entry| matches!(entry, TransactionSchemaCatalogEntry::VisibleSchemas(_)));
        if should_materialize {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_schema_catalog_load();
            let entry = self
                .catalogs_by_version
                .remove(version_id)
                .expect("schema catalog entry should exist after load");
            let TransactionSchemaCatalogEntry::VisibleSchemas(schemas) = entry else {
                unreachable!("catalog entry was checked as raw visible schemas");
            };
            let catalog = TransactionSchemaCatalog::from_visible_schemas(&schemas)?;
            self.catalogs_by_version.insert(
                version_id.to_string(),
                TransactionSchemaCatalogEntry::Catalog(catalog),
            );
        }
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        version_id: &str,
    ) -> Result<&mut TransactionSchemaCatalog, LixError> {
        self.load_catalog_for_version(live_state, Some(staged), version_id)
            .await?;
        match self
            .catalogs_by_version
            .get_mut(version_id)
            .expect("catalog cache should contain requested version")
        {
            TransactionSchemaCatalogEntry::Catalog(catalog) => Ok(catalog),
            TransactionSchemaCatalogEntry::VisibleSchemas(_) => {
                unreachable!("schema catalog should be materialized before mutable access")
            }
        }
    }

    pub(crate) async fn catalog_for_validation(
        &mut self,
        live_state: &dyn LiveStateReader,
        version_id: &str,
    ) -> Result<&TransactionSchemaCatalog, LixError> {
        self.load_catalog_for_version(live_state, None, version_id)
            .await?;
        match self
            .catalogs_by_version
            .get(version_id)
            .expect("catalog cache should contain requested version")
        {
            TransactionSchemaCatalogEntry::Catalog(catalog) => Ok(catalog),
            TransactionSchemaCatalogEntry::VisibleSchemas(_) => {
                unreachable!("schema catalog should be materialized before validation access")
            }
        }
    }

    pub(crate) fn remember_visible_schemas(
        &mut self,
        version_id: impl Into<String>,
        schemas: Vec<JsonValue>,
    ) {
        let version_id = version_id.into();
        self.catalogs_by_version.insert(
            version_id,
            TransactionSchemaCatalogEntry::VisibleSchemas(schemas),
        );
    }
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
