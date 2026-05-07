use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::live_state::{
    LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::schema_registry::SchemaRegistry;
use crate::transaction::domain::Domain;
use crate::transaction::live_state_overlay::overlay_scan_rows;
use crate::transaction::normalization::TransactionSchemaCatalog;
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::LixError;

pub(crate) struct TransactionSchemaResolver {
    registry: Arc<SchemaRegistry>,
    catalogs_by_domain: BTreeMap<Domain, TransactionSchemaCatalogEntry>,
}

enum TransactionSchemaCatalogEntry {
    VisibleSchemas(Vec<JsonValue>),
    Catalog(TransactionSchemaCatalog),
}

impl TransactionSchemaResolver {
    pub(crate) fn new(registry: Arc<SchemaRegistry>) -> Self {
        Self {
            registry,
            catalogs_by_domain: BTreeMap::new(),
        }
    }

    async fn load_catalog_for_version(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: Option<&PreparedStateRowOverlay>,
        version_id: &str,
        untracked: bool,
    ) -> Result<(), LixError> {
        let domain = Domain::schema_catalog(version_id.to_string(), untracked);
        let needs_load = !self.catalogs_by_domain.contains_key(&domain);
        if needs_load {
            let schemas = if let Some(staged) = staged {
                let reader = TransactionSchemaLiveStateReader {
                    base: live_state,
                    staged,
                };
                self.registry
                    .visible_schemas_for_domain(&reader, version_id, untracked)
                    .await?
            } else {
                self.registry
                    .visible_schemas_for_domain(live_state, version_id, untracked)
                    .await?
            };
            self.catalogs_by_domain.insert(
                domain.clone(),
                TransactionSchemaCatalogEntry::VisibleSchemas(schemas),
            );
        }

        let should_materialize = self
            .catalogs_by_domain
            .get(&domain)
            .is_some_and(|entry| matches!(entry, TransactionSchemaCatalogEntry::VisibleSchemas(_)));
        if should_materialize {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_schema_catalog_load();
            let entry = self
                .catalogs_by_domain
                .remove(&domain)
                .expect("schema catalog entry should exist after load");
            let TransactionSchemaCatalogEntry::VisibleSchemas(schemas) = entry else {
                unreachable!("catalog entry was checked as raw visible schemas");
            };
            let catalog = TransactionSchemaCatalog::from_visible_schemas(&schemas)?;
            self.catalogs_by_domain
                .insert(domain, TransactionSchemaCatalogEntry::Catalog(catalog));
        }
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        version_id: &str,
        untracked: bool,
    ) -> Result<&mut TransactionSchemaCatalog, LixError> {
        self.load_catalog_for_version(live_state, Some(staged), version_id, untracked)
            .await?;
        let domain = Domain::schema_catalog(version_id.to_string(), untracked);
        match self
            .catalogs_by_domain
            .get_mut(&domain)
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
        untracked: bool,
    ) -> Result<&TransactionSchemaCatalog, LixError> {
        self.load_catalog_for_version(live_state, None, version_id, untracked)
            .await?;
        let domain = Domain::schema_catalog(version_id.to_string(), untracked);
        match self
            .catalogs_by_domain
            .get(&domain)
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
        self.catalogs_by_domain.insert(
            Domain::schema_catalog(version_id, true),
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
