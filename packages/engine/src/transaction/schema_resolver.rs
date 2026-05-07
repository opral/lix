use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::domain::Domain;
use crate::live_state::{
    LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
};
use crate::schema_catalog::{SchemaCatalog, SchemaCatalogFact, SchemaCatalogSource};
use crate::transaction::live_state_overlay::overlay_scan_rows;
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::LixError;

pub(crate) struct TransactionSchemaResolver {
    source: Arc<SchemaCatalogSource>,
    catalogs_by_domain: BTreeMap<Domain, SchemaCatalogEntry>,
}

enum SchemaCatalogEntry {
    SchemaFacts(Vec<SchemaCatalogFact>),
    Catalog(SchemaCatalog),
}

impl TransactionSchemaResolver {
    pub(crate) fn new(source: Arc<SchemaCatalogSource>) -> Self {
        Self {
            source,
            catalogs_by_domain: BTreeMap::new(),
        }
    }

    async fn load_catalog_for_domain(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: Option<&PreparedStateRowOverlay>,
        domain: &Domain,
    ) -> Result<(), LixError> {
        let domain = domain.schema_catalog_domain();
        let needs_load = !self.catalogs_by_domain.contains_key(&domain);
        if needs_load {
            let facts = if let Some(staged) = staged {
                let reader = TransactionSchemaLiveStateReader {
                    base: live_state,
                    staged,
                };
                self.source
                    .schema_facts_for_domain(&reader, &domain)
                    .await?
            } else {
                self.source
                    .schema_facts_for_domain(live_state, &domain)
                    .await?
            };
            self.catalogs_by_domain
                .insert(domain.clone(), SchemaCatalogEntry::SchemaFacts(facts));
        }

        let should_materialize = self
            .catalogs_by_domain
            .get(&domain)
            .is_some_and(|entry| matches!(entry, SchemaCatalogEntry::SchemaFacts(_)));
        if should_materialize {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_schema_catalog_load();
            let entry = self
                .catalogs_by_domain
                .remove(&domain)
                .expect("schema catalog entry should exist after load");
            let SchemaCatalogEntry::SchemaFacts(facts) = entry else {
                unreachable!("catalog entry was checked as schema facts");
            };
            let catalog = SchemaCatalog::from_schema_facts(&facts)?;
            self.catalogs_by_domain
                .insert(domain, SchemaCatalogEntry::Catalog(catalog));
        }
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&mut SchemaCatalog, LixError> {
        self.load_catalog_for_domain(live_state, Some(staged), domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        match self
            .catalogs_by_domain
            .get_mut(&domain)
            .expect("catalog cache should contain requested version")
        {
            SchemaCatalogEntry::Catalog(catalog) => Ok(catalog),
            SchemaCatalogEntry::SchemaFacts(_) => {
                unreachable!("schema catalog should be materialized before mutable access")
            }
        }
    }

    pub(crate) async fn catalog_for_validation(
        &mut self,
        live_state: &dyn LiveStateReader,
        domain: &Domain,
    ) -> Result<&SchemaCatalog, LixError> {
        self.load_catalog_for_domain(live_state, None, domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        match self
            .catalogs_by_domain
            .get(&domain)
            .expect("catalog cache should contain requested version")
        {
            SchemaCatalogEntry::Catalog(catalog) => Ok(catalog),
            SchemaCatalogEntry::SchemaFacts(_) => {
                unreachable!("schema catalog should be materialized before validation access")
            }
        }
    }

    pub(crate) fn remember_schema_facts(&mut self, domain: &Domain, facts: Vec<SchemaCatalogFact>) {
        self.catalogs_by_domain.insert(
            domain.schema_catalog_domain(),
            SchemaCatalogEntry::SchemaFacts(facts),
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
