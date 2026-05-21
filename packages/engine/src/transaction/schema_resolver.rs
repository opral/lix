use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::catalog::{CatalogContext, CatalogSnapshot, SchemaCatalogFact};
use crate::domain::Domain;
use crate::live_state::{
    overlay_scan_rows, LiveStateReader, LiveStateRowRequest, LiveStateScanRequest,
    MaterializedLiveStateRow,
};
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::LixError;

pub(crate) struct TransactionSchemaResolver {
    context: Arc<CatalogContext>,
    catalogs_by_domain: BTreeMap<Domain, CatalogEntry>,
}

enum CatalogEntry {
    SchemaFacts(Vec<SchemaCatalogFact>),
    Catalog(CatalogSnapshot),
}

impl TransactionSchemaResolver {
    pub(crate) fn new(context: Arc<CatalogContext>) -> Self {
        Self {
            context,
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
                self.context
                    .schema_facts_for_domain(&reader, &domain)
                    .await?
            } else {
                self.context
                    .schema_facts_for_domain(live_state, &domain)
                    .await?
            };
            self.catalogs_by_domain
                .insert(domain.clone(), CatalogEntry::SchemaFacts(facts));
        }

        let should_materialize = self
            .catalogs_by_domain
            .get(&domain)
            .is_some_and(|entry| matches!(entry, CatalogEntry::SchemaFacts(_)));
        if should_materialize {
            #[cfg(feature = "storage-benches")]
            crate::storage_bench::record_transaction_schema_catalog_load();
            let entry = self
                .catalogs_by_domain
                .remove(&domain)
                .expect("schema catalog entry should exist after load");
            let CatalogEntry::SchemaFacts(facts) = entry else {
                unreachable!("catalog entry was checked as schema facts");
            };
            let catalog = CatalogSnapshot::from_schema_facts(&facts)?;
            self.catalogs_by_domain
                .insert(domain, CatalogEntry::Catalog(catalog));
        }
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&mut CatalogSnapshot, LixError> {
        self.load_catalog_for_domain(live_state, Some(staged), domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        match self
            .catalogs_by_domain
            .get_mut(&domain)
            .expect("catalog cache should contain requested version")
        {
            CatalogEntry::Catalog(catalog) => Ok(catalog),
            CatalogEntry::SchemaFacts(_) => {
                unreachable!("schema catalog should be materialized before mutable access")
            }
        }
    }

    pub(crate) async fn catalog_for_validation(
        &mut self,
        live_state: &dyn LiveStateReader,
        domain: &Domain,
    ) -> Result<&CatalogSnapshot, LixError> {
        self.load_catalog_for_domain(live_state, None, domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        match self
            .catalogs_by_domain
            .get(&domain)
            .expect("catalog cache should contain requested version")
        {
            CatalogEntry::Catalog(catalog) => Ok(catalog),
            CatalogEntry::SchemaFacts(_) => {
                unreachable!("schema catalog should be materialized before validation access")
            }
        }
    }

    pub(crate) fn remember_schema_facts(&mut self, domain: &Domain, facts: Vec<SchemaCatalogFact>) {
        self.catalogs_by_domain.insert(
            domain.schema_catalog_domain(),
            CatalogEntry::SchemaFacts(facts),
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
        Ok(self
            .scan_rows(&LiveStateScanRequest {
                filter: crate::live_state::LiveStateFilter {
                    schema_keys: vec![request.schema_key.clone()],
                    entity_pks: vec![request.entity_pk.clone()],
                    version_ids: vec![request.version_id.clone()],
                    file_ids: vec![request.file_id.clone()],
                    ..Default::default()
                },
                limit: Some(1),
                ..Default::default()
            })
            .await?
            .into_iter()
            .next())
    }
}
