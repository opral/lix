use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;
use crate::catalog::{CatalogContext, CatalogSnapshot, TransactionCatalog};
use crate::domain::Domain;
use crate::live_state::{
    LiveStateExactBatchRequest, LiveStateReader, LiveStateScanRequest, MaterializedLiveStateBatch,
    MaterializedLiveStateExactBatch, StagedLiveStateRows, overlay_load_exact_batch,
    overlay_scan_batch, overlay_scan_tracked_batch,
};
use crate::transaction::staging::PreparedStateRowOverlay;

pub(crate) struct TransactionSchemaResolver {
    context: Arc<CatalogContext>,
    catalogs_by_domain: BTreeMap<Domain, TransactionCatalog>,
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
        if self.catalogs_by_domain.contains_key(&domain) {
            return Ok(());
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_transaction_schema_catalog_load();
        let catalog = if let Some(staged) = staged {
            let reader = TransactionSchemaLiveStateReader {
                base: live_state,
                staged,
            };
            self.context
                .compiled_catalog_for_domain(&reader, &domain)
                .await?
        } else {
            self.context
                .compiled_catalog_for_domain(live_state, &domain)
                .await?
        };
        self.catalogs_by_domain
            .insert(domain, TransactionCatalog::Shared(catalog));
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&mut TransactionCatalog, LixError> {
        self.load_catalog_for_domain(live_state, Some(staged), domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        Ok(self
            .catalogs_by_domain
            .get_mut(&domain)
            .expect("catalog cache should contain requested branch"))
    }

    pub(crate) async fn catalog_for_validation(
        &mut self,
        live_state: &dyn LiveStateReader,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&CatalogSnapshot, LixError> {
        self.load_catalog_for_domain(live_state, Some(staged), domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        Ok(self
            .catalogs_by_domain
            .get(&domain)
            .expect("catalog cache should contain requested branch")
            .snapshot())
    }

    pub(crate) fn remember_compiled_catalog(
        &mut self,
        domain: &Domain,
        catalog: Arc<CatalogSnapshot>,
    ) {
        self.catalogs_by_domain.insert(
            domain.schema_catalog_domain(),
            TransactionCatalog::Shared(catalog),
        );
    }

    /// Drops transaction-private compiled catalogs after a statement rollback.
    /// The next normalization or validation lazily rebuilds from the restored
    /// staging overlay, retaining registrations from earlier successful
    /// statements without retaining a failed statement's copy-on-write plan.
    pub(crate) fn clear_cached_catalogs(&mut self) {
        self.catalogs_by_domain.clear();
    }

    #[cfg(test)]
    pub(crate) fn has_cached_catalog_for_test(&self, domain: &Domain) -> bool {
        self.catalogs_by_domain
            .contains_key(&domain.schema_catalog_domain())
    }
}

struct TransactionSchemaLiveStateReader<'a, S: StagedLiveStateRows + Sync + ?Sized> {
    base: &'a dyn LiveStateReader,
    staged: &'a S,
}

#[async_trait]
impl<S> LiveStateReader for TransactionSchemaLiveStateReader<'_, S>
where
    S: StagedLiveStateRows + Sync + ?Sized,
{
    async fn scan_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        overlay_scan_batch(self.base, self.staged, request).await
    }

    async fn scan_tracked_batch(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<MaterializedLiveStateBatch, LixError> {
        overlay_scan_tracked_batch(self.base, self.staged, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &LiveStateExactBatchRequest,
    ) -> Result<MaterializedLiveStateExactBatch, LixError> {
        overlay_load_exact_batch(self.base, self.staged, request).await
    }
}
