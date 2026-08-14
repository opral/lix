use std::collections::BTreeMap;
use std::sync::Arc;

use crate::LixError;
use crate::catalog::{CatalogContext, CatalogSnapshot, TransactionCatalog};
use crate::domain::Domain;
use crate::state::TransactionStateView;
use crate::storage_adapter::StorageAdapterRead;

/// Transaction-local schema owner backed by the operation's authenticated
/// ForkTree state view. Staged schema rows are incorporated by the caller's
/// `TransactionStateView` before this resolver is invoked; this type never
/// reconstructs an older scan request or owns a second reader.
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

    async fn load_catalog_for_domain<R>(
        &mut self,
        state: &TransactionStateView<R>,
        domain: &Domain,
    ) -> Result<(), LixError>
    where
        R: StorageAdapterRead,
    {
        let domain = domain.schema_catalog_domain();
        if self.catalogs_by_domain.contains_key(&domain) {
            return Ok(());
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_transaction_schema_catalog_load();
        let catalog = self
            .context
            .compiled_catalog_for_transaction_state(state, &domain)
            .await?;
        self.catalogs_by_domain
            .insert(domain, TransactionCatalog::Shared(catalog));
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization<R>(
        &mut self,
        state: &TransactionStateView<R>,
        domain: &Domain,
    ) -> Result<&mut TransactionCatalog, LixError>
    where
        R: StorageAdapterRead,
    {
        self.load_catalog_for_domain(state, domain).await?;
        Ok(self
            .catalogs_by_domain
            .get_mut(&domain.schema_catalog_domain())
            .expect("catalog cache should contain requested branch"))
    }

    pub(crate) async fn catalog_for_validation<R>(
        &mut self,
        state: &TransactionStateView<R>,
        domain: &Domain,
    ) -> Result<&CatalogSnapshot, LixError>
    where
        R: StorageAdapterRead,
    {
        self.load_catalog_for_domain(state, domain).await?;
        Ok(self
            .catalogs_by_domain
            .get(&domain.schema_catalog_domain())
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

    pub(crate) fn clear_cached_catalogs(&mut self) {
        self.catalogs_by_domain.clear();
    }
}
