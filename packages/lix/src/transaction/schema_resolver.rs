use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::{CatalogContext, CatalogSnapshot, TransactionCatalog};
use crate::domain::Domain;
use crate::forktree::ForkTreeReadFacade;
use crate::live_state::{
    LiveStateFilter, LiveStateScanRequest, MaterializedLiveStateBatch, overlay_scan_batch,
    overlay_scan_tracked_batch,
};
use crate::storage_adapter::StorageAdapterRead;
use crate::transaction::staging::PreparedStateRowOverlay;
use crate::{LixError, NullableKeyFilter};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";

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
        forktree: &ForkTreeReadFacade<R>,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<(), LixError>
    where
        R: StorageAdapterRead + 'static,
    {
        let domain = domain.schema_catalog_domain();
        if self.catalogs_by_domain.contains_key(&domain) {
            return Ok(());
        }
        #[cfg(feature = "storage-benches")]
        crate::storage_bench::record_transaction_schema_catalog_load();
        let rows = load_transaction_catalog_rows(forktree, staged, &domain).await?;
        let catalog = self
            .context
            .compiled_catalog_for_materialized_domain_rows(rows)?;
        self.catalogs_by_domain
            .insert(domain, TransactionCatalog::Shared(catalog));
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization<R>(
        &mut self,
        forktree: &ForkTreeReadFacade<R>,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&mut TransactionCatalog, LixError>
    where
        R: StorageAdapterRead + 'static,
    {
        self.load_catalog_for_domain(forktree, staged, domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        Ok(self
            .catalogs_by_domain
            .get_mut(&domain)
            .expect("catalog cache should contain requested branch"))
    }

    pub(crate) async fn catalog_for_validation<R>(
        &mut self,
        forktree: &ForkTreeReadFacade<R>,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&CatalogSnapshot, LixError>
    where
        R: StorageAdapterRead + 'static,
    {
        self.load_catalog_for_domain(forktree, staged, domain)
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
}

async fn load_transaction_catalog_rows<R>(
    forktree: &ForkTreeReadFacade<R>,
    staged: &PreparedStateRowOverlay,
    domain: &Domain,
) -> Result<Vec<(Domain, MaterializedLiveStateBatch)>, LixError>
where
    R: StorageAdapterRead + 'static,
{
    let schema_domains = domain.schema_catalog_domains();
    let mut catalog_rows = Vec::with_capacity(schema_domains.len());
    for schema_domain in schema_domains {
        let request = schema_catalog_scan_request(&schema_domain);
        let rows = if schema_domain.untracked() {
            overlay_scan_batch(forktree, staged, &request).await?
        } else {
            overlay_scan_tracked_batch(forktree, staged, &request).await?
        };
        catalog_rows.push((schema_domain, rows));
    }
    Ok(catalog_rows)
}

fn schema_catalog_scan_request(domain: &Domain) -> LiveStateScanRequest {
    LiveStateScanRequest {
        filter: LiveStateFilter {
            schema_keys: vec![REGISTERED_SCHEMA_KEY.to_string()],
            branch_ids: vec![domain.branch_id().to_string()],
            file_ids: vec![NullableKeyFilter::Null],
            untracked: Some(domain.untracked()),
            include_tombstones: false,
            ..LiveStateFilter::default()
        },
        ..LiveStateScanRequest::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_catalog_requests_preserve_exact_domain_and_null_file_scope() {
        for untracked in [false, true] {
            let request = schema_catalog_scan_request(&Domain::schema_catalog("branch", untracked));
            assert_eq!(
                request.filter.schema_keys,
                vec![REGISTERED_SCHEMA_KEY.to_string()]
            );
            assert_eq!(request.filter.branch_ids, vec!["branch".to_string()]);
            assert_eq!(request.filter.file_ids, vec![NullableKeyFilter::Null]);
            assert_eq!(request.filter.untracked, Some(untracked));
            assert!(!request.filter.include_tombstones);
            assert_eq!(request.limit, None);
        }
    }

    #[test]
    fn transaction_schema_resolution_has_no_generic_reader_owner() {
        let source = include_str!("schema_resolver.rs");
        assert!(!source.contains(concat!("TransactionSchema", "LiveStateReader")));
        assert!(!source.contains(concat!("dyn LiveState", "Reader")));
        assert!(source.contains("ForkTreeReadFacade"));
        assert!(source.contains("compiled_catalog_for_materialized_domain_rows"));
    }
}
