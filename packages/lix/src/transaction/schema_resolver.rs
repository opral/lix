use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;
use crate::catalog::{CatalogContext, CatalogSnapshot, TransactionCatalog};
use crate::domain::Domain;
use crate::hot_state::{
    HotStateExactBatchRequest, HotStateReader, HotStateScanRequest, MaterializedHotStateBatch,
    MaterializedHotStateExactBatch, StagedHotStateRows, overlay_load_exact_batch,
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
        hot_state: &dyn HotStateReader,
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
            let reader = TransactionSchemaHotStateReader {
                base: hot_state,
                staged,
            };
            self.context
                .compiled_catalog_for_domain(&reader, &domain)
                .await?
        } else {
            self.context
                .compiled_catalog_for_domain(hot_state, &domain)
                .await?
        };
        self.catalogs_by_domain
            .insert(domain, TransactionCatalog::Shared(catalog));
        Ok(())
    }

    pub(crate) async fn catalog_for_row_normalization(
        &mut self,
        hot_state: &dyn HotStateReader,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&mut TransactionCatalog, LixError> {
        self.load_catalog_for_domain(hot_state, Some(staged), domain)
            .await?;
        let domain = domain.schema_catalog_domain();
        Ok(self
            .catalogs_by_domain
            .get_mut(&domain)
            .expect("catalog cache should contain requested branch"))
    }

    pub(crate) async fn catalog_for_validation(
        &mut self,
        hot_state: &dyn HotStateReader,
        staged: &PreparedStateRowOverlay,
        domain: &Domain,
    ) -> Result<&CatalogSnapshot, LixError> {
        self.load_catalog_for_domain(hot_state, Some(staged), domain)
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

    /// Compiles and indexes both catalogs under the revision published by a
    /// successful schema mutation. Schema registration already pays this work;
    /// doing it at that boundary prevents the next ordinary transaction from
    /// becoming the accidental cold-compile path.
    pub(crate) async fn warm_committed_catalogs(
        &self,
        hot_state: &dyn HotStateReader,
        branch_id: &str,
        revision: &crate::catalog::CatalogRevision,
    ) -> Result<(), LixError> {
        self.context
            .compiled_catalog_for_transaction_open(
                hot_state,
                &Domain::schema_catalog(branch_id.to_string(), true),
                Some(revision),
            )
            .await?;
        self.context
            .compiled_catalog_for_transaction_open(
                hot_state,
                &Domain::schema_catalog(branch_id.to_string(), false),
                Some(revision),
            )
            .await?;
        Ok(())
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

struct TransactionSchemaHotStateReader<'a, S: StagedHotStateRows + Sync + ?Sized> {
    base: &'a dyn HotStateReader,
    staged: &'a S,
}

#[async_trait]
impl<S> HotStateReader for TransactionSchemaHotStateReader<'_, S>
where
    S: StagedHotStateRows + Sync + ?Sized,
{
    async fn scan_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        overlay_scan_batch(self.base, self.staged, request).await
    }

    async fn scan_tracked_batch(
        &self,
        request: &HotStateScanRequest,
    ) -> Result<MaterializedHotStateBatch, LixError> {
        overlay_scan_tracked_batch(self.base, self.staged, request).await
    }

    async fn load_exact_batch(
        &self,
        request: &HotStateExactBatchRequest,
    ) -> Result<MaterializedHotStateExactBatch, LixError> {
        overlay_load_exact_batch(self.base, self.staged, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::LixTimestamp;
    use crate::row_pk::RowPk;
    use crate::hot_state::{HotStateFilter, MaterializedHotStateRow};

    struct SplitCurrentAndTrackedReader {
        canonical: MaterializedHotStateRow,
        tracked: MaterializedHotStateRow,
    }

    #[async_trait]
    impl HotStateReader for SplitCurrentAndTrackedReader {
        async fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(row_matches(&self.canonical, request)
                .then(|| self.canonical.clone())
                .into_iter()
                .collect::<Vec<_>>()
                .into())
        }

        async fn scan_tracked_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(row_matches(&self.tracked, request)
                .then(|| self.tracked.clone())
                .into_iter()
                .collect::<Vec<_>>()
                .into())
        }
    }

    struct StaticStagedRows(Vec<MaterializedHotStateRow>);

    impl StagedHotStateRows for StaticStagedRows {
        fn staged_batch(
            &self,
            request: &HotStateScanRequest,
        ) -> Result<MaterializedHotStateBatch, LixError> {
            Ok(MaterializedHotStateBatch::from_rows(
                self.0
                    .iter()
                    .filter(|row| row_matches(row, request))
                    .cloned()
                    .collect(),
            ))
        }

        fn load_exact_batch(
            &self,
            request: &HotStateExactBatchRequest,
        ) -> Result<MaterializedHotStateExactBatch, LixError> {
            Ok(MaterializedHotStateExactBatch::from_rows(
                request
                    .rows
                    .iter()
                    .map(|request_row| {
                        self.0
                            .iter()
                            .find(|row| row_matches(row, &request.row_scan_request(request_row)))
                            .cloned()
                    })
                    .collect(),
            ))
        }
    }

    #[tokio::test]
    async fn tracked_schema_scan_uses_tracked_head_when_canonical_row_is_untracked() {
        let base = SplitCurrentAndTrackedReader {
            canonical: schema_row("untracked schema", true),
            tracked: schema_row("tracked schema", false),
        };
        let staged = StaticStagedRows(vec![schema_row("newer untracked schema", true)]);
        let reader = TransactionSchemaHotStateReader {
            base: &base,
            staged: &staged,
        };

        let rows = reader
            .scan_tracked_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_registered_schema".to_string()],
                    branch_ids: vec!["main".to_string()],
                    untracked: Some(false),
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .await
            .expect("tracked schema scan should succeed")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(rows[0].snapshot_content.as_deref(), Some("tracked schema"));
    }

    #[tokio::test]
    async fn tracked_schema_scan_overlays_staged_tracked_schema_on_tracked_head() {
        let base = SplitCurrentAndTrackedReader {
            canonical: schema_row("untracked schema", true),
            tracked: schema_row("old tracked schema", false),
        };
        let staged = StaticStagedRows(vec![schema_row("new tracked schema", false)]);
        let reader = TransactionSchemaHotStateReader {
            base: &base,
            staged: &staged,
        };

        let rows = reader
            .scan_tracked_batch(&HotStateScanRequest {
                filter: HotStateFilter {
                    schema_keys: vec!["lix_registered_schema".to_string()],
                    branch_ids: vec!["main".to_string()],
                    ..HotStateFilter::default()
                },
                ..HotStateScanRequest::default()
            })
            .await
            .expect("tracked staged schema scan should succeed")
            .into_rows();

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("new tracked schema")
        );
    }

    fn schema_row(snapshot_content: &str, untracked: bool) -> MaterializedHotStateRow {
        MaterializedHotStateRow {
            row_pk: RowPk::single("example_schema"),
            schema_key: "lix_registered_schema".to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string().into()),
            metadata: None,
            deleted: false,
            created_at: LixTimestamp::expect_parse("created_at", "2026-01-01T00:00:00.000Z"),
            updated_at: LixTimestamp::expect_parse("updated_at", "2026-01-01T00:00:00.000Z"),
            global: false,
            change_id: None,
            commit_id: None,
            untracked,
            branch_id: "main".into(),
        }
    }

    fn row_matches(row: &MaterializedHotStateRow, request: &HotStateScanRequest) -> bool {
        (request.filter.schema_keys.is_empty()
            || request.filter.schema_keys.contains(&row.schema_key))
            && (request.filter.branch_ids.is_empty()
                || request
                    .filter
                    .branch_ids
                    .iter()
                    .any(|branch_id| branch_id == row.branch_id.as_ref()))
            && request
                .filter
                .untracked
                .is_none_or(|untracked| row.untracked == untracked)
    }
}
