use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;

use crate::LixError;
use crate::catalog::{CatalogContext, CatalogSnapshot, TransactionCatalog};
use crate::domain::Domain;
use crate::live_state::{
    LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    StagedLiveStateRows, overlay_scan_rows, overlay_scan_tracked_rows,
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
        domain: &Domain,
    ) -> Result<&CatalogSnapshot, LixError> {
        self.load_catalog_for_domain(live_state, None, domain)
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
    async fn scan_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        overlay_scan_rows(self.base, self.staged, request).await
    }

    async fn scan_tracked_rows(
        &self,
        request: &LiveStateScanRequest,
    ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
        overlay_scan_tracked_rows(self.base, self.staged, request).await
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
                    branch_ids: vec![request.branch_id.clone()],
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_pk::EntityPk;
    use crate::live_state::LiveStateFilter;

    struct SplitCurrentAndTrackedReader {
        canonical: MaterializedLiveStateRow,
        tracked: MaterializedLiveStateRow,
    }

    #[async_trait]
    impl LiveStateReader for SplitCurrentAndTrackedReader {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(row_matches(&self.canonical, request)
                .then(|| self.canonical.clone())
                .into_iter()
                .collect())
        }

        async fn scan_tracked_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(row_matches(&self.tracked, request)
                .then(|| self.tracked.clone())
                .into_iter()
                .collect())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(Some(self.canonical.clone()))
        }
    }

    struct StaticStagedRows(Vec<MaterializedLiveStateRow>);

    impl StagedLiveStateRows for StaticStagedRows {
        fn staged_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(self
                .0
                .iter()
                .filter(|row| row_matches(row, request))
                .cloned()
                .collect())
        }
    }

    #[tokio::test]
    async fn tracked_schema_scan_uses_tracked_head_when_canonical_row_is_untracked() {
        let base = SplitCurrentAndTrackedReader {
            canonical: schema_row("untracked schema", true),
            tracked: schema_row("tracked schema", false),
        };
        let staged = StaticStagedRows(vec![schema_row("newer untracked schema", true)]);
        let reader = TransactionSchemaLiveStateReader {
            base: &base,
            staged: &staged,
        };

        let rows = reader
            .scan_tracked_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_registered_schema".to_string()],
                    branch_ids: vec!["main".to_string()],
                    untracked: Some(false),
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
            .expect("tracked schema scan should succeed");

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
        let reader = TransactionSchemaLiveStateReader {
            base: &base,
            staged: &staged,
        };

        let rows = reader
            .scan_tracked_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec!["lix_registered_schema".to_string()],
                    branch_ids: vec!["main".to_string()],
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await
            .expect("tracked staged schema scan should succeed");

        assert_eq!(rows.len(), 1);
        assert!(!rows[0].untracked);
        assert_eq!(
            rows[0].snapshot_content.as_deref(),
            Some("new tracked schema")
        );
    }

    fn schema_row(snapshot_content: &str, untracked: bool) -> MaterializedLiveStateRow {
        MaterializedLiveStateRow {
            entity_pk: EntityPk::single("example_schema"),
            schema_key: "lix_registered_schema".to_string(),
            file_id: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            deleted: false,
            created_at: "2026-01-01T00:00:00.000Z".to_string(),
            updated_at: "2026-01-01T00:00:00.000Z".to_string(),
            global: false,
            change_id: None,
            commit_id: None,
            untracked,
            branch_id: "main".to_string(),
        }
    }

    fn row_matches(row: &MaterializedLiveStateRow, request: &LiveStateScanRequest) -> bool {
        (request.filter.schema_keys.is_empty()
            || request.filter.schema_keys.contains(&row.schema_key))
            && (request.filter.branch_ids.is_empty()
                || request.filter.branch_ids.contains(&row.branch_id))
            && request
                .filter
                .untracked
                .is_none_or(|untracked| row.untracked == untracked)
    }
}
