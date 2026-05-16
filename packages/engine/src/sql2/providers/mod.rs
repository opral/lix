use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::LixError;

mod change;
mod directory;
mod directory_history;
mod entity;
mod entity_history;
mod file;
mod file_history;
mod history;
mod lix_state;
mod version;

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceKind};
use crate::sql2::{SqlExecutionContext, SqlWriteContext};

pub(crate) async fn register_read(
    session: &SessionContext,
    ctx: &dyn SqlExecutionContext,
) -> Result<(), LixError> {
    let version_ref = ctx.version_ref();
    let commit_store_query_source = ctx.commit_store_query_source();
    let catalog = PublicCatalog::from_visible_schemas(&ctx.list_visible_schemas()?)?;
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::LixState => {
                lix_state::register_lix_state_active_provider(
                    session,
                    &surface.name,
                    ctx.active_version_id(),
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                )
                .await?;
            }
            PublicSurfaceKind::LixStateByVersion => {
                lix_state::register_lix_state_by_version_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                )
                .await?;
            }
            PublicSurfaceKind::Version => {
                version::register_lix_version_read_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                )
                .await?;
            }
            PublicSurfaceKind::Change => {
                change::register_lix_change_read_provider(
                    session,
                    &surface.name,
                    commit_store_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::History => {
                history::register_history_provider(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    commit_store_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::File => {
                file::register_lix_file_active_provider(
                    session,
                    &surface.name,
                    ctx.active_version_id(),
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                    ctx.blob_reader(),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::FileByVersion => {
                file::register_lix_file_by_version_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                    ctx.blob_reader(),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::FileHistory => {
                file_history::register_lix_file_history_surface(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    commit_store_query_source.clone(),
                    ctx.blob_reader(),
                )
                .await?;
            }
            PublicSurfaceKind::Directory => {
                directory::register_lix_directory_active_provider(
                    session,
                    &surface.name,
                    ctx.active_version_id(),
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryByVersion => {
                directory::register_lix_directory_by_version_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&version_ref),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryHistory => {
                directory_history::register_lix_directory_history_surface(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    commit_store_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::EntityBase { .. }
            | PublicSurfaceKind::EntityByVersion { .. }
            | PublicSurfaceKind::EntityHistory { .. } => {}
        }
    }
    entity::register_entity_providers(
        session,
        ctx.active_version_id(),
        ctx.live_state(),
        Arc::clone(&version_ref),
        Arc::new(tokio::sync::Mutex::new(ctx.commit_graph())),
        commit_store_query_source,
        &catalog,
    )
    .await?;

    Ok(())
}

pub(crate) async fn register_write(
    session: &SessionContext,
    write_ctx: SqlWriteContext,
) -> Result<(), LixError> {
    let catalog = PublicCatalog::from_visible_schemas(&write_ctx.list_visible_schemas()?)?;
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::LixState => {
                lix_state::register_lix_state_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::LixStateByVersion => {
                lix_state::register_lix_state_by_version_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Version => {
                version::register_lix_version_write_surface(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::File => {
                file::register_lix_file_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::FileByVersion => {
                file::register_lix_file_by_version_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Directory => {
                directory::register_lix_directory_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryByVersion => {
                directory::register_lix_directory_by_version_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Change
            | PublicSurfaceKind::History
            | PublicSurfaceKind::FileHistory
            | PublicSurfaceKind::DirectoryHistory => {}
            PublicSurfaceKind::EntityBase { .. }
            | PublicSurfaceKind::EntityByVersion { .. }
            | PublicSurfaceKind::EntityHistory { .. } => {}
        }
    }
    entity::register_entity_write_providers(session, write_ctx.clone(), &catalog).await?;

    if !write_ctx.supports_committed_read_surfaces() {
        return Ok(());
    }

    let commit_store_query_source = write_ctx.commit_store_query_source().await?;
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::Change => {
                change::register_lix_change_read_provider(
                    session,
                    &surface.name,
                    commit_store_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::History => {
                history::register_history_provider(
                    session,
                    &surface.name,
                    write_ctx.commit_graph()?,
                    commit_store_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::FileHistory => {
                file_history::register_lix_file_history_surface(
                    session,
                    &surface.name,
                    write_ctx.commit_graph()?,
                    commit_store_query_source.clone(),
                    write_ctx.blob_reader(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryHistory => {
                directory_history::register_lix_directory_history_surface(
                    session,
                    &surface.name,
                    write_ctx.commit_graph()?,
                    commit_store_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::LixState
            | PublicSurfaceKind::LixStateByVersion
            | PublicSurfaceKind::Version
            | PublicSurfaceKind::File
            | PublicSurfaceKind::FileByVersion
            | PublicSurfaceKind::Directory
            | PublicSurfaceKind::DirectoryByVersion
            | PublicSurfaceKind::EntityBase { .. }
            | PublicSurfaceKind::EntityByVersion { .. }
            | PublicSurfaceKind::EntityHistory { .. } => {}
        }
    }
    entity::register_entity_history_providers(
        session,
        Arc::new(tokio::sync::Mutex::new(write_ctx.commit_graph()?)),
        commit_store_query_source,
        &catalog,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::json;

    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::prelude::SessionContext;

    use crate::commit_graph::{
        CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphCommit,
        CommitGraphEdge, CommitGraphReader, ReachableCommitGraphCommit,
    };
    use crate::commit_store::CommitStoreContext;
    use crate::json_store::JsonStoreContext;
    use crate::live_state::{
        LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::sql2::catalog::{derive_entity_surface_spec_from_schema, PublicCatalog};
    use crate::sql2::{CommitStoreQuerySource, SqlReadStore};
    use crate::storage::{StorageContext, StorageReadScope};
    use crate::version::{VersionHead, VersionRefReader};
    use crate::LixError;

    use super::{
        change, directory, directory_history, entity, file, file_history, history, lix_state,
        version,
    };

    #[test]
    fn provider_history_schemas_match_catalog_contract_order() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");

        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_state",
            lix_state::lix_state_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_state_by_version",
            lix_state::lix_state_by_version_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file",
            file::lix_file_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file_by_version",
            file::lix_file_by_version_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory",
            directory::lix_directory_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory_by_version",
            directory::lix_directory_by_version_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_version",
            version::lix_version_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_change",
            change::lix_change_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_state_history",
            history::lix_state_history_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file_history",
            file_history::lix_file_history_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory_history",
            directory_history::lix_directory_history_schema(),
        );
    }

    #[tokio::test]
    async fn provider_entity_schemas_match_catalog_contract_order() {
        let schema = json!({
            "x-lix-key": "phase8_entity",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": {
                "id": { "type": "string" },
                "count": { "type": "integer" },
                "body": { "type": "object" }
            }
        });
        let catalog =
            PublicCatalog::from_visible_schemas(&[schema.clone()]).expect("catalog should build");
        let _spec = derive_entity_surface_spec_from_schema(&schema).expect("schema should derive");
        let session = SessionContext::new();
        entity::register_entity_providers(
            &session,
            "version-a",
            Arc::new(EmptyLiveStateReader),
            Arc::new(EmptyVersionRefReader),
            Arc::new(tokio::sync::Mutex::new(Box::new(EmptyCommitGraphReader))),
            empty_commit_store_query_source().await,
            &catalog,
        )
        .await
        .expect("entity providers should register");

        assert_registered_table_schema_matches_catalog(&session, &catalog, "phase8_entity").await;
        assert_registered_table_schema_matches_catalog(
            &session,
            &catalog,
            "phase8_entity_by_version",
        )
        .await;
        assert_registered_table_schema_matches_catalog(&session, &catalog, "phase8_entity_history")
            .await;
    }

    async fn assert_registered_table_schema_matches_catalog(
        session: &SessionContext,
        catalog: &PublicCatalog,
        surface_name: &str,
    ) {
        let provider = session
            .table_provider(surface_name)
            .await
            .unwrap_or_else(|error| panic!("{surface_name} provider should load: {error}"));
        assert_surface_schema_matches_provider_schema(catalog, surface_name, provider.schema());
    }

    fn assert_surface_schema_matches_provider_schema(
        catalog: &PublicCatalog,
        surface_name: &str,
        provider_schema: SchemaRef,
    ) {
        let surface = catalog
            .surface(surface_name)
            .unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
        let catalog_column_names = surface
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let provider_field_names = provider_schema
            .fields()
            .iter()
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            catalog_column_names, provider_field_names,
            "{surface_name} column order"
        );

        let catalog_schema = catalog
            .surface_schema(surface_name)
            .unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
        assert_eq!(
            catalog_schema.fields(),
            provider_schema.fields(),
            "{surface_name}"
        );
    }

    async fn empty_commit_store_query_source() -> crate::sql2::SqlCommitStoreQuerySource {
        let storage =
            StorageContext::new(Arc::new(crate::backend::testing::UnitTestBackend::new()));
        let read_scope = StorageReadScope::new(SqlReadStore::scoped(
            StorageReadScope::new(
                storage
                    .begin_read_transaction()
                    .await
                    .expect("read transaction should open"),
            )
            .store(),
        ));
        CommitStoreQuerySource {
            commit_store_reader: Arc::new(CommitStoreContext::new().reader(read_scope.store())),
            json_reader: JsonStoreContext::new().reader(read_scope.store()),
        }
    }

    struct EmptyLiveStateReader;

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn scan_rows(
            &self,
            _request: &LiveStateScanRequest,
        ) -> Result<Vec<MaterializedLiveStateRow>, LixError> {
            Ok(Vec::new())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<MaterializedLiveStateRow>, LixError> {
            Ok(None)
        }
    }

    struct EmptyVersionRefReader;

    #[async_trait]
    impl VersionRefReader for EmptyVersionRefReader {
        async fn load_head(&self, version_id: &str) -> Result<Option<VersionHead>, LixError> {
            Ok(Some(VersionHead {
                version_id: version_id.to_string(),
                commit_id: format!("commit-{version_id}"),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<VersionHead>, LixError> {
            Ok(Vec::new())
        }
    }

    struct EmptyCommitGraphReader;

    #[async_trait]
    impl CommitGraphReader for EmptyCommitGraphReader {
        async fn load_commit(
            &mut self,
            _commit_id: &str,
        ) -> Result<Option<CommitGraphCommit>, LixError> {
            Ok(None)
        }

        async fn all_commits(&mut self) -> Result<Vec<CommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn reachable_commits(
            &mut self,
            _head_commit_id: &str,
        ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn best_common_ancestors(
            &mut self,
            _left_commit_id: &str,
            _right_commit_id: &str,
        ) -> Result<Vec<CommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn merge_base(
            &mut self,
            _left_commit_id: &str,
            _right_commit_id: &str,
        ) -> Result<CommitGraphCommit, LixError> {
            Err(LixError::new(
                "LIX_ERROR_UNKNOWN",
                "empty commit graph reader cannot resolve merge base",
            ))
        }

        fn commit_edges(&self, _commits: &[CommitGraphCommit]) -> Vec<CommitGraphEdge> {
            Vec::new()
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &str,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
            Ok(Vec::new())
        }
    }
}
