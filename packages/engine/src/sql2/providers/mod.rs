#![allow(clippy::cloned_ref_to_slice_refs, clippy::match_same_arms)]

use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::LixError;

mod branch;
mod change;
mod columns;
mod directory;
mod directory_history;
mod entity;
mod entity_history;
mod file;
mod file_history;
mod filesystem_history_path;
mod history;
mod lix_state;
mod spec;
mod upsert;
mod values;

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceKind};
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{SqlExecutionContext, SqlWriteContext};

use datafusion::catalog::TableProvider;

pub(crate) use file::{FastLixFilePathWriteConflict, execute_fast_lix_file_path_writes};
pub(crate) use upsert::{UpsertAction, excluded_field_name};

/// Execute an `INSERT ... ON CONFLICT` against a registered table provider.
/// The four builtin writable surfaces are all [`spec::SpecTableProvider`]s.
pub(crate) async fn execute_spec_upsert(
    table: &Arc<dyn TableProvider>,
    input: &Arc<dyn ExecutionPlan>,
    proposed_batches: Vec<datafusion::arrow::record_batch::RecordBatch>,
    target_columns: &[String],
    action: &UpsertAction,
) -> Result<u64, LixError> {
    let provider = table
        .as_any()
        .downcast_ref::<spec::SpecTableProvider>()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "INSERT ON CONFLICT is not supported on this table",
            )
        })?;
    provider
        .execute_upsert(input, proposed_batches, target_columns, action)
        .await
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)
}

/// Validate an `INSERT ... ON CONFLICT` against a registered table provider.
pub(crate) async fn validate_spec_upsert(
    table: &Arc<dyn TableProvider>,
    input: &Arc<dyn ExecutionPlan>,
    target_columns: &[String],
) -> Result<(), LixError> {
    let provider = table
        .as_any()
        .downcast_ref::<spec::SpecTableProvider>()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "INSERT ON CONFLICT is not supported on this table",
            )
        })?;
    let _ = provider
        .validate_upsert(input, target_columns)
        .await
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)?;
    Ok(())
}

pub(crate) async fn register_read<C>(session: &SessionContext, ctx: &C) -> Result<(), LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let branch_ref = ctx.branch_ref();
    let history_query_source = ctx.history_query_source();
    let changelog_query_source = ctx.changelog_query_source();
    let catalog = PublicCatalog::from_visible_schemas(&ctx.list_visible_schemas()?)?;
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::LixState => {
                lix_state::register_lix_state_active_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id(),
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::LixStateByBranch => {
                lix_state::register_lix_state_by_branch_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::Branch => {
                branch::register_lix_branch_read_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::Change => {
                change::register_lix_change_read_provider(
                    session,
                    &surface.name,
                    changelog_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::History => {
                history::register_history_provider(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    history_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::File => {
                file::register_lix_file_active_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id(),
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::FileByBranch => {
                file::register_lix_file_by_branch_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::FileHistory => {
                file_history::register_lix_file_history_surface(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    history_query_source.clone(),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                )
                .await?;
            }
            PublicSurfaceKind::Directory => {
                directory::register_lix_directory_active_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id(),
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryByBranch => {
                directory::register_lix_directory_by_branch_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    Arc::clone(&branch_ref),
                    ctx.functions(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryHistory => {
                directory_history::register_lix_directory_history_surface(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    history_query_source.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::EntityBase { .. }
            | PublicSurfaceKind::EntityByBranch { .. }
            | PublicSurfaceKind::EntityHistory { .. } => {}
        }
    }
    entity::register_entity_providers(
        session,
        ctx.active_branch_id(),
        ctx.live_state(),
        Arc::clone(&branch_ref),
        Arc::new(tokio::sync::Mutex::new(ctx.commit_graph())),
        history_query_source,
        &catalog,
    )
    .await?;

    Ok(())
}

pub(crate) async fn register_write(
    session: &SessionContext,
    write_ctx: SqlWriteContext,
    options: SqlWriteSessionOptions,
) -> Result<(), LixError> {
    let catalog = PublicCatalog::from_visible_schemas(&write_ctx.list_visible_schemas()?)?;
    for surface in catalog.surfaces() {
        match &surface.kind {
            PublicSurfaceKind::LixState => {
                replace_registered_table(session, &surface.name)?;
                lix_state::register_lix_state_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::LixStateByBranch => {
                replace_registered_table(session, &surface.name)?;
                lix_state::register_lix_state_by_branch_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Branch => {
                replace_registered_table(session, &surface.name)?;
                branch::register_write_provider(session, &surface.name, write_ctx.clone()).await?;
            }
            PublicSurfaceKind::File => {
                replace_registered_table(session, &surface.name)?;
                file::register_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    options.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::FileByBranch => {
                replace_registered_table(session, &surface.name)?;
                file::register_by_branch_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    options.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Directory => {
                replace_registered_table(session, &surface.name)?;
                directory::register_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryByBranch => {
                replace_registered_table(session, &surface.name)?;
                directory::register_by_branch_write_provider(
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
            | PublicSurfaceKind::EntityByBranch { .. }
            | PublicSurfaceKind::EntityHistory { .. } => {}
        }
    }
    for surface in catalog.surfaces() {
        if matches!(
            surface.kind,
            PublicSurfaceKind::EntityBase { .. } | PublicSurfaceKind::EntityByBranch { .. }
        ) {
            replace_registered_table(session, &surface.name)?;
        }
    }
    entity::register_entity_write_providers(session, write_ctx.clone(), &catalog).await?;
    Ok(())
}

fn replace_registered_table(session: &SessionContext, name: &str) -> Result<(), LixError> {
    match session.deregister_table(name) {
        Ok(_) => Ok(()),
        Err(error) if error.to_string().contains("not found") => Ok(()),
        Err(error) => Err(LixError::new(
            LixError::CODE_UNKNOWN,
            format!("sql2 DataFusion error: {error}"),
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::json;

    use datafusion::arrow::datatypes::SchemaRef;
    use datafusion::prelude::SessionContext;

    use crate::LixError;
    use crate::branch::{BranchHead, BranchRefReader};
    use crate::changelog::CommitId;
    use crate::commit_graph::{
        CommitGraphChangeHistoryEntry, CommitGraphChangeHistoryRequest, CommitGraphCommit,
        CommitGraphReader, ReachableCommitGraphCommit,
    };
    use crate::json_store::JsonStoreContext;
    use crate::live_state::{
        LiveStateReader, LiveStateRowRequest, LiveStateScanRequest, MaterializedLiveStateRow,
    };
    use crate::sql2::HistoryQuerySource;
    use crate::sql2::catalog::{PublicCatalog, derive_entity_surface_spec_from_schema};
    use crate::storage::{
        InMemoryStorageBackend, InMemoryStorageRead, SharedStorageRead, StorageContext,
        StorageReadOptions,
    };

    use super::{
        branch, change, directory, directory_history, entity, file, file_history, history,
        lix_state,
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
            "lix_state_by_branch",
            lix_state::lix_state_by_branch_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file",
            file::lix_file_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file_by_branch",
            file::lix_file_by_branch_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory",
            directory::lix_directory_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory_by_branch",
            directory::lix_directory_by_branch_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_branch",
            branch::lix_branch_schema(),
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
            "branch-a",
            Arc::new(EmptyLiveStateReader),
            Arc::new(EmptyBranchRefReader),
            Arc::new(tokio::sync::Mutex::new(Box::new(EmptyCommitGraphReader))),
            empty_history_query_source().await,
            &catalog,
        )
        .await
        .expect("entity providers should register");

        assert_registered_table_schema_matches_catalog(&session, &catalog, "phase8_entity").await;
        assert_registered_table_schema_matches_catalog(
            &session,
            &catalog,
            "phase8_entity_by_branch",
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

    async fn empty_history_query_source()
    -> crate::sql2::SqlHistoryQuerySource<SharedStorageRead<InMemoryStorageRead>> {
        let storage = StorageContext::new(InMemoryStorageBackend::new());
        let read_scope = storage
            .begin_read(StorageReadOptions::default())
            .expect("read should open");
        HistoryQuerySource {
            json_reader: JsonStoreContext::new().reader(SharedStorageRead::new(read_scope)),
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

    struct EmptyBranchRefReader;

    #[async_trait]
    impl BranchRefReader for EmptyBranchRefReader {
        async fn load_head(&self, branch_id: &str) -> Result<Option<BranchHead>, LixError> {
            Ok(Some(BranchHead {
                branch_id: branch_id.to_string(),
                commit_id: CommitId::for_test_label(&format!("commit-{branch_id}")),
            }))
        }

        async fn scan_heads(&self) -> Result<Vec<BranchHead>, LixError> {
            Ok(Vec::new())
        }
    }

    struct EmptyCommitGraphReader;

    #[async_trait]
    impl CommitGraphReader for EmptyCommitGraphReader {
        async fn load_commit(
            &mut self,
            _commit_id: &CommitId,
        ) -> Result<Option<CommitGraphCommit>, LixError> {
            Ok(None)
        }

        async fn reachable_commits(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Vec<ReachableCommitGraphCommit>, LixError> {
            Ok(Vec::new())
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &CommitId,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<Vec<CommitGraphChangeHistoryEntry>, LixError> {
            Ok(Vec::new())
        }
    }
}
