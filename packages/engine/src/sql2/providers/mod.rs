#![allow(clippy::cloned_ref_to_slice_refs, clippy::match_same_arms)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;

use crate::LixError;
use crate::branch::BranchRefReader;

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

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{SqlExecutionContext, SqlWriteContext};

use datafusion::catalog::TableProvider;

pub(crate) use file::{
    ExactLixFileReadColumn, ExactLixFileReadSelector, FastLixFilePathWriteConflict,
    execute_exact_lix_file_read, execute_fast_lix_file_data_update_by_id,
    execute_fast_lix_file_path_writes,
};
pub(crate) use spec::DmlReturning;
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

/// Plan a DELETE with a pre-delete `RETURNING` projection against a registered
/// spec provider.  The projection is captured by the provider's existing
/// one-pass DML executor, not by a separate SELECT.
pub(crate) async fn execute_spec_delete_with_returning(
    table: &Arc<dyn TableProvider>,
    state: &dyn datafusion::catalog::Session,
    filters: Vec<datafusion::logical_expr::Expr>,
    returning: DmlReturning,
) -> Result<Arc<dyn ExecutionPlan>, LixError> {
    let provider = table
        .as_any()
        .downcast_ref::<spec::SpecTableProvider>()
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_UNSUPPORTED_SQL,
                "DELETE RETURNING is not supported on this table",
            )
        })?;
    provider
        .delete_with_returning(state, filters, returning)
        .await
        .map_err(crate::sql2::error::datafusion_error_to_lix_error)
}

pub(crate) async fn register_read<C>(
    session: &SessionContext,
    ctx: &C,
    branch_ref: Arc<dyn BranchRefReader>,
    selection: &ProviderSelection,
) -> Result<(), LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    if selection.is_empty() {
        return Ok(());
    }
    let dynamic_catalog;
    let catalog = if selection.requires_visible_schemas() {
        dynamic_catalog = PublicCatalog::from_visible_schemas(&ctx.load_visible_schemas().await?)?;
        &dynamic_catalog
    } else {
        PublicCatalog::fixed_system()
    };
    register_read_from_catalog(
        session,
        ctx,
        branch_ref,
        catalog,
        ReadProviderScope::All,
        selection,
    )
    .await
}

/// Snapshot-local providers needed to plan already-bound SQL.
///
/// For reads, DataFusion's resolver is deliberately used instead of maintaining
/// a second SQL AST walker. It is the same resolver called by
/// `SessionState::statement_to_plan`, including its CTE scoping and identifier
/// normalization rules. Bound target-only writes select their known target
/// directly. The selection retains names only; providers and plans remain
/// scoped to the current storage snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProviderSelection {
    /// Register every surface when catalog-wide visibility is part of the SQL
    /// semantics (notably `information_schema` and rewritten `SHOW` queries),
    /// or when reference resolution cannot prove a narrower set is sufficient.
    All,
    /// Register the union of concrete table names referenced by the statements.
    Only(BTreeSet<String>),
}

impl ProviderSelection {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Only(names) if names.is_empty())
    }

    fn includes(&self, surface: &PublicSurfaceContract) -> bool {
        match self {
            Self::All => true,
            Self::Only(names) => names.contains(&surface.name),
        }
    }

    /// Whether resolving this selection requires the storage-backed catalog.
    ///
    /// Table-free reads and references satisfied by the immutable system catalog
    /// can install providers without scanning `lix_registered_schema` rows.
    /// Runtime registration rejects schema keys whose generated table names
    /// would shadow these fixed providers.
    /// `All` and every unknown name remain conservative: they load the full
    /// visible catalog so information-schema, custom entities, and normal
    /// unknown-table errors keep their current semantics.
    fn requires_visible_schemas(&self) -> bool {
        match self {
            Self::All => true,
            Self::Only(names) => names
                .iter()
                .any(|name| PublicCatalog::fixed_system().surface(name).is_none()),
        }
    }
}

pub(crate) fn read_provider_selection(
    session: &SessionContext,
    statements: &[datafusion::sql::parser::Statement],
) -> ProviderSelection {
    let mut names = BTreeSet::new();
    let state = session.state();
    for statement in statements {
        if statement_requires_all_providers(statement) {
            return ProviderSelection::All;
        }
        let Ok(references) = state.resolve_table_references(statement) else {
            return ProviderSelection::All;
        };
        for reference in references {
            if reference.schema() == Some("information_schema") {
                return ProviderSelection::All;
            }
            names.insert(reference.table().to_string());
        }
    }
    ProviderSelection::Only(names)
}

fn statement_requires_all_providers(statement: &datafusion::sql::parser::Statement) -> bool {
    use datafusion::sql::parser::Statement as DataFusionStatement;
    use datafusion::sql::sqlparser::ast::Statement as SqlStatement;

    fn sql_statement_requires_all_providers(statement: &SqlStatement) -> bool {
        match statement {
            SqlStatement::ShowFunctions { .. }
            | SqlStatement::ShowVariable { .. }
            | SqlStatement::ShowStatus { .. }
            | SqlStatement::ShowVariables { .. }
            | SqlStatement::ShowCreate { .. }
            | SqlStatement::ShowColumns { .. }
            | SqlStatement::ShowTables { .. }
            | SqlStatement::ShowCollation { .. } => true,
            SqlStatement::Explain { statement, .. } => {
                sql_statement_requires_all_providers(statement)
            }
            _ => false,
        }
    }

    match statement {
        DataFusionStatement::Statement(statement) => {
            sql_statement_requires_all_providers(statement)
        }
        DataFusionStatement::Explain(explain) => {
            statement_requires_all_providers(explain.statement.as_ref())
        }
        _ => false,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReadProviderScope {
    All,
    ReadOnly,
}

impl ReadProviderScope {
    fn includes(self, surface: &PublicSurfaceContract) -> bool {
        self == Self::All || !is_write_surface(surface)
    }
}

fn is_write_surface(surface: &PublicSurfaceContract) -> bool {
    surface.capabilities.insert || surface.capabilities.update || surface.capabilities.delete
}

async fn register_read_from_catalog<C>(
    session: &SessionContext,
    ctx: &C,
    branch_ref: Arc<dyn BranchRefReader>,
    catalog: &PublicCatalog,
    scope: ReadProviderScope,
    selection: &ProviderSelection,
) -> Result<(), LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    let needs_history_query_source = catalog.surfaces().any(|surface| {
        scope.includes(surface)
            && selection.includes(surface)
            && matches!(
                &surface.kind,
                PublicSurfaceKind::History
                    | PublicSurfaceKind::FileHistory
                    | PublicSurfaceKind::DirectoryHistory
                    | PublicSurfaceKind::EntityHistory { .. }
            )
    });
    let history_query_source = needs_history_query_source.then(|| ctx.history_query_source());
    let history_query_source_for_provider = || {
        history_query_source.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "selected history provider is missing its query source",
            )
        })
    };
    for surface in catalog.surfaces() {
        if !scope.includes(surface) || !selection.includes(surface) {
            continue;
        }
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
                    ctx.changelog_query_source(),
                )
                .await?;
            }
            PublicSurfaceKind::History => {
                history::register_history_provider(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    history_query_source_for_provider()?,
                )
                .await?;
            }
            PublicSurfaceKind::File => {
                file::register_lix_file_active_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id(),
                    ctx.live_state(),
                    ctx.filesystem_path_index(),
                    Arc::clone(&branch_ref),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.functions(),
                    ctx.session_file_views(),
                )
                .await?;
            }
            PublicSurfaceKind::FileByBranch => {
                file::register_lix_file_by_branch_provider(
                    session,
                    &surface.name,
                    ctx.live_state(),
                    ctx.filesystem_path_index(),
                    Arc::clone(&branch_ref),
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                    ctx.functions(),
                    ctx.session_file_views(),
                )
                .await?;
            }
            PublicSurfaceKind::FileHistory => {
                file_history::register_lix_file_history_surface(
                    session,
                    &surface.name,
                    ctx.commit_graph(),
                    history_query_source_for_provider()?,
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
                    ctx.filesystem_path_index(),
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
                    ctx.filesystem_path_index(),
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
                    history_query_source_for_provider()?,
                )
                .await?;
            }
            PublicSurfaceKind::EntityBase { .. }
            | PublicSurfaceKind::EntityByBranch { .. }
            | PublicSurfaceKind::EntityHistory { .. } => {}
        }
    }
    let needs_entity_history = catalog.surfaces().any(|surface| {
        scope.includes(surface)
            && selection.includes(surface)
            && matches!(&surface.kind, PublicSurfaceKind::EntityHistory { .. })
    });
    entity::register_entity_providers(
        session,
        ctx.active_branch_id(),
        ctx.live_state(),
        Arc::clone(&branch_ref),
        needs_entity_history.then(|| Arc::new(tokio::sync::Mutex::new(ctx.commit_graph()))),
        if needs_entity_history {
            Some(history_query_source_for_provider()?)
        } else {
            None
        },
        catalog,
        scope == ReadProviderScope::All,
        selection,
    )
    .await?;

    Ok(())
}

pub(crate) async fn register_write(
    session: &SessionContext,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
    options: SqlWriteSessionOptions,
    selection: &ProviderSelection,
) -> Result<(), LixError> {
    let catalog = write_ctx.public_catalog()?;
    register_write_from_catalog(session, write_ctx, branch_ref, options, &catalog, selection).await
}

pub(crate) async fn register_transaction<C>(
    session: &SessionContext,
    read_ctx: &C,
    read_branch_ref: Arc<dyn BranchRefReader>,
    write_ctx: SqlWriteContext,
    write_branch_ref: Arc<dyn BranchRefReader>,
    options: SqlWriteSessionOptions,
    selection: &ProviderSelection,
) -> Result<(), LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    // Both capabilities project the same transaction-scoped schema snapshot.
    // Reuse that immutable metadata, then install read-only providers from the
    // committed read capability and writable providers from the overlay.
    let catalog = write_ctx.public_catalog()?;
    register_read_from_catalog(
        session,
        read_ctx,
        read_branch_ref,
        &catalog,
        ReadProviderScope::ReadOnly,
        selection,
    )
    .await?;
    register_write_from_catalog(
        session,
        write_ctx,
        write_branch_ref,
        options,
        &catalog,
        selection,
    )
    .await
}

async fn register_write_from_catalog(
    session: &SessionContext,
    write_ctx: SqlWriteContext,
    branch_ref: Arc<dyn BranchRefReader>,
    options: SqlWriteSessionOptions,
    catalog: &PublicCatalog,
    selection: &ProviderSelection,
) -> Result<(), LixError> {
    for surface in catalog.surfaces() {
        if !selection.includes(surface) {
            continue;
        }
        match &surface.kind {
            PublicSurfaceKind::LixState => {
                lix_state::register_lix_state_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::LixStateByBranch => {
                lix_state::register_lix_state_by_branch_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::Branch => {
                branch::register_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::File => {
                file::register_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                    options.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::FileByBranch => {
                file::register_by_branch_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                    options.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Directory => {
                directory::register_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryByBranch => {
                directory::register_by_branch_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
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
    entity::register_entity_write_providers(session, write_ctx, branch_ref, catalog, selection)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::sync::Arc;

    use async_trait::async_trait;
    use serde_json::json;

    use datafusion::arrow::datatypes::{DataType, SchemaRef};
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
    use crate::storage_adapter::{
        Memory, MemoryRead, SharedStorageAdapterRead, StorageAdapter, StorageReadOptions,
    };

    use super::{
        ProviderSelection, ReadProviderScope, branch, change, directory, directory_history, entity,
        file, file_history, history, is_write_surface, lix_state, read_provider_selection,
    };

    fn selection_for_sql(sql: &[&str]) -> ProviderSelection {
        let statements = sql
            .iter()
            .map(|sql| crate::sql2::parse_statement(sql).expect("SQL should parse"))
            .collect::<Vec<_>>();
        read_provider_selection(&SessionContext::new(), &statements)
    }

    fn selected_names(names: &[&str]) -> ProviderSelection {
        ProviderSelection::Only(names.iter().map(|name| (*name).to_string()).collect())
    }

    #[test]
    fn referenced_provider_selection_uses_datafusion_cte_and_set_operation_resolution() {
        let selection = selection_for_sql(&["WITH shadowed AS (\
                 SELECT entity_pk FROM lix_state \
                 WHERE EXISTS (SELECT 1 FROM lix_file)\
             ) \
             SELECT left_side.entity_pk \
             FROM shadowed AS left_side \
             JOIN (\
                 SELECT entity_pk FROM lix_change \
                 UNION ALL \
                 SELECT entity_pk FROM lix_change\
             ) AS right_side \
               ON left_side.entity_pk = right_side.entity_pk \
             JOIN public.\"lix_directory\" AS directory_a ON true \
             JOIN public.\"lix_directory\" AS directory_b ON true"]);

        assert_eq!(
            selection,
            selected_names(&["lix_change", "lix_directory", "lix_file", "lix_state"])
        );
    }

    #[test]
    fn referenced_provider_selection_excludes_shadowed_and_recursive_cte_names() {
        assert_eq!(
            selection_for_sql(&["WITH lix_file AS (SELECT entity_pk FROM lix_state) \
                 SELECT * FROM lix_file",]),
            selected_names(&["lix_state"])
        );
        assert_eq!(
            selection_for_sql(&["WITH RECURSIVE walk(id) AS (\
                     SELECT id FROM lix_branch \
                     UNION ALL \
                     SELECT branch.id FROM lix_branch AS branch \
                     JOIN walk ON branch.id = walk.id\
                 ) \
                 SELECT * FROM walk",]),
            selected_names(&["lix_branch"])
        );
    }

    #[test]
    fn referenced_provider_selection_unions_batches_and_preserves_unknown_names() {
        assert_eq!(
            selection_for_sql(&[
                "SELECT * FROM lix_file",
                "SELECT * FROM public.lix_state JOIN \"UnknownTable\" ON true",
            ]),
            selected_names(&["UnknownTable", "lix_file", "lix_state"])
        );
    }

    #[test]
    fn referenced_provider_selection_registers_none_for_table_free_queries() {
        assert_eq!(
            selection_for_sql(&["SELECT 1, lix_uuid_v7()"]),
            ProviderSelection::Only(BTreeSet::new())
        );
    }

    #[test]
    fn referenced_provider_selection_keeps_catalog_wide_information_schema_semantics() {
        assert_eq!(
            selection_for_sql(&["SELECT * FROM information_schema.tables"]),
            ProviderSelection::All
        );
        assert_eq!(selection_for_sql(&["SHOW TABLES"]), ProviderSelection::All);
    }

    #[test]
    fn visible_schema_loading_boundary_is_conservative() {
        assert!(!selection_for_sql(&["SELECT 1"]).requires_visible_schemas());
        assert!(!selection_for_sql(&["SELECT * FROM lix_key_value"]).requires_visible_schemas());
        assert!(
            !selection_for_sql(&["SELECT * FROM lix_key_value_history"]).requires_visible_schemas()
        );
        assert!(
            !selection_for_sql(&["SELECT * FROM lix_key_value JOIN lix_state ON false"])
                .requires_visible_schemas()
        );
        assert!(selection_for_sql(&["SELECT * FROM custom_entity"]).requires_visible_schemas());
        assert!(
            selection_for_sql(&["SELECT * FROM lix_key_value JOIN custom_entity ON false",])
                .requires_visible_schemas()
        );
        assert!(
            selection_for_sql(&["SELECT * FROM information_schema.tables"])
                .requires_visible_schemas()
        );
    }

    #[test]
    fn referenced_provider_selection_filters_transaction_capabilities_symmetrically() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");
        let selection = selected_names(&["lix_file", "lix_state_history"]);

        let committed_read_names = catalog
            .surfaces()
            .filter(|surface| {
                ReadProviderScope::ReadOnly.includes(surface) && selection.includes(surface)
            })
            .map(|surface| surface.name.as_str())
            .collect::<Vec<_>>();
        let overlay_write_names = catalog
            .surfaces()
            .filter(|surface| is_write_surface(surface) && selection.includes(surface))
            .map(|surface| surface.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(committed_read_names, vec!["lix_state_history"]);
        assert_eq!(overlay_write_names, vec!["lix_file"]);
    }

    #[test]
    fn transaction_registration_partitions_provider_construction_once() {
        let schema = json!({
            "x-lix-key": "phase8_entity",
            "x-lix-primary-key": ["/id"],
            "type": "object",
            "properties": { "id": { "type": "string" } }
        });
        let catalog = PublicCatalog::from_visible_schemas(&[schema]).expect("catalog should build");

        let read_only = catalog
            .surfaces()
            .filter(|surface| ReadProviderScope::ReadOnly.includes(surface))
            .map(|surface| surface.name.as_str())
            .collect::<Vec<_>>();
        let writable = catalog
            .surfaces()
            .filter(|surface| is_write_surface(surface))
            .map(|surface| surface.name.as_str())
            .collect::<Vec<_>>();
        let all_read = catalog
            .surfaces()
            .filter(|surface| ReadProviderScope::All.includes(surface))
            .count();

        assert_eq!(
            read_only,
            vec![
                "lix_change",
                "lix_directory_history",
                "lix_file_history",
                "lix_state_history",
                "phase8_entity_history",
            ]
        );
        assert_eq!(
            writable,
            vec![
                "lix_branch",
                "lix_directory",
                "lix_directory_by_branch",
                "lix_file",
                "lix_file_by_branch",
                "lix_state",
                "lix_state_by_branch",
                "phase8_entity",
                "phase8_entity_by_branch",
            ]
        );
        assert_eq!(read_only.len() + writable.len(), catalog.surfaces().count());
        assert_eq!(all_read + writable.len(), 23, "previous construction count");
        assert_eq!(
            read_only.len() + writable.len(),
            14,
            "new construction count"
        );
    }

    #[test]
    fn target_write_selection_reduces_provider_construction_count_to_one() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");
        let all_writable = catalog
            .surfaces()
            .filter(|surface| is_write_surface(surface))
            .count();
        let selection = selected_names(&["lix_file"]);
        let selected_writable = catalog
            .surfaces()
            .filter(|surface| is_write_surface(surface) && selection.includes(surface))
            .map(|surface| surface.name.as_str())
            .collect::<Vec<_>>();

        assert_eq!(all_writable, 7, "previous standalone write count");
        assert_eq!(selected_writable, vec!["lix_file"]);
    }

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

    #[test]
    fn file_data_surfaces_use_large_binary() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");

        for surface_name in ["lix_file", "lix_file_by_branch", "lix_file_history"] {
            let schema = catalog
                .surface_schema(surface_name)
                .unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
            let data_field = schema
                .field_with_name("data")
                .unwrap_or_else(|_| panic!("{surface_name}.data should exist"));

            assert_eq!(
                data_field.data_type(),
                &DataType::LargeBinary,
                "{surface_name}.data should avoid Arrow Binary's 32-bit offset limit",
            );
        }
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
            Some(Arc::new(tokio::sync::Mutex::new(Box::new(
                EmptyCommitGraphReader,
            )))),
            Some(empty_history_query_source().await),
            &catalog,
            true,
            &ProviderSelection::All,
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
    -> crate::sql2::SqlHistoryQuerySource<SharedStorageAdapterRead<MemoryRead>> {
        let storage = StorageAdapter::new(Memory::new());
        let read_scope = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        HistoryQuerySource {
            json_reader: JsonStoreContext::new().reader(SharedStorageAdapterRead::new(read_scope)),
        }
    }

    struct EmptyLiveStateReader;

    #[async_trait]
    impl LiveStateReader for EmptyLiveStateReader {
        async fn load_exact_rows(
            &self,
            request: &crate::live_state::LiveStateExactBatchRequest,
        ) -> Result<Vec<Option<MaterializedLiveStateRow>>, LixError> {
            crate::live_state::load_exact_rows_via_scan_for_test(self, request).await
        }

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
