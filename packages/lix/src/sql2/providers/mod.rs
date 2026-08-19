#![allow(clippy::cloned_ref_to_slice_refs, clippy::match_same_arms)]

use std::collections::BTreeSet;
use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::LixError;
use crate::branch::BranchRefReader;

mod branch;
mod branch_selection;
mod change;
mod columns;
mod diff;
mod directory;
mod directory_history;
pub(crate) use diff::register_diff_function;
mod diff_command;
mod schema;
mod schema_history;
mod file;
mod file_history;
mod filesystem_history_path;
mod filesystem_working_diff;
mod history_table_function;
mod history_util;
mod spec;
pub(crate) use spec::{PhysicalScanKey, SpecScanExec, StatementScanKey};
mod upsert;
mod values;
mod working_diff;

use crate::sql2::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{SqlExecutionContext, SqlWriteContext};

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::TableSource;

pub(crate) use directory::execute_exact_lix_directory_root_listing;
pub(crate) use schema::{execute_exact_schema_batch_read, execute_exact_schema_point_read};
pub(crate) use file::{
    ExactLixFileReadColumn, ExactLixFileReadSelector, FastLixFilePathWriteConflict,
    execute_exact_lix_file_batch_read, execute_exact_lix_file_id_manifest_batch_read,
    execute_exact_lix_file_read, execute_exact_lix_file_root_listing,
    execute_fast_lix_file_content_update_by_id,
    execute_fast_lix_file_content_update_by_id_with_metadata, execute_fast_lix_file_id_path_writes,
    execute_fast_lix_file_path_writes, execute_fast_lix_file_prepared_path_write,
};
#[cfg(test)]
pub(crate) use filesystem_working_diff::filesystem_working_diff_schema;
pub(crate) use spec::{DmlReturning, SpecWriteTarget, WriteTargetRegistry};
pub(crate) use upsert::{UpsertAction, excluded_field_name};

pub(crate) fn history_anchor_column(source: &dyn TableSource) -> Option<&'static str> {
    let source = source.as_any().downcast_ref::<DefaultTableSource>()?;
    let provider = source
        .table_provider
        .as_any()
        .downcast_ref::<spec::SpecTableProvider>()?;
    provider.history_anchor_column()
}

pub(crate) async fn register_read<C>(
    session: &SessionContext,
    ctx: &C,
    branch_ref: Arc<dyn BranchRefReader>,
    active_branch_commit_id: Option<String>,
    selection: &ProviderSelection,
) -> Result<(), LixError>
where
    C: SqlExecutionContext + ?Sized,
{
    if selection.is_empty() {
        return Ok(());
    }
    let catalog = if selection.requires_visible_schemas() {
        ctx.public_catalog().await?
    } else {
        Arc::clone(PublicCatalog::fixed_system_shared())
    };
    register_read_from_catalog(
        session,
        ctx,
        branch_ref,
        active_branch_commit_id,
        catalog.as_ref(),
        ReadProviderScope::All,
        selection,
    )
    .await?;
    register_information_schema(session, selection, catalog)
}

/// Installs the `information_schema` views only for statements that can reach
/// them.
///
/// `read_provider_selection` widens to [`ProviderSelection::All`] for every
/// `information_schema`-qualified reference and every `SHOW` form, so a narrowed
/// selection provably never resolves an information-schema table. Registering
/// the schema anyway cost one catalog write lock and one `SchemaProvider`
/// allocation on every ordinary statement.
fn register_information_schema(
    session: &SessionContext,
    selection: &ProviderSelection,
    catalog: Arc<PublicCatalog>,
) -> Result<(), LixError> {
    if !matches!(selection, ProviderSelection::All) {
        return Ok(());
    }
    crate::sql2::information_schema::register(session, catalog)
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
    /// visible catalog so information-schema, custom rows, and normal
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
    state: &datafusion::execution::session_state::SessionState,
    statements: &[datafusion::sql::parser::Statement],
) -> ProviderSelection {
    let mut names = BTreeSet::new();
    // Resolving references only reads the SQL parser configuration, so the
    // statement's pooled session state is used directly instead of cloning the
    // live one.
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
    active_branch_commit_id: Option<String>,
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
            && match &surface.kind {
                PublicSurfaceKind::FileHistory | PublicSurfaceKind::DirectoryHistory => true,
                PublicSurfaceKind::SchemaHistory { schema_key } => {
                    schema_key != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
                }
                _ => false,
            }
    });
    let history_query_source = if needs_history_query_source {
        let active_branch_commit_id = active_branch_commit_id.ok_or_else(|| {
            LixError::branch_not_found(
                ctx.active_branch_id(),
                "register SQL history providers",
                "active branch",
            )
        })?;
        Some(ctx.history_query_source(active_branch_commit_id))
    } else {
        None
    };
    let history_query_source_for_provider = || {
        history_query_source.clone().ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "selected history provider is missing its query source",
            )
        })
    };
    let needs_checkpoint_history = catalog.surfaces().any(|surface| {
        scope.includes(surface)
            && selection.includes(surface)
            && matches!(
                &surface.kind,
                PublicSurfaceKind::SchemaHistory { schema_key }
                    if schema_key == crate::checkpoint::CHECKPOINT_SCHEMA_KEY
            )
    });
    let checkpoint_history_query_source = if needs_checkpoint_history {
        let global_head = branch_ref
            .load_head_commit_id(crate::GLOBAL_BRANCH_ID)
            .await?
            .ok_or_else(|| {
                LixError::branch_not_found(
                    crate::GLOBAL_BRANCH_ID,
                    "register checkpoint history provider",
                    "global branch",
                )
            })?;
        Some(ctx.history_query_source(global_head.to_string()))
    } else {
        None
    };
    for surface in catalog.surfaces() {
        if !scope.includes(surface) || !selection.includes(surface) {
            continue;
        }
        match &surface.kind {
            PublicSurfaceKind::Branch => {
                branch::register_lix_branch_read_provider(
                    session,
                    &surface.name,
                    ctx.hot_state(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::WorkingDiff => {
                working_diff::register_working_diff_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id().to_string(),
                    Arc::clone(&branch_ref),
                    ctx.commit_graph(),
                    ctx.changelog_query_source(),
                )
                .await?;
            }
            PublicSurfaceKind::FileWorkingDiff => {
                filesystem_working_diff::register_filesystem_working_diff_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id().to_string(),
                    Arc::clone(&branch_ref),
                    ctx.commit_graph(),
                    ctx.changelog_query_source(),
                    filesystem_working_diff::FilesystemWorkingDiffKind::File,
                )
                .await?;
            }
            PublicSurfaceKind::DirectoryWorkingDiff => {
                filesystem_working_diff::register_filesystem_working_diff_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id().to_string(),
                    Arc::clone(&branch_ref),
                    ctx.commit_graph(),
                    ctx.changelog_query_source(),
                    filesystem_working_diff::FilesystemWorkingDiffKind::Directory,
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
            PublicSurfaceKind::File => {
                file::register_lix_file_active_provider(
                    session,
                    &surface.name,
                    ctx.active_branch_id(),
                    ctx.hot_state(),
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
                    ctx.hot_state(),
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
            PublicSurfaceKind::SchemaBase { .. } | PublicSurfaceKind::SchemaHistory { .. }
            | PublicSurfaceKind::Revert
            | PublicSurfaceKind::Apply
            | PublicSurfaceKind::CreateCheckpoint => {}
        }
    }
    let needs_row_history = catalog.surfaces().any(|surface| {
        scope.includes(surface)
            && selection.includes(surface)
            && matches!(&surface.kind, PublicSurfaceKind::SchemaHistory { .. })
    });
    schema::register_row_providers(
        session,
        ctx.active_branch_id(),
        ctx.hot_state(),
        ctx.row_snapshot_reader(),
        Arc::clone(&branch_ref),
        needs_row_history.then(|| Arc::new(tokio::sync::Mutex::new(ctx.commit_graph()))),
        history_query_source,
        checkpoint_history_query_source,
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
    register_write_from_catalog(session, write_ctx, branch_ref, options, &catalog, selection)
        .await?;
    register_information_schema(session, selection, catalog)
}

pub(crate) async fn register_transaction<C>(
    session: &SessionContext,
    read_ctx: &C,
    read_branch_ref: Arc<dyn BranchRefReader>,
    active_branch_commit_id: Option<String>,
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
        active_branch_commit_id,
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
    .await?;
    register_information_schema(session, selection, catalog)
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
            PublicSurfaceKind::Directory => {
                directory::register_active_write_provider(
                    session,
                    &surface.name,
                    write_ctx.clone(),
                    Arc::clone(&branch_ref),
                )
                .await?;
            }
            PublicSurfaceKind::Revert => {
                diff_command::register_diff_command_provider(
                    session,
                    &surface.name,
                    crate::sql2::DiffCommand::Revert,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Apply => {
                diff_command::register_diff_command_provider(
                    session,
                    &surface.name,
                    crate::sql2::DiffCommand::Apply,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::CreateCheckpoint => {
                diff_command::register_diff_command_provider(
                    session,
                    &surface.name,
                    crate::sql2::DiffCommand::CreateCheckpoint,
                    write_ctx.clone(),
                )
                .await?;
            }
            PublicSurfaceKind::Change
            | PublicSurfaceKind::WorkingDiff
            | PublicSurfaceKind::FileWorkingDiff
            | PublicSurfaceKind::DirectoryWorkingDiff
            | PublicSurfaceKind::FileHistory
            | PublicSurfaceKind::DirectoryHistory => {}
            PublicSurfaceKind::SchemaBase { .. } | PublicSurfaceKind::SchemaHistory { .. } => {}
        }
    }
    schema::register_row_write_providers(session, write_ctx, branch_ref, catalog, selection)
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
        CommitGraphChangeHistoryRequest, CommitGraphNode, CommitGraphReader,
        ReachableCommitGraphNode,
    };
    use crate::hot_state::{HotStateReader, HotStateScanRequest};
    use crate::json_store::JsonStoreContext;
    use crate::sql2::HistoryQuerySource;
    use crate::sql2::catalog::{
        PublicCatalog, PublicSurfaceKind, derive_schema_surface_spec_from_schema,
    };
    use crate::storage_adapter::{
        Memory, MemoryRead, SharedStorageAdapterRead, StorageAdapter, StorageReadOptions,
    };

    use super::{
        ProviderSelection, ReadProviderScope, branch, change, directory, directory_history, file,
        file_history, filesystem_working_diff, is_write_surface, read_provider_selection, schema,
        working_diff,
    };

    fn selection_for_sql(sql: &[&str]) -> ProviderSelection {
        let statements = sql
            .iter()
            .map(|sql| crate::sql2::parse_statement(sql).expect("SQL should parse"))
            .collect::<Vec<_>>();
        read_provider_selection(&SessionContext::new().state(), &statements)
    }

    fn selected_names(names: &[&str]) -> ProviderSelection {
        ProviderSelection::Only(names.iter().map(|name| (*name).to_string()).collect())
    }

    #[test]
    fn referenced_provider_selection_uses_datafusion_cte_and_set_operation_resolution() {
        let selection = selection_for_sql(&["WITH shadowed AS (\
                 SELECT id FROM lix_key_value \
                 WHERE EXISTS (SELECT 1 FROM lix_file)\
             ) \
             SELECT left_side.id \
             FROM shadowed AS left_side \
             JOIN (\
                 SELECT row_pk FROM lix_change \
                 UNION ALL \
                 SELECT row_pk FROM lix_change\
             ) AS right_side \
               ON left_side.id = right_side.row_pk \
             JOIN public.\"lix_directory\" AS directory_a ON true \
             JOIN public.\"lix_directory\" AS directory_b ON true"]);

        assert_eq!(
            selection,
            selected_names(&["lix_change", "lix_directory", "lix_file", "lix_key_value"])
        );
    }

    #[test]
    fn referenced_provider_selection_excludes_shadowed_and_recursive_cte_names() {
        assert_eq!(
            selection_for_sql(&["WITH lix_file AS (SELECT id FROM lix_key_value) \
                 SELECT * FROM lix_file",]),
            selected_names(&["lix_key_value"])
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
                "SELECT * FROM public.lix_key_value JOIN \"UnknownTable\" ON true",
            ]),
            selected_names(&["UnknownTable", "lix_file", "lix_key_value"])
        );
    }

    #[test]
    fn referenced_provider_selection_registers_none_for_table_free_queries() {
        assert_eq!(
            selection_for_sql(&["SELECT 1, uuidv7()"]),
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
            !selection_for_sql(&["SELECT * FROM lix_key_value JOIN lix_file ON false"])
                .requires_visible_schemas()
        );
        assert!(selection_for_sql(&["SELECT * FROM custom_row"]).requires_visible_schemas());
        assert!(
            selection_for_sql(&["SELECT * FROM lix_key_value JOIN custom_row ON false",])
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
        let selection = selected_names(&["lix_file", "lix_file_history"]);

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

        assert_eq!(committed_read_names, vec!["lix_file_history"]);
        assert_eq!(overlay_write_names, vec!["lix_file"]);
    }

    #[test]
    fn transaction_registration_partitions_provider_construction_once() {
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "phase8_row",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
            ],
            "primary_key": ["id"],
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
                "lix_directory_working_diff",
                "lix_file_history",
                "lix_file_working_diff",
                "lix_working_diff",
                "phase8_row_history",
            ]
        );
        assert_eq!(
            writable,
            vec![
                "lix_apply",
                "lix_branch",
                "lix_create_checkpoint",
                "lix_directory",
                "lix_file",
                "lix_revert",
                "phase8_row",
            ]
        );
        assert_eq!(read_only.len() + writable.len(), catalog.surfaces().count());
        assert_eq!(all_read + writable.len(), 21, "previous construction count");
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

        assert_eq!(all_writable, 6, "previous standalone write count");
        assert_eq!(selected_writable, vec!["lix_file"]);
    }

    #[test]
    fn provider_history_schemas_match_catalog_contract_order() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");

        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file",
            file::lix_file_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_file_working_diff",
            filesystem_working_diff::filesystem_working_diff_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory_working_diff",
            filesystem_working_diff::filesystem_working_diff_schema(),
        );
        assert_surface_schema_matches_provider_schema(
            &catalog,
            "lix_directory",
            directory::lix_directory_schema(),
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
            "lix_working_diff",
            working_diff::working_diff_schema(),
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
    fn file_content_surfaces_use_large_binary() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");

        for surface_name in ["lix_file", "lix_file_history"] {
            let schema = catalog
                .surface_schema(surface_name)
                .unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
            let content_field = schema
                .field_with_name("content")
                .unwrap_or_else(|_| panic!("{surface_name}.content should exist"));

            assert_eq!(
                content_field.data_type(),
                &DataType::LargeBinary,
                "{surface_name}.content should avoid Arrow Binary's 32-bit offset limit",
            );
        }
    }

    #[tokio::test]
    async fn provider_row_schemas_match_catalog_contract_order() {
        let schema = json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "phase8_row",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "count", "type": "int8", "nullable": true },
                { "name": "body", "type": "jsonb", "nullable": true },
            ],
            "primary_key": ["id"],
        });
        let catalog =
            PublicCatalog::from_visible_schemas(&[schema.clone()]).expect("catalog should build");
        let _spec = derive_schema_surface_spec_from_schema(&schema).expect("schema should derive");
        let session = SessionContext::new();
        schema::register_row_providers(
            &session,
            "01920000-0000-7000-8000-0000000000a1",
            Arc::new(EmptyHotStateReader),
            None,
            Arc::new(EmptyBranchRefReader),
            Some(Arc::new(tokio::sync::Mutex::new(Box::new(
                EmptyCommitGraphReader,
            )))),
            Some(empty_history_query_source().await),
            None,
            &catalog,
            true,
            &ProviderSelection::All,
        )
        .await
        .expect("row providers should register");

        assert_registered_table_schema_matches_catalog(&session, &catalog, "phase8_row").await;
        assert_registered_table_schema_matches_catalog(&session, &catalog, "phase8_row_history")
            .await;
    }

    async fn assert_registered_table_schema_matches_catalog(
        session: &SessionContext,
        catalog: &PublicCatalog,
        surface_name: &str,
    ) {
        let surface = catalog
            .surface(surface_name)
            .unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
        let provider = if matches!(
            surface.kind,
            PublicSurfaceKind::SchemaHistory { .. }
                | PublicSurfaceKind::FileHistory
                | PublicSurfaceKind::DirectoryHistory
        ) {
            session
                .table_function(surface_name)
                .unwrap_or_else(|error| panic!("{surface_name} function should load: {error}"))
                .create_table_provider(&[])
                .unwrap_or_else(|error| panic!("{surface_name} function should bind: {error}"))
        } else {
            session
                .table_provider(surface_name)
                .await
                .unwrap_or_else(|error| panic!("{surface_name} provider should load: {error}"))
        };
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
        let history_surface = matches!(
            surface.kind,
            PublicSurfaceKind::SchemaHistory { .. }
                | PublicSurfaceKind::FileHistory
                | PublicSurfaceKind::DirectoryHistory
        );
        let catalog_column_names = surface
            .columns
            .iter()
            .filter(|column| !history_surface || column.is_public())
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let provider_field_names = provider_schema
            .fields()
            .iter()
            .filter(|field| {
                !history_surface
                    || field.name() != crate::sql2::history_route::HISTORY_COL_AS_OF_COMMIT_ID
            })
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            catalog_column_names, provider_field_names,
            "{surface_name} column order"
        );

        if !history_surface {
            let catalog_schema = catalog
                .surface_schema(surface_name)
                .unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
            assert_eq!(
                catalog_schema.fields(),
                provider_schema.fields(),
                "{surface_name}"
            );
        }
    }

    async fn empty_history_query_source()
    -> crate::sql2::SqlHistoryQuerySource<SharedStorageAdapterRead<MemoryRead>> {
        let storage = StorageAdapter::new(Memory::new());
        let read_scope = storage
            .begin_read(StorageReadOptions::default())
            .await
            .expect("read should open");
        let read_scope = SharedStorageAdapterRead::new(read_scope);
        HistoryQuerySource {
            store: read_scope.clone(),
            json_reader: JsonStoreContext::new().reader(read_scope),
            certified_history_reader: None,
            default_as_of_commit_id: CommitId::for_test_label("history-default").to_string(),
        }
    }

    struct EmptyHotStateReader;

    #[async_trait]
    impl HotStateReader for EmptyHotStateReader {
        async fn load_exact_batch(
            &self,
            request: &crate::hot_state::HotStateExactBatchRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateExactBatch, LixError> {
            crate::hot_state::load_exact_batch_via_scan_for_test(self, request).await
        }

        async fn scan_batch(
            &self,
            _request: &HotStateScanRequest,
        ) -> Result<crate::hot_state::MaterializedHotStateBatch, LixError> {
            Ok(Vec::new().into())
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
            Ok(Vec::new().into())
        }
    }

    struct EmptyCommitGraphReader;

    #[async_trait]
    impl CommitGraphReader for EmptyCommitGraphReader {
        async fn load_node(
            &mut self,
            _commit_id: &CommitId,
        ) -> Result<Option<CommitGraphNode>, LixError> {
            Ok(None)
        }

        async fn reachable_nodes(
            &mut self,
            _head_commit_id: &CommitId,
        ) -> Result<Arc<[ReachableCommitGraphNode]>, LixError> {
            Ok(Vec::new().into())
        }

        async fn change_history_from_commit(
            &mut self,
            _start_commit_id: &CommitId,
            _request: &CommitGraphChangeHistoryRequest,
        ) -> Result<crate::commit_graph::CommitGraphHistory, LixError> {
            Ok(crate::commit_graph::CommitGraphHistory {
                entries: Vec::new(),
                reachable_nodes: Arc::from([]),
            })
        }
    }
}

#[cfg(test)]
pub(crate) use file_history::{file_history_context_census, reset_file_history_context_census};
