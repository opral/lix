#![allow(clippy::cloned_ref_to_slice_refs, clippy::match_same_arms)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use datafusion::prelude::SessionContext;

use crate::LixError;
use crate::branch::BranchRefReader;

mod branch;
mod change;
mod columns;
mod commit_ancestry;
pub(crate) use commit_ancestry::commit_ancestry_schema;
mod diff;
mod directory;
mod directory_history;
pub(crate) use diff::relation_diff_schema;
mod diff_command;
mod file;
mod file_history;
#[cfg(test)]
pub(crate) use file_history::{
    file_history_anchor_probe_census, file_history_bounded_frontier_census,
    file_history_raw_probe_limit_census, reset_file_history_anchor_probe_census,
};
mod filesystem_history_path;
mod history_table_function;
mod history_util;
mod schema;
mod schema_history;
mod state_at;
#[cfg(test)]
pub(crate) use state_at::{arm_state_at_traversal_probe, take_state_at_traversal_probe};
mod spec;
pub(crate) use spec::{PhysicalScanKey, SpecScanExec, StatementScanKey};
mod upsert;
mod values;

use crate::sql2::catalog::{
    PublicCatalog, PublicHistoryKind, PublicSurfaceContract, PublicSurfaceKind,
};
use crate::sql2::session::SqlWriteSessionOptions;
use crate::sql2::{SqlExecutionContext, SqlWriteContext};

use datafusion::datasource::DefaultTableSource;
use datafusion::logical_expr::TableSource;

pub(crate) use directory::execute_exact_lix_directory_root_listing;
pub(crate) use file::{
    ExactLixFileReadColumn, ExactLixFileReadSelector, FastLixFilePathWriteConflict,
    execute_exact_lix_file_batch_read, execute_exact_lix_file_id_manifest_batch_read,
    execute_exact_lix_file_read, execute_exact_lix_file_root_listing,
    execute_fast_lix_file_content_update_by_id,
    execute_fast_lix_file_content_update_by_id_with_metadata, execute_fast_lix_file_id_path_writes,
    execute_fast_lix_file_path_writes, execute_fast_lix_file_prepared_path_write,
};
pub(crate) use schema::{execute_exact_schema_batch_read, execute_exact_schema_point_read};
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
    let catalog = if selection.requires_visible_schemas() {
        ctx.public_catalog().await?
    } else {
        Arc::clone(PublicCatalog::fixed_system_shared())
    };
    crate::sql2::udfs::register_row_ref_function(session, Arc::clone(&catalog));
    if catalog
        .surface("lix_diff")
        .is_some_and(|surface| selection.includes(surface))
    {
        diff::register_diff_function(session, ctx.changelog_query_source(), Arc::clone(&catalog));
    }
    if catalog
        .surface("lix_state_at")
        .is_some_and(|surface| selection.includes(surface))
    {
        state_at::register_state_at_function(
            session,
            ctx.changelog_query_source(),
            Arc::clone(&catalog),
            ctx.active_branch_id().to_string(),
            ctx.blob_reader(),
        );
    }
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
    if !matches!(
        selection,
        ProviderSelection::All | ProviderSelection::AllWithHistory(_)
    ) {
        return Ok(());
    }
    crate::sql2::information_schema::register(session, catalog)
}

/// Snapshot-local providers needed to plan already-bound SQL.
///
/// Ordinary reads use DataFusion's resolver, including its CTE scoping and
/// identifier normalization rules. A narrow AST walk additionally extracts the
/// plan-time relation literal from `lix_history(...)`; provider construction
/// cannot depend on a runtime parameter because each relation has a different
/// result schema. Bound target-only writes select their known target directly.
/// Providers and plans remain scoped to the current storage snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ProviderSelection {
    /// Register every surface when catalog-wide visibility is part of the SQL
    /// semantics (notably `information_schema` and rewritten `SHOW` queries),
    /// or when reference resolution cannot prove a narrower set is sufficient.
    All,
    /// Register every ordinary surface while constructing history providers
    /// only for relation literals that occur in the statement.
    AllWithHistory(BTreeSet<String>),
    /// Register the union of concrete table names referenced by the statements.
    Only {
        names: BTreeSet<String>,
        history_relations: BTreeSet<String>,
    },
}

impl ProviderSelection {
    fn is_empty(&self) -> bool {
        matches!(self, Self::Only { names, history_relations } if names.is_empty() && history_relations.is_empty())
    }

    fn includes(&self, surface: &PublicSurfaceContract) -> bool {
        match self {
            Self::All | Self::AllWithHistory(_) => true,
            Self::Only { names, .. } => names.contains(&surface.name),
        }
    }

    fn includes_history_relation(&self, relation_name: &str) -> bool {
        match self {
            Self::All => false,
            Self::AllWithHistory(history_relations) => history_relations.contains(relation_name),
            Self::Only {
                history_relations, ..
            } => history_relations.contains(relation_name),
        }
    }

    fn requested_history_relations(&self) -> Option<&BTreeSet<String>> {
        match self {
            Self::All => Some(empty_history_relations()),
            Self::AllWithHistory(history_relations) => Some(history_relations),
            Self::Only {
                history_relations, ..
            } => Some(history_relations),
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
            Self::All | Self::AllWithHistory(_) => true,
            Self::Only {
                names,
                history_relations,
            } => {
                names
                    .iter()
                    .any(|name| PublicCatalog::fixed_system().surface(name).is_none())
                    || history_relations.iter().any(|name| {
                        PublicCatalog::fixed_system()
                            .history_relation(name)
                            .is_none()
                    })
            }
        }
    }
}

pub(crate) fn read_provider_selection(
    state: &datafusion::execution::session_state::SessionState,
    statements: &[datafusion::sql::parser::Statement],
) -> ProviderSelection {
    let mut names = BTreeSet::new();
    let mut history_relations = BTreeSet::new();
    let mut requires_all = false;
    // Resolving references only reads the SQL parser configuration, so the
    // statement's pooled session state is used directly instead of cloning the
    // live one.
    for statement in statements {
        collect_history_relation_literals(statement, &mut history_relations);
        collect_dynamic_relation_literals(statement, &mut names);
        if statement_requires_all_providers(statement) {
            requires_all = true;
            continue;
        }
        let Ok(references) = state.resolve_table_references(statement) else {
            requires_all = true;
            continue;
        };
        for reference in references {
            if reference.schema() == Some("information_schema") {
                requires_all = true;
            }
            names.insert(reference.table().to_string());
        }
    }
    if requires_all {
        return all_provider_selection(history_relations);
    }
    ProviderSelection::Only {
        names,
        history_relations,
    }
}

fn all_provider_selection(history_relations: BTreeSet<String>) -> ProviderSelection {
    if history_relations.is_empty() {
        ProviderSelection::All
    } else {
        ProviderSelection::AllWithHistory(history_relations)
    }
}

fn empty_history_relations() -> &'static BTreeSet<String> {
    static EMPTY: std::sync::OnceLock<BTreeSet<String>> = std::sync::OnceLock::new();
    EMPTY.get_or_init(BTreeSet::new)
}

fn collect_history_relation_literals(
    statement: &datafusion::sql::parser::Statement,
    relations: &mut BTreeSet<String>,
) {
    use std::ops::ControlFlow;

    use datafusion::sql::parser::Statement as DataFusionStatement;
    use datafusion::sql::sqlparser::ast::{
        Expr as SqlExpr, FunctionArg, FunctionArgExpr, TableFactor, Value as SqlValue, Visit,
        Visitor,
    };

    struct HistoryRelationVisitor<'a>(&'a mut BTreeSet<String>);

    impl Visitor for HistoryRelationVisitor<'_> {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(arguments),
                ..
            } = table_factor
            else {
                return ControlFlow::Continue(());
            };
            if !crate::sql2::parse::object_name_is_public_function(name, "lix_history") {
                return ControlFlow::Continue(());
            }
            let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(value)))) =
                arguments.args.first()
            else {
                return ControlFlow::Continue(());
            };
            if let SqlValue::SingleQuotedString(relation_name) = &value.value {
                self.0.insert(relation_name.clone());
            }
            ControlFlow::Continue(())
        }
    }

    match statement {
        DataFusionStatement::Statement(statement) => {
            let _ = statement.visit(&mut HistoryRelationVisitor(relations));
        }
        DataFusionStatement::Explain(explain) => {
            collect_history_relation_literals(explain.statement.as_ref(), relations);
        }
        _ => {}
    }
}

/// Diff results inherit their relation's Arrow schema, so runtime schema
/// literals must participate in snapshot-local catalog selection even though
/// DataFusion only resolves the table-function name itself.
fn collect_dynamic_relation_literals(
    statement: &datafusion::sql::parser::Statement,
    relations: &mut BTreeSet<String>,
) {
    use std::ops::ControlFlow;

    use datafusion::sql::parser::Statement as DataFusionStatement;
    use datafusion::sql::sqlparser::ast::{
        Expr as SqlExpr, FunctionArg, FunctionArgExpr, TableFactor, Value as SqlValue, Visit,
        Visitor,
    };

    struct DiffRelationVisitor<'a>(&'a mut BTreeSet<String>);

    impl Visitor for DiffRelationVisitor<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expression: &SqlExpr) -> ControlFlow<Self::Break> {
            let SqlExpr::Function(function) = expression else {
                return ControlFlow::Continue(());
            };
            if !crate::sql2::parse::object_name_is_public_function(
                &function.name,
                "lix_row_ref",
            ) {
                return ControlFlow::Continue(());
            }
            let datafusion::sql::sqlparser::ast::FunctionArguments::List(arguments) =
                &function.args
            else {
                return ControlFlow::Continue(());
            };
            let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(value)))) =
                arguments.args.first()
            else {
                return ControlFlow::Continue(());
            };
            if let SqlValue::SingleQuotedString(relation_name) = &value.value {
                self.0.insert(relation_name.clone());
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            let TableFactor::Table {
                name,
                args: Some(arguments),
                ..
            } = table_factor
            else {
                return ControlFlow::Continue(());
            };
            if !crate::sql2::parse::object_name_is_public_function(name, "lix_diff")
                && !crate::sql2::parse::object_name_is_public_function(name, "lix_state_at")
            {
                return ControlFlow::Continue(());
            }
            let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(value)))) =
                arguments.args.first()
            else {
                return ControlFlow::Continue(());
            };
            if let SqlValue::SingleQuotedString(relation_name) = &value.value {
                self.0.insert(relation_name.clone());
            }
            ControlFlow::Continue(())
        }
    }

    match statement {
        DataFusionStatement::Statement(statement) => {
            let _ = statement.visit(&mut DiffRelationVisitor(relations));
        }
        DataFusionStatement::Explain(explain) => {
            collect_dynamic_relation_literals(explain.statement.as_ref(), relations);
        }
        _ => {}
    }
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
    if let Some(requested) = selection.requested_history_relations() {
        for relation_name in requested {
            if catalog.history_relation(relation_name).is_none() {
                return Err(LixError::new(
                    LixError::CODE_UNSUPPORTED_SQL,
                    format!("lix_history does not support relation '{relation_name}'"),
                ));
            }
        }
    }
    let selected_history = catalog
        .history_relations()
        .filter(|history| selection.includes_history_relation(&history.relation_name))
        .collect::<Vec<_>>();
    let needs_history_query_source = selected_history.iter().any(|history| match &history.kind {
        PublicHistoryKind::File | PublicHistoryKind::Directory => true,
        PublicHistoryKind::Schema { schema_key } => {
            schema_key != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
        }
    });
    let history_query_source = if needs_history_query_source {
        let active_branch_commit_id = active_branch_commit_id.clone().ok_or_else(|| {
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
    let needs_checkpoint_history = selected_history.iter().any(|history| {
        matches!(
            &history.kind,
            PublicHistoryKind::Schema { schema_key }
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
            PublicSurfaceKind::Change => {
                change::register_lix_change_read_provider(
                    session,
                    &surface.name,
                    ctx.changelog_query_source(),
                )
                .await?;
            }
            PublicSurfaceKind::CommitAncestryFunction => {
                let active_branch_commit_id = active_branch_commit_id.clone().ok_or_else(|| {
                    LixError::branch_not_found(
                        ctx.active_branch_id(),
                        "register lix_commit_ancestry",
                        "active branch",
                    )
                })?;
                commit_ancestry::register_commit_ancestry_function(
                    session,
                    &surface.name,
                    active_branch_commit_id,
                    ctx.commit_graph(),
                );
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
            PublicSurfaceKind::SchemaBase { .. }
            | PublicSurfaceKind::HistoryFunction
            | PublicSurfaceKind::DiffFunction
            | PublicSurfaceKind::StateAtFunction
            | PublicSurfaceKind::Revert
            | PublicSurfaceKind::Apply
            | PublicSurfaceKind::CreateCheckpoint
            | PublicSurfaceKind::Restore => {}
        }
    }
    schema::register_row_providers(
        session,
        ctx.active_branch_id(),
        ctx.hot_state(),
        ctx.row_snapshot_reader(),
        Arc::clone(&branch_ref),
        catalog,
        scope == ReadProviderScope::All,
        selection,
    )
    .await?;

    if catalog
        .surface("lix_history")
        .is_some_and(|surface| scope.includes(surface) && selection.includes(surface))
    {
        let row_commit_graph = selected_history
            .iter()
            .any(|history| matches!(history.kind, PublicHistoryKind::Schema { .. }))
            .then(|| Arc::new(tokio::sync::Mutex::new(ctx.commit_graph())));
        let mut providers = BTreeMap::new();
        for history in selected_history {
            let provider = match &history.kind {
                PublicHistoryKind::File => file_history::build_lix_file_history_provider(
                    ctx.commit_graph(),
                    history_query_source_for_provider()?,
                    ctx.blob_reader(),
                    ctx.plugin_host(),
                ),
                PublicHistoryKind::Directory => {
                    directory_history::build_lix_directory_history_provider(
                        ctx.commit_graph(),
                        history_query_source_for_provider()?,
                    )
                }
                PublicHistoryKind::Schema { schema_key } => {
                    let query_source = if schema_key == crate::checkpoint::CHECKPOINT_SCHEMA_KEY {
                        checkpoint_history_query_source.as_ref()
                    } else {
                        history_query_source.as_ref()
                    }
                    .ok_or_else(|| {
                        LixError::new(
                            LixError::CODE_INTERNAL_ERROR,
                            "selected row history provider is missing its query source",
                        )
                    })?;
                    schema_history::build_row_history_provider(
                        &history.relation_name,
                        schema::catalog_schema_spec(catalog, schema_key)?,
                        Arc::clone(row_commit_graph.as_ref().ok_or_else(|| {
                            LixError::new(
                                LixError::CODE_INTERNAL_ERROR,
                                "selected row history provider is missing its commit graph",
                            )
                        })?),
                        query_source.clone(),
                    )
                }
            };
            providers.insert(history.relation_name.clone(), provider);
        }
        history_table_function::register_history_table_function(session, providers)?;
    }

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
    crate::sql2::udfs::register_row_ref_function(session, Arc::clone(&catalog));
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
    crate::sql2::udfs::register_row_ref_function(session, Arc::clone(&catalog));
    if catalog
        .surface("lix_diff")
        .is_some_and(|surface| selection.includes(surface))
    {
        diff::register_diff_function(
            session,
            read_ctx.changelog_query_source(),
            Arc::clone(&catalog),
        );
    }
    if catalog
        .surface("lix_state_at")
        .is_some_and(|surface| selection.includes(surface))
    {
        state_at::register_state_at_function(
            session,
            read_ctx.changelog_query_source(),
            Arc::clone(&catalog),
            read_ctx.active_branch_id().to_string(),
            read_ctx.blob_reader(),
        );
    }
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
            | PublicSurfaceKind::HistoryFunction
            | PublicSurfaceKind::DiffFunction
            | PublicSurfaceKind::StateAtFunction
            | PublicSurfaceKind::CommitAncestryFunction
            | PublicSurfaceKind::Restore => {}
            PublicSurfaceKind::SchemaBase { .. } => {}
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
    use crate::hot_state::{HotStateReader, HotStateScanRequest};
    use crate::sql2::catalog::{PublicCatalog, derive_schema_surface_spec_from_schema};

    use super::{
        ProviderSelection, ReadProviderScope, branch, change, directory, directory_history, file,
        file_history, is_write_surface, read_provider_selection, schema,
    };

    fn selection_for_sql(sql: &[&str]) -> ProviderSelection {
        let statements = sql
            .iter()
            .map(|sql| crate::sql2::parse_statement(sql).expect("SQL should parse"))
            .collect::<Vec<_>>();
        read_provider_selection(&SessionContext::new().state(), &statements)
    }

    fn selected_names(names: &[&str]) -> ProviderSelection {
        ProviderSelection::Only {
            names: names.iter().map(|name| (*name).to_string()).collect(),
            history_relations: BTreeSet::new(),
        }
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
            ProviderSelection::Only {
                names: BTreeSet::new(),
                history_relations: BTreeSet::new(),
            }
        );
    }

    #[test]
    fn history_provider_selection_keeps_the_literal_relation_separate_from_the_function() {
        assert_eq!(
            selection_for_sql(&["SELECT id FROM lix_history('lix_file', $1)"]),
            ProviderSelection::Only {
                names: BTreeSet::from(["lix_history".to_string()]),
                history_relations: BTreeSet::from(["lix_file".to_string()]),
            }
        );
        assert!(
            selection_for_sql(&["SELECT * FROM lix_history('runtime_note')"])
                .requires_visible_schemas()
        );
        assert_eq!(
            selection_for_sql(&[
                "SELECT * FROM information_schema.tables",
                "SELECT * FROM lix_history('lix_file')",
            ]),
            ProviderSelection::AllWithHistory(BTreeSet::from(["lix_file".to_string()]))
        );

        for sql in [
            "SELECT * FROM LIX_HISTORY('lix_file')",
            "SELECT * FROM public.lix_history('lix_file')",
            "SELECT * FROM datafusion.public.lix_history('lix_file')",
            "SELECT * FROM \"lix_history\"('lix_file')",
        ] {
            let selection = selection_for_sql(&[sql]);
            assert!(
                selection.includes_history_relation("lix_file"),
                "{sql} should select the literal history provider: {selection:?}",
            );
        }

        assert!(
            !selection_for_sql(&["SELECT * FROM \"LIX_HISTORY\"('lix_file')"])
                .includes_history_relation("lix_file"),
            "quoted identifiers retain their case",
        );
        assert!(
            !selection_for_sql(&["SELECT * FROM \"PUBLIC\".lix_history('lix_file')"])
                .includes_history_relation("lix_file"),
            "quoted schema identifiers retain their case",
        );
    }

    #[test]
    fn diff_provider_selection_loads_runtime_relation_schema_for_dynamic_side_columns() {
        assert_eq!(
            selection_for_sql(&["SELECT to_value FROM lix_diff('lix_key_value', $1, $2)"]),
            selected_names(&["lix_diff", "lix_key_value"]),
        );
        assert!(
            selection_for_sql(&["SELECT * FROM lix_diff('runtime_note', $1, $2)"])
                .requires_visible_schemas()
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
            !selection_for_sql(&["SELECT * FROM lix_history('lix_key_value')"])
                .requires_visible_schemas()
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
        let selection = selection_for_sql(&[
            "SELECT * FROM lix_file JOIN lix_history('lix_file') AS history ON false",
        ]);

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

        assert_eq!(committed_read_names, vec!["lix_history"]);
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
                "lix_commit_ancestry",
                "lix_diff",
                "lix_history",
                "lix_state_at",
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
                "lix_restore",
                "lix_revert",
                "phase8_row",
            ]
        );
        assert_eq!(read_only.len() + writable.len(), catalog.surfaces().count());
        assert_eq!(all_read + writable.len(), 21, "construction count");
        assert_eq!(read_only.len() + writable.len(), 13, "surface count");
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

        assert_eq!(all_writable, 7, "standalone write count");
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
        assert_history_schema_matches_provider_schema(
            &catalog,
            "lix_file",
            file_history::lix_file_history_schema(),
        );
        assert_history_schema_matches_provider_schema(
            &catalog,
            "lix_directory",
            directory_history::lix_directory_history_schema(),
        );
    }

    #[test]
    fn file_content_surfaces_use_large_binary() {
        let catalog = PublicCatalog::from_visible_schemas(&[]).expect("catalog should build");

        for (surface_name, schema) in [
            ("lix_file", catalog.surface_schema("lix_file")),
            (
                "lix_history('lix_file')",
                catalog.history_relation_schema("lix_file"),
            ),
        ] {
            let schema = schema.unwrap_or_else(|| panic!("{surface_name} should be in catalog"));
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
            &catalog,
            true,
            &ProviderSelection::All,
        )
        .await
        .expect("row providers should register");

        assert_registered_table_schema_matches_catalog(&session, &catalog, "phase8_row").await;
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

    fn assert_history_schema_matches_provider_schema(
        catalog: &PublicCatalog,
        relation_name: &str,
        provider_schema: SchemaRef,
    ) {
        let contract = catalog
            .history_relation(relation_name)
            .unwrap_or_else(|| panic!("{relation_name} history should be in catalog"));
        let catalog_columns = contract
            .columns
            .iter()
            .filter(|column| column.is_public())
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        let provider_columns = provider_schema
            .fields()
            .iter()
            .filter(|field| field.name() != crate::sql2::history_route::HISTORY_COL_AS_OF_COMMIT_ID)
            .map(|field| field.name().as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            catalog_columns, provider_columns,
            "{relation_name} history columns"
        );
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
}

#[cfg(test)]
pub(crate) use file_history::{file_history_context_census, reset_file_history_context_census};
