use std::collections::{BTreeMap, BTreeSet};

use crate::deterministic_mode::{DeterministicSettings, RuntimeFunctionProvider};
use crate::engine::{Engine, TransactionBackendAdapter};
use crate::functions::{LixFunctionProvider, SharedFunctionProvider};
use crate::sql::public::runtime::{
    finalize_public_write_execution, prepare_public_execution_with_internal_access,
    prepare_public_execution_with_registry_and_internal_access,
    prepared_public_write_mutates_public_surface_registry, PreparedPublicExecution,
    PreparedPublicRead, PreparedPublicReadExecution, PreparedPublicWrite,
    PublicWriteExecutionPartition,
};
use crate::sql::storage::sql_text::escape_sql_string;
use crate::state::commit::{
    build_prepared_batch_from_generate_commit_result_with_executor, generate_commit,
    load_commit_active_accounts, load_version_info_for_versions, CommitQueryExecutor,
    CreateCommitError, CreateCommitErrorKind, CreateCommitExpectedHead,
    CreateCommitInvariantChecker, CreateCommitPreconditions, CreateCommitWriteLane,
    DomainChangeInput, GenerateCommitArgs, GenerateCommitResult, MaterializedStateRow, VersionInfo,
};
use crate::state::validation::{
    validate_batch_local_write, validate_commit_time_write, validate_inserts, validate_updates,
};
use crate::{
    CanonicalPluginKey, CanonicalSchemaKey, CanonicalSchemaVersion, EntityId, FileId, LixBackend,
    LixError, LixTransaction, QueryResult, Value, VersionId,
};

use crate::schema::live_layout::{
    is_untracked_live_table, live_schema_key_for_table_name, load_live_row_access_with_backend,
    normalized_live_column_values,
};
use crate::schema::live_store::{
    load_live_rows_with_executor, logical_snapshot_text, LiveRowScope, LoadedLiveRow,
};
use crate::sql::execution::contracts::effects::PlanEffects;
use crate::sql::execution::contracts::execution_plan::ExecutionPlan;
use crate::sql::execution::contracts::planned_statement::{
    PlannedStatementSet, SchemaLiveTableRequirement,
};
use crate::sql::execution::contracts::requirements::PlanRequirements;
use crate::sql::execution::contracts::result_contract::ResultContract;
use crate::sql::execution::derive_requirements::derive_plan_requirements;
use crate::sql::execution::execute::SqlExecutionOutcome;
use crate::sql::execution::intent::{
    collect_execution_intent_with_backend, ExecutionIntent, IntentCollectionPolicy,
};
use crate::sql::execution::parse::parse_sql;
use crate::sql::execution::plan::build_execution_plan;
use crate::sql::execution::runtime_effects::{
    build_binary_blob_fastcdc_write_program, BinaryBlobWrite, FilesystemTransactionFileState,
};
use crate::sql::execution::write_program_runner::execute_write_program_with_transaction;
use crate::sql::execution::write_txn_plan::{
    PendingFilesystemOverlay, PendingRegisteredSchemaOverlay, PendingSemanticOverlay,
    PendingSemanticRow, PendingSemanticStorage,
};
use crate::state::internal::write_program::WriteProgram;
use crate::CanonicalJson;
use serde_json::{json, Value as JsonValue};
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderBy, OrderByExpr,
    OrderByKind, Select, SelectItem, SetExpr, Statement, TableFactor, Value as SqlValue,
};

const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const GLOBAL_VERSION_ID: &str = "global";

pub(crate) struct PreparationPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct PreparedExecutionContext {
    pub(crate) intent: ExecutionIntent,
    pub(crate) settings: DeterministicSettings,
    pub(crate) sequence_start: i64,
    pub(crate) functions: SharedFunctionProvider<RuntimeFunctionProvider>,
    pub(crate) plan: ExecutionPlan,
    pub(crate) public_read: Option<PreparedPublicRead>,
    pub(crate) public_write: Option<PreparedPublicWrite>,
}

pub(crate) fn prepared_execution_mutates_public_surface_registry(
    prepared: &PreparedExecutionContext,
) -> Result<bool, LixError> {
    if prepared.public_write.is_some() {
        return prepared
            .public_write
            .as_ref()
            .map(prepared_public_write_mutates_public_surface_registry)
            .transpose()
            .map(|value| value.unwrap_or(false));
    }

    if prepared.plan.preprocess.mutations.iter().any(|row| {
        row.schema_key == REGISTERED_SCHEMA_KEY
            && row.version_id == GLOBAL_VERSION_ID
            && !row.untracked
    }) {
        return Ok(true);
    }

    let dirty = match prepared.plan.preprocess.internal_state.as_ref() {
        Some(crate::state::internal::InternalStatePlan {
            postprocess: Some(crate::state::internal::PostprocessPlan::VtableUpdate(plan)),
        }) => plan.schema_key == REGISTERED_SCHEMA_KEY,
        Some(crate::state::internal::InternalStatePlan {
            postprocess: Some(crate::state::internal::PostprocessPlan::VtableDelete(plan)),
        }) => plan.schema_key == REGISTERED_SCHEMA_KEY,
        _ => false,
    };

    Ok(dirty)
}

#[derive(Debug, Clone)]
pub(crate) struct PendingPublicCommitSession {
    pub(crate) lane: CreateCommitWriteLane,
    pub(crate) commit_id: String,
    pub(crate) change_set_id: String,
    pub(crate) commit_change_id: String,
    pub(crate) commit_change_snapshot_id: String,
    pub(crate) commit_materialized_change_id: String,
    pub(crate) commit_schema_version: String,
    pub(crate) commit_file_id: String,
    pub(crate) commit_plugin_key: String,
    pub(crate) commit_snapshot: JsonValue,
}

pub(crate) struct PublicCommitInvariantChecker<'a> {
    planned_write: &'a crate::sql::public::planner::ir::PlannedWrite,
    schema_cache: crate::state::validation::SchemaCache,
}

impl<'a> PublicCommitInvariantChecker<'a> {
    pub(crate) fn new(planned_write: &'a crate::sql::public::planner::ir::PlannedWrite) -> Self {
        Self {
            planned_write,
            schema_cache: crate::state::validation::SchemaCache::new(),
        }
    }
}

#[async_trait::async_trait(?Send)]
impl CreateCommitInvariantChecker for PublicCommitInvariantChecker<'_> {
    async fn recheck_invariants(
        &mut self,
        transaction: &mut dyn LixTransaction,
    ) -> Result<(), CreateCommitError> {
        let backend = TransactionBackendAdapter::new(transaction);
        validate_commit_time_write(&backend, &self.schema_cache, self.planned_write)
            .await
            .map_err(|error| CreateCommitError {
                kind: CreateCommitErrorKind::Internal,
                message: error.description,
            })
    }
}

struct TransactionCommitExecutor<'a> {
    transaction: &'a mut dyn LixTransaction,
}

#[async_trait::async_trait(?Send)]
impl CommitQueryExecutor for TransactionCommitExecutor<'_> {
    fn dialect(&self) -> crate::SqlDialect {
        self.transaction.dialect()
    }

    async fn execute(&mut self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        self.transaction.execute(sql, params).await
    }
}

pub(crate) async fn prepare_execution_with_backend(
    engine: &Engine,
    backend: &dyn LixBackend,
    pending_registered_schema_overlay: Option<&PendingRegisteredSchemaOverlay>,
    pending_semantic_overlay: Option<&PendingSemanticOverlay>,
    pending_filesystem_overlay: Option<&PendingFilesystemOverlay>,
    parsed_statements: &[Statement],
    params: &[Value],
    active_version_id: &str,
    writer_key: Option<&str>,
    allow_internal_tables: bool,
    public_surface_registry_override: Option<&crate::sql::public::catalog::SurfaceRegistry>,
    policy: PreparationPolicy,
) -> Result<PreparedExecutionContext, LixError> {
    let overlay_backend;
    let backend: &dyn LixBackend = if pending_registered_schema_overlay.is_some()
        || pending_semantic_overlay.is_some()
        || pending_filesystem_overlay.is_some()
    {
        overlay_backend = TransactionOverlayBackend::new(
            backend,
            pending_registered_schema_overlay.cloned(),
            pending_semantic_overlay.cloned(),
            pending_filesystem_overlay.cloned(),
        );
        &overlay_backend
    } else {
        backend
    };

    let defer_runtime_sequence_load = !allow_internal_tables
        && !crate::filesystem::statements_require_generated_filesystem_insert_ids(
            parsed_statements,
        );
    let (settings, sequence_start, functions) = engine
        .prepare_runtime_functions_with_backend(backend, defer_runtime_sequence_load)
        .await?;

    let mut statements = parsed_statements.to_vec();
    crate::filesystem::ensure_generated_filesystem_insert_ids(&mut statements, &functions)?;

    let requirements = derive_plan_requirements(&statements);

    let public_execution = match public_surface_registry_override {
        Some(registry) => {
            prepare_public_execution_with_registry_and_internal_access(
                backend,
                registry,
                &statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await
        }
        None => {
            prepare_public_execution_with_internal_access(
                backend,
                &statements,
                params,
                active_version_id,
                writer_key,
                allow_internal_tables,
            )
            .await
        }
    }
    .map_err(|error| LixError {
        code: error.code,
        description: format!(
            "prepare_execution_with_backend public preparation failed: {}",
            error.description
        ),
    })?;
    let (public_read, mut public_write) = match public_execution {
        Some(PreparedPublicExecution::Read(prepared)) => (Some(prepared), None),
        Some(PreparedPublicExecution::Write(prepared)) => (None, Some(prepared)),
        None => (None, None),
    };
    let plan_statements = public_read
        .as_ref()
        .and_then(|prepared| {
            prepared
                .lowered_read()
                .map(|lowered| lowered.statements.clone())
        })
        .unwrap_or_else(|| statements.clone());

    let skip_side_effect_collection = policy.skip_side_effect_collection
        || public_write.as_ref().is_some_and(|prepared| {
            prepared
                .planned_write
                .resolved_write_plan
                .as_ref()
                .is_some_and(|resolved| {
                    resolved
                        .filesystem_state()
                        .files
                        .values()
                        .any(|file| file.data.is_some())
                })
        });

    let public_read_owns_execution = public_read.as_ref().is_some_and(|prepared| {
        matches!(prepared.execution, PreparedPublicReadExecution::Direct(_))
    });

    let intent = if let Some(public_write) = public_write.as_ref() {
        derived_public_execution_intent(public_write)
    } else if public_read_owns_execution {
        ExecutionIntent {
            filesystem_state: Default::default(),
        }
    } else {
        collect_execution_intent_with_backend(
            engine,
            backend,
            &statements,
            params,
            active_version_id,
            writer_key,
            &requirements,
            IntentCollectionPolicy {
                skip_side_effect_collection,
            },
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend intent collection failed: {}",
                error.description
            ),
        })?
    };

    let public_write_owns_execution = public_write.is_some();
    if let Some(public_write) = public_write.as_mut() {
        if let Some(execution) = public_write.execution.as_mut() {
            let planned_write = &public_write.planned_write;
            finalize_public_write_execution(execution, planned_write, &intent.filesystem_state)
                .map_err(|error| LixError {
                    code: error.code,
                    description: format!(
                        "prepare_execution_with_backend public execution finalization failed: {}",
                        error.description
                    ),
                })?;
        }
    }

    let plan = if public_write_owns_execution {
        passthrough_execution_plan_for_public_write(
            &statements,
            public_write
                .as_ref()
                .map(|prepared| {
                    prepared
                        .execution
                        .as_ref()
                        .map(|execution| {
                            execution
                                .partitions
                                .iter()
                                .filter_map(|partition| match partition {
                                    PublicWriteExecutionPartition::Tracked(execution) => {
                                        Some(execution.schema_live_table_requirements.clone())
                                    }
                                    PublicWriteExecutionPartition::Untracked(_) => None,
                                })
                                .flatten()
                                .collect::<Vec<_>>()
                        })
                        .unwrap_or_default()
                })
                .unwrap_or_default(),
        )
    } else if public_read_owns_execution {
        passthrough_execution_plan_for_public_read(&statements)
    } else {
        build_execution_plan(
            backend,
            &engine.cel_evaluator,
            plan_statements,
            params,
            public_read
                .as_ref()
                .and_then(|prepared| prepared.dependency_spec.clone()),
            functions.clone(),
            writer_key,
        )
        .await
        .map_err(LixError::from)
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend plan building failed: {}",
                error.description
            ),
        })?
    };

    if !public_write_owns_execution
        && !public_read_owns_execution
        && !plan.preprocess.mutations.is_empty()
    {
        validate_inserts(backend, &engine.schema_cache, &plan.preprocess.mutations)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend insert validation failed: {}",
                    error.description
                ),
            })?;
    }
    if !public_write_owns_execution
        && !public_read_owns_execution
        && !plan.preprocess.update_validations.is_empty()
    {
        validate_updates(
            backend,
            &engine.schema_cache,
            &plan.preprocess.update_validations,
            params,
        )
        .await
        .map_err(|error| LixError {
            code: error.code,
            description: format!(
                "prepare_execution_with_backend update validation failed: {}",
                error.description
            ),
        })?;
    }
    if let Some(public_write) = public_write.as_ref() {
        validate_batch_local_write(backend, &engine.schema_cache, &public_write.planned_write)
            .await
            .map_err(|error| LixError {
                code: error.code,
                description: format!(
                    "prepare_execution_with_backend public batch-local validation failed: {}",
                    error.description
                ),
            })?;
    }

    Ok(PreparedExecutionContext {
        intent,
        settings,
        sequence_start,
        functions,
        plan,
        public_read,
        public_write,
    })
}

struct TransactionOverlayBackend<'a> {
    base: &'a dyn LixBackend,
    registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
    semantic_overlay: Option<PendingSemanticOverlay>,
    filesystem_overlay: Option<PendingFilesystemOverlay>,
}

enum RegisteredSchemaOverlayQuery {
    FullScan,
    ExactEntityId(String),
    LatestBySchemaKey(String),
}

enum TransactionOverlayQuery {
    RegisteredSchema(RegisteredSchemaOverlayQuery),
    LiveTable(LiveTableOverlayQuery),
}

#[derive(Clone)]
struct LiveTableOverlayQuery {
    storage: PendingSemanticStorage,
    schema_key: String,
    projections: Vec<LiveProjection>,
    filters: Vec<LiveFilter>,
    order_by: Vec<LiveOrderClause>,
    limit: Option<usize>,
}

#[derive(Clone)]
struct LiveProjection {
    source_column: String,
    output_column: String,
}

#[derive(Clone)]
enum LiveFilter {
    Equals(String, Value),
    IsNotNull(String),
}

#[derive(Clone)]
struct LiveOrderClause {
    column: String,
    descending: bool,
}

#[derive(Clone)]
struct OverlayVisibleLiveRow {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
    metadata: Option<String>,
    change_id: Option<String>,
    snapshot_content: Option<String>,
    is_tombstone: bool,
    normalized_values: BTreeMap<String, Value>,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct OverlayVisibleLiveRowIdentity {
    entity_id: String,
    schema_key: String,
    schema_version: String,
    file_id: String,
    version_id: String,
    plugin_key: String,
}

impl<'a> TransactionOverlayBackend<'a> {
    fn new(
        base: &'a dyn LixBackend,
        registered_schema_overlay: Option<PendingRegisteredSchemaOverlay>,
        semantic_overlay: Option<PendingSemanticOverlay>,
        filesystem_overlay: Option<PendingFilesystemOverlay>,
    ) -> Self {
        Self {
            base,
            registered_schema_overlay,
            semantic_overlay,
            filesystem_overlay,
        }
    }

    async fn visible_registered_schema_rows(&self) -> Result<BTreeMap<String, String>, LixError> {
        let Some(overlay) = self.registered_schema_overlay.as_ref() else {
            return Ok(BTreeMap::new());
        };
        let sql = format!(
            "SELECT snapshot_content FROM {table} \
             WHERE version_id = '{global_version}' \
               AND is_tombstone = 0 \
               AND snapshot_content IS NOT NULL",
            table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
            global_version = GLOBAL_VERSION_ID,
        );
        let result = self.base.execute(&sql, &[]).await?;
        let mut rows = BTreeMap::new();
        for row in result.rows {
            let Some(Value::Text(snapshot_content)) = row.first() else {
                continue;
            };
            let snapshot: JsonValue =
                serde_json::from_str(snapshot_content).map_err(|error| LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "registered schema snapshot_content invalid JSON: {error}"
                    ),
                })?;
            let (key, _) = crate::schema::schema_from_registered_snapshot(&snapshot)?;
            rows.insert(key.entity_id(), snapshot_content.clone());
        }
        for (entity_id, pending) in overlay.visible_entries() {
            match pending.snapshot_content.as_ref() {
                Some(snapshot_content) => {
                    rows.insert(entity_id.to_string(), snapshot_content.clone());
                }
                None => {
                    rows.remove(entity_id);
                }
            }
        }
        Ok(rows)
    }

    fn classify_query(sql: &str) -> Option<TransactionOverlayQuery> {
        let parsed = parse_sql(sql).ok()?;
        let [Statement::Query(query)] = parsed.as_slice() else {
            return None;
        };
        if let Some(query) = Self::classify_live_table_query(query.as_ref()) {
            return Some(TransactionOverlayQuery::LiveTable(query));
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        if select.from.len() != 1 {
            return None;
        }
        let TableFactor::Table { name, .. } = &select.from[0].relation else {
            return None;
        };
        let table_name = name.0.last().and_then(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })?;
        if table_name != REGISTERED_SCHEMA_BOOTSTRAP_TABLE {
            return None;
        }

        if select.projection.len() == 2
            && matches!(select.projection[0], SelectItem::UnnamedExpr(Expr::Identifier(ref ident)) if ident.value == "schema_version")
            && matches!(select.projection[1], SelectItem::UnnamedExpr(Expr::Identifier(ref ident)) if ident.value == "snapshot_content")
        {
            let selection = select.selection.as_ref()?;
            for predicate in conjuncts(selection) {
                if let Some(schema_key) = latest_schema_key_predicate(predicate) {
                    return Some(TransactionOverlayQuery::RegisteredSchema(
                        RegisteredSchemaOverlayQuery::LatestBySchemaKey(schema_key),
                    ));
                }
            }
            return None;
        }

        if select.projection.len() == 1
            && matches!(select.projection[0], SelectItem::UnnamedExpr(Expr::Identifier(ref ident)) if ident.value == "snapshot_content")
        {
            if let Some(selection) = select.selection.as_ref() {
                for predicate in conjuncts(selection) {
                    if let Some(entity_id) = exact_entity_id_predicate(predicate) {
                        return Some(TransactionOverlayQuery::RegisteredSchema(
                            RegisteredSchemaOverlayQuery::ExactEntityId(entity_id),
                        ));
                    }
                }
            }
            return Some(TransactionOverlayQuery::RegisteredSchema(
                RegisteredSchemaOverlayQuery::FullScan,
            ));
        }

        None
    }

    fn classify_live_table_query(query: &sqlparser::ast::Query) -> Option<LiveTableOverlayQuery> {
        if query.with.is_some() || query.fetch.is_some() || query.for_clause.is_some() {
            return None;
        }
        let SetExpr::Select(select) = query.body.as_ref() else {
            return None;
        };
        Self::live_table_query_from_select(
            select.as_ref(),
            query.order_by.as_ref(),
            query.limit_clause.as_ref(),
        )
    }

    fn live_table_query_from_select(
        select: &Select,
        order_by: Option<&OrderBy>,
        limit_clause: Option<&sqlparser::ast::LimitClause>,
    ) -> Option<LiveTableOverlayQuery> {
        if select.from.len() != 1
            || !select.lateral_views.is_empty()
            || select.selection.is_none() && select.from[0].joins.len() > 0
            || !group_by_is_empty(&select.group_by)
            || select.having.is_some()
            || select.qualify.is_some()
        {
            return None;
        }
        if !select.from[0].joins.is_empty() {
            return None;
        }
        let TableFactor::Table { name, alias, .. } = &select.from[0].relation else {
            return None;
        };
        let table_name = name.0.last().and_then(|part| match part {
            sqlparser::ast::ObjectNamePart::Identifier(ident) => Some(ident.value.as_str()),
            _ => None,
        })?;
        let schema_key = live_schema_key_for_table_name(table_name)?.to_string();
        let storage = if is_untracked_live_table(table_name) {
            PendingSemanticStorage::Untracked
        } else {
            PendingSemanticStorage::Tracked
        };
        let table_alias = alias.as_ref().map(|alias| alias.name.value.as_str());
        let projections = select
            .projection
            .iter()
            .map(|item| live_projection_from_select_item(item, table_alias))
            .collect::<Option<Vec<_>>>()?;
        let filters = select
            .selection
            .as_ref()
            .map(conjuncts)
            .unwrap_or_default()
            .into_iter()
            .map(|predicate| live_filter_from_expr(predicate, table_alias))
            .collect::<Option<Vec<_>>>()?;
        let order_by = match order_by {
            Some(order_by) => live_order_by_from_clause(order_by, table_alias)?,
            None => Vec::new(),
        };
        let limit = live_limit_from_clause(limit_clause)?;
        Some(LiveTableOverlayQuery {
            storage,
            schema_key,
            projections,
            filters,
            order_by,
            limit,
        })
    }

    async fn execute_live_table_query(
        &self,
        query: &LiveTableOverlayQuery,
    ) -> Result<QueryResult, LixError> {
        let access = load_live_row_access_with_backend(self.base, &query.schema_key).await?;
        let mut text_filters = BTreeMap::new();
        for filter in &query.filters {
            if let LiveFilter::Equals(column, value) = filter {
                if let Some(text) = overlay_filter_text(value) {
                    text_filters.insert(column.as_str(), text);
                }
            }
        }
        let mut rows = load_live_rows_with_executor(
            &mut &*self.base,
            match query.storage {
                PendingSemanticStorage::Tracked => LiveRowScope::Tracked,
                PendingSemanticStorage::Untracked => LiveRowScope::Untracked,
            },
            &query.schema_key,
            &text_filters,
            &[],
            None,
        )
        .await?
        .into_iter()
        .map(|row| visible_live_row_from_loaded(&access, query.storage, row))
        .collect::<Result<Vec<_>, _>>()?;
        let mut by_identity = rows
            .drain(..)
            .map(|row| (visible_live_row_identity(&row), row))
            .collect::<BTreeMap<_, _>>();
        if let Some(overlay) = self.semantic_overlay.as_ref() {
            for row in overlay.visible_rows(query.storage, &query.schema_key) {
                let visible = visible_live_row_from_pending(&access, row)?;
                let identity = visible_live_row_identity(&visible);
                if visible.is_tombstone && matches!(query.storage, PendingSemanticStorage::Tracked)
                {
                    by_identity.remove(&identity);
                } else {
                    by_identity.insert(identity, visible);
                }
            }
        }
        self.apply_filesystem_overlay_to_rows(query, &access, &mut by_identity);
        let mut rows = by_identity
            .into_values()
            .filter(|row| {
                query
                    .filters
                    .iter()
                    .all(|filter| live_filter_matches_row(filter, row))
            })
            .collect::<Vec<_>>();
        rows.sort_by(|left, right| compare_live_rows(left, right, &query.order_by));
        if let Some(limit) = query.limit {
            rows.truncate(limit);
        }
        Ok(QueryResult {
            columns: query
                .projections
                .iter()
                .map(|projection| projection.output_column.clone())
                .collect(),
            rows: rows
                .into_iter()
                .map(|row| {
                    query
                        .projections
                        .iter()
                        .map(|projection| live_projection_value(&row, &projection.source_column))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    fn apply_filesystem_overlay_to_rows(
        &self,
        query: &LiveTableOverlayQuery,
        access: &crate::schema::live_layout::LiveRowAccess,
        rows: &mut BTreeMap<OverlayVisibleLiveRowIdentity, OverlayVisibleLiveRow>,
    ) {
        let Some(overlay) = self.filesystem_overlay.as_ref() else {
            return;
        };
        if query.storage != PendingSemanticStorage::Tracked
            || !matches!(
                query.schema_key.as_str(),
                "lix_file_descriptor" | "lix_directory_descriptor"
            )
        {
            return;
        }

        for pending in
            overlay.visible_directory_rows(PendingSemanticStorage::Tracked, &query.schema_key)
        {
            let Ok(visible) = visible_live_row_from_pending(access, pending) else {
                continue;
            };
            let identity = visible_live_row_identity(&visible);
            if visible.is_tombstone {
                rows.remove(&identity);
            } else {
                rows.insert(identity, visible);
            }
        }

        if query.schema_key != "lix_file_descriptor" {
            return;
        }

        for pending in overlay.visible_files() {
            if pending.deleted {
                rows.retain(|_, row| {
                    !(row.schema_key == "lix_file_descriptor"
                        && row.entity_id == pending.file_id
                        && row.version_id == pending.version_id)
                });
                continue;
            }

            if let Some(visible) = visible_live_row_from_pending_filesystem_state(access, pending) {
                let identity = visible_live_row_identity(&visible);
                rows.insert(identity, visible);
                continue;
            }

            for row in rows.values_mut() {
                if row.schema_key == "lix_file_descriptor"
                    && row.entity_id == pending.file_id
                    && row.version_id == pending.version_id
                {
                    row.metadata = pending.metadata_patch.apply(row.metadata.clone());
                }
            }
        }
    }
}

#[async_trait::async_trait(?Send)]
impl LixBackend for TransactionOverlayBackend<'_> {
    fn dialect(&self) -> crate::SqlDialect {
        self.base.dialect()
    }

    async fn execute(&self, sql: &str, params: &[Value]) -> Result<QueryResult, LixError> {
        let Some(query) = Self::classify_query(sql) else {
            return self.base.execute(sql, params).await;
        };

        match query {
            TransactionOverlayQuery::LiveTable(query) => {
                if self.semantic_overlay.is_none() && self.filesystem_overlay.is_none() {
                    return self.base.execute(sql, params).await;
                }
                self.execute_live_table_query(&query).await
            }
            TransactionOverlayQuery::RegisteredSchema(
                RegisteredSchemaOverlayQuery::LatestBySchemaKey(schema_key),
            ) => {
                let visible_rows = self.visible_registered_schema_rows().await?;
                let latest = visible_rows
                    .iter()
                    .filter_map(|(_, snapshot_content)| {
                        let snapshot: JsonValue = serde_json::from_str(snapshot_content).ok()?;
                        let (key, _) =
                            crate::schema::schema_from_registered_snapshot(&snapshot).ok()?;
                        (key.schema_key == schema_key).then_some((key, snapshot_content))
                    })
                    .max_by(|(left, _), (right, _)| {
                        match (left.version_number(), right.version_number()) {
                            (Some(left_version), Some(right_version)) => {
                                left_version.cmp(&right_version)
                            }
                            _ => left.schema_version.cmp(&right.schema_version),
                        }
                    });
                let Some((key, snapshot_content)) = latest else {
                    return Ok(QueryResult {
                        rows: Vec::new(),
                        columns: vec!["schema_version".to_string(), "snapshot_content".to_string()],
                    });
                };
                Ok(QueryResult {
                    rows: vec![vec![
                        Value::Text(key.schema_version),
                        Value::Text(snapshot_content.clone()),
                    ]],
                    columns: vec!["schema_version".to_string(), "snapshot_content".to_string()],
                })
            }
            TransactionOverlayQuery::RegisteredSchema(
                RegisteredSchemaOverlayQuery::ExactEntityId(entity_id),
            ) => {
                let visible_rows = self.visible_registered_schema_rows().await?;
                Ok(QueryResult {
                    rows: visible_rows
                        .get(entity_id.as_str())
                        .map(|snapshot_content| vec![vec![Value::Text(snapshot_content.clone())]])
                        .unwrap_or_default(),
                    columns: vec!["snapshot_content".to_string()],
                })
            }
            TransactionOverlayQuery::RegisteredSchema(RegisteredSchemaOverlayQuery::FullScan) => {
                let visible_rows = self.visible_registered_schema_rows().await?;
                Ok(QueryResult {
                    rows: visible_rows
                        .into_values()
                        .map(|snapshot_content| vec![Value::Text(snapshot_content)])
                        .collect(),
                    columns: vec!["snapshot_content".to_string()],
                })
            }
        }
    }

    async fn begin_transaction(&self) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        self.base.begin_transaction().await
    }

    async fn begin_savepoint(&self, name: &str) -> Result<Box<dyn LixTransaction + '_>, LixError> {
        self.base.begin_savepoint(name).await
    }
}

fn live_projection_from_select_item(
    item: &SelectItem,
    table_alias: Option<&str>,
) -> Option<LiveProjection> {
    match item {
        SelectItem::UnnamedExpr(expr) => Some(LiveProjection {
            source_column: live_identifier_name(expr, table_alias)?,
            output_column: live_identifier_name(expr, table_alias)?,
        }),
        SelectItem::ExprWithAlias { expr, alias } => Some(LiveProjection {
            source_column: live_identifier_name(expr, table_alias)?,
            output_column: alias.value.clone(),
        }),
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

fn group_by_is_empty(group_by: &sqlparser::ast::GroupByExpr) -> bool {
    match group_by {
        sqlparser::ast::GroupByExpr::Expressions(expressions, modifiers) => {
            expressions.is_empty() && modifiers.is_empty()
        }
        sqlparser::ast::GroupByExpr::All(_) => false,
    }
}

fn live_filter_from_expr(expr: &Expr, table_alias: Option<&str>) -> Option<LiveFilter> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match (left.as_ref(), right.as_ref()) {
            (left, Expr::Value(value)) | (Expr::Value(value), left) => Some(LiveFilter::Equals(
                live_identifier_name(left, table_alias)?,
                sql_value_as_engine_value(value)?,
            )),
            _ => None,
        },
        Expr::IsNotNull(expr) => Some(LiveFilter::IsNotNull(live_identifier_name(
            expr,
            table_alias,
        )?)),
        _ => None,
    }
}

fn live_order_by_from_clause(
    order_by: &OrderBy,
    table_alias: Option<&str>,
) -> Option<Vec<LiveOrderClause>> {
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return None;
    };
    expressions
        .iter()
        .map(|expr| live_order_clause_from_expr(expr, table_alias))
        .collect()
}

fn live_order_clause_from_expr(
    expr: &OrderByExpr,
    table_alias: Option<&str>,
) -> Option<LiveOrderClause> {
    Some(LiveOrderClause {
        column: live_identifier_name(&expr.expr, table_alias)?,
        descending: expr.options.asc == Some(false),
    })
}

fn live_limit_from_clause(
    limit_clause: Option<&sqlparser::ast::LimitClause>,
) -> Option<Option<usize>> {
    let Some(limit_clause) = limit_clause else {
        return Some(None);
    };
    match limit_clause {
        sqlparser::ast::LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        } => {
            if offset.is_some() || !limit_by.is_empty() {
                return None;
            }
            let Some(limit) = limit.as_ref() else {
                return Some(None);
            };
            let Expr::Value(value) = limit else {
                return None;
            };
            match &value.value {
                SqlValue::Number(value, _) => value.parse::<usize>().ok().map(Some),
                _ => None,
            }
        }
        sqlparser::ast::LimitClause::OffsetCommaLimit { .. } => None,
    }
}

fn live_identifier_name(expr: &Expr, table_alias: Option<&str>) -> Option<String> {
    match expr {
        Expr::Identifier(ident) => Some(ident.value.clone()),
        Expr::CompoundIdentifier(parts) if parts.len() == 2 => {
            let qualifier = &parts[0].value;
            let column = &parts[1].value;
            if table_alias.is_some_and(|alias| alias != qualifier) {
                return None;
            }
            Some(column.clone())
        }
        _ => None,
    }
}

fn overlay_filter_text(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Integer(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(if *value { "1" } else { "0" }.to_string()),
        Value::Real(value) => Some(value.to_string()),
        Value::Json(value) => Some(value.to_string()),
        Value::Null | Value::Blob(_) => None,
    }
}

fn sql_value_as_engine_value(value: &sqlparser::ast::ValueWithSpan) -> Option<Value> {
    match &value.value {
        SqlValue::Null => Some(Value::Null),
        SqlValue::Boolean(value) => Some(Value::Boolean(*value)),
        SqlValue::SingleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::DollarQuotedString(sqlparser::ast::DollarQuotedString {
            value: text, ..
        }) => Some(Value::Text(text.clone())),
        SqlValue::Number(value, _) => value
            .parse::<i64>()
            .map(Value::Integer)
            .or_else(|_| value.parse::<f64>().map(Value::Real))
            .ok(),
        _ => None,
    }
}

fn visible_live_row_from_loaded(
    access: &crate::schema::live_layout::LiveRowAccess,
    _storage: PendingSemanticStorage,
    row: LoadedLiveRow,
) -> Result<OverlayVisibleLiveRow, LixError> {
    let snapshot_content = logical_snapshot_text(access, &row)?;
    Ok(OverlayVisibleLiveRow {
        entity_id: row.entity_id,
        schema_key: row.schema_key,
        schema_version: row.schema_version,
        file_id: row.file_id,
        version_id: row.version_id,
        plugin_key: row.plugin_key,
        metadata: row.metadata,
        change_id: row.change_id,
        normalized_values: normalized_live_column_values(
            access.layout(),
            snapshot_content.as_deref(),
        )?,
        snapshot_content,
        is_tombstone: false,
    })
}

fn visible_live_row_from_pending(
    access: &crate::schema::live_layout::LiveRowAccess,
    row: &PendingSemanticRow,
) -> Result<OverlayVisibleLiveRow, LixError> {
    Ok(OverlayVisibleLiveRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
        change_id: None,
        normalized_values: normalized_live_column_values(
            access.layout(),
            row.snapshot_content.as_deref(),
        )?,
        snapshot_content: row.snapshot_content.clone(),
        is_tombstone: row.tombstone,
    })
}

fn visible_live_row_from_pending_filesystem_state(
    access: &crate::schema::live_layout::LiveRowAccess,
    row: &FilesystemTransactionFileState,
) -> Option<OverlayVisibleLiveRow> {
    let descriptor = row.descriptor.as_ref()?;
    let snapshot_content = serde_json::json!({
        "id": row.file_id,
        "directory_id": if descriptor.directory_id.is_empty() {
            serde_json::Value::Null
        } else {
            serde_json::Value::String(descriptor.directory_id.clone())
        },
        "name": descriptor.name,
        "extension": descriptor.extension,
        "metadata": descriptor.metadata,
        "hidden": descriptor.hidden,
    })
    .to_string();
    Some(OverlayVisibleLiveRow {
        entity_id: row.file_id.clone(),
        schema_key: "lix_file_descriptor".to_string(),
        schema_version: "1".to_string(),
        file_id: "lix".to_string(),
        version_id: row.version_id.clone(),
        plugin_key: "lix".to_string(),
        metadata: descriptor.metadata.clone(),
        change_id: None,
        normalized_values: normalized_live_column_values(access.layout(), Some(&snapshot_content))
            .ok()?,
        snapshot_content: Some(snapshot_content),
        is_tombstone: false,
    })
}

fn visible_live_row_identity(row: &OverlayVisibleLiveRow) -> OverlayVisibleLiveRowIdentity {
    OverlayVisibleLiveRowIdentity {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        schema_version: row.schema_version.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        plugin_key: row.plugin_key.clone(),
    }
}

fn live_filter_matches_row(filter: &LiveFilter, row: &OverlayVisibleLiveRow) -> bool {
    match filter {
        LiveFilter::Equals(column, expected) => {
            live_row_value(row, column).is_some_and(|actual| actual == *expected)
        }
        LiveFilter::IsNotNull(column) => {
            !matches!(live_row_value(row, column), Some(Value::Null) | None)
        }
    }
}

fn compare_live_rows(
    left: &OverlayVisibleLiveRow,
    right: &OverlayVisibleLiveRow,
    order_by: &[LiveOrderClause],
) -> std::cmp::Ordering {
    for clause in order_by {
        let ordering = compare_live_values(
            &live_row_value(left, &clause.column),
            &live_row_value(right, &clause.column),
        );
        if ordering != std::cmp::Ordering::Equal {
            return if clause.descending {
                ordering.reverse()
            } else {
                ordering
            };
        }
    }
    visible_live_row_identity(left).cmp(&visible_live_row_identity(right))
}

fn compare_live_values(left: &Option<Value>, right: &Option<Value>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (left, right) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(left), Some(right)) => format!("{left:?}").cmp(&format!("{right:?}")),
    }
}

fn live_projection_value(
    row: &OverlayVisibleLiveRow,
    source_column: &str,
) -> Result<Value, LixError> {
    live_row_value(row, source_column).ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("overlay query requested unsupported live column '{source_column}'"),
        )
    })
}

fn live_row_value(row: &OverlayVisibleLiveRow, column: &str) -> Option<Value> {
    match column {
        "entity_id" => Some(Value::Text(row.entity_id.clone())),
        "schema_key" => Some(Value::Text(row.schema_key.clone())),
        "schema_version" => Some(Value::Text(row.schema_version.clone())),
        "file_id" => Some(Value::Text(row.file_id.clone())),
        "version_id" => Some(Value::Text(row.version_id.clone())),
        "plugin_key" => Some(Value::Text(row.plugin_key.clone())),
        "metadata" => Some(row.metadata.clone().map(Value::Text).unwrap_or(Value::Null)),
        "change_id" => Some(
            row.change_id
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        "snapshot_content" => Some(
            row.snapshot_content
                .clone()
                .map(Value::Text)
                .unwrap_or(Value::Null),
        ),
        "is_tombstone" => Some(Value::Integer(i64::from(row.is_tombstone))),
        other => row.normalized_values.get(other).cloned(),
    }
}

fn conjuncts(expr: &Expr) -> Vec<&Expr> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            let mut predicates = conjuncts(left);
            predicates.extend(conjuncts(right));
            predicates
        }
        _ => vec![expr],
    }
}

fn exact_entity_id_predicate(expr: &Expr) -> Option<String> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };
    match (left.as_ref(), right.as_ref()) {
        (Expr::Identifier(ident), Expr::Value(value))
        | (Expr::Value(value), Expr::Identifier(ident))
            if ident.value == "entity_id" =>
        {
            sql_value_as_string(value)
        }
        _ => None,
    }
}

fn latest_schema_key_predicate(expr: &Expr) -> Option<String> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };
    let (function, value) = match (left.as_ref(), right.as_ref()) {
        (Expr::Function(function), Expr::Value(value))
        | (Expr::Value(value), Expr::Function(function)) => (function, value),
        _ => return None,
    };
    if function.name.to_string().to_lowercase() != "substr" {
        return None;
    }
    let FunctionArguments::List(arguments) = &function.args else {
        return None;
    };
    let [FunctionArg::Unnamed(FunctionArgExpr::Expr(Expr::Identifier(ident))), ..] =
        arguments.args.as_slice()
    else {
        return None;
    };
    if ident.value != "entity_id" {
        return None;
    }
    sql_value_as_string(value).and_then(|prefix| prefix.strip_suffix('~').map(str::to_string))
}

fn sql_value_as_string(value: &sqlparser::ast::ValueWithSpan) -> Option<String> {
    match &value.value {
        SqlValue::SingleQuotedString(text)
        | SqlValue::TripleSingleQuotedString(text)
        | SqlValue::EscapedStringLiteral(text)
        | SqlValue::SingleQuotedByteStringLiteral(text)
        | SqlValue::DoubleQuotedByteStringLiteral(text)
        | SqlValue::TripleSingleQuotedByteStringLiteral(text)
        | SqlValue::TripleDoubleQuotedByteStringLiteral(text) => Some(text.clone()),
        SqlValue::DollarQuotedString(dollar) => Some(dollar.value.clone()),
        _ => None,
    }
}

fn derived_public_execution_intent(
    prepared: &PreparedPublicWrite,
) -> crate::sql::execution::intent::ExecutionIntent {
    let Some(resolved) = prepared.planned_write.resolved_write_plan.as_ref() else {
        return crate::sql::execution::intent::ExecutionIntent {
            filesystem_state: Default::default(),
        };
    };

    crate::sql::execution::intent::ExecutionIntent {
        filesystem_state: resolved.filesystem_state(),
    }
}

fn passthrough_execution_plan_for_public_write(
    statements: &[Statement],
    live_table_requirements: Vec<SchemaLiveTableRequirement>,
) -> ExecutionPlan {
    ExecutionPlan {
        preprocess: PlannedStatementSet {
            sql: String::new(),
            prepared_statements: Vec::new(),
            live_table_requirements,
            internal_state: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        },
        result_contract: derive_result_contract_for_statements(statements),
        requirements: PlanRequirements::default(),
        dependency_spec: crate::sql::common::dependency_spec::DependencySpec::default(),
        effects: PlanEffects::default(),
    }
}

fn passthrough_execution_plan_for_public_read(statements: &[Statement]) -> ExecutionPlan {
    let mut requirements = PlanRequirements::default();
    requirements.read_only_query = true;

    ExecutionPlan {
        preprocess: PlannedStatementSet {
            sql: String::new(),
            prepared_statements: Vec::new(),
            live_table_requirements: Vec::new(),
            internal_state: None,
            mutations: Vec::new(),
            update_validations: Vec::new(),
        },
        result_contract: derive_result_contract_for_statements(statements),
        requirements,
        dependency_spec: crate::sql::common::dependency_spec::DependencySpec::default(),
        effects: PlanEffects::default(),
    }
}

fn derive_result_contract_for_statements(statements: &[Statement]) -> ResultContract {
    match statements.last() {
        Some(Statement::Query(_) | Statement::Explain { .. }) => ResultContract::Select,
        Some(Statement::Insert(insert)) => {
            if insert.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Update(update)) => {
            if update.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Delete(delete)) => {
            if delete.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(_) | None => ResultContract::Other,
    }
}

pub(crate) fn top_level_write_target_name(statement: &Statement) -> Option<String> {
    match statement {
        Statement::Insert(insert) => match &insert.table {
            sqlparser::ast::TableObject::TableName(name) => Some(name.to_string()),
            _ => None,
        },
        Statement::Update(update) => match &update.table.relation {
            sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
            _ => None,
        },
        Statement::Delete(delete) => {
            let tables = match &delete.from {
                sqlparser::ast::FromTable::WithFromKeyword(tables)
                | sqlparser::ast::FromTable::WithoutKeyword(tables) => tables,
            };
            match &tables.first()?.relation {
                sqlparser::ast::TableFactor::Table { name, .. } => Some(name.to_string()),
                _ => None,
            }
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::top_level_write_target_name;
    use crate::sql::ast::utils::parse_sql_statements;

    #[test]
    fn detects_top_level_write_targets() {
        let statements = parse_sql_statements(
            "UPDATE lix_file SET data = X'01' WHERE id = 'f1'; \
             DELETE FROM some_other_table WHERE id = 'x'",
        )
        .expect("parse");
        assert_eq!(
            top_level_write_target_name(&statements[0]).as_deref(),
            Some("lix_file")
        );

        let statements = parse_sql_statements(
            "INSERT INTO lix_directory_by_version (id, path, lixcol_version_id) VALUES ('d1', '/docs', 'v1')",
        )
        .expect("parse");
        assert_eq!(
            top_level_write_target_name(&statements[0]).as_deref(),
            Some("lix_directory_by_version")
        );

        let statements =
            parse_sql_statements("DELETE FROM lix_file_history WHERE id = 'f1'").expect("parse");
        assert_eq!(
            top_level_write_target_name(&statements[0]).as_deref(),
            Some("lix_file_history")
        );

        let statements =
            parse_sql_statements("SELECT * FROM lix_file WHERE id = 'f1'").expect("parse");
        assert_eq!(top_level_write_target_name(&statements[0]), None);
    }
}

pub(crate) fn empty_public_write_execution_outcome() -> SqlExecutionOutcome {
    SqlExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        postprocess_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        plan_effects_override: Some(PlanEffects::default()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }
}

pub(crate) fn pending_session_matches_create_commit(
    session: &PendingPublicCommitSession,
    preconditions: &CreateCommitPreconditions,
) -> bool {
    session.lane == preconditions.write_lane
        && match &preconditions.expected_head {
            CreateCommitExpectedHead::CurrentHead => true,
            CreateCommitExpectedHead::CommitId(commit_id) => commit_id == &session.commit_id,
            CreateCommitExpectedHead::CreateIfMissing => false,
        }
}

pub(crate) async fn build_pending_public_commit_session(
    transaction: &mut dyn LixTransaction,
    lane: CreateCommitWriteLane,
    commit_result: &GenerateCommitResult,
) -> Result<PendingPublicCommitSession, LixError> {
    let commit_row = commit_result
        .derived_apply_input
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session requires a lix_commit materialized row",
            )
        })?;
    let commit_snapshot = commit_row.snapshot_content.as_deref().ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public commit session requires commit snapshot_content",
        )
    })?;
    let commit_snapshot: JsonValue = serde_json::from_str(commit_snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("public commit session commit snapshot is invalid JSON: {error}"),
        )
    })?;
    let change_set_id = commit_snapshot
        .get("change_set_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session commit snapshot is missing change_set_id",
            )
        })?
        .to_string();
    let commit_change_id = commit_result
        .canonical_output
        .changes
        .iter()
        .find(|row| row.schema_key == "lix_commit" && row.entity_id == commit_row.entity_id)
        .map(|row| row.id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session requires a lix_commit change row",
            )
        })?;
    let snapshot_id_result = transaction
        .execute(
            "SELECT snapshot_id \
             FROM lix_internal_change \
             WHERE id = $1 \
               AND schema_key = 'lix_commit' \
               AND entity_id = $2 \
             LIMIT 1",
            &[
                Value::Text(commit_change_id.clone()),
                Value::Text(commit_row.entity_id.to_string()),
            ],
        )
        .await?;
    let commit_change_snapshot_id = snapshot_id_result
        .rows
        .first()
        .and_then(|row| row.first())
        .and_then(|value| match value {
            Value::Text(text) => Some(text.clone()),
            _ => None,
        })
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public commit session could not load commit snapshot_id",
            )
        })?;

    Ok(PendingPublicCommitSession {
        lane,
        commit_id: commit_row.entity_id.to_string(),
        change_set_id,
        commit_change_id,
        commit_change_snapshot_id,
        commit_materialized_change_id: commit_row.id.clone(),
        commit_schema_version: commit_row.schema_version.to_string(),
        commit_file_id: commit_row.file_id.to_string(),
        commit_plugin_key: commit_row.plugin_key.to_string(),
        commit_snapshot,
    })
}

pub(crate) async fn merge_public_domain_change_batch_into_pending_commit(
    transaction: &mut dyn LixTransaction,
    session: &mut PendingPublicCommitSession,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
    binary_blob_writes: &[BinaryBlobWrite],
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
    timestamp: &str,
) -> Result<(), LixError> {
    let domain_changes = batch
        .changes
        .iter()
        .map(|change| {
            Ok::<DomainChangeInput, LixError>(DomainChangeInput {
                id: functions.uuid_v7(),
                entity_id: EntityId::new(change.entity_id.clone())?,
                schema_key: CanonicalSchemaKey::new(change.schema_key.clone())?,
                schema_version: CanonicalSchemaVersion::new(
                    change.schema_version.clone().ok_or_else(|| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public merge requires schema_version for '{}:{}'",
                                change.schema_key, change.entity_id
                            ),
                        )
                    })?,
                )?,
                file_id: FileId::new(change.file_id.clone().ok_or_else(|| {
                    LixError::new(
                        "LIX_ERROR_UNKNOWN",
                        format!(
                            "public merge requires file_id for '{}:{}'",
                            change.schema_key, change.entity_id
                        ),
                    )
                })?)?,
                plugin_key: CanonicalPluginKey::new(change.plugin_key.clone().ok_or_else(
                    || {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!(
                                "public merge requires plugin_key for '{}:{}'",
                                change.schema_key, change.entity_id
                            ),
                        )
                    },
                )?)?,
                snapshot_content: canonicalize_optional_json_text(
                    change.snapshot_content.as_deref(),
                    "snapshot_content",
                    &change.schema_key,
                    &change.entity_id,
                )?,
                metadata: canonicalize_optional_json_text(
                    change.metadata.as_deref(),
                    "metadata",
                    &change.schema_key,
                    &change.entity_id,
                )?,
                created_at: timestamp.to_string(),
                version_id: VersionId::new(change.version_id.clone())?,
                writer_key: change.writer_key.clone(),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let active_accounts = {
        let mut executor = TransactionCommitExecutor { transaction };
        load_commit_active_accounts(&mut executor, &domain_changes).await?
    };
    let versions = load_version_info_for_domain_changes(transaction, &domain_changes).await?;
    let generated = generate_commit(
        GenerateCommitArgs {
            timestamp: timestamp.to_string(),
            active_accounts: active_accounts.clone(),
            changes: domain_changes.clone(),
            versions,
        },
        || functions.uuid_v7(),
    )?;

    extend_json_array_strings(
        &mut session.commit_snapshot,
        "change_ids",
        domain_changes.iter().map(|change| change.id.clone()),
    );
    extend_json_array_strings(
        &mut session.commit_snapshot,
        "author_account_ids",
        active_accounts.iter().cloned(),
    );

    transaction
        .execute(
            "UPDATE lix_internal_snapshot \
             SET content = $1 \
             WHERE id = $2",
            &[
                Value::Text(session.commit_snapshot.to_string()),
                Value::Text(session.commit_change_snapshot_id.clone()),
            ],
        )
        .await?;

    let rewritten = rewrite_generated_commit_result_for_pending_session(
        session,
        generated,
        domain_changes.len(),
        timestamp,
    )?;
    execute_generated_commit_result(transaction, rewritten, binary_blob_writes, functions).await
}

fn canonicalize_optional_json_text(
    value: Option<&str>,
    field_name: &str,
    schema_key: &str,
    entity_id: &str,
) -> Result<Option<CanonicalJson>, LixError> {
    value
        .map(CanonicalJson::from_text)
        .transpose()
        .map_err(|error| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "public merge requires valid JSON {field_name} for '{schema_key}:{entity_id}': {}",
                    error.description
                ),
            )
        })
}

async fn load_version_info_for_domain_changes(
    transaction: &mut dyn LixTransaction,
    domain_changes: &[DomainChangeInput],
) -> Result<BTreeMap<String, VersionInfo>, LixError> {
    let affected_versions = domain_changes
        .iter()
        .map(|change| change.version_id.to_string())
        .collect::<BTreeSet<_>>();
    let mut executor = TransactionCommitExecutor { transaction };
    load_version_info_for_versions(&mut executor, &affected_versions).await
}

fn rewrite_generated_commit_result_for_pending_session(
    session: &PendingPublicCommitSession,
    generated: GenerateCommitResult,
    domain_change_count: usize,
    timestamp: &str,
) -> Result<GenerateCommitResult, LixError> {
    let temporary_commit_id = generated
        .derived_apply_input
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_commit")
        .map(|row| row.entity_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public merge rewrite requires a generated lix_commit row",
            )
        })?;
    let temporary_change_set_id = generated
        .derived_apply_input
        .live_state_rows
        .iter()
        .find(|row| row.schema_key == "lix_change_set")
        .map(|row| row.entity_id.clone())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public merge rewrite requires a generated lix_change_set row",
            )
        })?;
    let version_ref_entity_id = pending_session_version_ref_entity_id(&session.lane);

    let mut live_state_rows = Vec::new();
    for mut row in generated.derived_apply_input.live_state_rows {
        if is_pending_commit_meta_row(
            &row,
            &temporary_commit_id,
            &temporary_change_set_id,
            version_ref_entity_id,
        )? {
            continue;
        }

        match row.schema_key.as_str() {
            "lix_change_set_element" => {
                let (entity_id, snapshot_content) = rewrite_change_set_element_snapshot(
                    row.snapshot_content.as_deref(),
                    &session.change_set_id,
                )?;
                row.entity_id = EntityId::new(entity_id)?;
                row.snapshot_content = Some(snapshot_content);
                row.lixcol_commit_id = session.commit_id.clone();
            }
            "lix_change_author" => {
                row.id = session.commit_change_id.clone();
                row.lixcol_commit_id = session.commit_id.clone();
            }
            _ => {
                row.lixcol_commit_id = session.commit_id.clone();
            }
        }
        live_state_rows.push(row);
    }

    live_state_rows.push(MaterializedStateRow {
        id: session.commit_materialized_change_id.clone(),
        entity_id: EntityId::new(session.commit_id.clone())?,
        schema_key: CanonicalSchemaKey::new("lix_commit".to_string())?,
        schema_version: CanonicalSchemaVersion::new(session.commit_schema_version.clone())?,
        file_id: FileId::new(session.commit_file_id.clone())?,
        plugin_key: CanonicalPluginKey::new(session.commit_plugin_key.clone())?,
        snapshot_content: Some(CanonicalJson::from_value(session.commit_snapshot.clone())?),
        metadata: None,
        created_at: timestamp.to_string(),
        lixcol_version_id: VersionId::new(GLOBAL_VERSION_ID.to_string())?,
        lixcol_commit_id: session.commit_id.clone(),
        writer_key: None,
    });

    Ok(GenerateCommitResult {
        canonical_output: crate::state::commit::CanonicalCommitOutput {
            changes: generated
                .canonical_output
                .changes
                .into_iter()
                .take(domain_change_count)
                .collect(),
        },
        derived_apply_input: crate::state::commit::DerivedCommitApplyInput {
            live_state_rows,
            live_layouts: generated.derived_apply_input.live_layouts,
        },
    })
}

fn is_pending_commit_meta_row(
    row: &MaterializedStateRow,
    temporary_commit_id: &str,
    temporary_change_set_id: &str,
    version_ref_entity_id: &str,
) -> Result<bool, LixError> {
    match row.schema_key.as_str() {
        "lix_change_set" => Ok(row.entity_id == temporary_change_set_id),
        "lix_commit" => Ok(row.entity_id == temporary_commit_id),
        "lix_commit_edge" => Ok(row.entity_id.ends_with(&format!("~{temporary_commit_id}"))),
        "lix_version_ref" if row.entity_id == version_ref_entity_id => {
            let snapshot = row.snapshot_content.as_deref().unwrap_or("");
            let parsed: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("public merge rewrite saw invalid version ref JSON: {error}"),
                )
            })?;
            Ok(parsed
                .get("commit_id")
                .and_then(JsonValue::as_str)
                .is_some_and(|value| value == temporary_commit_id))
        }
        _ => Ok(false),
    }
}

fn rewrite_change_set_element_snapshot(
    snapshot: Option<&str>,
    change_set_id: &str,
) -> Result<(String, CanonicalJson), LixError> {
    let snapshot = snapshot.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            "public merge rewrite requires change_set_element snapshot_content",
        )
    })?;
    let mut parsed: JsonValue = serde_json::from_str(snapshot).map_err(|error| {
        LixError::new(
            "LIX_ERROR_UNKNOWN",
            format!("public merge rewrite saw invalid change_set_element JSON: {error}"),
        )
    })?;
    let change_id = parsed
        .get("change_id")
        .and_then(JsonValue::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                "public merge rewrite requires change_set_element change_id",
            )
        })?
        .to_string();
    parsed["change_set_id"] = JsonValue::String(change_set_id.to_string());
    Ok((
        format!("{change_set_id}~{change_id}"),
        CanonicalJson::from_value(parsed)?,
    ))
}

fn pending_session_version_ref_entity_id(lane: &CreateCommitWriteLane) -> &str {
    match lane {
        CreateCommitWriteLane::Version(version_id) => version_id.as_str(),
        CreateCommitWriteLane::GlobalAdmin => GLOBAL_VERSION_ID,
    }
}

fn extend_json_array_strings<I>(snapshot: &mut JsonValue, key: &str, values: I)
where
    I: IntoIterator<Item = String>,
{
    if !snapshot.is_object() {
        *snapshot = json!({});
    }
    let JsonValue::Object(map) = snapshot else {
        return;
    };
    let entry = map
        .entry(key.to_string())
        .or_insert_with(|| JsonValue::Array(Vec::new()));
    if !entry.is_array() {
        *entry = JsonValue::Array(Vec::new());
    }
    let JsonValue::Array(array) = entry else {
        return;
    };
    let mut seen = array
        .iter()
        .filter_map(|value| value.as_str().map(ToString::to_string))
        .collect::<BTreeSet<_>>();
    for value in values {
        if seen.insert(value.clone()) {
            array.push(JsonValue::String(value));
        }
    }
}

async fn execute_generated_commit_result(
    transaction: &mut dyn LixTransaction,
    result: GenerateCommitResult,
    binary_blob_writes: &[BinaryBlobWrite],
    functions: &mut SharedFunctionProvider<RuntimeFunctionProvider>,
) -> Result<(), LixError> {
    let mut executor = &mut *transaction;
    let prepared = build_prepared_batch_from_generate_commit_result_with_executor(
        &mut executor,
        result,
        functions,
    )
    .await?;
    let mut program = WriteProgram::new();
    if !binary_blob_writes.is_empty() {
        let payloads = binary_blob_writes
            .iter()
            .map(BinaryBlobWrite::as_input)
            .collect::<Vec<_>>();
        program.extend(build_binary_blob_fastcdc_write_program(
            transaction.dialect(),
            &payloads,
        )?);
    }
    program.push_batch(prepared);
    execute_write_program_with_transaction(transaction, program).await?;
    Ok(())
}

pub(crate) fn public_write_filesystem_payload_changes_already_committed(
    prepared: &PreparedExecutionContext,
) -> bool {
    let Some(public_write) = prepared.public_write.as_ref() else {
        return false;
    };
    matches!(
        public_write
            .planned_write
            .command
            .target
            .descriptor
            .public_name
            .as_str(),
        "lix_file" | "lix_file_by_version"
    ) && public_write.execution.as_ref().is_some_and(|execution| {
        execution
            .partitions
            .iter()
            .any(|partition| matches!(partition, PublicWriteExecutionPartition::Tracked(_)))
    })
}

pub(crate) fn create_commit_error_to_lix_error(
    error: crate::state::commit::CreateCommitError,
) -> LixError {
    LixError {
        code: "LIX_ERROR_UNKNOWN".to_string(),
        description: error.message,
    }
}

pub(crate) async fn mirror_public_registered_schema_bootstrap_rows(
    transaction: &mut dyn LixTransaction,
    commit_result: &crate::state::commit::GenerateCommitResult,
) -> Result<(), LixError> {
    for row in &commit_result.derived_apply_input.live_state_rows {
        if row.schema_key != REGISTERED_SCHEMA_KEY || row.lixcol_version_id != GLOBAL_VERSION_ID {
            continue;
        }

        let snapshot_sql = row
            .snapshot_content
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let metadata_sql = row
            .metadata
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let writer_key_sql = row
            .writer_key
            .as_ref()
            .map(|value| format!("'{}'", escape_sql_string(value)))
            .unwrap_or_else(|| "NULL".to_string());
        let is_tombstone = if row.snapshot_content.is_some() { 0 } else { 1 };

        let sql = format!(
            "INSERT INTO {table} (\
             entity_id, schema_key, schema_version, file_id, version_id, global, plugin_key, snapshot_content, change_id, metadata, writer_key, is_tombstone, created_at, updated_at\
             ) VALUES (\
             '{entity_id}', '{schema_key}', '{schema_version}', '{file_id}', '{version_id}', true, '{plugin_key}', {snapshot_content}, '{change_id}', {metadata}, {writer_key}, {is_tombstone}, '{created_at}', '{updated_at}'\
             ) ON CONFLICT (entity_id, file_id, version_id) DO UPDATE SET \
             schema_key = excluded.schema_key, \
             schema_version = excluded.schema_version, \
             global = excluded.global, \
             plugin_key = excluded.plugin_key, \
             snapshot_content = excluded.snapshot_content, \
             change_id = excluded.change_id, \
             metadata = excluded.metadata, \
             writer_key = excluded.writer_key, \
             is_tombstone = excluded.is_tombstone, \
             updated_at = excluded.updated_at",
            table = REGISTERED_SCHEMA_BOOTSTRAP_TABLE,
            entity_id = escape_sql_string(&row.entity_id),
            schema_key = escape_sql_string(&row.schema_key),
            schema_version = escape_sql_string(&row.schema_version),
            file_id = escape_sql_string(&row.file_id),
            version_id = escape_sql_string(&row.lixcol_version_id),
            plugin_key = escape_sql_string(&row.plugin_key),
            snapshot_content = snapshot_sql,
            change_id = escape_sql_string(&row.id),
            metadata = metadata_sql,
            writer_key = writer_key_sql,
            is_tombstone = is_tombstone,
            created_at = escape_sql_string(&row.created_at),
            updated_at = escape_sql_string(&row.created_at),
        );

        transaction.execute(&sql, &[]).await?;
    }

    Ok(())
}

pub(crate) async fn apply_public_version_last_checkpoint_side_effects(
    transaction: &mut dyn LixTransaction,
    public_write: &PreparedPublicWrite,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Result<(), LixError> {
    if public_write
        .planned_write
        .command
        .target
        .descriptor
        .public_name
        != "lix_version"
    {
        return Ok(());
    }

    match public_write.planned_write.command.operation_kind {
        crate::sql::public::planner::ir::WriteOperationKind::Insert => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                true,
            )
            .await
        }
        crate::sql::public::planner::ir::WriteOperationKind::Update => {
            upsert_last_checkpoint_rows(
                transaction,
                &version_checkpoint_rows_from_resolved_write(public_write, batch),
                false,
            )
            .await
        }
        crate::sql::public::planner::ir::WriteOperationKind::Delete => {
            let version_ids = version_ids_from_resolved_write(public_write, batch);
            delete_last_checkpoint_rows(transaction, &version_ids).await
        }
    }
}

fn version_checkpoint_rows_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Vec<(String, String)> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let rows = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                row.schema_key == crate::version::version_ref_schema_key() && !row.tombstone
            })
            .filter_map(|row| {
                row.values
                    .get("snapshot_content")
                    .and_then(|value| match value {
                        Value::Text(snapshot) => {
                            serde_json::from_str::<serde_json::Value>(snapshot)
                                .ok()
                                .and_then(|snapshot| {
                                    snapshot
                                        .get("commit_id")
                                        .and_then(serde_json::Value::as_str)
                                        .map(|commit_id| {
                                            (row.entity_id.to_string(), commit_id.to_string())
                                        })
                                })
                        }
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();
        if !rows.is_empty() {
            return rows;
        }
    }

    batch
        .changes
        .iter()
        .filter(|change| change.schema_key == crate::version::version_ref_schema_key())
        .filter_map(|change| {
            change.snapshot_content.as_deref().and_then(|snapshot| {
                serde_json::from_str::<serde_json::Value>(snapshot)
                    .ok()
                    .and_then(|snapshot| {
                        snapshot
                            .get("commit_id")
                            .and_then(serde_json::Value::as_str)
                            .map(|commit_id| (change.entity_id.to_string(), commit_id.to_string()))
                    })
            })
        })
        .collect()
}

fn version_ids_from_resolved_write(
    public_write: &PreparedPublicWrite,
    batch: &crate::sql::public::planner::semantics::domain_changes::DomainChangeBatch,
) -> Vec<String> {
    if let Some(resolved) = public_write.planned_write.resolved_write_plan.as_ref() {
        let version_ids = resolved
            .partitions
            .iter()
            .flat_map(|partition| partition.intended_post_state.iter())
            .filter(|row| {
                matches!(
                    row.schema_key.as_str(),
                    "lix_version_ref" | "lix_version_descriptor"
                )
            })
            .map(|row| row.entity_id.to_string())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !version_ids.is_empty() {
            return version_ids;
        }
    }

    batch
        .changes
        .iter()
        .map(|change| change.entity_id.to_string())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
}

async fn upsert_last_checkpoint_rows(
    transaction: &mut dyn LixTransaction,
    rows: &[(String, String)],
    update_existing: bool,
) -> Result<(), LixError> {
    if rows.is_empty() {
        return Ok(());
    }

    let values_sql = rows
        .iter()
        .map(|(version_id, checkpoint_commit_id)| {
            format!(
                "('{}', '{}')",
                escape_sql_string(version_id),
                escape_sql_string(checkpoint_commit_id)
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let on_conflict = if update_existing {
        "DO UPDATE SET checkpoint_commit_id = excluded.checkpoint_commit_id"
    } else {
        "DO NOTHING"
    };
    let sql = format!(
        "INSERT INTO lix_internal_last_checkpoint (version_id, checkpoint_commit_id) \
         VALUES {values_sql} \
         ON CONFLICT (version_id) {on_conflict}"
    );
    transaction.execute(&sql, &[]).await?;
    Ok(())
}

async fn delete_last_checkpoint_rows(
    transaction: &mut dyn LixTransaction,
    version_ids: &[String],
) -> Result<(), LixError> {
    if version_ids.is_empty() {
        return Ok(());
    }

    let in_list = version_ids
        .iter()
        .map(|id| format!("'{}'", escape_sql_string(id)))
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!("DELETE FROM lix_internal_last_checkpoint WHERE version_id IN ({in_list})");
    transaction.execute(&sql, &[]).await?;
    Ok(())
}
