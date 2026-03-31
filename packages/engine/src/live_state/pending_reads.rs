use std::collections::BTreeMap;

use crate::contracts::read::{
    decode_public_read_result, execute_prepared_public_read,
    execute_prepared_public_read_in_transaction,
    execute_prepared_public_read_without_freshness_check, resolve_placeholder_index,
    PlaceholderState, PreparedPublicRead,
};
use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};
use crate::contracts::traits::{PendingSemanticRow, PendingSemanticStorage, PendingView};
use crate::live_state::constraints::{ScanConstraint, ScanField, ScanOperator};
use crate::live_state::schema_access::{live_read_contract_from_layout, LiveReadContract};
use crate::live_state::shared::identity::RowIdentity;
use crate::read::contracts::{
    committed_read_mode_from_prepared_public_read, CommittedReadMode, PublicReadExecutionMode,
};
use crate::{LixBackend, LixBackendTransaction, LixError, QueryResult, Value};
use serde_json::Value as JsonValue;
use sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, OrderBy, OrderByExpr,
    OrderByKind, SelectItem, UnaryOperator, Value as SqlValue,
};

use super::{scan_live_rows, LiveReadRow, LiveStorageLane};

const REGISTERED_SCHEMA_BOOTSTRAP_TABLE: &str = "lix_internal_registered_schema_bootstrap";
const REGISTERED_SCHEMA_KEY: &str = "lix_registered_schema";
const GLOBAL_VERSION_ID: &str = "global";

struct TransactionReadModel<'a> {
    base: &'a dyn LixBackend,
    pending_view: Option<&'a dyn PendingView>,
}

impl<'a> TransactionReadModel<'a> {
    fn new(base: &'a dyn LixBackend, pending_view: Option<&'a dyn PendingView>) -> Self {
        Self { base, pending_view }
    }

    fn has_pending_visibility(&self) -> bool {
        self.pending_view.is_some_and(PendingView::has_overlays)
    }

    async fn bootstrap_public_surface_registry(
        &self,
    ) -> Result<crate::contracts::surface::SurfaceRegistry, LixError> {
        if !self.has_pending_visibility() {
            return crate::contracts::surface::SurfaceRegistry::bootstrap_with_backend(self.base)
                .await;
        }

        let mut registry = crate::contracts::surface::SurfaceRegistry::with_builtin_surfaces();
        for snapshot_content in self.visible_registered_schema_rows().await?.into_values() {
            let snapshot: JsonValue = serde_json::from_str(&snapshot_content).map_err(|error| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("registered schema snapshot_content invalid JSON: {error}"),
                )
            })?;
            registry.replace_dynamic_entity_surfaces_from_stored_snapshot(&snapshot)?;
        }
        Ok(registry)
    }

    async fn execute_prepared_public_read(
        &self,
        public_read: &PreparedPublicRead,
    ) -> Result<QueryResult, LixError> {
        if !self.has_pending_visibility() {
            return execute_prepared_public_read(self.base, public_read).await;
        }

        match public_read_execution_mode(public_read) {
            PublicReadExecutionMode::PendingView => {
                let query = live_table_query_from_prepared_public_read(public_read)
                    .expect("pending-view public reads must lower to a typed live-table query");
                let result = self.execute_live_table_query(&query).await?;
                if let Some(lowered) = public_read.lowered_read() {
                    return Ok(decode_public_read_result(result, lowered));
                }
                Ok(result)
            }
            PublicReadExecutionMode::Committed(CommittedReadMode::CommittedOnly)
            | PublicReadExecutionMode::Committed(CommittedReadMode::MaterializedState) => {
                execute_prepared_public_read_without_freshness_check(self.base, public_read).await
            }
        }
    }

    async fn visible_registered_schema_rows(&self) -> Result<BTreeMap<String, String>, LixError> {
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

        if let Some(pending_view) = self.pending_view {
            for (entity_id, snapshot_content) in pending_view.visible_registered_schema_entries() {
                match snapshot_content {
                    Some(snapshot_content) => {
                        rows.insert(entity_id, snapshot_content);
                    }
                    None => {
                        rows.remove(&entity_id);
                    }
                }
            }

            for row in pending_view
                .visible_semantic_rows(PendingSemanticStorage::Tracked, REGISTERED_SCHEMA_KEY)
            {
                if row.version_id != GLOBAL_VERSION_ID {
                    continue;
                }
                match row.snapshot_content.as_ref().filter(|_| !row.tombstone) {
                    Some(snapshot_content) => {
                        rows.insert(row.entity_id.clone(), snapshot_content.clone());
                    }
                    None => {
                        rows.remove(&row.entity_id);
                    }
                }
            }
        }

        Ok(rows)
    }

    async fn execute_live_table_query(
        &self,
        query: &LiveTableOverlayQuery,
    ) -> Result<QueryResult, LixError> {
        let access = self.load_live_row_access(&query.schema_key).await?;
        let constraints = scan_constraints_from_live_filters(&query.filters);
        let required_columns = access
            .columns()
            .iter()
            .map(|column| column.property_name.clone())
            .collect::<Vec<_>>();
        let mut rows = match query.storage {
            PendingSemanticStorage::Tracked => scan_live_rows(
                self.base,
                LiveStorageLane::Tracked,
                &query.schema_key,
                &query.version_id,
                &constraints,
                &required_columns,
            )
            .await?
            .into_iter()
            .map(|row| visible_live_row_from_raw(&access, row))
            .collect::<Result<Vec<_>, _>>()?,
            PendingSemanticStorage::Untracked => scan_live_rows(
                self.base,
                LiveStorageLane::Untracked,
                &query.schema_key,
                &query.version_id,
                &constraints,
                &required_columns,
            )
            .await?
            .into_iter()
            .map(|row| visible_live_row_from_raw(&access, row))
            .collect::<Result<Vec<_>, _>>()?,
        };
        let mut by_identity = rows
            .drain(..)
            .map(|row| (visible_live_row_identity(&row), row))
            .collect::<BTreeMap<_, _>>();
        if let Some(pending_view) = self.pending_view {
            for row in pending_view.visible_semantic_rows(query.storage, &query.schema_key) {
                let visible = visible_live_row_from_pending(&access, &row)?;
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
        self.apply_workspace_writer_key_overlay_to_rows(query, &mut by_identity);
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
        if query
            .projections
            .iter()
            .all(|projection| matches!(projection, LiveProjection::CountAll { .. }))
        {
            return Ok(QueryResult {
                columns: query
                    .projections
                    .iter()
                    .map(|projection| match projection {
                        LiveProjection::CountAll { output_column } => output_column.clone(),
                        LiveProjection::Column { output_column, .. } => output_column.clone(),
                    })
                    .collect(),
                rows: vec![query
                    .projections
                    .iter()
                    .map(|_| Value::Integer(rows.len() as i64))
                    .collect()],
            });
        }
        Ok(QueryResult {
            columns: query
                .projections
                .iter()
                .map(|projection| match projection {
                    LiveProjection::Column { output_column, .. }
                    | LiveProjection::CountAll { output_column } => output_column.clone(),
                })
                .collect(),
            rows: rows
                .into_iter()
                .map(|row| {
                    query
                        .projections
                        .iter()
                        .map(|projection| live_projection_value(&row, projection))
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()?,
        })
    }

    async fn load_live_row_access(&self, schema_key: &str) -> Result<LiveReadContract, LixError> {
        if let Some(layout) = super::storage::builtin_live_table_layout(schema_key)? {
            return Ok(live_read_contract_from_layout(layout));
        }

        let rows = self
            .visible_registered_schema_rows()
            .await?
            .into_values()
            .map(|snapshot_content| vec![Value::Text(snapshot_content)])
            .collect::<Vec<_>>();
        let layout = super::storage::compile_registered_live_layout(schema_key, rows)?;
        Ok(live_read_contract_from_layout(layout))
    }

    fn apply_filesystem_overlay_to_rows(
        &self,
        query: &LiveTableOverlayQuery,
        access: &LiveReadContract,
        rows: &mut BTreeMap<OverlayVisibleLiveRowIdentity, OverlayVisibleLiveRow>,
    ) {
        let Some(pending_view) = self.pending_view else {
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
            pending_view.visible_directory_rows(PendingSemanticStorage::Tracked, &query.schema_key)
        {
            let Ok(visible) = visible_live_row_from_pending(access, &pending) else {
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

        for pending in pending_view.visible_files() {
            if pending.deleted {
                rows.retain(|_, row| {
                    !(row.schema_key == "lix_file_descriptor"
                        && row.entity_id == pending.file_id
                        && row.version_id == pending.version_id)
                });
                continue;
            }

            if let Some(visible) = visible_live_row_from_pending_filesystem_state(access, &pending)
            {
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

    fn apply_workspace_writer_key_overlay_to_rows(
        &self,
        query: &LiveTableOverlayQuery,
        rows: &mut BTreeMap<OverlayVisibleLiveRowIdentity, OverlayVisibleLiveRow>,
    ) {
        if query.storage != PendingSemanticStorage::Tracked {
            return;
        }
        let Some(pending_view) = self.pending_view else {
            return;
        };

        for row in rows.values_mut() {
            let identity = RowIdentity {
                schema_key: row.schema_key.clone(),
                version_id: row.version_id.clone(),
                entity_id: row.entity_id.clone(),
                file_id: row.file_id.clone(),
            };
            let Some(writer_key) = pending_view.workspace_writer_key_annotation(&identity) else {
                continue;
            };
            row.normalized_values.insert(
                "writer_key".to_string(),
                writer_key.clone().map(Value::Text).unwrap_or(Value::Null),
            );
        }
    }
}

pub(crate) async fn bootstrap_public_surface_registry_with_pending_transaction_view(
    base: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
) -> Result<crate::contracts::surface::SurfaceRegistry, LixError> {
    TransactionReadModel::new(base, pending_transaction_view)
        .bootstrap_public_surface_registry()
        .await
}

pub(crate) async fn execute_prepared_public_read_with_pending_transaction_view(
    base: &dyn LixBackend,
    pending_transaction_view: Option<&dyn PendingView>,
    public_read: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    let Some(pending_transaction_view) = pending_transaction_view else {
        return execute_prepared_public_read(base, public_read).await;
    };
    TransactionReadModel::new(base, Some(pending_transaction_view))
        .execute_prepared_public_read(public_read)
        .await
}

pub(crate) async fn execute_prepared_public_read_with_pending_transaction_view_in_transaction(
    transaction: &mut dyn LixBackendTransaction,
    pending_transaction_view: Option<&dyn PendingView>,
    public_read: &PreparedPublicRead,
) -> Result<QueryResult, LixError> {
    let execution_mode = public_read_execution_mode(public_read);
    match (pending_transaction_view, execution_mode) {
        (Some(pending_transaction_view), PublicReadExecutionMode::PendingView) => {
            let backend = crate::runtime::TransactionBackendAdapter::new(transaction);
            TransactionReadModel::new(&backend, Some(pending_transaction_view))
                .execute_prepared_public_read(public_read)
                .await
        }
        _ => execute_prepared_public_read_in_transaction(transaction, public_read).await,
    }
}

pub(crate) fn public_read_execution_mode(
    public_read: &PreparedPublicRead,
) -> PublicReadExecutionMode {
    if live_table_query_from_prepared_public_read(public_read).is_some() {
        return PublicReadExecutionMode::PendingView;
    }

    PublicReadExecutionMode::Committed(committed_read_mode_from_prepared_public_read(public_read))
}

#[derive(Clone)]
struct LiveTableOverlayQuery {
    storage: PendingSemanticStorage,
    schema_key: String,
    version_id: String,
    projections: Vec<LiveProjection>,
    filters: Vec<LiveFilter>,
    order_by: Vec<LiveOrderClause>,
    limit: Option<usize>,
}

#[derive(Clone)]
enum LiveProjection {
    Column {
        source_column: String,
        output_column: String,
    },
    CountAll {
        output_column: String,
    },
}

#[derive(Clone)]
enum LiveFilter {
    Equals(String, Value),
    In(String, Vec<Value>),
    IsNull(String),
    IsNotNull(String),
    Like {
        column: String,
        pattern: String,
        case_insensitive: bool,
    },
    And(Vec<LiveFilter>),
    Or(Vec<LiveFilter>),
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

fn live_projection_from_select_item(
    item: &SelectItem,
    table_alias: Option<&str>,
) -> Option<LiveProjection> {
    match item {
        SelectItem::UnnamedExpr(expr) => live_projection_from_expr(
            expr,
            table_alias,
            live_identifier_name(expr, table_alias).unwrap_or_else(|| expr.to_string()),
        ),
        SelectItem::ExprWithAlias { expr, alias } => {
            live_projection_from_expr(expr, table_alias, alias.value.clone())
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => None,
    }
}

fn live_table_query_from_prepared_public_read(
    public_read: &PreparedPublicRead,
) -> Option<LiveTableOverlayQuery> {
    let structured_read = public_read.structured_read()?;
    if !matches!(
        structured_read.surface_binding.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity
    ) {
        return None;
    }
    if matches!(
        structured_read.surface_binding.descriptor.surface_variant,
        SurfaceVariant::History | SurfaceVariant::WorkingChanges
    ) {
        return None;
    }

    let table_alias = structured_read
        .query
        .source_alias
        .as_ref()
        .map(|alias| alias.name.value.as_str());
    let mut placeholder_state = PlaceholderState::new();
    let bound_parameters = &structured_read.bound_parameters;

    Some(LiveTableOverlayQuery {
        storage: PendingSemanticStorage::Tracked,
        schema_key: structured_read
            .surface_binding
            .implicit_overrides
            .fixed_schema_key
            .clone()
            .or_else(|| {
                let request = public_read.effective_state_request()?;
                (request.schema_set.len() == 1)
                    .then(|| request.schema_set.iter().next().cloned())
                    .flatten()
            })?,
        version_id: structured_read.requested_version_id.clone()?,
        projections: structured_read
            .query
            .projection
            .iter()
            .map(|item| live_projection_from_select_item(item, table_alias))
            .collect::<Option<Vec<_>>>()?,
        filters: structured_read
            .query
            .selection_predicates
            .iter()
            .map(|predicate| {
                live_filter_from_expr(
                    predicate,
                    table_alias,
                    bound_parameters,
                    &mut placeholder_state,
                )
            })
            .collect::<Option<Vec<_>>>()?,
        order_by: structured_read
            .query
            .order_by
            .as_ref()
            .map(|order_by| live_order_by_from_clause(order_by, table_alias))
            .flatten()
            .unwrap_or_default(),
        limit: live_limit_from_clause(structured_read.query.limit_clause.as_ref())?,
    })
}

fn live_projection_from_expr(
    expr: &Expr,
    table_alias: Option<&str>,
    output_column: String,
) -> Option<LiveProjection> {
    if live_expr_is_count_all(expr) {
        return Some(LiveProjection::CountAll { output_column });
    }

    Some(LiveProjection::Column {
        source_column: live_identifier_name(expr, table_alias)?,
        output_column,
    })
}

fn live_filter_from_expr(
    expr: &Expr,
    table_alias: Option<&str>,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<LiveFilter> {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => Some(LiveFilter::And(vec![
            live_filter_from_expr(left, table_alias, params, placeholder_state)?,
            live_filter_from_expr(right, table_alias, params, placeholder_state)?,
        ])),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Or,
            right,
        } => Some(LiveFilter::Or(vec![
            live_filter_from_expr(left, table_alias, params, placeholder_state)?,
            live_filter_from_expr(right, table_alias, params, placeholder_state)?,
        ])),
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => match (
            left.as_ref(),
            live_value_from_expr(right, params, placeholder_state),
            right.as_ref(),
            live_value_from_expr(left, params, placeholder_state),
        ) {
            (left, Some(value), _, _) => Some(LiveFilter::Equals(
                live_identifier_name(left, table_alias)?,
                value,
            )),
            (_, _, right, Some(value)) => Some(LiveFilter::Equals(
                live_identifier_name(right, table_alias)?,
                value,
            )),
            _ => None,
        },
        Expr::InList {
            expr,
            list,
            negated: false,
        } => Some(LiveFilter::In(
            live_identifier_name(expr, table_alias)?,
            list.iter()
                .map(|expr| live_value_from_expr(expr, params, placeholder_state))
                .collect::<Option<Vec<_>>>()?,
        )),
        Expr::IsNull(expr) => Some(LiveFilter::IsNull(live_identifier_name(expr, table_alias)?)),
        Expr::IsNotNull(expr) => Some(LiveFilter::IsNotNull(live_identifier_name(
            expr,
            table_alias,
        )?)),
        Expr::Like {
            expr,
            pattern,
            negated: false,
            ..
        } => Some(LiveFilter::Like {
            column: live_identifier_name(expr, table_alias)?,
            pattern: live_value_from_expr(pattern, params, placeholder_state)
                .and_then(|value| overlay_filter_text(&value))?,
            case_insensitive: false,
        }),
        Expr::ILike {
            expr,
            pattern,
            negated: false,
            ..
        } => Some(LiveFilter::Like {
            column: live_identifier_name(expr, table_alias)?,
            pattern: live_value_from_expr(pattern, params, placeholder_state)
                .and_then(|value| overlay_filter_text(&value))?,
            case_insensitive: true,
        }),
        Expr::Nested(inner) => live_filter_from_expr(inner, table_alias, params, placeholder_state),
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
            let qualifier = parts[0].value.as_str();
            let column = parts[1].value.clone();
            match table_alias {
                Some(alias) if alias.eq_ignore_ascii_case(qualifier) => Some(column),
                None => Some(column),
                _ => None,
            }
        }
        _ => None,
    }
}

fn live_expr_is_count_all(expr: &Expr) -> bool {
    let Expr::Function(function) = expr else {
        return false;
    };
    function.name.to_string().eq_ignore_ascii_case("count")
        && matches!(
            &function.args,
            FunctionArguments::List(list)
                if list.args.len() == 1
                    && matches!(
                        &list.args[0],
                        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
                    )
        )
}

fn live_value_from_expr(
    expr: &Expr,
    params: &[Value],
    placeholder_state: &mut PlaceholderState,
) -> Option<Value> {
    match expr {
        Expr::Nested(inner) => live_value_from_expr(inner, params, placeholder_state),
        Expr::UnaryOp { op, expr } => {
            let value = live_value_from_expr(expr, params, placeholder_state)?;
            match (op, value) {
                (UnaryOperator::Minus, Value::Integer(value)) => Some(Value::Integer(-value)),
                (UnaryOperator::Minus, Value::Real(value)) => Some(Value::Real(-value)),
                (UnaryOperator::Plus, value) => Some(value),
                _ => None,
            }
        }
        Expr::Value(value) => match &value.value {
            SqlValue::Placeholder(token) => {
                let index =
                    resolve_placeholder_index(token, params.len(), placeholder_state).ok()?;
                params.get(index).cloned()
            }
            _ => sql_value_as_engine_value(value),
        },
        _ => None,
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

fn scan_constraints_from_live_filters(filters: &[LiveFilter]) -> Vec<ScanConstraint> {
    filters
        .iter()
        .filter_map(|filter| match filter {
            LiveFilter::Equals(column, value) => {
                let field = match column.as_str() {
                    "entity_id" => ScanField::EntityId,
                    "file_id" => ScanField::FileId,
                    "plugin_key" => ScanField::PluginKey,
                    "schema_version" => ScanField::SchemaVersion,
                    _ => return None,
                };
                Some(ScanConstraint {
                    field,
                    operator: ScanOperator::Eq(value.clone()),
                })
            }
            _ => None,
        })
        .collect()
}

fn visible_live_row_from_raw(
    access: &LiveReadContract,
    row: LiveReadRow,
) -> Result<OverlayVisibleLiveRow, LixError> {
    let snapshot_content = row.snapshot_text(access)?;
    Ok(OverlayVisibleLiveRow {
        entity_id: row.entity_id().to_string(),
        schema_key: row.schema_key().to_string(),
        schema_version: row.schema_version().to_string(),
        file_id: row.file_id().to_string(),
        version_id: row.version_id().to_string(),
        plugin_key: row.plugin_key().to_string(),
        metadata: row.metadata().map(ToOwned::to_owned),
        change_id: row.change_id().map(ToOwned::to_owned),
        normalized_values: row.values().clone(),
        snapshot_content: Some(snapshot_content),
        is_tombstone: false,
    })
}

fn visible_live_row_from_pending(
    access: &LiveReadContract,
    pending: &PendingSemanticRow,
) -> Result<OverlayVisibleLiveRow, LixError> {
    Ok(OverlayVisibleLiveRow {
        entity_id: pending.entity_id.clone(),
        schema_key: pending.schema_key.clone(),
        schema_version: pending.schema_version.clone(),
        file_id: pending.file_id.clone(),
        version_id: pending.version_id.clone(),
        plugin_key: pending.plugin_key.clone(),
        metadata: pending.metadata.clone(),
        change_id: None,
        snapshot_content: pending.snapshot_content.clone(),
        is_tombstone: pending.tombstone,
        normalized_values: access.normalized_values(pending.snapshot_content.as_deref())?,
    })
}

fn visible_live_row_from_pending_filesystem_state(
    access: &LiveReadContract,
    pending: &crate::filesystem::runtime::FilesystemTransactionFileState,
) -> Option<OverlayVisibleLiveRow> {
    let descriptor = pending.descriptor.as_ref()?;
    let snapshot_content = serde_json::json!({
        "id": pending.file_id,
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
        entity_id: pending.file_id.clone(),
        schema_key: "lix_file_descriptor".to_string(),
        schema_version: "1".to_string(),
        file_id: "lix".to_string(),
        version_id: pending.version_id.clone(),
        plugin_key: "lix".to_string(),
        metadata: descriptor.metadata.clone(),
        change_id: None,
        snapshot_content: Some(snapshot_content.clone()),
        is_tombstone: false,
        normalized_values: access.normalized_values(Some(&snapshot_content)).ok()?,
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
        LiveFilter::And(filters) => filters
            .iter()
            .all(|filter| live_filter_matches_row(filter, row)),
        LiveFilter::Or(filters) => filters
            .iter()
            .any(|filter| live_filter_matches_row(filter, row)),
        LiveFilter::Equals(column, expected) => {
            live_row_value(row, column).is_some_and(|actual| actual == *expected)
        }
        LiveFilter::In(column, expected) => live_row_value(row, column)
            .is_some_and(|actual| expected.iter().any(|candidate| candidate == &actual)),
        LiveFilter::IsNull(column) => {
            matches!(live_row_value(row, column), Some(Value::Null) | None)
        }
        LiveFilter::IsNotNull(column) => {
            !matches!(live_row_value(row, column), Some(Value::Null) | None)
        }
        LiveFilter::Like {
            column,
            pattern,
            case_insensitive,
        } => live_row_value(row, column)
            .and_then(|actual| overlay_filter_text(&actual))
            .is_some_and(|actual| sql_like_matches(&actual, pattern, *case_insensitive)),
    }
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

fn live_projection_value(
    row: &OverlayVisibleLiveRow,
    projection: &LiveProjection,
) -> Result<Value, LixError> {
    match projection {
        LiveProjection::Column { source_column, .. } => live_row_value(row, source_column)
            .ok_or_else(|| {
                LixError::new(
                    "LIX_ERROR_UNKNOWN",
                    format!("overlay query requested unsupported live column '{source_column}'"),
                )
            }),
        LiveProjection::CountAll { .. } => Ok(Value::Integer(1)),
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

fn sql_like_matches(actual: &str, pattern: &str, case_insensitive: bool) -> bool {
    let actual_chars = if case_insensitive {
        actual.to_ascii_lowercase().chars().collect::<Vec<_>>()
    } else {
        actual.chars().collect::<Vec<_>>()
    };
    let pattern_chars = if case_insensitive {
        pattern.to_ascii_lowercase().chars().collect::<Vec<_>>()
    } else {
        pattern.chars().collect::<Vec<_>>()
    };

    let mut dp = vec![false; actual_chars.len() + 1];
    dp[0] = true;

    for pattern_char in pattern_chars {
        let mut next = vec![false; actual_chars.len() + 1];
        match pattern_char {
            '%' => {
                let mut seen = false;
                for index in 0..=actual_chars.len() {
                    seen |= dp[index];
                    next[index] = seen;
                }
            }
            '_' => {
                for index in 0..actual_chars.len() {
                    if dp[index] {
                        next[index + 1] = true;
                    }
                }
            }
            literal => {
                for index in 0..actual_chars.len() {
                    if dp[index] && actual_chars[index] == literal {
                        next[index + 1] = true;
                    }
                }
            }
        }
        dp = next;
    }

    dp[actual_chars.len()]
}
