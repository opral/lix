use crate::commit::{
    load_exact_committed_state_row, CommitQueryExecutor, ExactCommittedStateRow,
    ExactCommittedStateRowRequest,
};
use crate::sql2::catalog::{SurfaceFamily, SurfaceVariant};
use crate::sql2::planner::canonicalize::CanonicalizedRead;
use crate::sql2::planner::ir::{CanonicalStateScan, ReadPlan, VersionScope};
use crate::sql_shared::dependency_spec::DependencySpec;
use crate::version::GLOBAL_VERSION_ID;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, OrderByKind, Query, SelectItem, Statement, UnaryOperator, Visit, Visitor,
};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateSourceAuthority {
    AuthoritativeCommitted,
    Untracked,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OverlayLane {
    GlobalTracked,
    LocalTracked,
    GlobalUntracked,
    LocalUntracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveStateRequest {
    pub(crate) schema_set: BTreeSet<String>,
    pub(crate) version_scope: VersionScope,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
    pub(crate) include_tombstones: bool,
    pub(crate) predicate_classes: Vec<String>,
    pub(crate) required_columns: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffectiveStatePlan {
    pub(crate) state_source: StateSourceAuthority,
    pub(crate) overlay_lanes: Vec<OverlayLane>,
    pub(crate) pushdown_safe_predicates: Vec<String>,
    pub(crate) residual_predicates: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ResolvedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) lineage_commit_id: Option<String>,
    pub(crate) lineage_change_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct ResolvedStateRows {
    pub(crate) visible_rows: Vec<ResolvedStateRow>,
    pub(crate) hidden_rows: Vec<ResolvedStateRow>,
    pub(crate) lineage_metadata: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactEffectiveStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) exact_filters: BTreeMap<String, Value>,
    pub(crate) include_global_overlay: bool,
    pub(crate) include_untracked_overlay: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactEffectiveStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) source_change_id: Option<String>,
    pub(crate) overlay_lane: OverlayLane,
}

pub(crate) fn build_effective_state(
    canonicalized: &CanonicalizedRead,
    dependency_spec: Option<&DependencySpec>,
) -> Option<(EffectiveStateRequest, EffectiveStatePlan)> {
    let scan = canonical_state_scan(&canonicalized.read_command.root)?;
    let request = EffectiveStateRequest {
        schema_set: schema_set_for_read(canonicalized, dependency_spec),
        version_scope: scan.version_scope,
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: scan.include_tombstones,
        predicate_classes: predicate_classes_for_read(canonicalized),
        required_columns: required_columns_for_read(canonicalized, scan),
    };
    let all_predicates = read_predicates(canonicalized);
    let pushdown_safe_predicates = pushdown_safe_predicates(canonicalized);
    let plan = EffectiveStatePlan {
        state_source: StateSourceAuthority::AuthoritativeCommitted,
        overlay_lanes: overlay_lanes_for_request(&request),
        pushdown_safe_predicates: pushdown_safe_predicates.clone(),
        residual_predicates: all_predicates
            .into_iter()
            .filter(|predicate| {
                !pushdown_safe_predicates
                    .iter()
                    .any(|candidate| candidate == predicate)
            })
            .collect(),
    };
    Some((request, plan))
}

fn canonical_state_scan(read_plan: &ReadPlan) -> Option<&CanonicalStateScan> {
    match read_plan {
        ReadPlan::Scan(scan) => Some(scan),
        ReadPlan::AdminScan(_) | ReadPlan::ChangeScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_state_scan(input),
    }
}

fn schema_set_for_read(
    canonicalized: &CanonicalizedRead,
    dependency_spec: Option<&DependencySpec>,
) -> BTreeSet<String> {
    let mut schema_set = BTreeSet::new();
    if let Some(schema_key) = canonicalized
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        schema_set.insert(schema_key);
    }
    if let Some(spec) = dependency_spec {
        schema_set.extend(
            spec.schema_keys
                .iter()
                .filter(|schema_key| schema_key.as_str() != "lix_active_version")
                .cloned(),
        );
    }
    schema_set
}

fn predicate_classes_for_read(canonicalized: &CanonicalizedRead) -> Vec<String> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return Vec::new();
    };

    struct Collector {
        classes: BTreeSet<String>,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(column) = filter_column_name(expr) {
                self.classes.insert(format!("column:{column}"));
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        classes: BTreeSet::new(),
    };
    let _ = query.visit(&mut collector);
    collector.classes.into_iter().collect()
}

fn required_columns_for_read(
    canonicalized: &CanonicalizedRead,
    scan: &CanonicalStateScan,
) -> Vec<String> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return scan.binding.exposed_columns.clone();
    };
    let mut required = BTreeSet::new();

    if let Some(entity_projection) = &scan.entity_projection {
        required.extend(entity_projection.visible_columns.iter().cloned());
    }

    collect_projection_columns(query, &mut required);
    collect_expression_columns(query, &mut required);
    if required.is_empty() {
        required.extend(scan.binding.exposed_columns.iter().cloned());
    }
    required.insert("entity_id".to_string());
    required.insert("schema_key".to_string());
    if scan.expose_version_id || scan.version_scope != VersionScope::ActiveVersion {
        required.insert("version_id".to_string());
    }

    required.into_iter().collect()
}

fn collect_projection_columns(query: &Query, required: &mut BTreeSet<String>) {
    let Some(select) = select_query(query) else {
        return;
    };
    let wildcard_projection = select.projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    });
    if wildcard_projection {
        return;
    }

    for item in &select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => collect_columns_from_expr(expr, required),
            SelectItem::ExprWithAlias { expr, .. } => collect_columns_from_expr(expr, required),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
}

fn collect_expression_columns(query: &Query, required: &mut BTreeSet<String>) {
    let Some(select) = select_query(query) else {
        return;
    };
    if let Some(selection) = &select.selection {
        collect_columns_from_expr(selection, required);
    }
    if let Some(order_by) = &query.order_by {
        let OrderByKind::Expressions(ordering) = &order_by.kind else {
            return;
        };
        for item in ordering {
            collect_columns_from_expr(&item.expr, required);
        }
    }
}

fn select_query(query: &Query) -> Option<&sqlparser::ast::Select> {
    let sqlparser::ast::SetExpr::Select(select) = query.body.as_ref() else {
        return None;
    };
    Some(select)
}

fn collect_columns_from_expr(expr: &Expr, required: &mut BTreeSet<String>) {
    struct Collector<'a> {
        required: &'a mut BTreeSet<String>,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(column) = filter_column_name(expr) {
                self.required.insert(column.to_string());
            } else if let Expr::Identifier(ident) = expr {
                self.required.insert(ident.value.clone());
            } else if let Expr::CompoundIdentifier(parts) = expr {
                if let Some(last) = parts.last() {
                    self.required.insert(last.value.clone());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector { required };
    let _ = expr.visit(&mut collector);
}

fn filter_column_name(expr: &Expr) -> Option<&'static str> {
    let column = match expr {
        Expr::Identifier(identifier) => Some(identifier.value.as_str()),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|identifier| identifier.value.as_str()),
        Expr::Nested(inner) => return filter_column_name(inner),
        _ => None,
    }?;

    match column.to_ascii_lowercase().as_str() {
        "schema_key" => Some("schema_key"),
        "entity_id" => Some("entity_id"),
        "file_id" => Some("file_id"),
        "version_id" | "lixcol_version_id" => Some("version_id"),
        _ => None,
    }
}

fn overlay_lanes_for_request(request: &EffectiveStateRequest) -> Vec<OverlayLane> {
    let mut lanes = vec![OverlayLane::LocalTracked];
    if request.include_untracked_overlay {
        lanes.insert(0, OverlayLane::LocalUntracked);
    }
    if request.include_global_overlay {
        if request.include_untracked_overlay {
            lanes.push(OverlayLane::GlobalUntracked);
        }
        lanes.push(OverlayLane::GlobalTracked);
    }
    lanes
}

pub(crate) async fn resolve_exact_effective_state_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let requested_untracked = request
        .exact_filters
        .get("untracked")
        .and_then(bool_from_value);
    let requested_global = request
        .exact_filters
        .get("global")
        .and_then(bool_from_value);

    let mut lanes = vec![OverlayLane::LocalTracked];
    if request.include_untracked_overlay {
        lanes.insert(0, OverlayLane::LocalUntracked);
    }
    if request.include_global_overlay && request.version_id != GLOBAL_VERSION_ID {
        if request.include_untracked_overlay {
            lanes.push(OverlayLane::GlobalUntracked);
        }
        lanes.push(OverlayLane::GlobalTracked);
    }

    for lane in lanes {
        if !lane_matches_global_filter(lane, requested_global)
            || !lane_matches_untracked_filter(lane, requested_untracked)
        {
            continue;
        }

        let version_id = if matches!(lane, OverlayLane::GlobalUntracked) {
            GLOBAL_VERSION_ID.to_string()
        } else {
            request.version_id.clone()
        };

        let row = match lane {
            OverlayLane::LocalTracked | OverlayLane::GlobalTracked => {
                load_exact_tracked_effective_row(backend, request, lane).await?
            }
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked => {
                load_exact_untracked_effective_row(backend, request, &version_id, lane).await?
            }
        };

        if row.is_some() {
            return Ok(row);
        }
    }

    Ok(None)
}

async fn load_exact_tracked_effective_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    overlay_lane: OverlayLane,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let Some(entity_id) = request
        .exact_filters
        .get("entity_id")
        .and_then(text_from_value)
        .map(ToString::to_string)
    else {
        return Ok(None);
    };

    let mut exact_filters = request.exact_filters.clone();
    exact_filters.remove("entity_id");
    exact_filters.remove("global");
    exact_filters.remove("untracked");
    let global_filter = if request.version_id == GLOBAL_VERSION_ID {
        request
            .exact_filters
            .get("global")
            .and_then(bool_from_value)
    } else {
        Some(matches!(overlay_lane, OverlayLane::GlobalTracked))
    };

    let row = load_exact_committed_state_row(
        backend,
        &ExactCommittedStateRowRequest {
            entity_id,
            schema_key: request.schema_key.clone(),
            version_id: request.version_id.clone(),
            global_filter,
            exact_filters,
        },
    )
    .await?;

    Ok(row.map(|row| exact_effective_state_row_from_tracked(row, overlay_lane)))
}

async fn load_exact_untracked_effective_row(
    backend: &dyn LixBackend,
    request: &ExactEffectiveStateRowRequest,
    version_id: &str,
    overlay_lane: OverlayLane,
) -> Result<Option<ExactEffectiveStateRow>, LixError> {
    let mut executor = backend;
    let row = load_exact_untracked_state_row(
        &mut executor,
        &ExactUntrackedStateRowRequest {
            schema_key: request.schema_key.clone(),
            version_id: version_id.to_string(),
            exact_filters: request.exact_filters.clone(),
        },
    )
    .await?;

    Ok(row.map(|row| exact_effective_state_row_from_untracked(row, overlay_lane)))
}

fn exact_effective_state_row_from_tracked(
    row: ExactCommittedStateRow,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        values: row.values,
        source_change_id: row.source_change_id,
        overlay_lane,
    }
}

fn exact_effective_state_row_from_untracked(
    row: ExactUntrackedStateRow,
    overlay_lane: OverlayLane,
) -> ExactEffectiveStateRow {
    ExactEffectiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        version_id: row.version_id.clone(),
        values: row.values,
        source_change_id: Some("untracked".to_string()),
        overlay_lane,
    }
}

#[derive(Debug, Clone, PartialEq)]
struct ExactUntrackedStateRowRequest {
    schema_key: String,
    version_id: String,
    exact_filters: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExactUntrackedStateRow {
    entity_id: String,
    schema_key: String,
    file_id: String,
    version_id: String,
    values: BTreeMap<String, Value>,
}

async fn load_exact_untracked_state_row(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactUntrackedStateRowRequest,
) -> Result<Option<ExactUntrackedStateRow>, LixError> {
    let mut predicates = vec![
        format!("schema_key = '{}'", escape_sql_string(&request.schema_key)),
        format!("version_id = '{}'", escape_sql_string(&request.version_id)),
        "snapshot_content IS NOT NULL".to_string(),
    ];
    for column in [
        "entity_id",
        "file_id",
        "plugin_key",
        "schema_version",
        "writer_key",
    ] {
        if let Some(value) = request.exact_filters.get(column) {
            let Some(value) = text_from_value(value) else {
                return Err(LixError {
                    code: "LIX_ERROR_UNKNOWN".to_string(),
                    description: format!(
                        "sql2 effective-state resolver requires text-compatible exact filter values for '{column}'"
                    ),
                });
            };
            predicates.push(format!("{column} = '{}'", escape_sql_string(value)));
        }
    }

    let sql = format!(
        "SELECT \
             entity_id, schema_key, schema_version, file_id, version_id, plugin_key, \
             snapshot_content, metadata \
         FROM lix_internal_state_untracked \
         WHERE {predicates} \
         LIMIT 2",
        predicates = predicates.join(" AND "),
    );
    let mut result = executor.execute(&sql, &[]).await?;
    if result.rows.len() > 1 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!(
                "sql2 effective-state resolver requires exactly one untracked target row for '{}@{}'",
                request.schema_key, request.version_id
            ),
        });
    }
    let Some(row) = result.rows.pop() else {
        return Ok(None);
    };
    if row.len() < 8 {
        return Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: "sql2 effective-state resolver query returned too few columns".to_string(),
        });
    }

    let entity_id = required_text_value(&row[0], "entity_id")?;
    let schema_key = required_text_value(&row[1], "schema_key")?;
    let schema_version = required_text_value(&row[2], "schema_version")?;
    let file_id = required_text_value(&row[3], "file_id")?;
    let version_id = required_text_value(&row[4], "version_id")?;
    let plugin_key = required_text_value(&row[5], "plugin_key")?;
    let snapshot_content = required_text_value(&row[6], "snapshot_content")?;
    let metadata = optional_value(&row[7]);

    let mut values = BTreeMap::new();
    values.insert("entity_id".to_string(), Value::Text(entity_id.clone()));
    values.insert("schema_key".to_string(), Value::Text(schema_key.clone()));
    values.insert(
        "schema_version".to_string(),
        Value::Text(schema_version.clone()),
    );
    values.insert("file_id".to_string(), Value::Text(file_id.clone()));
    values.insert("version_id".to_string(), Value::Text(version_id.clone()));
    values.insert("plugin_key".to_string(), Value::Text(plugin_key));
    values.insert(
        "snapshot_content".to_string(),
        Value::Text(snapshot_content),
    );
    if let Some(metadata) = metadata {
        values.insert("metadata".to_string(), metadata);
    }

    Ok(Some(ExactUntrackedStateRow {
        entity_id,
        schema_key,
        file_id,
        version_id,
        values,
    }))
}

fn lane_matches_global_filter(lane: OverlayLane, requested_global: Option<bool>) -> bool {
    match requested_global {
        Some(true) => matches!(
            lane,
            OverlayLane::GlobalTracked | OverlayLane::GlobalUntracked
        ),
        Some(false) => matches!(
            lane,
            OverlayLane::LocalTracked | OverlayLane::LocalUntracked
        ),
        None => true,
    }
}

fn lane_matches_untracked_filter(lane: OverlayLane, requested_untracked: Option<bool>) -> bool {
    match requested_untracked {
        Some(true) => matches!(
            lane,
            OverlayLane::LocalUntracked | OverlayLane::GlobalUntracked
        ),
        Some(false) => matches!(lane, OverlayLane::LocalTracked | OverlayLane::GlobalTracked),
        None => true,
    }
}

fn bool_from_value(value: &Value) -> Option<bool> {
    match value {
        Value::Boolean(value) => Some(*value),
        Value::Integer(value) => Some(*value != 0),
        Value::Text(value) => match value.trim().to_ascii_lowercase().as_str() {
            "1" | "true" => Some(true),
            "0" | "false" => Some(false),
            _ => None,
        },
        _ => None,
    }
}

fn text_from_value(value: &Value) -> Option<&str> {
    match value {
        Value::Text(value) => Some(value.as_str()),
        _ => None,
    }
}

fn required_text_value(value: &Value, label: &str) -> Result<String, LixError> {
    match value {
        Value::Text(value) => Ok(value.clone()),
        _ => Err(LixError {
            code: "LIX_ERROR_UNKNOWN".to_string(),
            description: format!("sql2 effective-state resolver expected text for '{label}'"),
        }),
    }
}

fn optional_value(value: &Value) -> Option<Value> {
    match value {
        Value::Null => None,
        other => Some(other.clone()),
    }
}

fn escape_sql_string(value: &str) -> String {
    value.replace('\'', "''")
}

fn read_predicates(canonicalized: &CanonicalizedRead) -> Vec<String> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return Vec::new();
    };
    let Some(select) = select_query(query) else {
        return Vec::new();
    };
    let Some(selection) = &select.selection else {
        return Vec::new();
    };

    split_conjunctive_predicates(selection)
        .into_iter()
        .map(ToString::to_string)
        .collect()
}

fn pushdown_safe_predicates(canonicalized: &CanonicalizedRead) -> Vec<String> {
    let family = canonicalized.surface_binding.descriptor.surface_family;
    let variant = canonicalized.surface_binding.descriptor.surface_variant;
    let state_backed_history_entity =
        family == SurfaceFamily::Entity && variant == SurfaceVariant::History;
    if family != SurfaceFamily::State && !state_backed_history_entity {
        return Vec::new();
    }

    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return Vec::new();
    };
    let Some(select) = select_query(query) else {
        return Vec::new();
    };
    let Some(selection) = &select.selection else {
        return Vec::new();
    };
    split_conjunctive_predicates(selection)
        .into_iter()
        .filter(|predicate| state_predicate_is_pushdown_safe(predicate, variant))
        .map(ToString::to_string)
        .collect()
}

fn split_conjunctive_predicates(expr: &Expr) -> Vec<&Expr> {
    let mut predicates = Vec::new();
    collect_conjunctive_predicates(expr, &mut predicates);
    predicates
}

fn collect_conjunctive_predicates<'a>(expr: &'a Expr, predicates: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            collect_conjunctive_predicates(left, predicates);
            collect_conjunctive_predicates(right, predicates);
        }
        Expr::Nested(inner) => collect_conjunctive_predicates(inner, predicates),
        _ => predicates.push(expr),
    }
}

fn state_predicate_is_pushdown_safe(expr: &Expr, variant: SurfaceVariant) -> bool {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => state_pushdown_column(left, variant).is_some() && constant_like_expr(right),
        Expr::InList {
            expr,
            list,
            negated: false,
        } => state_pushdown_column(expr, variant).is_some() && list.iter().all(constant_like_expr),
        Expr::Nested(inner) => state_predicate_is_pushdown_safe(inner, variant),
        _ => false,
    }
}

fn state_pushdown_column<'a>(expr: &'a Expr, variant: SurfaceVariant) -> Option<&'a str> {
    let column = identifier_column_name(expr)?;
    match variant {
        SurfaceVariant::Default => match column.to_ascii_lowercase().as_str() {
            "schema_key" | "entity_id" | "file_id" | "plugin_key" | "schema_version" => {
                Some(column)
            }
            _ => None,
        },
        SurfaceVariant::ByVersion => match column.to_ascii_lowercase().as_str() {
            "schema_key" | "entity_id" | "file_id" | "plugin_key" | "schema_version"
            | "version_id" | "lixcol_version_id" => Some(column),
            _ => None,
        },
        SurfaceVariant::History => match column.to_ascii_lowercase().as_str() {
            "root_commit_id" | "lixcol_root_commit_id" => Some(column),
            _ => None,
        },
        SurfaceVariant::Active | SurfaceVariant::WorkingChanges => None,
    }
}

fn identifier_column_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.as_str()),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|identifier| identifier.value.as_str()),
        Expr::Nested(inner) => identifier_column_name(inner),
        _ => None,
    }
}

fn constant_like_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::Nested(inner) => constant_like_expr(inner),
        Expr::UnaryOp {
            op: UnaryOperator::Plus | UnaryOperator::Minus,
            expr,
        } => constant_like_expr(expr),
        Expr::Cast { expr, .. } => constant_like_expr(expr),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::{build_effective_state, OverlayLane, StateSourceAuthority};
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_read;
    use crate::sql2::planner::semantics::dependency_spec::derive_dependency_spec_from_canonicalized_read;
    use crate::{SqlDialect, Value};

    fn canonicalized_read(
        registry: &SurfaceRegistry,
        sql: &str,
        params: Vec<Value>,
    ) -> crate::sql2::planner::canonicalize::CanonicalizedRead {
        let mut statements = parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = BoundStatement::from_statement(
            statement,
            params,
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        canonicalize_read(bound, registry).expect("query should canonicalize")
    }

    #[test]
    fn builds_effective_state_request_for_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalized_read(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
            Vec::new(),
        );
        let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized)
            .expect("dependency spec");

        let (request, plan) = build_effective_state(&canonicalized, Some(&dependency_spec))
            .expect("effective-state plan should build");

        assert_eq!(
            request.schema_set.into_iter().collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
        assert!(!request.include_tombstones);
        assert!(request.required_columns.contains(&"key".to_string()));
        assert!(request.required_columns.contains(&"value".to_string()));
        assert_eq!(
            plan.state_source,
            StateSourceAuthority::AuthoritativeCommitted
        );
        assert_eq!(
            plan.overlay_lanes,
            vec![
                OverlayLane::LocalUntracked,
                OverlayLane::LocalTracked,
                OverlayLane::GlobalUntracked,
                OverlayLane::GlobalTracked,
            ]
        );
        assert_eq!(plan.residual_predicates, vec!["key = 'hello'".to_string()]);
    }

    #[test]
    fn history_surfaces_include_tombstones_and_version_columns() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalized_read(
            &registry,
            "SELECT entity_id, version_id FROM lix_state_history WHERE schema_key = 'message'",
            Vec::new(),
        );
        let dependency_spec = derive_dependency_spec_from_canonicalized_read(&canonicalized)
            .expect("dependency spec");

        let (request, _plan) = build_effective_state(&canonicalized, Some(&dependency_spec))
            .expect("effective-state plan should build");

        assert!(request.include_tombstones);
        assert!(request.required_columns.contains(&"version_id".to_string()));
        assert!(request
            .predicate_classes
            .contains(&"column:schema_key".to_string()));
    }

    #[test]
    fn extracts_exact_state_pushdown_predicates_from_top_level_conjunctions() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalized_read(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value' AND file_id = 'lix'",
            Vec::new(),
        );

        let (_request, plan) =
            build_effective_state(&canonicalized, None).expect("effective-state plan should build");

        assert_eq!(
            plan.pushdown_safe_predicates,
            vec![
                "schema_key = 'lix_key_value'".to_string(),
                "file_id = 'lix'".to_string()
            ]
        );
        assert!(plan.residual_predicates.is_empty());
    }
}
