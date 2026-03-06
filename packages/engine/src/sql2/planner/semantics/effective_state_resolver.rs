use crate::sql2::planner::canonicalize::CanonicalizedRead;
use crate::sql2::planner::ir::{CanonicalStateScan, ReadPlan, VersionScope};
use crate::sql_shared::dependency_spec::DependencySpec;
use sqlparser::ast::{Expr, OrderByKind, Query, SelectItem, Statement, Visit, Visitor};
use std::collections::BTreeSet;
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
    let plan = EffectiveStatePlan {
        state_source: StateSourceAuthority::AuthoritativeCommitted,
        overlay_lanes: overlay_lanes_for_request(&request),
        pushdown_safe_predicates: Vec::new(),
        residual_predicates: residual_predicates(&canonicalized.read_command.root),
    };
    Some((request, plan))
}

fn canonical_state_scan(read_plan: &ReadPlan) -> Option<&CanonicalStateScan> {
    match read_plan {
        ReadPlan::Scan(scan) => Some(scan),
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
    let mut lanes = vec![OverlayLane::GlobalTracked, OverlayLane::LocalTracked];
    if request.include_untracked_overlay {
        lanes.push(OverlayLane::GlobalUntracked);
        lanes.push(OverlayLane::LocalUntracked);
    }
    lanes
}

fn residual_predicates(read_plan: &ReadPlan) -> Vec<String> {
    match read_plan {
        ReadPlan::Scan(_) => Vec::new(),
        ReadPlan::Filter { input, predicate } => {
            let mut predicates = residual_predicates(input);
            predicates.push(predicate.sql.clone());
            predicates
        }
        ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => residual_predicates(input),
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
                OverlayLane::GlobalTracked,
                OverlayLane::LocalTracked,
                OverlayLane::GlobalUntracked,
                OverlayLane::LocalUntracked,
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
}
