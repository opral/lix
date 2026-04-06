use crate::contracts::artifacts::{EffectiveStateRequest, EffectiveStateVersionScope};
use crate::sql::logical_plan::public_ir::{
    CanonicalStateRowKey, CanonicalStateScan, ReadPlan, StructuredPublicRead, VersionScope,
};
use crate::sql::logical_plan::DependencySpec;
use crate::sql::semantic_ir::semantics::surface_semantics::{
    canonical_filter_column_name, effective_state_pushdown_predicates, overlay_lanes, OverlayLane,
};
use crate::Value;
use sqlparser::ast::{Expr, OrderBy, OrderByKind, SelectItem, Visit, Visitor};
use std::collections::{BTreeMap, BTreeSet};
use std::ops::ControlFlow;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum StateSourceAuthority {
    AuthoritativeCommitted,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct EffectiveStatePlan {
    pub(crate) state_source: StateSourceAuthority,
    pub(crate) overlay_lanes: Vec<OverlayLane>,
    pub(crate) pushdown_safe_predicates: Vec<Expr>,
    pub(crate) residual_predicates: Vec<Expr>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExactEffectiveStateRowRequest {
    pub(crate) schema_key: String,
    pub(crate) version_id: String,
    pub(crate) row_key: CanonicalStateRowKey,
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
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
) -> Option<(EffectiveStateRequest, EffectiveStatePlan)> {
    let scan = canonical_state_scan(&structured_read.read_command.root)?;
    let request = EffectiveStateRequest {
        schema_set: schema_set_for_read(structured_read, dependency_spec),
        version_scope: effective_state_version_scope(scan.version_scope),
        include_global_overlay: true,
        include_untracked_overlay: true,
        include_tombstones: scan.include_tombstones,
        predicate_classes: predicate_classes_for_read(structured_read),
        required_columns: required_columns_for_read(structured_read, scan),
    };
    let all_predicates = structured_read.query.selection_predicates.clone();
    let pushdown_safe_predicates =
        effective_state_pushdown_predicates(&structured_read.surface_binding, &all_predicates);
    let plan = EffectiveStatePlan {
        state_source: StateSourceAuthority::AuthoritativeCommitted,
        overlay_lanes: overlay_lanes(
            request.include_global_overlay,
            request.include_untracked_overlay,
        ),
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

fn effective_state_version_scope(version_scope: VersionScope) -> EffectiveStateVersionScope {
    match version_scope {
        VersionScope::ActiveVersion => EffectiveStateVersionScope::ActiveVersion,
        VersionScope::ExplicitVersion => EffectiveStateVersionScope::ExplicitVersion,
        VersionScope::History => EffectiveStateVersionScope::History,
    }
}

fn canonical_state_scan(read_plan: &ReadPlan) -> Option<&CanonicalStateScan> {
    match read_plan {
        ReadPlan::Scan(scan) => Some(scan),
        ReadPlan::FilesystemScan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_state_scan(input),
    }
}

fn schema_set_for_read(
    structured_read: &StructuredPublicRead,
    dependency_spec: Option<&DependencySpec>,
) -> BTreeSet<String> {
    let mut schema_set = BTreeSet::new();
    if let Some(schema_key) = structured_read
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        schema_set.insert(schema_key);
    }
    if let Some(spec) = dependency_spec {
        schema_set.extend(spec.schema_keys.iter().cloned());
    }
    schema_set
}

fn predicate_classes_for_read(structured_read: &StructuredPublicRead) -> Vec<String> {
    struct Collector {
        classes: BTreeSet<String>,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(column) = canonical_filter_column_name(expr) {
                self.classes.insert(format!("column:{column}"));
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        classes: BTreeSet::new(),
    };
    for predicate in &structured_read.query.selection_predicates {
        let _ = predicate.visit(&mut collector);
    }
    collector.classes.into_iter().collect()
}

fn required_columns_for_read(
    structured_read: &StructuredPublicRead,
    scan: &CanonicalStateScan,
) -> Vec<String> {
    let mut required = BTreeSet::new();

    if let Some(entity_projection) = &scan.entity_projection {
        required.extend(entity_projection.visible_columns.iter().cloned());
    }

    collect_projection_columns(&structured_read.query.projection, &mut required);
    collect_expression_columns(
        structured_read.query.selection.as_ref(),
        structured_read.query.order_by.as_ref(),
        &mut required,
    );
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

fn collect_projection_columns(projection: &[SelectItem], required: &mut BTreeSet<String>) {
    let wildcard_projection = projection.iter().any(|item| {
        matches!(
            item,
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _)
        )
    });
    if wildcard_projection {
        return;
    }

    for item in projection {
        match item {
            SelectItem::UnnamedExpr(expr) => collect_columns_from_expr(expr, required),
            SelectItem::ExprWithAlias { expr, .. } => collect_columns_from_expr(expr, required),
            SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => {}
        }
    }
}

fn collect_expression_columns(
    selection: Option<&Expr>,
    order_by: Option<&OrderBy>,
    required: &mut BTreeSet<String>,
) {
    if let Some(selection) = selection {
        collect_columns_from_expr(selection, required);
    }
    if let Some(order_by) = order_by {
        let OrderByKind::Expressions(ordering) = &order_by.kind else {
            return;
        };
        for item in ordering {
            collect_columns_from_expr(&item.expr, required);
        }
    }
}

fn collect_columns_from_expr(expr: &Expr, required: &mut BTreeSet<String>) {
    struct Collector<'a> {
        required: &'a mut BTreeSet<String>,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if let Some(column) = canonical_filter_column_name(expr) {
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

#[cfg(test)]
mod tests {
    use super::{build_effective_state, OverlayLane, StateSourceAuthority};
    use crate::contracts::surface::SurfaceRegistry;
    use crate::sql::binder::bind_statement;
    use crate::sql::logical_plan::public_ir::StructuredPublicRead;
    use crate::sql::semantic_ir::canonicalize::canonicalize_read;
    use crate::sql::semantic_ir::semantics::dependency_spec::derive_dependency_spec_from_structured_public_read;
    use crate::sql::semantic_ir::ExecutionContext;
    use crate::{SqlDialect, Value};

    fn structured_read(
        registry: &SurfaceRegistry,
        sql: &str,
        params: Vec<Value>,
    ) -> StructuredPublicRead {
        let mut statements = crate::sql::parser::parse_sql_script(sql).expect("SQL should parse");
        let statement = statements.pop().expect("single statement");
        let bound = bind_statement(
            statement,
            params,
            ExecutionContext::with_dialect(SqlDialect::Sqlite),
        );
        canonicalize_read(bound, registry)
            .expect("query should canonicalize")
            .structured_read()
    }

    #[test]
    fn builds_effective_state_request_for_entity_surface() {
        let registry = crate::schema::build_builtin_surface_registry();
        let structured_read = structured_read(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
            Vec::new(),
        );
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read)
            .expect("dependency spec");

        let (request, plan) = build_effective_state(&structured_read, Some(&dependency_spec))
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
        assert_eq!(
            plan.residual_predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec!["key = 'hello'".to_string()]
        );
    }

    #[test]
    fn history_surfaces_include_tombstones_and_version_columns() {
        let registry = crate::schema::build_builtin_surface_registry();
        let structured_read = structured_read(
            &registry,
            "SELECT entity_id, version_id FROM lix_state_history WHERE schema_key = 'message'",
            Vec::new(),
        );
        let dependency_spec = derive_dependency_spec_from_structured_public_read(&structured_read)
            .expect("dependency spec");

        let (request, _plan) = build_effective_state(&structured_read, Some(&dependency_spec))
            .expect("effective-state plan should build");

        assert!(request.include_tombstones);
        assert!(request.required_columns.contains(&"version_id".to_string()));
        assert!(request
            .predicate_classes
            .contains(&"column:schema_key".to_string()));
    }

    #[test]
    fn extracts_exact_state_pushdown_predicates_from_top_level_conjunctions() {
        let registry = crate::schema::build_builtin_surface_registry();
        let structured_read = structured_read(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value' AND file_id = 'lix'",
            Vec::new(),
        );

        let (_request, plan) = build_effective_state(&structured_read, None)
            .expect("effective-state plan should build");

        assert_eq!(
            plan.pushdown_safe_predicates
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>(),
            vec![
                "schema_key = 'lix_key_value'".to_string(),
                "file_id = 'lix'".to_string()
            ]
        );
        assert!(plan.residual_predicates.is_empty());
    }
}
