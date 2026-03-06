use crate::engine::sql::ast::utils::{resolve_placeholder_index, PlaceholderState};
use crate::sql2::planner::canonicalize::CanonicalizedRead;
use crate::sql2::planner::ir::{ReadPlan, VersionScope};
use crate::sql_shared::dependency_spec::{DependencyPrecision, DependencySpec};
use crate::Value;
use sqlparser::ast::{
    BinaryOperator, Expr, Query, Statement, TableFactor, UnaryOperator, Value as SqlValue, Visit,
    Visitor,
};
use std::collections::BTreeSet;
use std::ops::ControlFlow;

pub(crate) fn derive_dependency_spec_from_canonicalized_read(
    canonicalized: &CanonicalizedRead,
) -> Option<DependencySpec> {
    let Statement::Query(query) = &canonicalized.bound_statement.statement else {
        return None;
    };
    let Some(scan) = canonical_state_scan(&canonicalized.read_command.root) else {
        return None;
    };

    let relation_name = canonicalized
        .surface_binding
        .descriptor
        .public_name
        .to_ascii_lowercase();
    let mut pinned_schema_keys = BTreeSet::new();
    if let Some(schema_key) = canonicalized
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        pinned_schema_keys.insert(schema_key);
    }

    let mut collector = DependencyCollector {
        params: &canonicalized.bound_statement.bound_parameters,
        placeholder_state: PlaceholderState::new(),
        relation_name: relation_name.clone(),
        pinned_schema_keys: pinned_schema_keys.clone(),
        spec: DependencySpec {
            relations: [relation_name].into_iter().collect(),
            schema_keys: pinned_schema_keys,
            ..DependencySpec::default()
        },
        query_count: 0,
        has_cte: false,
        has_derived_tables: false,
        has_expression_subqueries: false,
    };
    if let ControlFlow::Break(()) = query.visit(&mut collector) {
        return None;
    }
    if !collector.is_safe_day_one_shape() {
        return None;
    }

    let mut spec = collector.spec;
    if scan.version_scope == VersionScope::ActiveVersion {
        spec.depends_on_active_version = true;
        spec.schema_keys.insert("lix_active_version".to_string());
        spec.entity_ids.clear();
        spec.file_ids.clear();
        spec.version_ids.clear();
    }

    Some(spec)
}

fn canonical_state_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql2::planner::ir::CanonicalStateScan> {
    match read_plan {
        ReadPlan::Scan(scan) => Some(scan),
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_state_scan(input),
    }
}

struct DependencyCollector<'a> {
    params: &'a [Value],
    placeholder_state: PlaceholderState,
    relation_name: String,
    pinned_schema_keys: BTreeSet<String>,
    spec: DependencySpec,
    query_count: usize,
    has_cte: bool,
    has_derived_tables: bool,
    has_expression_subqueries: bool,
}

impl DependencyCollector<'_> {
    fn is_safe_day_one_shape(&self) -> bool {
        self.query_count == 1
            && !self.has_cte
            && !self.has_derived_tables
            && !self.has_expression_subqueries
            && self.spec.relations.len() == 1
            && self.spec.relations.contains(&self.relation_name)
    }

    fn mark_conservative(&mut self) {
        self.spec.precision = DependencyPrecision::Conservative;
        self.spec.schema_keys = self.pinned_schema_keys.clone();
        self.spec.entity_ids.clear();
        self.spec.file_ids.clear();
        self.spec.version_ids.clear();
    }
}

impl Visitor for DependencyCollector<'_> {
    type Break = ();

    fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
        self.query_count += 1;
        if query.with.is_some() {
            self.has_cte = true;
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_table_factor(&mut self, table_factor: &TableFactor) -> ControlFlow<Self::Break> {
        match table_factor {
            TableFactor::Table { name, .. } => {
                if let Some(identifier) = name
                    .0
                    .last()
                    .and_then(sqlparser::ast::ObjectNamePart::as_ident)
                {
                    self.spec
                        .relations
                        .insert(identifier.value.to_ascii_lowercase());
                }
            }
            TableFactor::Derived { .. } => {
                self.has_derived_tables = true;
            }
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
        match expr {
            Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                self.has_expression_subqueries = true;
            }
            _ => {}
        }

        if self.spec.precision == DependencyPrecision::Conservative {
            return ControlFlow::Continue(());
        }
        if expr_is_non_representable_for_commit_filter(expr) {
            self.mark_conservative();
            return ControlFlow::Continue(());
        }

        let mut local_state = self.placeholder_state;
        collect_literal_filters_from_expr(expr, self.params, &mut local_state, &mut self.spec);
        ControlFlow::Continue(())
    }

    fn pre_visit_value(&mut self, value: &SqlValue) -> ControlFlow<Self::Break> {
        let SqlValue::Placeholder(token) = value else {
            return ControlFlow::Continue(());
        };
        if resolve_placeholder_index(token, self.params.len(), &mut self.placeholder_state).is_err()
        {
            self.mark_conservative();
        }
        ControlFlow::Continue(())
    }
}

fn expr_is_non_representable_for_commit_filter(expr: &Expr) -> bool {
    match expr {
        Expr::BinaryOp { left, op, right } => {
            matches!(op, BinaryOperator::Or | BinaryOperator::Xor)
                && (expr_contains_filter_column(left) || expr_contains_filter_column(right))
        }
        Expr::UnaryOp {
            op: UnaryOperator::Not,
            expr,
        } => expr_contains_filter_column(expr),
        Expr::InList {
            expr,
            negated: true,
            ..
        } => expr_contains_filter_column(expr),
        _ => false,
    }
}

fn expr_contains_filter_column(expr: &Expr) -> bool {
    struct Collector {
        found: bool,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if extract_filter_column(expr).is_some() {
                self.found = true;
                return ControlFlow::Break(());
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector { found: false };
    let _ = expr.visit(&mut collector);
    collector.found
}

fn collect_literal_filters_from_expr(
    expr: &Expr,
    params: &[Value],
    state: &mut PlaceholderState,
    spec: &mut DependencySpec,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let Some(column) = extract_filter_column(left) {
                if let Some(value) = extract_filter_literal(right, params, state) {
                    add_filter_literal(column, value, spec);
                }
            }
            if let Some(column) = extract_filter_column(right) {
                if let Some(value) = extract_filter_literal(left, params, state) {
                    add_filter_literal(column, value, spec);
                }
            }
        }
        Expr::InList {
            expr,
            list,
            negated: false,
        } => {
            if let Some(column) = extract_filter_column(expr) {
                for item in list {
                    if let Some(value) = extract_filter_literal(item, params, state) {
                        add_filter_literal(column, value, spec);
                    }
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Copy)]
enum FilterColumn {
    SchemaKey,
    EntityId,
    FileId,
    VersionId,
}

fn extract_filter_column(expr: &Expr) -> Option<FilterColumn> {
    let column = match expr {
        Expr::Identifier(identifier) => Some(identifier.value.as_str()),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|identifier| identifier.value.as_str()),
        Expr::Nested(inner) => return extract_filter_column(inner),
        _ => None,
    }?;

    match column.to_ascii_lowercase().as_str() {
        "schema_key" => Some(FilterColumn::SchemaKey),
        "entity_id" => Some(FilterColumn::EntityId),
        "file_id" => Some(FilterColumn::FileId),
        "version_id" | "lixcol_version_id" => Some(FilterColumn::VersionId),
        _ => None,
    }
}

fn extract_filter_literal(
    expr: &Expr,
    params: &[Value],
    state: &mut PlaceholderState,
) -> Option<String> {
    match expr {
        Expr::Value(value) => extract_sql_value_literal(&value.value, params, state),
        Expr::Nested(inner) => extract_filter_literal(inner, params, state),
        _ => None,
    }
}

fn extract_sql_value_literal(
    value: &SqlValue,
    params: &[Value],
    state: &mut PlaceholderState,
) -> Option<String> {
    if let Some(literal) = value.clone().into_string() {
        return Some(literal);
    }

    if let SqlValue::Placeholder(token) = value {
        let index = resolve_placeholder_index(token, params.len(), state).ok()?;
        return value_to_filter_literal(params.get(index)?);
    }

    None
}

fn value_to_filter_literal(value: &Value) -> Option<String> {
    match value {
        Value::Text(text) => Some(text.clone()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::Integer(number) => Some(number.to_string()),
        Value::Real(number) => Some(number.to_string()),
        Value::Null | Value::Blob(_) => None,
    }
}

fn add_filter_literal(column: FilterColumn, value: String, spec: &mut DependencySpec) {
    match column {
        FilterColumn::SchemaKey => {
            spec.schema_keys.insert(value);
        }
        FilterColumn::EntityId => {
            spec.entity_ids.insert(value);
        }
        FilterColumn::FileId => {
            spec.file_ids.insert(value);
        }
        FilterColumn::VersionId => {
            spec.version_ids.insert(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::derive_dependency_spec_from_canonicalized_read;
    use crate::sql2::catalog::SurfaceRegistry;
    use crate::sql2::core::contracts::{BoundStatement, ExecutionContext};
    use crate::sql2::core::parser::parse_sql_script;
    use crate::sql2::planner::canonicalize::canonicalize_read;
    use crate::sql_shared::dependency_spec::DependencyPrecision;
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
    fn derives_fixed_schema_dependency_for_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalized_read(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
            Vec::new(),
        );

        let spec = derive_dependency_spec_from_canonicalized_read(&canonicalized)
            .expect("entity read should derive dependency spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert!(spec.depends_on_active_version);
        assert_eq!(
            spec.schema_keys.into_iter().collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_key_value".to_string()
            ]
        );
    }

    #[test]
    fn derives_version_filter_from_placeholder_on_explicit_version_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalized_read(
            &registry,
            "SELECT entity_id FROM lix_state_by_version WHERE version_id = ? AND schema_key = ?",
            vec![
                Value::Text("v1".to_string()),
                Value::Text("message".to_string()),
            ],
        );

        let spec = derive_dependency_spec_from_canonicalized_read(&canonicalized)
            .expect("explicit version read should derive dependency spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert!(!spec.depends_on_active_version);
        assert_eq!(
            spec.version_ids.into_iter().collect::<Vec<_>>(),
            vec!["v1".to_string()]
        );
        assert_eq!(
            spec.schema_keys.into_iter().collect::<Vec<_>>(),
            vec!["message".to_string()]
        );
    }

    #[test]
    fn marks_non_representable_filter_shapes_as_conservative() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let canonicalized = canonicalized_read(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'a' OR entity_id = 'b'",
            Vec::new(),
        );

        let spec = derive_dependency_spec_from_canonicalized_read(&canonicalized)
            .expect("state read should derive dependency spec");

        assert_eq!(spec.precision, DependencyPrecision::Conservative);
        assert_eq!(
            spec.schema_keys.into_iter().collect::<Vec<_>>(),
            vec!["lix_active_version".to_string()]
        );
        assert!(spec.entity_ids.is_empty());
    }
}
