use crate::session::contracts::SessionDependency;
use crate::sql::catalog::{SurfaceBinding, SurfaceFamily};
use crate::sql::logical_plan::public_ir::{
    CanonicalAdminKind, CanonicalAdminScan, CanonicalChangeScan, CanonicalFilesystemScan,
    CanonicalWorkingChangesScan, FilesystemKind, ReadPlan, StructuredPublicRead, VersionScope,
};
use crate::sql::logical_plan::{DependencyPrecision, DependencySpec};
use crate::sql::parser::placeholders::{resolve_placeholder_index, PlaceholderState};
use crate::Value;
use sqlparser::ast::{
    BinaryOperator, Expr, OrderByKind, SelectItem, UnaryOperator, Value as SqlValue, Visit, Visitor,
};
use std::collections::BTreeSet;
use std::ops::ControlFlow;

pub(crate) fn derive_dependency_spec_from_structured_public_read(
    structured_read: &StructuredPublicRead,
) -> Option<DependencySpec> {
    if let Some(change_scan) = canonical_change_scan(&structured_read.read_command.root) {
        return Some(with_public_surface_registry_dependency(
            dependency_spec_for_change_scan(&change_scan.binding),
        ));
    }
    if canonical_working_changes_scan(&structured_read.read_command.root).is_some() {
        return Some(with_public_surface_registry_dependency(
            dependency_spec_for_working_changes_scan(),
        ));
    }
    if let Some(filesystem_scan) = canonical_filesystem_scan(&structured_read.read_command.root) {
        return Some(with_public_surface_registry_dependency(
            dependency_spec_for_filesystem_scan(
                filesystem_scan.kind,
                &structured_read.surface_binding,
            ),
        ));
    }
    if let Some(admin_scan) = canonical_admin_scan(&structured_read.read_command.root) {
        return Some(with_public_surface_registry_dependency(
            dependency_spec_for_admin_scan(admin_scan.kind),
        ));
    }
    let Some(scan) = canonical_state_scan(&structured_read.read_command.root) else {
        return None;
    };
    if query_contains_expression_subqueries(&structured_read.query) {
        return None;
    }

    let relation_name = structured_read
        .surface_binding
        .descriptor
        .public_name
        .to_ascii_lowercase();
    let mut pinned_schema_keys = BTreeSet::new();
    if let Some(schema_key) = structured_read
        .surface_binding
        .implicit_overrides
        .fixed_schema_key
        .clone()
    {
        pinned_schema_keys.insert(schema_key);
    }

    let mut spec = DependencySpec {
        relations: [relation_name].into_iter().collect(),
        schema_keys: pinned_schema_keys.clone(),
        ..DependencySpec::default()
    };
    let mut placeholder_state = PlaceholderState::new();
    for predicate in &structured_read.query.selection_predicates {
        if expr_is_non_representable_for_commit_filter(predicate) {
            spec.precision = DependencyPrecision::Conservative;
            spec.schema_keys = pinned_schema_keys.clone();
            spec.entity_ids.clear();
            spec.file_ids.clear();
            spec.version_ids.clear();
            break;
        }
        collect_literal_filters_from_expr(
            predicate,
            &structured_read.bound_parameters,
            &mut placeholder_state,
            &mut spec,
        );
    }

    if state_like_surface_depends_on_active_version(&scan.binding, scan.version_scope) {
        spec.depends_on_active_version = true;
        spec.session_dependencies
            .insert(SessionDependency::ActiveVersion);
        spec.entity_ids.clear();
        spec.file_ids.clear();
        spec.version_ids.clear();
    }

    Some(with_public_surface_registry_dependency(spec))
}

fn query_contains_expression_subqueries(
    query: &crate::sql::logical_plan::public_ir::NormalizedPublicReadQuery,
) -> bool {
    query
        .selection_predicates
        .iter()
        .any(expr_contains_expression_subquery)
        || query
            .projection
            .iter()
            .any(select_item_contains_expression_subquery)
        || query
            .order_by
            .as_ref()
            .is_some_and(order_by_contains_expression_subquery)
}

fn order_by_contains_expression_subquery(order_by: &sqlparser::ast::OrderBy) -> bool {
    let OrderByKind::Expressions(expressions) = &order_by.kind else {
        return false;
    };
    expressions
        .iter()
        .any(|expression| expr_contains_expression_subquery(&expression.expr))
}

fn select_item_contains_expression_subquery(item: &SelectItem) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
            expr_contains_expression_subquery(expr)
        }
        SelectItem::Wildcard(_) | SelectItem::QualifiedWildcard(_, _) => false,
    }
}

fn expr_contains_expression_subquery(expr: &Expr) -> bool {
    struct Collector {
        found: bool,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if matches!(
                expr,
                Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. }
            ) {
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

pub(crate) fn derive_dependency_spec_from_bound_public_surface_bindings(
    bindings: &[SurfaceBinding],
) -> Option<DependencySpec> {
    let mut iter = bindings.iter();
    let first = iter.next()?;
    let mut merged = dependency_spec_for_bound_surface(first);
    for binding in iter {
        merged = merge_dependency_specs(merged, dependency_spec_for_bound_surface(binding));
    }
    merged.precision = DependencyPrecision::Conservative;
    merged.entity_ids.clear();
    merged.file_ids.clear();
    merged.version_ids.clear();
    Some(with_public_surface_registry_dependency(merged))
}

fn with_public_surface_registry_dependency(mut spec: DependencySpec) -> DependencySpec {
    spec.session_dependencies
        .insert(SessionDependency::PublicSurfaceRegistryGeneration);
    spec
}

fn dependency_spec_for_admin_scan(kind: CanonicalAdminKind) -> DependencySpec {
    let mut spec = DependencySpec::default();
    match kind {
        CanonicalAdminKind::Version => {
            spec.relations.insert("lix_version".to_string());
            spec.schema_keys
                .insert("lix_version_descriptor".to_string());
            spec.schema_keys.insert("lix_version_ref".to_string());
        }
    }
    spec
}

fn dependency_spec_for_bound_surface(binding: &SurfaceBinding) -> DependencySpec {
    if let Some(admin_scan) = CanonicalAdminScan::from_surface_binding(binding.clone()) {
        return dependency_spec_for_admin_scan(admin_scan.kind);
    }
    if let Some(filesystem_scan) = CanonicalFilesystemScan::from_surface_binding(binding.clone()) {
        return dependency_spec_for_filesystem_scan(filesystem_scan.kind, binding);
    }
    if CanonicalWorkingChangesScan::from_surface_binding(binding.clone()).is_some() {
        return dependency_spec_for_working_changes_scan();
    }
    if CanonicalChangeScan::from_surface_binding(binding.clone()).is_some() {
        return dependency_spec_for_change_scan(binding);
    }

    dependency_spec_for_state_like_surface(binding)
}

fn dependency_spec_for_working_changes_scan() -> DependencySpec {
    DependencySpec {
        relations: ["lix_working_changes".to_string()].into_iter().collect(),
        precision: DependencyPrecision::Conservative,
        ..DependencySpec::default()
    }
}

fn dependency_spec_for_filesystem_scan(
    kind: FilesystemKind,
    binding: &crate::sql::catalog::SurfaceBinding,
) -> DependencySpec {
    let mut spec = DependencySpec {
        relations: [binding.descriptor.public_name.clone()]
            .into_iter()
            .collect(),
        ..DependencySpec::default()
    };

    match kind {
        FilesystemKind::File => {
            spec.schema_keys.insert("lix_file_descriptor".to_string());
            spec.schema_keys.insert("lix_binary_blob_ref".to_string());
            spec.schema_keys
                .insert("lix_directory_descriptor".to_string());
        }
        FilesystemKind::Directory => {
            spec.schema_keys
                .insert("lix_directory_descriptor".to_string());
        }
    }

    if matches!(
        binding.descriptor.public_name.as_str(),
        "lix_file" | "lix_directory" | "lix_file_history" | "lix_directory_history"
    ) {
        spec.depends_on_active_version = true;
        spec.session_dependencies
            .insert(SessionDependency::ActiveVersion);
    }

    spec.precision = DependencyPrecision::Conservative;
    spec
}

fn dependency_spec_for_change_scan(binding: &SurfaceBinding) -> DependencySpec {
    DependencySpec {
        relations: [binding.descriptor.public_name.clone()]
            .into_iter()
            .collect(),
        precision: DependencyPrecision::Conservative,
        ..DependencySpec::default()
    }
}

fn dependency_spec_for_state_like_surface(binding: &SurfaceBinding) -> DependencySpec {
    let mut spec = DependencySpec {
        relations: [binding.descriptor.public_name.clone()]
            .into_iter()
            .collect(),
        ..DependencySpec::default()
    };
    if matches!(
        binding.descriptor.surface_family,
        SurfaceFamily::State | SurfaceFamily::Entity
    ) {
        if let Some(schema_key) = binding.implicit_overrides.fixed_schema_key.clone() {
            spec.schema_keys.insert(schema_key);
        }
        let version_scope = match binding.default_scope {
            crate::sql::catalog::DefaultScopeSemantics::ActiveVersion => {
                Some(VersionScope::ActiveVersion)
            }
            crate::sql::catalog::DefaultScopeSemantics::ExplicitVersion => {
                Some(VersionScope::ExplicitVersion)
            }
            crate::sql::catalog::DefaultScopeSemantics::History => Some(VersionScope::History),
            crate::sql::catalog::DefaultScopeSemantics::GlobalAdmin
            | crate::sql::catalog::DefaultScopeSemantics::WorkingChanges => None,
        };
        if version_scope
            .is_some_and(|scope| state_like_surface_depends_on_active_version(binding, scope))
        {
            spec.depends_on_active_version = true;
            spec.session_dependencies
                .insert(SessionDependency::ActiveVersion);
        }
        spec.precision = DependencyPrecision::Conservative;
    }
    spec
}

fn state_like_surface_depends_on_active_version(
    binding: &SurfaceBinding,
    version_scope: VersionScope,
) -> bool {
    version_scope == VersionScope::ActiveVersion
        || (version_scope == VersionScope::History
            && !binding
                .descriptor
                .public_name
                .ends_with("_history_by_version"))
}

fn merge_dependency_specs(mut left: DependencySpec, right: DependencySpec) -> DependencySpec {
    left.relations.extend(right.relations);
    left.schema_keys.extend(right.schema_keys);
    left.entity_ids.extend(right.entity_ids);
    left.file_ids.extend(right.file_ids);
    left.version_ids.extend(right.version_ids);
    left.session_dependencies.extend(right.session_dependencies);
    left.writer_filter
        .include
        .extend(right.writer_filter.include);
    left.writer_filter
        .exclude
        .extend(right.writer_filter.exclude);
    left.include_untracked |= right.include_untracked;
    left.depends_on_active_version |= right.depends_on_active_version;
    if left.precision != right.precision {
        left.precision = DependencyPrecision::Conservative;
    }
    left
}

fn canonical_state_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalStateScan> {
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

fn canonical_filesystem_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalFilesystemScan> {
    match read_plan {
        ReadPlan::FilesystemScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_filesystem_scan(input),
    }
}

fn canonical_admin_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalAdminScan> {
    match read_plan {
        ReadPlan::AdminScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::ChangeScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_admin_scan(input),
    }
}

fn canonical_working_changes_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalWorkingChangesScan> {
    match read_plan {
        ReadPlan::WorkingChangesScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::ChangeScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_working_changes_scan(input),
    }
}

fn canonical_change_scan(
    read_plan: &ReadPlan,
) -> Option<&crate::sql::logical_plan::public_ir::CanonicalChangeScan> {
    match read_plan {
        ReadPlan::ChangeScan(scan) => Some(scan),
        ReadPlan::Scan(_)
        | ReadPlan::FilesystemScan(_)
        | ReadPlan::AdminScan(_)
        | ReadPlan::WorkingChangesScan(_) => None,
        ReadPlan::Filter { input, .. }
        | ReadPlan::Project { input, .. }
        | ReadPlan::Sort { input, .. }
        | ReadPlan::Limit { input, .. } => canonical_change_scan(input),
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
        Value::Json(value) => Some(value.to_string()),
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
    use super::derive_dependency_spec_from_structured_public_read;
    use crate::session::contracts::SessionDependency;
    use crate::sql::binder::bind_statement;
    use crate::sql::catalog::SurfaceRegistry;
    use crate::sql::logical_plan::public_ir::StructuredPublicRead;
    use crate::sql::logical_plan::DependencyPrecision;
    use crate::sql::semantic_ir::canonicalize::canonicalize_read;
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
    fn derives_fixed_schema_dependency_for_entity_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let structured_read = structured_read(
            &registry,
            "SELECT key, value FROM lix_key_value WHERE key = 'hello'",
            Vec::new(),
        );

        let spec = derive_dependency_spec_from_structured_public_read(&structured_read)
            .expect("entity read should derive dependency spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert!(spec.depends_on_active_version);
        assert!(spec
            .session_dependencies
            .contains(&SessionDependency::ActiveVersion));
        assert!(spec
            .session_dependencies
            .contains(&SessionDependency::PublicSurfaceRegistryGeneration));
        assert_eq!(
            spec.schema_keys.into_iter().collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
    }

    #[test]
    fn derives_version_filter_from_placeholder_on_explicit_version_surface() {
        let registry = SurfaceRegistry::with_builtin_surfaces();
        let structured_read = structured_read(
            &registry,
            "SELECT entity_id FROM lix_state_by_version WHERE version_id = ? AND schema_key = ?",
            vec![
                Value::Text("v1".to_string()),
                Value::Text("message".to_string()),
            ],
        );

        let spec = derive_dependency_spec_from_structured_public_read(&structured_read)
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
        let structured_read = structured_read(
            &registry,
            "SELECT entity_id FROM lix_state WHERE schema_key = 'a' OR entity_id = 'b'",
            Vec::new(),
        );

        let spec = derive_dependency_spec_from_structured_public_read(&structured_read)
            .expect("state read should derive dependency spec");

        assert_eq!(spec.precision, DependencyPrecision::Conservative);
        assert!(spec
            .session_dependencies
            .contains(&SessionDependency::ActiveVersion));
        assert!(spec.schema_keys.is_empty());
        assert!(spec.entity_ids.is_empty());
    }
}
