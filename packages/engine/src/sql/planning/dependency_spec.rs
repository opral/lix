use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{
    BinaryOperator, Expr, ObjectNamePart, Query, Statement, TableFactor, UnaryOperator,
    Value as SqlValue,
};
use sqlparser::ast::{Visit, Visitor};

use super::super::ast::utils::{bind_sql_with_state, parse_sql_statements, PlaceholderState};
use super::super::contracts::dependency_spec::{DependencyPrecision, DependencySpec};
use crate::state_commit_stream::StateCommitStreamFilter;
use crate::{LixError, SqlDialect, Value};

pub(crate) fn derive_dependency_spec_from_statements(
    statements: &[Statement],
    params: &[Value],
) -> Result<DependencySpec, LixError> {
    let mut spec = DependencySpec::default();
    let mut placeholder_state = PlaceholderState::new();
    let mut allow_literal_filters = statements.len() == 1;
    if statements.len() != 1 {
        spec.precision = DependencyPrecision::Conservative;
    }

    for statement in statements {
        let statement_sql = statement.to_string();
        let bound = bind_sql_with_state(
            &statement_sql,
            params,
            SqlDialect::Sqlite,
            placeholder_state,
        )?;
        placeholder_state = bound.state;

        let mut rebound_statements = parse_sql_statements(&bound.sql)?;
        if rebound_statements.len() != 1 {
            spec.precision = DependencyPrecision::Conservative;
            continue;
        }
        let rebound_statement = rebound_statements.remove(0);
        let Statement::Query(query) = rebound_statement else {
            continue;
        };

        collect_relation_names_from_query(&query, &mut spec.relations);
        if allow_literal_filters {
            let representable = collect_literal_filters_from_query(
                &query,
                &bound.params,
                &mut spec.schema_keys,
                &mut spec.entity_ids,
                &mut spec.file_ids,
                &mut spec.version_ids,
            );
            if !representable {
                allow_literal_filters = false;
                spec.precision = DependencyPrecision::Conservative;
                spec.schema_keys.clear();
                spec.entity_ids.clear();
                spec.file_ids.clear();
                spec.version_ids.clear();
            }
        }
    }

    Ok(finalize_dependency_spec(spec))
}

pub(crate) fn dependency_spec_to_state_commit_stream_filter(
    spec: &DependencySpec,
) -> StateCommitStreamFilter {
    StateCommitStreamFilter {
        schema_keys: spec.schema_keys.iter().cloned().collect(),
        entity_ids: spec.entity_ids.iter().cloned().collect(),
        file_ids: spec.file_ids.iter().cloned().collect(),
        version_ids: spec.version_ids.iter().cloned().collect(),
        writer_keys: spec.writer_filter.include.iter().cloned().collect(),
        exclude_writer_keys: spec.writer_filter.exclude.iter().cloned().collect(),
        include_untracked: spec.include_untracked,
    }
}

fn finalize_dependency_spec(mut spec: DependencySpec) -> DependencySpec {
    let mut compiled_schema_keys = BTreeSet::new();
    let mut uses_dynamic_state_relations = false;
    let mut depends_on_active_version = false;

    for relation in &spec.relations {
        match relation.as_str() {
            "lix_state" => {
                uses_dynamic_state_relations = true;
                depends_on_active_version = true;
            }
            "lix_state_by_version"
            | "lix_state_history"
            | "lix_state_history_by_version"
            | "lix_internal_state_vtable"
            | "lix_internal_state_untracked" => {
                uses_dynamic_state_relations = true;
            }
            "lix_working_changes" => {
                uses_dynamic_state_relations = true;
                depends_on_active_version = true;
            }
            "lix_file"
            | "lix_file_by_version"
            | "lix_file_history"
            | "lix_file_history_by_version" => {
                compiled_schema_keys.insert("lix_file_descriptor".to_string());
                if relation == "lix_file" {
                    depends_on_active_version = true;
                }
            }
            "lix_directory" | "lix_directory_by_version" | "lix_directory_history" => {
                compiled_schema_keys.insert("lix_directory_descriptor".to_string());
                if relation == "lix_directory" {
                    depends_on_active_version = true;
                }
            }
            "lix_version" | "lix_version_by_version" => {
                compiled_schema_keys.insert("lix_version_descriptor".to_string());
                compiled_schema_keys.insert("lix_version_tip".to_string());
            }
            "lix_active_version" => {
                compiled_schema_keys.insert("lix_active_version".to_string());
            }
            "lix_active_account" => {
                compiled_schema_keys.insert("lix_active_account".to_string());
            }
            "lix_change" => {
                compiled_schema_keys.insert("lix_change".to_string());
            }
            _ => {
                if relation.starts_with("lix_") && !relation.starts_with("lix_internal_") {
                    compiled_schema_keys.insert(normalize_relation_schema_key(relation));
                }
            }
        }
    }

    if uses_dynamic_state_relations {
        compiled_schema_keys.extend(spec.schema_keys.iter().cloned());
    }
    if depends_on_active_version {
        compiled_schema_keys.insert("lix_active_version".to_string());
        spec.entity_ids.clear();
        spec.file_ids.clear();
        spec.version_ids.clear();
    }

    spec.schema_keys = compiled_schema_keys;
    spec.depends_on_active_version = depends_on_active_version;
    spec
}

fn normalize_relation_schema_key(relation: &str) -> String {
    relation
        .strip_suffix("_by_version")
        .or_else(|| relation.strip_suffix("_history"))
        .filter(|base| !base.is_empty())
        .unwrap_or(relation)
        .to_string()
}

fn collect_relation_names_from_query(query: &Query, relation_names: &mut BTreeSet<String>) {
    struct Collector<'a> {
        relation_names: &'a mut BTreeSet<String>,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, .. } = table_factor {
                if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                    self.relation_names
                        .insert(identifier.value.to_ascii_lowercase());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector { relation_names };
    let _ = query.visit(&mut collector);
}

fn collect_literal_filters_from_query(
    query: &Query,
    params: &[Value],
    schema_keys: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
) -> bool {
    struct Collector<'a> {
        params: &'a [Value],
        schema_keys: &'a mut BTreeSet<String>,
        entity_ids: &'a mut BTreeSet<String>,
        file_ids: &'a mut BTreeSet<String>,
        version_ids: &'a mut BTreeSet<String>,
        representable: bool,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if expr_is_non_representable_for_commit_filter(expr) {
                self.representable = false;
                return ControlFlow::Break(());
            }
            collect_literal_filters_from_expr(
                expr,
                self.params,
                self.schema_keys,
                self.entity_ids,
                self.file_ids,
                self.version_ids,
            );
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        params,
        schema_keys,
        entity_ids,
        file_ids,
        version_ids,
        representable: true,
    };
    let _ = query.visit(&mut collector);
    collector.representable
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
    schema_keys: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let Some(column) = extract_filter_column(left) {
                if let Some(value) = extract_filter_literal(right, params) {
                    add_filter_literal(
                        column,
                        value,
                        schema_keys,
                        entity_ids,
                        file_ids,
                        version_ids,
                    );
                }
            }
            if let Some(column) = extract_filter_column(right) {
                if let Some(value) = extract_filter_literal(left, params) {
                    add_filter_literal(
                        column,
                        value,
                        schema_keys,
                        entity_ids,
                        file_ids,
                        version_ids,
                    );
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
                    if let Some(value) = extract_filter_literal(item, params) {
                        add_filter_literal(
                            column,
                            value,
                            schema_keys,
                            entity_ids,
                            file_ids,
                            version_ids,
                        );
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

fn extract_filter_literal(expr: &Expr, params: &[Value]) -> Option<String> {
    match expr {
        Expr::Value(value) => extract_sql_value_literal(&value.value, params),
        Expr::Nested(inner) => extract_filter_literal(inner, params),
        _ => None,
    }
}

fn extract_sql_value_literal(value: &SqlValue, params: &[Value]) -> Option<String> {
    if let Some(literal) = value.clone().into_string() {
        return Some(literal);
    }

    if let SqlValue::Placeholder(token) = value {
        return extract_placeholder_literal(token, params);
    }

    None
}

fn extract_placeholder_literal(token: &str, params: &[Value]) -> Option<String> {
    let trimmed = token.trim();
    let numeric = if let Some(rest) = trimmed.strip_prefix('?') {
        rest
    } else if let Some(rest) = trimmed.strip_prefix('$') {
        rest
    } else {
        return None;
    };
    let index = numeric.parse::<usize>().ok()?;
    if index == 0 {
        return None;
    }
    value_to_filter_literal(params.get(index - 1)?)
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

fn add_filter_literal(
    column: FilterColumn,
    value: String,
    schema_keys: &mut BTreeSet<String>,
    entity_ids: &mut BTreeSet<String>,
    file_ids: &mut BTreeSet<String>,
    version_ids: &mut BTreeSet<String>,
) {
    match column {
        FilterColumn::SchemaKey => {
            schema_keys.insert(value);
        }
        FilterColumn::EntityId => {
            entity_ids.insert(value);
        }
        FilterColumn::FileId => {
            file_ids.insert(value);
        }
        FilterColumn::VersionId => {
            version_ids.insert(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        dependency_spec_to_state_commit_stream_filter, derive_dependency_spec_from_statements,
        parse_sql_statements,
    };
    use crate::engine::sql::contracts::dependency_spec::DependencyPrecision;
    use crate::Value;

    #[test]
    fn derive_dependency_spec_extracts_state_dependency_and_active_version() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state WHERE schema_key = 'lix_key_value'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert!(spec.depends_on_active_version);
        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_key_value".to_string()
            ]
        );
        assert!(spec.entity_ids.is_empty());
    }

    #[test]
    fn derive_dependency_spec_marks_or_as_conservative() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state WHERE schema_key = 'a' OR entity_id = 'b'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(spec.precision, DependencyPrecision::Conservative);
        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec!["lix_active_version".to_string()]
        );
        assert!(spec.entity_ids.is_empty());
        assert!(spec.file_ids.is_empty());
        assert!(spec.version_ids.is_empty());
    }

    #[test]
    fn derive_dependency_spec_marks_multi_statement_as_conservative() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state WHERE schema_key = 'a'; \
             SELECT entity_id FROM lix_state WHERE entity_id = 'b'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");
        assert_eq!(spec.precision, DependencyPrecision::Conservative);
    }

    #[test]
    fn derive_dependency_spec_handles_join_and_subquery() {
        let statements = parse_sql_statements(
            "SELECT f.id \
             FROM lix_file f \
             JOIN (SELECT file_id FROM lix_state_by_version WHERE schema_key = 'lix_key_value') s \
             ON f.id = s.file_id",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");
        assert!(spec.relations.contains("lix_file"));
        assert!(spec.relations.contains("lix_state_by_version"));
        assert!(spec.schema_keys.contains("lix_file_descriptor"));
        assert!(spec.schema_keys.contains("lix_key_value"));
    }

    #[test]
    fn compile_dependency_spec_to_state_commit_filter() {
        let statements = parse_sql_statements(
            "SELECT id, path FROM lix_file_by_version WHERE lixcol_version_id = 'v-1'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");
        let filter = dependency_spec_to_state_commit_stream_filter(&spec);
        assert_eq!(filter.schema_keys, vec!["lix_file_descriptor".to_string()]);
        assert!(filter.entity_ids.is_empty());
    }

    #[test]
    fn derive_dependency_spec_maps_direct_schema_view_reads() {
        let statements =
            parse_sql_statements("SELECT entity_id FROM lix_key_value LIMIT 1").expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
        assert!(!spec.depends_on_active_version);
    }

    #[test]
    fn derive_dependency_spec_maps_file_reads_to_descriptor_and_active_version() {
        let statements =
            parse_sql_statements("SELECT id, path FROM lix_file WHERE path = '/docs/a.md'")
                .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_file_descriptor".to_string()
            ]
        );
        assert!(spec.depends_on_active_version);
    }

    #[test]
    fn derive_dependency_spec_does_not_add_active_version_for_file_by_version_reads() {
        let statements = parse_sql_statements(
            "SELECT id, path FROM lix_file_by_version WHERE lixcol_version_id = 'v-1'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec!["lix_file_descriptor".to_string()]
        );
        assert!(!spec.depends_on_active_version);
    }

    #[test]
    fn derive_dependency_spec_drops_entity_filters_for_active_state_queries() {
        let statements = parse_sql_statements(
            "SELECT snapshot_content \
             FROM lix_state \
             WHERE schema_key = 'lix_key_value' AND entity_id = 'entity-a'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec![
                "lix_active_version".to_string(),
                "lix_key_value".to_string()
            ]
        );
        assert!(spec.entity_ids.is_empty());
    }

    #[test]
    fn derive_dependency_spec_maps_versioned_and_history_views_to_base_schema_key() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_key_value_by_version WHERE key = 'a'; \
             SELECT entity_id FROM lix_key_value_history WHERE key = 'a'",
        )
        .expect("parse sql");
        let spec = derive_dependency_spec_from_statements(&statements, &[]).expect("derive spec");

        assert_eq!(spec.precision, DependencyPrecision::Conservative);
        assert_eq!(
            spec.schema_keys.iter().cloned().collect::<Vec<_>>(),
            vec!["lix_key_value".to_string()]
        );
    }

    #[test]
    fn derive_dependency_spec_resolves_placeholder_literals() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state \
             WHERE schema_key = $1 AND entity_id IN ($2, 'entity-b') AND version_id = 'v-1'",
        )
        .expect("parse sql");

        let spec = derive_dependency_spec_from_statements(
            &statements,
            &[
                Value::Text("lix_key_value".to_string()),
                Value::Text("entity-a".to_string()),
            ],
        )
        .expect("derive spec");
        assert_eq!(spec.precision, DependencyPrecision::Precise);
        assert!(spec.schema_keys.contains("lix_key_value"));
        assert!(spec.depends_on_active_version);
    }

    #[test]
    fn derive_dependency_spec_is_deterministic_for_same_sql_and_params() {
        let statements = parse_sql_statements(
            "SELECT entity_id \
             FROM lix_state \
             WHERE schema_key = $1 AND entity_id = $2",
        )
        .expect("parse sql");
        let params = [
            Value::Text("lix_key_value".to_string()),
            Value::Text("entity-a".to_string()),
        ];

        let a = derive_dependency_spec_from_statements(&statements, &params).expect("derive a");
        let b = derive_dependency_spec_from_statements(&statements, &params).expect("derive b");
        assert_eq!(a, b);
    }
}
