use crate::engine::sql::ast::utils::{bind_sql_with_state, parse_sql_statements, PlaceholderState};
use crate::engine::{Engine, ExecuteOptions};
use crate::state_commit_stream::{StateCommitStream, StateCommitStreamFilter};
use crate::{LixError, QueryResult, SqlDialect, Value};
use serde::{Deserialize, Serialize};
use sqlparser::ast::{
    BinaryOperator, Expr, ObjectNamePart, Query, Statement, TableFactor, UnaryOperator,
    Value as SqlValue,
};
use sqlparser::ast::{Visit, Visitor};
use std::collections::BTreeSet;
use std::ops::ControlFlow;
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveQuery {
    pub sql: String,
    pub params: Vec<Value>,
}

impl ObserveQuery {
    pub fn new(sql: impl Into<String>, params: Vec<Value>) -> Self {
        Self {
            sql: sql.into(),
            params,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ObserveEvent {
    pub sequence: u64,
    pub rows: QueryResult,
    pub state_commit_sequence: Option<u64>,
}

pub struct ObserveEvents<'a> {
    engine: &'a Engine,
    state: ObserveState,
}

pub struct ObserveEventsOwned {
    engine: Arc<Engine>,
    state: ObserveState,
}

struct ObserveState {
    query: ObserveQuery,
    state_commits: StateCommitStream,
    last_result: Option<QueryResult>,
    emitted_initial: bool,
    next_sequence: u64,
    closed: bool,
}

impl ObserveEvents<'_> {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_engine(self.engine).await
    }

    pub fn close(&mut self) {
        self.state.close();
    }
}

impl Drop for ObserveEvents<'_> {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveEventsOwned {
    pub async fn next(&mut self) -> Result<Option<ObserveEvent>, LixError> {
        self.state.next_with_engine(self.engine.as_ref()).await
    }

    pub fn close(&mut self) {
        self.state.close();
    }
}

impl Drop for ObserveEventsOwned {
    fn drop(&mut self) {
        self.close();
    }
}

impl ObserveState {
    async fn next_with_engine(
        &mut self,
        engine: &Engine,
    ) -> Result<Option<ObserveEvent>, LixError> {
        if self.closed {
            return Ok(None);
        }

        if !self.emitted_initial {
            self.emitted_initial = true;
            let rows = execute_observe_query(engine, &self.query).await?;
            self.last_result = Some(rows.clone());
            return Ok(Some(self.make_event(rows, None)));
        }

        loop {
            let Some(batch) = self.state_commits.next().await else {
                self.closed = true;
                return Ok(None);
            };

            let rows = execute_observe_query(engine, &self.query).await?;

            if self
                .last_result
                .as_ref()
                .is_some_and(|previous| *previous == rows)
            {
                continue;
            }

            self.last_result = Some(rows.clone());
            return Ok(Some(self.make_event(rows, Some(batch.sequence))));
        }
    }

    fn make_event(
        &mut self,
        rows: QueryResult,
        state_commit_sequence: Option<u64>,
    ) -> ObserveEvent {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.saturating_add(1);
        ObserveEvent {
            sequence,
            rows,
            state_commit_sequence,
        }
    }

    fn close(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        self.state_commits.close();
    }
}

async fn execute_observe_query(
    engine: &Engine,
    query: &ObserveQuery,
) -> Result<QueryResult, LixError> {
    engine
        .execute(&query.sql, &query.params, ExecuteOptions::default())
        .await
}

impl Engine {
    pub fn observe(&self, query: ObserveQuery) -> Result<ObserveEvents<'_>, LixError> {
        let state = build_observe_state(self, query)?;
        Ok(ObserveEvents {
            engine: self,
            state,
        })
    }
}

pub fn observe_owned(
    engine: Arc<Engine>,
    query: ObserveQuery,
) -> Result<ObserveEventsOwned, LixError> {
    let state = build_observe_state(engine.as_ref(), query)?;
    Ok(ObserveEventsOwned { engine, state })
}

fn build_observe_state(engine: &Engine, query: ObserveQuery) -> Result<ObserveState, LixError> {
    let statements = parse_sql_statements(&query.sql)?;
    if statements.is_empty()
        || !statements
            .iter()
            .all(|statement| matches!(statement, Statement::Query(_)))
    {
        return Err(LixError {
            message: "observe requires one or more SELECT statements".to_string(),
        });
    }

    let filter = derive_state_commit_stream_filter(&statements, &query.params)?;
    let state_commits = engine.state_commit_stream(filter);

    Ok(ObserveState {
        query,
        state_commits,
        last_result: None,
        emitted_initial: false,
        next_sequence: 0,
        closed: false,
    })
}

#[derive(Default)]
struct DerivedObserveFilter {
    relations: BTreeSet<String>,
    schema_keys: BTreeSet<String>,
    entity_ids: BTreeSet<String>,
    file_ids: BTreeSet<String>,
    version_ids: BTreeSet<String>,
}

fn derive_state_commit_stream_filter(
    statements: &[Statement],
    params: &[Value],
) -> Result<StateCommitStreamFilter, LixError> {
    let mut derived = DerivedObserveFilter::default();
    let mut placeholder_state = PlaceholderState::new();
    // One conjunctive filter cannot represent OR across independent statements.
    let mut allow_literal_filters = statements.len() == 1;

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
            continue;
        }
        let rebound_statement = rebound_statements.remove(0);
        let Statement::Query(query) = rebound_statement else {
            continue;
        };

        collect_relation_names_from_query(&query, &mut derived.relations);
        if allow_literal_filters {
            let representable =
                collect_literal_filters_from_query(&query, &bound.params, &mut derived);
            if !representable {
                allow_literal_filters = false;
                derived.schema_keys.clear();
                derived.entity_ids.clear();
                derived.file_ids.clear();
                derived.version_ids.clear();
            }
        }
    }

    Ok(state_commit_filter_from_derived(derived))
}

fn state_commit_filter_from_derived(derived: DerivedObserveFilter) -> StateCommitStreamFilter {
    let mut schema_keys = BTreeSet::new();
    let mut uses_dynamic_state_relations = false;

    for relation in &derived.relations {
        match relation.as_str() {
            "lix_state"
            | "lix_state_by_version"
            | "lix_state_history"
            | "lix_working_changes"
            | "lix_internal_state_vtable"
            | "lix_internal_state_untracked" => {
                uses_dynamic_state_relations = true;
            }
            "lix_file" | "lix_file_by_version" | "lix_file_history" => {
                schema_keys.insert("lix_file_descriptor".to_string());
            }
            "lix_directory" | "lix_directory_by_version" | "lix_directory_history" => {
                schema_keys.insert("lix_directory_descriptor".to_string());
            }
            "lix_version" | "lix_version_by_version" => {
                schema_keys.insert("lix_version_descriptor".to_string());
                schema_keys.insert("lix_version_tip".to_string());
            }
            "lix_active_version" => {
                schema_keys.insert("lix_active_version".to_string());
            }
            "lix_active_account" => {
                schema_keys.insert("lix_active_account".to_string());
            }
            "lix_change" => {
                schema_keys.insert("lix_change".to_string());
            }
            _ => {
                if relation.starts_with("lix_") && !relation.starts_with("lix_internal_") {
                    schema_keys.insert(normalize_relation_schema_key(relation));
                }
            }
        }
    }

    if uses_dynamic_state_relations {
        schema_keys.extend(derived.schema_keys);
    }

    StateCommitStreamFilter {
        schema_keys: schema_keys.into_iter().collect(),
        entity_ids: derived.entity_ids.into_iter().collect(),
        file_ids: derived.file_ids.into_iter().collect(),
        version_ids: derived.version_ids.into_iter().collect(),
        ..StateCommitStreamFilter::default()
    }
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
    out: &mut DerivedObserveFilter,
) -> bool {
    struct Collector<'a> {
        params: &'a [Value],
        out: &'a mut DerivedObserveFilter,
        representable: bool,
    }

    impl Visitor for Collector<'_> {
        type Break = ();

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            if expr_is_non_representable_for_commit_filter(expr) {
                self.representable = false;
                return ControlFlow::Break(());
            }
            collect_literal_filters_from_expr(expr, self.params, self.out);
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        params,
        out,
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
    out: &mut DerivedObserveFilter,
) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::Eq,
            right,
        } => {
            if let Some(column) = extract_filter_column(left) {
                if let Some(value) = extract_filter_literal(right, params) {
                    add_filter_literal(out, column, value);
                }
            }
            if let Some(column) = extract_filter_column(right) {
                if let Some(value) = extract_filter_literal(left, params) {
                    add_filter_literal(out, column, value);
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
                        add_filter_literal(out, column, value);
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

fn add_filter_literal(out: &mut DerivedObserveFilter, column: FilterColumn, value: String) {
    match column {
        FilterColumn::SchemaKey => {
            out.schema_keys.insert(value);
        }
        FilterColumn::EntityId => {
            out.entity_ids.insert(value);
        }
        FilterColumn::FileId => {
            out.file_ids.insert(value);
        }
        FilterColumn::VersionId => {
            out.version_ids.insert(value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{derive_state_commit_stream_filter, parse_sql_statements};
    use crate::Value;

    #[test]
    fn derive_filter_extracts_schema_entity_and_version_literals() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state \
             WHERE schema_key = $1 AND entity_id IN ($2, 'entity-b') AND version_id = 'v-1'",
        )
        .expect("parse sql");

        let filter = derive_state_commit_stream_filter(
            &statements,
            &[
                Value::Text("lix_key_value".to_string()),
                Value::Text("entity-a".to_string()),
            ],
        )
        .expect("derive filter");

        assert_eq!(filter.schema_keys, vec!["lix_key_value".to_string()]);
        assert_eq!(
            filter.entity_ids,
            vec!["entity-a".to_string(), "entity-b".to_string()]
        );
        assert_eq!(filter.version_ids, vec!["v-1".to_string()]);
    }

    #[test]
    fn derive_filter_maps_file_reads_to_file_descriptor_schema() {
        let statements =
            parse_sql_statements("SELECT id, path FROM lix_file WHERE path = '/docs/a.md'")
                .expect("parse sql");

        let filter = derive_state_commit_stream_filter(&statements, &[]).expect("derive filter");
        assert_eq!(filter.schema_keys, vec!["lix_file_descriptor".to_string()]);
    }

    #[test]
    fn derive_filter_maps_direct_schema_view_reads() {
        let statements =
            parse_sql_statements("SELECT entity_id FROM lix_key_value LIMIT 1").expect("parse sql");

        let filter = derive_state_commit_stream_filter(&statements, &[]).expect("derive filter");
        assert_eq!(filter.schema_keys, vec!["lix_key_value".to_string()]);
    }

    #[test]
    fn derive_filter_maps_versioned_and_history_entity_views_to_base_schema_key() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_key_value_by_version WHERE key = 'a'; \
             SELECT entity_id FROM lix_key_value_history WHERE key = 'a'",
        )
        .expect("parse sql");

        let filter = derive_state_commit_stream_filter(&statements, &[]).expect("derive filter");
        assert_eq!(filter.schema_keys, vec!["lix_key_value".to_string()]);
    }

    #[test]
    fn derive_filter_falls_back_for_or_across_tracked_columns() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state \
             WHERE schema_key = 'a' OR entity_id = 'b'",
        )
        .expect("parse sql");

        let filter = derive_state_commit_stream_filter(&statements, &[]).expect("derive filter");
        assert!(filter.schema_keys.is_empty());
        assert!(filter.entity_ids.is_empty());
        assert!(filter.file_ids.is_empty());
        assert!(filter.version_ids.is_empty());
    }

    #[test]
    fn derive_filter_falls_back_for_multiple_statements() {
        let statements = parse_sql_statements(
            "SELECT entity_id FROM lix_state WHERE schema_key = 'a'; \
             SELECT entity_id FROM lix_state WHERE entity_id = 'b'",
        )
        .expect("parse sql");

        let filter = derive_state_commit_stream_filter(&statements, &[]).expect("derive filter");
        assert!(filter.schema_keys.is_empty());
        assert!(filter.entity_ids.is_empty());
        assert!(filter.file_ids.is_empty());
        assert!(filter.version_ids.is_empty());
    }
}
