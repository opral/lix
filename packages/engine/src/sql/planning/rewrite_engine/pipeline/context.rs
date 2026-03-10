use std::collections::BTreeSet;

use sqlparser::ast::Query;

use super::walker::{walk_query, QueryWalkSummary};

#[derive(Debug, Clone)]
pub(crate) struct AnalysisContext {
    relation_names: BTreeSet<String>,
    has_cte: bool,
    has_derived_tables: bool,
    has_expression_subqueries: bool,
    materialized_schema_keys_cache: Option<Vec<String>>,
}

impl AnalysisContext {
    pub(crate) fn from_query(query: &Query) -> Self {
        let mut context = Self {
            relation_names: BTreeSet::new(),
            has_cte: false,
            has_derived_tables: false,
            has_expression_subqueries: false,
            materialized_schema_keys_cache: None,
        };
        context.refresh_from_query(query);
        context
    }

    pub(crate) fn refresh_from_query(&mut self, query: &Query) {
        let summary: QueryWalkSummary = walk_query(query);
        self.relation_names = summary.relation_names;
        self.has_cte = summary.has_cte;
        self.has_derived_tables = summary.has_derived_tables;
        self.has_expression_subqueries = summary.has_expression_subqueries;
    }

    pub(crate) fn references_relation(&self, name: &str) -> bool {
        self.relation_names.contains(name)
    }

    pub(crate) fn has_nested_query_shapes(&self) -> bool {
        self.has_cte || self.has_derived_tables || self.has_expression_subqueries
    }

    pub(crate) fn materialized_schema_keys_cache(&self) -> Option<&[String]> {
        self.materialized_schema_keys_cache.as_deref()
    }

    pub(crate) fn set_materialized_schema_keys_cache(&mut self, keys: Vec<String>) {
        self.materialized_schema_keys_cache = Some(keys);
    }
}
