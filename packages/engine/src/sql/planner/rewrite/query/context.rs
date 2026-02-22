use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{Expr, ObjectNamePart, Query, TableFactor, Visit, Visitor};

const FILESYSTEM_VIEW_NAMES: &[&str] = &[
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];

#[derive(Debug, Clone)]
pub(super) struct AnalysisContext {
    relation_names: BTreeSet<String>,
    has_cte: bool,
    has_derived_tables: bool,
    has_expression_subqueries: bool,
}

impl AnalysisContext {
    pub(super) fn from_query(query: &Query) -> Self {
        let mut context = Self {
            relation_names: BTreeSet::new(),
            has_cte: false,
            has_derived_tables: false,
            has_expression_subqueries: false,
        };
        context.refresh_from_query(query);
        context
    }

    pub(super) fn refresh_from_query(&mut self, query: &Query) {
        let summary: QueryWalkSummary = walk_query(query);
        self.relation_names = summary.relation_names;
        self.has_cte = summary.has_cte;
        self.has_derived_tables = summary.has_derived_tables;
        self.has_expression_subqueries = summary.has_expression_subqueries;
    }

    pub(super) fn references_relation(&self, name: &str) -> bool {
        self.relation_names.contains(name)
    }

    pub(super) fn references_any_filesystem_view(&self) -> bool {
        FILESYSTEM_VIEW_NAMES
            .iter()
            .any(|name| self.references_relation(name))
    }

    pub(super) fn references_entity_views(&self) -> bool {
        self.relation_names
            .iter()
            .any(|name| !is_physical_internal_relation(name) && !is_builtin_logical_relation(name))
    }

    pub(super) fn has_nested_query_shapes(&self) -> bool {
        self.has_cte || self.has_derived_tables || self.has_expression_subqueries
    }

    pub(super) fn references_any_logical_read_view(&self) -> bool {
        self.references_any_filesystem_view()
            || self.references_entity_views()
            || self.references_relation("lix_version")
            || self.references_relation("lix_active_version")
            || self.references_relation("lix_active_account")
            || self.references_relation("lix_state")
            || self.references_relation("lix_state_by_version")
            || self.references_relation("lix_state_history")
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct QueryWalkSummary {
    pub(super) relation_names: BTreeSet<String>,
    pub(super) has_cte: bool,
    pub(super) has_derived_tables: bool,
    pub(super) has_expression_subqueries: bool,
}

pub(super) fn walk_query(query: &Query) -> QueryWalkSummary {
    struct Collector {
        summary: QueryWalkSummary,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_query(&mut self, query: &Query) -> ControlFlow<Self::Break> {
            if query.with.is_some() {
                self.summary.has_cte = true;
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            match table_factor {
                TableFactor::Table { name, .. } => {
                    if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                        self.summary
                            .relation_names
                            .insert(identifier.value.to_ascii_lowercase());
                    }
                }
                TableFactor::Derived { .. } => {
                    self.summary.has_derived_tables = true;
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_expr(&mut self, expr: &Expr) -> ControlFlow<Self::Break> {
            match expr {
                Expr::Subquery(_) | Expr::Exists { .. } | Expr::InSubquery { .. } => {
                    self.summary.has_expression_subqueries = true;
                }
                _ => {}
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        summary: QueryWalkSummary {
            relation_names: BTreeSet::new(),
            has_cte: false,
            has_derived_tables: false,
            has_expression_subqueries: false,
        },
    };
    let _ = query.visit(&mut collector);
    collector.summary
}

fn is_physical_internal_relation(name: &str) -> bool {
    name == "lix_internal_state_vtable"
        || name == "lix_internal_state_untracked"
        || name.starts_with("lix_internal_state_materialized_v1_")
}

fn is_builtin_logical_relation(name: &str) -> bool {
    name == "lix_version"
        || name == "lix_active_version"
        || name == "lix_active_account"
        || name == "lix_state"
        || name == "lix_state_by_version"
        || name == "lix_state_history"
        || FILESYSTEM_VIEW_NAMES.contains(&name)
}
