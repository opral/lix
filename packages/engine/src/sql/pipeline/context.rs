use std::collections::BTreeSet;

use sqlparser::ast::Query;

use super::walker::{walk_query, QueryWalkSummary};

const FILESYSTEM_VIEW_NAMES: &[&str] = &[
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];

#[derive(Debug, Clone)]
pub(crate) struct AnalysisContext {
    relation_names: BTreeSet<String>,
    has_cte: bool,
    has_derived_tables: bool,
    has_expression_subqueries: bool,
}

impl AnalysisContext {
    pub(crate) fn from_query(query: &Query) -> Self {
        let summary: QueryWalkSummary = walk_query(query);
        Self {
            relation_names: summary.relation_names,
            has_cte: summary.has_cte,
            has_derived_tables: summary.has_derived_tables,
            has_expression_subqueries: summary.has_expression_subqueries,
        }
    }

    pub(crate) fn references_relation(&self, name: &str) -> bool {
        self.relation_names.contains(name)
    }

    pub(crate) fn references_any_filesystem_view(&self) -> bool {
        FILESYSTEM_VIEW_NAMES
            .iter()
            .any(|name| self.references_relation(name))
    }

    pub(crate) fn references_state_views(&self) -> bool {
        self.references_relation("lix_state") || self.references_relation("lix_state_by_version")
    }

    pub(crate) fn references_entity_views(&self) -> bool {
        self.relation_names
            .iter()
            .any(|name| !is_physical_internal_relation(name) && !is_builtin_logical_relation(name))
    }

    pub(crate) fn has_nested_query_shapes(&self) -> bool {
        self.has_cte || self.has_derived_tables || self.has_expression_subqueries
    }

    pub(crate) fn references_any_logical_read_view(&self) -> bool {
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
