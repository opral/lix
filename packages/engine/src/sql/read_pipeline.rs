use std::collections::BTreeSet;
use std::ops::ControlFlow;

use sqlparser::ast::{ObjectNamePart, Query, TableFactor, Visit, Visitor};

use crate::sql::entity_views::read as entity_view_read;
use crate::sql::steps::{
    filesystem_step, lix_active_account_view_read, lix_active_version_view_read,
    lix_state_by_version_view_read, lix_state_history_view_read, lix_state_view_read,
    lix_version_view_read, vtable_read,
};
use crate::sql::{
    object_name_matches, rewrite_query_with_select_rewriter, rewrite_table_factors_in_select,
};
use crate::{LixBackend, LixError, Value};

const MAX_READ_REWRITE_PASSES: usize = 32;
const LOGICAL_READ_VIEW_NAMES: &[&str] = &[
    "lix_active_account",
    "lix_active_version",
    "lix_state",
    "lix_state_by_version",
    "lix_state_history",
    "lix_version",
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];

pub(crate) fn rewrite_read_query(query: Query) -> Result<Query, LixError> {
    let mut current = query;
    for _ in 0..MAX_READ_REWRITE_PASSES {
        let relation_names = collect_relation_names(&current);
        let mut changed = false;
        if references_any(&relation_names, FILESYSTEM_VIEW_NAMES) {
            current = apply_sync_rule(current, &mut changed, filesystem_step::rewrite_query)?;
        }
        if references_entity_views(&relation_names) {
            current = apply_sync_rule(current, &mut changed, entity_view_read::rewrite_query)?;
        }
        if references_relation(&relation_names, "lix_version") {
            current = apply_sync_rule(current, &mut changed, lix_version_view_read::rewrite_query)?;
        }
        if references_relation(&relation_names, "lix_active_account") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_active_account_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_active_version") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_active_version_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_state_by_version") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_state_by_version_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_state") {
            current = apply_sync_rule(current, &mut changed, lix_state_view_read::rewrite_query)?;
        }
        if references_relation(&relation_names, "lix_state_history") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_state_history_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_internal_state_vtable") {
            current = apply_sync_rule(current, &mut changed, vtable_read::rewrite_query)?;
        }
        if !changed {
            validate_no_unresolved_logical_read_views(&current)?;
            return Ok(current);
        }
    }
    Err(LixError {
        message: "read rewrite exceeded maximum pass count".to_string(),
    })
}

pub(crate) async fn rewrite_read_query_with_backend_and_params(
    backend: &dyn LixBackend,
    query: Query,
    params: &[Value],
) -> Result<Query, LixError> {
    let mut current = query;
    for _ in 0..MAX_READ_REWRITE_PASSES {
        let relation_names = collect_relation_names(&current);
        let mut changed = false;
        if references_any(&relation_names, FILESYSTEM_VIEW_NAMES) {
            current = apply_sync_rule(current, &mut changed, |query| {
                filesystem_step::rewrite_query_with_params(query, params)
            })?;
        }
        if references_entity_views(&relation_names) {
            current = apply_async_rule(current, &mut changed, |query| async move {
                entity_view_read::rewrite_query_with_backend(backend, query).await
            })
            .await?;
        }
        if references_relation(&relation_names, "lix_version") {
            current = apply_sync_rule(current, &mut changed, lix_version_view_read::rewrite_query)?;
        }
        if references_relation(&relation_names, "lix_active_account") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_active_account_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_active_version") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_active_version_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_state_by_version") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_state_by_version_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_state") {
            current = apply_sync_rule(current, &mut changed, lix_state_view_read::rewrite_query)?;
        }
        if references_relation(&relation_names, "lix_state_history") {
            current = apply_sync_rule(
                current,
                &mut changed,
                lix_state_history_view_read::rewrite_query,
            )?;
        }
        if references_relation(&relation_names, "lix_internal_state_vtable") {
            current = apply_async_rule(current, &mut changed, |query| async move {
                vtable_read::rewrite_query_with_backend(backend, query).await
            })
            .await?;
        }
        if !changed {
            validate_no_unresolved_logical_read_views(&current)?;
            return Ok(current);
        }
    }
    Err(LixError {
        message: "read rewrite with backend exceeded maximum pass count".to_string(),
    })
}

pub(crate) async fn rewrite_read_query_with_backend(
    backend: &dyn LixBackend,
    query: Query,
) -> Result<Query, LixError> {
    rewrite_read_query_with_backend_and_params(backend, query, &[]).await
}

fn apply_sync_rule(
    query: Query,
    changed: &mut bool,
    rule: impl FnOnce(Query) -> Result<Option<Query>, LixError>,
) -> Result<Query, LixError> {
    if let Some(rewritten) = rule(query.clone())? {
        *changed = true;
        Ok(rewritten)
    } else {
        Ok(query)
    }
}

async fn apply_async_rule<F, Fut>(
    query: Query,
    changed: &mut bool,
    rule: F,
) -> Result<Query, LixError>
where
    F: FnOnce(Query) -> Fut,
    Fut: std::future::Future<Output = Result<Option<Query>, LixError>>,
{
    if let Some(rewritten) = rule(query.clone()).await? {
        *changed = true;
        Ok(rewritten)
    } else {
        Ok(query)
    }
}

const FILESYSTEM_VIEW_NAMES: &[&str] = &[
    "lix_file",
    "lix_file_by_version",
    "lix_file_history",
    "lix_directory",
    "lix_directory_by_version",
    "lix_directory_history",
];

fn references_relation(relation_names: &BTreeSet<String>, name: &str) -> bool {
    relation_names.contains(name)
}

fn references_any(relation_names: &BTreeSet<String>, names: &[&str]) -> bool {
    names.iter().any(|name| references_relation(relation_names, name))
}

fn references_entity_views(relation_names: &BTreeSet<String>) -> bool {
    relation_names
        .iter()
        .any(|name| !is_physical_internal_relation(name))
}

fn is_physical_internal_relation(name: &str) -> bool {
    name == "lix_internal_state_vtable"
        || name == "lix_internal_state_untracked"
        || name.starts_with("lix_internal_state_materialized_v1_")
}

fn collect_relation_names(query: &Query) -> BTreeSet<String> {
    struct Collector {
        names: BTreeSet<String>,
    }

    impl Visitor for Collector {
        type Break = ();

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            if let TableFactor::Table { name, .. } = table_factor {
                if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                    self.names.insert(identifier.value.to_ascii_lowercase());
                }
            }
            ControlFlow::Continue(())
        }
    }

    let mut collector = Collector {
        names: BTreeSet::new(),
    };
    let _ = query.visit(&mut collector);
    collector.names
}

fn validate_no_unresolved_logical_read_views(query: &Query) -> Result<(), LixError> {
    let mut unresolved = BTreeSet::new();
    let mut inspect_select =
        |select: &mut sqlparser::ast::Select, _changed: &mut bool| -> Result<(), LixError> {
            let mut ignored = false;
            rewrite_table_factors_in_select(
                select,
                &mut |relation, _changed| {
                    let sqlparser::ast::TableFactor::Table { name, .. } = relation else {
                        return Ok(());
                    };
                    for candidate in LOGICAL_READ_VIEW_NAMES {
                        if object_name_matches(name, candidate) {
                            unresolved.insert((*candidate).to_string());
                        }
                    }
                    Ok(())
                },
                &mut ignored,
            )?;
            Ok(())
        };
    let _ = rewrite_query_with_select_rewriter(query.clone(), &mut inspect_select)?;
    if unresolved.is_empty() {
        return Ok(());
    }
    Err(LixError {
        message: format!(
            "read rewrite left unresolved logical views: {}",
            unresolved.into_iter().collect::<Vec<_>>().join(", ")
        ),
    })
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::{rewrite_read_query, validate_no_unresolved_logical_read_views};

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            sqlparser::ast::Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn validator_rejects_unresolved_logical_views() {
        let query = parse_query("SELECT version_id FROM lix_active_version");
        let err =
            validate_no_unresolved_logical_read_views(&query).expect_err("validator should fail");
        assert!(err.message.contains("lix_active_version"));
    }

    #[test]
    fn rewrite_engine_rewrites_nested_lix_active_version() {
        let query = parse_query(
            "SELECT COUNT(*) \
             FROM lix_state_by_version \
             WHERE schema_key = 'bench_schema' \
               AND version_id IN (SELECT version_id FROM lix_active_version)",
        );
        let rewritten = rewrite_read_query(query).expect("rewrite should succeed");
        let sql = rewritten.to_string();
        assert!(!sql.contains("FROM lix_active_version"));
        assert!(sql.contains("lix_internal_state_vtable"));
    }
}
