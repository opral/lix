#[cfg(debug_assertions)]
use std::collections::BTreeSet;

use sqlparser::ast::Query;
#[cfg(debug_assertions)]
use sqlparser::ast::{ObjectNamePart, TableFactor};

#[cfg(debug_assertions)]
use crate::engine::sql::planning::rewrite_engine::{
    visit_query_selects, visit_table_factors_in_select,
};
use crate::LixError;

#[cfg(debug_assertions)]
use crate::engine::sql::planning::rewrite_engine::pipeline::walker::walk_query;

#[cfg(not(debug_assertions))]
pub(crate) fn validate_relation_discovery_consistency(_query: &Query) -> Result<(), LixError> {
    Ok(())
}

#[cfg(debug_assertions)]
pub(crate) fn validate_relation_discovery_consistency(query: &Query) -> Result<(), LixError> {
    let walker_relations = walk_query(query).relation_names;
    let select_visit_relations = collect_relation_names_via_select_visit(query)?;

    if select_visit_relations.is_subset(&walker_relations) {
        return Ok(());
    }

    let missing_from_walker: BTreeSet<_> = select_visit_relations
        .difference(&walker_relations)
        .cloned()
        .collect();

    Err(LixError { code: "LIX_ERROR_UNKNOWN".to_string(), title: "Unknown error".to_string(), description: format!(
            "analyze phase relation discovery mismatch: walker missing {:?} (walker={:?} select_visit={:?})",
            missing_from_walker, walker_relations, select_visit_relations
        ),
    })
}

#[cfg(debug_assertions)]
fn collect_relation_names_via_select_visit(query: &Query) -> Result<BTreeSet<String>, LixError> {
    let mut names = BTreeSet::new();
    visit_query_selects(query, &mut |select| {
        visit_table_factors_in_select(select, &mut |relation| {
            let TableFactor::Table { name, .. } = relation else {
                return Ok(());
            };
            if let Some(identifier) = name.0.last().and_then(ObjectNamePart::as_ident) {
                names.insert(identifier.value.to_ascii_lowercase());
            }
            Ok(())
        })
    })?;
    Ok(names)
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::validate_relation_discovery_consistency;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn relation_discovery_matches_across_cte_and_subqueries() {
        let query = parse_query(
            "WITH c AS (SELECT * FROM lix_state) \
             SELECT * FROM c WHERE EXISTS (SELECT 1 FROM lix_active_version)",
        );

        validate_relation_discovery_consistency(&query)
            .expect("analyze relation discovery should remain consistent");
    }
}
