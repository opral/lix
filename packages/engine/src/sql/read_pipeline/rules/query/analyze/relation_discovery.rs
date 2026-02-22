use std::collections::BTreeSet;

use sqlparser::ast::{ObjectNamePart, Query, TableFactor};

use crate::sql::read_pipeline::walker::walk_query;
use crate::sql::{visit_query_selects, visit_table_factors_in_select};
use crate::LixError;

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

    Err(LixError {
        message: format!(
            "analyze phase relation discovery mismatch: walker missing {:?} (walker={:?} select_visit={:?})",
            missing_from_walker, walker_relations, select_visit_relations
        ),
    })
}

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
