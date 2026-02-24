use sqlparser::ast::{Expr, Query, Select, SelectItem};

use crate::engine::sql::planning::rewrite_engine::{rewrite_query_selects, RewriteDecision};
use crate::LixError;

pub(crate) fn rewrite_query(query: Query) -> Result<Option<Query>, LixError> {
    rewrite_query_selects(query, &mut rewrite_select)
}

fn rewrite_select(select: &mut Select) -> Result<RewriteDecision, LixError> {
    let mut changed = false;
    for item in &mut select.projection {
        let SelectItem::ExprWithAlias { expr, alias } = item else {
            continue;
        };

        // Keep quoted/explicit aliases intact.
        if alias.quote_style.is_some() {
            continue;
        }

        let removable = match expr {
            Expr::Identifier(ident) => {
                ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case(&alias.value)
            }
            Expr::CompoundIdentifier(parts) => parts.last().is_some_and(|ident| {
                ident.quote_style.is_none() && ident.value.eq_ignore_ascii_case(&alias.value)
            }),
            _ => false,
        };

        if removable {
            *item = SelectItem::UnnamedExpr(expr.clone());
            changed = true;
        }
    }

    if changed {
        Ok(RewriteDecision::Changed)
    } else {
        Ok(RewriteDecision::Unchanged)
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use super::rewrite_query;

    fn parse_query(sql: &str) -> sqlparser::ast::Query {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        match statements.remove(0) {
            Statement::Query(query) => *query,
            other => panic!("expected query, got {other:?}"),
        }
    }

    #[test]
    fn removes_redundant_alias_for_simple_identifier_projection() {
        let query = parse_query("SELECT entity_id AS entity_id FROM lix_state");

        let rewritten = rewrite_query(query)
            .expect("projection cleanup should succeed")
            .expect("projection cleanup should rewrite");

        assert_eq!(rewritten.to_string(), "SELECT entity_id FROM lix_state");
    }

    #[test]
    fn keeps_quoted_aliases() {
        let query = parse_query("SELECT entity_id AS \"Entity_Id\" FROM lix_state");

        let rewritten = rewrite_query(query).expect("projection cleanup should succeed");
        assert!(rewritten.is_none());
    }
}
