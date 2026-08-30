use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{TableFactor, Visit, Visitor};

/// Execution ownership for a parsed public SQL statement.
///
/// Replica hot reads are the only statements that may execute against the
/// local serving plane. Mutations and historical reads belong to the
/// authority; keeping this classification beside the SQL parser avoids a
/// second, text-based dialect in clients.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatementAuthorityRoute {
    /// A certified current-state read which may execute on the hot replica.
    HotRead,
    /// A historical read which must execute on the authority.
    AuthorityRead,
    /// A mutation which must execute on the authority.
    AuthorityWrite,
}

pub(crate) fn statement_authority_route(
    statement: &DataFusionStatement,
) -> Result<StatementAuthorityRoute, crate::LixError> {
    if super::bind_statement_route(statement)? == super::BoundStatementRoute::Write {
        return Ok(StatementAuthorityRoute::AuthorityWrite);
    }
    if super::statement_has_durable_runtime_function(statement) {
        return Ok(StatementAuthorityRoute::AuthorityWrite);
    }

    struct AuthoritySurfaceVisitor;

    impl Visitor for AuthoritySurfaceVisitor {
        type Break = ();

        fn pre_visit_table_factor(&mut self, table: &TableFactor) -> ControlFlow<Self::Break> {
            let TableFactor::Table { name, args, .. } = table else {
                return ControlFlow::Continue(());
            };
            let historical_table = args.is_none()
                && ["lix_change", "lix_checkpoint", "lix_commit"]
                    .iter()
                    .any(|surface| {
                        crate::sql2::parse::object_name_is_public_function(name, surface)
                    });
            let historical_function = args.is_some()
                && [
                    "lix_commit_ancestry",
                    "lix_diff",
                    "lix_history",
                    "lix_state_at",
                ]
                .iter()
                .any(|surface| crate::sql2::parse::object_name_is_public_function(name, surface));
            if historical_table || historical_function {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        }
    }

    let historical = match statement {
        DataFusionStatement::Statement(statement) => {
            statement.visit(&mut AuthoritySurfaceVisitor).is_break()
        }
        DataFusionStatement::Explain(explain) => {
            return statement_authority_route(explain.statement.as_ref());
        }
        _ => false,
    };
    Ok(if historical {
        StatementAuthorityRoute::AuthorityRead
    } else {
        StatementAuthorityRoute::HotRead
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql2::SqlPlanningCache;

    fn route(sql: &str) -> StatementAuthorityRoute {
        let cache = SqlPlanningCache::<crate::catalog::CatalogFingerprint>::default();
        let statement = cache.parse_statement(sql).expect("statement parses");
        statement_authority_route(&statement).expect("statement routes")
    }

    #[test]
    fn routes_only_certified_current_surfaces_to_the_hot_plane() {
        assert_eq!(
            route("SELECT * FROM lix_file"),
            StatementAuthorityRoute::HotRead
        );
        assert_eq!(
            route("SELECT * FROM lix_working_diff('lix_file')"),
            StatementAuthorityRoute::HotRead
        );
    }

    #[test]
    fn routes_history_and_mutations_to_the_authority() {
        for sql in [
            "SELECT * FROM lix_change",
            "SELECT * FROM lix_checkpoint",
            "SELECT * FROM lix_commit",
            "SELECT * FROM lix_history('lix_file')",
            "SELECT * FROM lix_state_at('lix_file', $1)",
            "SELECT * FROM lix_diff('lix_file', $1, $2)",
            "SELECT * FROM lix_commit_ancestry($1)",
        ] {
            assert_eq!(route(sql), StatementAuthorityRoute::AuthorityRead, "{sql}");
        }
        assert_eq!(
            route("UPDATE lix_file SET path = '/b' WHERE path = '/a'"),
            StatementAuthorityRoute::AuthorityWrite
        );
        for sql in ["SELECT uuidv7()", "SELECT current_timestamp"] {
            assert_eq!(route(sql), StatementAuthorityRoute::AuthorityWrite, "{sql}");
        }
    }
}
