use std::ops::ControlFlow;

use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{TableFactor, Visit, Visitor};

use super::catalog::{PublicCatalog, PublicSurfaceContract, PublicSurfaceKind};

/// Classifies a parsed statement's state requirements and mutation behavior.
///
/// Sync replicas execute every class locally. Historical reads may demand
/// deferred immutable data; writes enter the durable upload queue. Keeping
/// the distinction beside the parser makes whole-read retries safe without
/// accidentally replaying a mutation. The authority route names also remain
/// useful to protocol callers that own their execution lifecycle.
#[doc(hidden)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum StatementAuthorityRoute {
    /// A read over the locally available current-state serving plane.
    HotRead,
    /// A historical read which may need lazy authority hydration.
    AuthorityRead,
    /// A local mutation whose accepted result is decided by the authority.
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
            let fixed_surface = PublicCatalog::fixed_system().surfaces().find(|surface| {
                crate::sql2::parse::object_name_is_public_function(name, &surface.name)
            });
            if fixed_surface.is_some_and(|surface| {
                !is_certified_hot_surface(surface, args.as_ref().map(|args| args.args.len()))
            }) {
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

/// Positive ownership policy for every fixed public SQL surface. Runtime
/// schema relations are the only surfaces absent from the fixed catalog and
/// are always current-state `SchemaBase` relations; CTE aliases are likewise
/// absent and inherit the ownership of the table factors in their bodies.
fn is_certified_hot_surface(
    surface: &PublicSurfaceContract,
    argument_count: Option<usize>,
) -> bool {
    match &surface.kind {
        PublicSurfaceKind::File | PublicSurfaceKind::Directory | PublicSurfaceKind::Branch => true,
        PublicSurfaceKind::SchemaBase { .. } => {
            !matches!(surface.name.as_str(), "lix_checkpoint" | "lix_commit")
        }
        PublicSurfaceKind::DiffFunction => argument_count == Some(1),
        PublicSurfaceKind::HistoryFunction
        | PublicSurfaceKind::CheckpointFunction
        | PublicSurfaceKind::StateAtFunction
        | PublicSurfaceKind::CommitAncestryFunction
        | PublicSurfaceKind::Revert
        | PublicSurfaceKind::Apply
        | PublicSurfaceKind::Restore
        | PublicSurfaceKind::Change => false,
    }
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
            route("SELECT * FROM lix_diff('lix_file')"),
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

    #[test]
    fn every_fixed_catalog_surface_has_explicit_ownership() {
        for surface in PublicCatalog::fixed_system().surfaces() {
            let argument_count = match surface.kind {
                PublicSurfaceKind::HistoryFunction
                | PublicSurfaceKind::DiffFunction
                | PublicSurfaceKind::CheckpointFunction
                | PublicSurfaceKind::StateAtFunction
                | PublicSurfaceKind::CommitAncestryFunction => Some(1),
                _ => None,
            };
            let hot = is_certified_hot_surface(surface, argument_count);
            let expected_hot =
                matches!(
                    surface.kind,
                    PublicSurfaceKind::File
                        | PublicSurfaceKind::Directory
                        | PublicSurfaceKind::Branch
                        | PublicSurfaceKind::DiffFunction
                        | PublicSurfaceKind::SchemaBase { .. }
                ) && !matches!(surface.name.as_str(), "lix_checkpoint" | "lix_commit");
            assert_eq!(hot, expected_hot, "surface {}", surface.name);
        }
    }
}
