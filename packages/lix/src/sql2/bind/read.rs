use datafusion::sql::parser::Statement as DataFusionStatement;
use datafusion::sql::sqlparser::ast::{
    ObjectNamePart, Query, Statement, TableFactor, Visit, Visitor,
};
use std::ops::ControlFlow;

use crate::LixError;

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct BoundRead {
    pub(crate) query: Box<Query>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum BoundStatementRoute {
    Read,
    Write,
}

pub(crate) fn bind_statement_route(
    statement: &DataFusionStatement,
) -> Result<BoundStatementRoute, LixError> {
    if super::super::checkpoint_function_plan(statement)?.is_some() {
        return Ok(BoundStatementRoute::Write);
    }
    match super::classify::classify_datafusion_statement(statement) {
        super::classify::SqlStatementKind::Read => Ok(BoundStatementRoute::Read),
        super::classify::SqlStatementKind::Write => Ok(BoundStatementRoute::Write),
        super::classify::SqlStatementKind::Other => Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL statement is not supported by Lix SQL",
        )),
    }
}

pub(crate) fn bind_read_statement(
    _sql: &str,
    statement: &DataFusionStatement,
) -> Result<(), LixError> {
    if bind_statement_route(statement)? == BoundStatementRoute::Write {
        return Err(LixError::new(
            LixError::CODE_UNSUPPORTED_SQL,
            "SQL writes must use the bound write planning path",
        ));
    }
    super::classify::validate_supported_datafusion_statement_ast(statement)?;
    super::public_udf::validate_public_udf_calls_in_datafusion_statement(statement)?;
    Ok(())
}

/// Returns whether a read is a standalone query over one authoritative,
/// repository-global history relation.
///
/// A partial replica may retry this narrow shape against its authority. The
/// complete table-factor walk intentionally rejects joins, subqueries, table
/// functions, and other relation shapes: executing those remotely would move
/// the whole query away from the local replica and silently omit local edits.
pub(crate) fn is_standalone_global_history_read(statement: &DataFusionStatement) -> bool {
    if super::statement_has_session_dependent_function(statement) {
        return false;
    }

    struct RelationVisitor {
        history_relation: Option<&'static str>,
        invalid: bool,
        query_count: usize,
        table_count: usize,
    }

    impl Visitor for RelationVisitor {
        type Break = ();

        fn pre_visit_query(
            &mut self,
            _query: &Query,
        ) -> ControlFlow<Self::Break> {
            self.query_count += 1;
            if self.query_count > 1 {
                self.invalid = true;
            }
            ControlFlow::Continue(())
        }

        fn pre_visit_table_factor(
            &mut self,
            table_factor: &TableFactor,
        ) -> ControlFlow<Self::Break> {
            self.table_count += 1;
            let TableFactor::Table {
                name, args: None, ..
            } = table_factor
            else {
                self.invalid = true;
                return ControlFlow::Continue(());
            };

            let [ObjectNamePart::Identifier(identifier)] = name.0.as_slice() else {
                self.invalid = true;
                return ControlFlow::Continue(());
            };
            let relation = if identifier.quote_style.is_some() {
                match identifier.value.as_str() {
                    "lix_commit" => "lix_commit",
                    "lix_change" => "lix_change",
                    _ => {
                        self.invalid = true;
                        return ControlFlow::Continue(());
                    }
                }
            } else if identifier.value.eq_ignore_ascii_case("lix_commit") {
                "lix_commit"
            } else if identifier.value.eq_ignore_ascii_case("lix_change") {
                "lix_change"
            } else {
                self.invalid = true;
                return ControlFlow::Continue(());
            };

            if self.history_relation.replace(relation).is_some() {
                self.invalid = true;
            }
            ControlFlow::Continue(())
        }
    }

    let DataFusionStatement::Statement(statement) = statement else {
        return false;
    };
    if !matches!(statement.as_ref(), Statement::Query(_)) {
        return false;
    }

    let mut visitor = RelationVisitor {
        history_relation: None,
        invalid: false,
        query_count: 0,
        table_count: 0,
    };
    let _ = statement.visit(&mut visitor);
    !visitor.invalid && visitor.table_count == 1 && visitor.history_relation.is_some()
}

#[cfg(test)]
mod tests {
    use super::is_standalone_global_history_read;
    use crate::sql2::parse_statement;

    fn is_history_read(sql: &str) -> bool {
        is_standalone_global_history_read(&parse_statement(sql).unwrap())
    }

    #[test]
    fn accepts_one_global_history_relation() {
        assert!(is_history_read("SELECT count(*) FROM lix_commit"));
        assert!(is_history_read("SELECT * FROM LIX_CHANGE WHERE id = $1"));
    }

    #[test]
    fn rejects_mixed_or_nested_relation_shapes() {
        assert!(!is_history_read(
            "SELECT * FROM lix_change JOIN lix_file ON lix_file.id = lix_change.entity_id"
        ));
        assert!(!is_history_read(
            "SELECT * FROM lix_commit WHERE id IN (SELECT id FROM lix_file)"
        ));
        assert!(!is_history_read(
            "WITH x AS (SELECT 1) SELECT * FROM lix_commit"
        ));
        assert!(!is_history_read(
            "SELECT (SELECT 1) AS marker FROM lix_commit"
        ));
        assert!(!is_history_read(
            "SELECT lix_active_branch_id(), count(*) FROM lix_commit"
        ));
        assert!(!is_history_read("SELECT * FROM lix_history('lix_file')"));
    }
}
