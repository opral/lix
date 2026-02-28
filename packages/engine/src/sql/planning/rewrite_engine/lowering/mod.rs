mod json_fn;
mod logical_fn;

use std::ops::ControlFlow;

use sqlparser::ast::{Expr, Statement};
use sqlparser::ast::{VisitMut, VisitorMut};

use crate::backend::SqlDialect;
use crate::LixError;

use self::json_fn::{
    lower_lix_empty_blob, lower_lix_json, lower_lix_json_extract, lower_lix_text_decode,
    lower_lix_text_encode,
};
use self::logical_fn::{
    parse_lix_empty_blob, parse_lix_json, parse_lix_json_extract, parse_lix_text_decode,
    parse_lix_text_encode,
};

pub(crate) fn lower_statement(
    statement: Statement,
    dialect: SqlDialect,
) -> Result<Statement, LixError> {
    let mut statement = statement;
    let mut lowerer = LogicalFunctionLowerer { dialect };
    if let ControlFlow::Break(error) = statement.visit(&mut lowerer) {
        return Err(error);
    }
    Ok(statement)
}

struct LogicalFunctionLowerer {
    dialect: SqlDialect,
}

impl VisitorMut for LogicalFunctionLowerer {
    type Break = LixError;

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        let Expr::Function(function) = expr else {
            return ControlFlow::Continue(());
        };

        let parsed = match parse_lix_json_extract(function) {
            Ok(parsed) => parsed,
            Err(error) => return ControlFlow::Break(error),
        };

        let Some(call) = parsed else {
            let parsed_json = match parse_lix_json(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_json {
                *expr = lower_lix_json(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_empty_blob = match parse_lix_empty_blob(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if parsed_empty_blob.is_some() {
                *expr = lower_lix_empty_blob(self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_text_encode = match parse_lix_text_encode(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_text_encode {
                *expr = lower_lix_text_encode(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            let parsed_text_decode = match parse_lix_text_decode(function) {
                Ok(parsed) => parsed,
                Err(error) => return ControlFlow::Break(error),
            };
            if let Some(call) = parsed_text_decode {
                *expr = lower_lix_text_decode(&call, self.dialect);
                return ControlFlow::Continue(());
            }
            return ControlFlow::Continue(());
        };

        let lowered = lower_lix_json_extract(&call, self.dialect);
        *expr = lowered;
        ControlFlow::Continue(())
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::ast::{SetExpr, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    use crate::backend::SqlDialect;

    use super::lower_statement;

    fn lower_query(sql: &str, dialect: SqlDialect) -> String {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        assert_eq!(statements.len(), 1);
        let lowered = lower_statement(statements.remove(0), dialect).expect("lower statement");
        lowered.to_string()
    }

    fn select_expr(sql: &str) -> String {
        let mut statements = Parser::parse_sql(&GenericDialect {}, sql).expect("parse SQL");
        let statement = statements.remove(0);
        match statement {
            Statement::Query(query) => match *query.body {
                SetExpr::Select(select) => select.projection[0].to_string(),
                _ => panic!("expected select"),
            },
            _ => panic!("expected query"),
        }
    }

    #[test]
    fn lowers_lix_json_extract_to_sqlite_json_extract() {
        let lowered = lower_query(
            "SELECT lix_json_extract(snapshot_content, 'id', 'commit_id') FROM foo",
            SqlDialect::Sqlite,
        );
        let projection = select_expr(&lowered);
        assert_eq!(
            projection,
            "CASE json_type(snapshot_content, '$.\"id\".\"commit_id\"') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE json_extract(snapshot_content, '$.\"id\".\"commit_id\"') || '' END"
        );
    }

    #[test]
    fn lowers_lix_json_extract_numeric_segment_to_sqlite_array_index() {
        let lowered = lower_query(
            "SELECT lix_json_extract(snapshot_content, 'items', '0', 'commit_id') FROM foo",
            SqlDialect::Sqlite,
        );
        let projection = select_expr(&lowered);
        assert_eq!(
            projection,
            "CASE json_type(snapshot_content, '$.\"items\"[0].\"commit_id\"') WHEN 'true' THEN 'true' WHEN 'false' THEN 'false' ELSE json_extract(snapshot_content, '$.\"items\"[0].\"commit_id\"') || '' END"
        );
    }

    #[test]
    fn lowers_lix_json_extract_to_postgres_jsonb_extract_path_text() {
        let lowered = lower_query(
            "SELECT lix_json_extract(snapshot_content, 'id', 'commit_id') FROM foo",
            SqlDialect::Postgres,
        );
        let projection = select_expr(&lowered);
        assert_eq!(
            projection,
            "jsonb_extract_path_text(CAST(snapshot_content AS JSONB), 'id', 'commit_id')"
        );
    }

    #[test]
    fn leaves_unrelated_functions_untouched() {
        let lowered = lower_query("SELECT lix_uuid_v7() FROM foo", SqlDialect::Sqlite);
        let projection = select_expr(&lowered);
        assert_eq!(projection, "lix_uuid_v7()");
    }

    #[test]
    fn lowers_lix_empty_blob_to_sqlite_zeroblob() {
        let lowered = lower_query("SELECT lix_empty_blob() FROM foo", SqlDialect::Sqlite);
        let projection = select_expr(&lowered);
        assert_eq!(projection, "zeroblob(0)");
    }

    #[test]
    fn lowers_lix_empty_blob_to_postgres_decode_hex() {
        let lowered = lower_query("SELECT lix_empty_blob() FROM foo", SqlDialect::Postgres);
        let projection = select_expr(&lowered);
        assert_eq!(projection, "decode('', 'hex')");
    }

    #[test]
    fn lowers_lix_json_to_sqlite_json_function() {
        let lowered = lower_query("SELECT lix_json(payload) FROM foo", SqlDialect::Sqlite);
        let projection = select_expr(&lowered);
        assert_eq!(projection, "json(payload)");
    }

    #[test]
    fn lowers_lix_json_to_postgres_jsonb_cast() {
        let lowered = lower_query("SELECT lix_json(payload) FROM foo", SqlDialect::Postgres);
        let projection = select_expr(&lowered);
        assert_eq!(projection, "CAST(payload AS JSONB)");
    }

    #[test]
    fn lowers_lix_text_encode_default_to_sqlite_blob_cast() {
        let lowered = lower_query(
            "SELECT lix_text_encode(payload) FROM foo",
            SqlDialect::Sqlite,
        );
        let projection = select_expr(&lowered);
        assert_eq!(projection, "CAST(payload AS BLOB)");
    }

    #[test]
    fn lowers_lix_text_decode_default_to_sqlite_text_cast() {
        let lowered = lower_query(
            "SELECT lix_text_decode(payload) FROM foo",
            SqlDialect::Sqlite,
        );
        let projection = select_expr(&lowered);
        assert_eq!(projection, "CAST(payload AS TEXT)");
    }

    #[test]
    fn lowers_lix_text_encode_default_to_postgres_convert_to() {
        let lowered = lower_query(
            "SELECT lix_text_encode(payload) FROM foo",
            SqlDialect::Postgres,
        );
        let projection = select_expr(&lowered);
        assert_eq!(projection, "convert_to(CAST(payload AS TEXT), 'UTF8')");
    }

    #[test]
    fn lowers_lix_text_decode_default_to_postgres_convert_from() {
        let lowered = lower_query(
            "SELECT lix_text_decode(payload) FROM foo",
            SqlDialect::Postgres,
        );
        let projection = select_expr(&lowered);
        assert_eq!(projection, "convert_from(CAST(payload AS BYTEA), 'UTF8')");
    }
}
