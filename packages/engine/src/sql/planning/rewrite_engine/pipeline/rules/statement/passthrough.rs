use sqlparser::ast::Statement;

use crate::engine::sql::planning::rewrite_engine::types::RewriteOutput;

pub(crate) fn apply(statement: Statement) -> RewriteOutput {
    RewriteOutput {
        statements: vec![statement],
        params: Vec::new(),
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}
