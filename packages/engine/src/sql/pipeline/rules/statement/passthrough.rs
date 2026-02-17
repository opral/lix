use sqlparser::ast::Statement;

use crate::sql::types::RewriteOutput;

pub(crate) fn apply(statement: Statement) -> RewriteOutput {
    RewriteOutput {
        statements: vec![statement],
        registrations: Vec::new(),
        postprocess: None,
        mutations: Vec::new(),
        update_validations: Vec::new(),
    }
}
