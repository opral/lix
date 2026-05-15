use datafusion::sql::sqlparser::ast::ObjectName;

use crate::LixError;

pub(crate) fn bind_exact_table_name(name: &ObjectName) -> Result<String, LixError> {
    if name.0.len() != 1 {
        return Err(super::error::unsupported(
            "qualified SQL table names are not supported",
        ));
    }
    name.0
        .first()
        .and_then(|part| part.as_ident())
        .map(|ident| ident.value.to_ascii_lowercase())
        .ok_or_else(|| super::error::unsupported("unsupported SQL table name"))
}
