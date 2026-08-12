use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;

pub(crate) const DATAFUSION_SQL_DIALECT: &str = "postgresql";

pub(crate) fn lix_sql_dialect() -> PostgreSqlDialect {
    PostgreSqlDialect {}
}
