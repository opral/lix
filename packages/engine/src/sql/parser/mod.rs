//! SQL front-end parsing ownership.
//!
//! This stage owns SQL text -> AST/script parsing entrypoints.

pub(crate) mod parse;
pub(crate) mod placeholders;

#[cfg(test)]
pub(crate) use parse::parse_sql_script;
pub(crate) use parse::parse_sql_with_timing;
pub(crate) use parse::{parse_sql, parse_sql_statements};
