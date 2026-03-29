//! SQL front-end parsing ownership.
//!
//! This stage owns SQL text -> AST/script parsing entrypoints.

pub(crate) mod parse;
pub(crate) mod placeholders;

pub(crate) use parse::{parse_sql, parse_sql_script, parse_sql_statements};
