//! Shared SQL-free vocabulary for structured scan constraints.

mod sql;

pub use crate::contracts::artifacts::{Bound, ScanConstraint, ScanField, ScanOperator};

pub(crate) use sql::{
    escape_sql_string, quote_ident, render_constraint_sql, sql_literal, sql_literal_text,
};
