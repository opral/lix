//! Shared SQL-free vocabulary for structured scan constraints.

mod sql;

use crate::Value;

/// Which indexed field a live-state scan constraint applies to.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScanField {
    EntityId,
    FileId,
    PluginKey,
    SchemaVersion,
}

/// Inclusive or exclusive range bound.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct Bound {
    pub value: Value,
    pub inclusive: bool,
}

/// SQL-free structured scan constraint.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct ScanConstraint {
    pub field: ScanField,
    pub operator: ScanOperator,
}

/// Structured scan operator aligned with the current planner/storage split.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum ScanOperator {
    Eq(Value),
    In(Vec<Value>),
    Range {
        lower: Option<Bound>,
        upper: Option<Bound>,
    },
}

pub(crate) use sql::{
    escape_sql_string, quote_ident, render_constraint_sql, sql_literal, sql_literal_text,
};
