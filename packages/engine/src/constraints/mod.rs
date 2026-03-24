//! Shared SQL-free vocabulary for structured scan constraints.

use crate::Value;

/// Which indexed field a scan constraint applies to.
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
///
/// `Vec<ScanConstraint>` is conjunctive: multiple constraints combine with `AND`.
/// Partition selectors such as `schema_key` and `version_id` stay outside this type.
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
