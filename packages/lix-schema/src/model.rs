use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, validate::validate_schema};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    #[serde(rename = "$schema")]
    pub schema: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub columns: Vec<Column>,
    pub primary_key: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique: Vec<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub foreign_keys: Vec<ForeignKey>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_refs: Vec<RowRefConstraint>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deprecated: bool,
}

impl Schema {
    pub fn validate(&self) -> Result<(), Error> {
        validate_schema(self)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: DataType,
    #[serde(default = "default_nullable")]
    pub nullable: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_value: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_expression: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<Value>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub deprecated: bool,
}

const fn default_nullable() -> bool {
    true
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum DataType {
    #[serde(rename = "text")]
    Text,
    #[serde(rename = "uuid")]
    Uuid,
    #[serde(rename = "int8")]
    Int8,
    #[serde(rename = "float8")]
    Float8,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "jsonb")]
    Jsonb,
    #[serde(rename = "timestamptz")]
    Timestamptz,
    /// A canonical Lix row reference (`lix_row_ref(...)`). Stored as text, but
    /// it is its own SQL type (`ROW_REF`) and every value must resolve to an
    /// existing row when it is written. `row_refs` may set its delete action.
    #[serde(rename = "row_ref")]
    RowRef,
}

impl DataType {
    /// The Schema v1 type name. Every type except the Lix-specific `row_ref`
    /// is also its PostgreSQL type name.
    pub const fn postgres_name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Uuid => "uuid",
            Self::Int8 => "int8",
            Self::Float8 => "float8",
            Self::Boolean => "boolean",
            Self::Jsonb => "jsonb",
            Self::Timestamptz => "timestamptz",
            Self::RowRef => "row_ref",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub references: ForeignKeyReference,
    #[serde(default, skip_serializing_if = "DeleteAction::is_no_action")]
    pub on_delete: DeleteAction,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeyReference {
    pub schema_key: String,
    pub columns: Vec<String>,
}

/// The delete action of a `row_ref` column.
///
/// Every [`DataType::RowRef`] column is a Lix logical row reference: unlike a
/// [`ForeignKey`], it can target any schema key at runtime, and the target
/// relation, optional file scope, and typed primary key are carried by the
/// value itself. A `row_ref` column without an entry uses
/// [`DeleteAction::NoAction`], so an entry names `cascade` or `detach`.
/// PostgreSQL has no direct DDL equivalent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowRefConstraint {
    pub column: String,
    /// `cascade` or `detach`; a column without an entry uses `no_action`.
    pub on_delete: DeleteAction,
}

/// Action executed when a referenced row is deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeleteAction {
    #[default]
    NoAction,
    Cascade,
    /// Keep the referencing row unchanged, including its reference value. The
    /// reference is no longer enforced once its target is gone; it resolves
    /// again if the target returns. Supported for row references only.
    Detach,
}

impl DeleteAction {
    pub const fn is_no_action(&self) -> bool {
        matches!(self, Self::NoAction)
    }
}
