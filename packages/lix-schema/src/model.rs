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
    #[serde(rename = "bigint")]
    BigInt,
    #[serde(rename = "double precision")]
    DoublePrecision,
    #[serde(rename = "boolean")]
    Boolean,
    #[serde(rename = "jsonb")]
    Jsonb,
}

impl DataType {
    pub const fn postgres_name(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Uuid => "uuid",
            Self::BigInt => "bigint",
            Self::DoublePrecision => "double precision",
            Self::Boolean => "boolean",
            Self::Jsonb => "jsonb",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKey {
    pub columns: Vec<String>,
    pub references: ForeignKeyReference,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ForeignKeyReference {
    pub schema_key: String,
    pub columns: Vec<String>,
}
