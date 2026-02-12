use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Statement};

use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRegistration {
    pub schema_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VtableUpdatePlan {
    pub schema_key: String,
    pub explicit_writer_key: Option<Option<String>>,
    pub writer_key_assignment_present: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VtableDeletePlan {
    pub schema_key: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationPlan {
    pub table: String,
    pub where_clause: Option<Expr>,
    pub snapshot_content: Option<JsonValue>,
    pub snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOperation {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationRow {
    pub operation: MutationOperation,
    pub entity_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub file_id: String,
    pub version_id: String,
    pub plugin_key: String,
    pub snapshot_content: Option<JsonValue>,
    pub untracked: bool,
}

#[derive(Debug, Clone)]
pub enum PostprocessPlan {
    VtableUpdate(VtableUpdatePlan),
    VtableDelete(VtableDeletePlan),
}

#[derive(Debug, Clone)]
pub struct RewriteOutput {
    pub statements: Vec<Statement>,
    pub registrations: Vec<SchemaRegistration>,
    pub postprocess: Option<PostprocessPlan>,
    pub mutations: Vec<MutationRow>,
    pub update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone)]
pub struct PreprocessOutput {
    pub sql: String,
    pub params: Vec<Value>,
    pub registrations: Vec<SchemaRegistration>,
    pub postprocess: Option<PostprocessPlan>,
    pub mutations: Vec<MutationRow>,
    pub update_validations: Vec<UpdateValidationPlan>,
}
