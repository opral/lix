use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Statement};

use crate::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaRegistration {
    pub schema_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileDataAssignmentPlan {
    Uniform(Vec<u8>),
    ByFileId(BTreeMap<String, Vec<u8>>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VtableUpdatePlan {
    pub schema_key: String,
    pub explicit_writer_key: Option<Option<String>>,
    pub writer_key_assignment_present: bool,
    pub file_data_assignment: Option<FileDataAssignmentPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VtableDeletePlan {
    pub schema_key: String,
    pub effective_scope_fallback: bool,
    pub effective_scope_selection_sql: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationPlan {
    pub table: String,
    pub where_clause: Option<Expr>,
    pub snapshot_content: Option<JsonValue>,
    pub snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct MutationRow {
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
    pub params: Vec<Value>,
    pub registrations: Vec<SchemaRegistration>,
    pub postprocess: Option<PostprocessPlan>,
    pub mutations: Vec<MutationRow>,
    pub update_validations: Vec<UpdateValidationPlan>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct PreparedStatement {
    pub statement: Statement,
    pub sql: String,
    pub params: Vec<Value>,
}

#[derive(Debug, Clone)]
pub struct PreprocessOutput {
    pub sql: String,
    pub params: Vec<Value>,
    pub prepared_statements: Vec<PreparedStatement>,
}
