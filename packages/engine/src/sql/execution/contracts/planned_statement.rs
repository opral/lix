use std::collections::BTreeMap;

use serde_json::Value as JsonValue;

use crate::state::internal::InternalStatePlan;
use crate::{LixError, Value};
use sqlparser::ast::Expr;

use super::prepared_statement::PreparedStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaLiveTableRequirement {
    pub(crate) schema_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MutationOperation {
    Insert,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct MutationRow {
    pub(crate) operation: MutationOperation,
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<JsonValue>,
    pub(crate) untracked: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct UpdateValidationPlan {
    pub(crate) table: String,
    pub(crate) where_clause: Option<Expr>,
    pub(crate) snapshot_content: Option<JsonValue>,
    pub(crate) snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone)]
pub(crate) struct PlannedStatementSet {
    pub(crate) sql: String,
    pub(crate) prepared_statements: Vec<PreparedStatement>,
    pub(crate) live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub(crate) internal_state: Option<InternalStatePlan>,
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl PlannedStatementSet {
    pub(crate) fn single_statement_params(&self) -> Result<&[Value], LixError> {
        match self.prepared_statements.as_slice() {
            [statement] => Ok(statement.params.as_slice()),
            [] => Ok(&[]),
            _ => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "preprocess output expected a single prepared statement".to_string(),
            }),
        }
    }
}
