use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value as JsonValue;
use sqlparser::ast::{Expr, Statement};

use crate::backend::prepared::PreparedStatement;
use crate::session::contracts::SessionStateDelta;
use crate::state::stream::StateCommitStreamChange;
use crate::{LixError, Value};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaLiveTableRequirement {
    pub(crate) schema_key: String,
    pub(crate) schema_definition: Option<JsonValue>,
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
    pub(crate) delete: bool,
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
    pub(crate) mutations: Vec<MutationRow>,
    pub(crate) update_validations: Vec<UpdateValidationPlan>,
}

impl PlannedStatementSet {
    pub(crate) fn single_statement_params(&self) -> Result<&[Value], LixError> {
        match self.prepared_statements.as_slice() {
            [statement] => Ok(statement.params.as_slice()),
            [] => Ok(&[]),
            statements
                if statements
                    .iter()
                    .all(|statement| statement.params.is_empty()) =>
            {
                Ok(&[])
            }
            _ => Err(LixError {
                code: "LIX_ERROR_UNKNOWN".to_string(),
                description: "preprocess output expected a single prepared statement".to_string(),
            }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemPayloadDomainChange {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) schema_version: String,
    pub(crate) file_id: String,
    pub(crate) version_id: String,
    pub(crate) untracked: bool,
    pub(crate) plugin_key: String,
    pub(crate) snapshot_content: Option<String>,
    pub(crate) metadata: Option<String>,
    pub(crate) writer_key: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct PlanEffects {
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) session_delta: SessionStateDelta,
    pub(crate) file_cache_refresh_targets: BTreeSet<(String, String)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResultContract {
    Select,
    DmlNoReturning,
    DmlReturning,
    Other,
}

pub(crate) fn result_contract_for_statements(statements: &[Statement]) -> ResultContract {
    match statements.last() {
        Some(Statement::Query(_) | Statement::Explain { .. }) => ResultContract::Select,
        Some(Statement::Insert(insert)) => {
            if insert.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Update(update)) => {
            if update.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(Statement::Delete(delete)) => {
            if delete.returning.is_some() {
                ResultContract::DmlReturning
            } else {
                ResultContract::DmlNoReturning
            }
        }
        Some(_) | None => ResultContract::Other,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WriteMode {
    Tracked,
    Untracked,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WriteLane {
    ActiveVersion,
    SingleVersion(String),
    GlobalAdmin,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PlannedStateRow {
    pub(crate) entity_id: String,
    pub(crate) schema_key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) values: BTreeMap<String, Value>,
    pub(crate) writer_key: Option<String>,
    pub(crate) tombstone: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OptionalTextPatch {
    Unchanged,
}

impl OptionalTextPatch {
    pub(crate) fn apply(&self, current: Option<String>) -> Option<String> {
        current
    }
}
