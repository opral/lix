use std::collections::BTreeMap;

use serde_json::Value as JsonValue;
use sqlparser::ast::Expr;

use crate::backend::PreparedStatement;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaLiveTableRequirement {
    pub schema_key: String,
    pub schema_definition: Option<JsonValue>,
}

pub fn is_untracked_live_table(_table_name: &str) -> bool {
    false
}

pub fn coalesce_live_table_requirements(
    requirements: &[SchemaLiveTableRequirement],
) -> Vec<SchemaLiveTableRequirement> {
    let mut by_schema = BTreeMap::<String, SchemaLiveTableRequirement>::new();
    for requirement in requirements {
        by_schema
            .entry(requirement.schema_key.clone())
            .and_modify(|existing| {
                if existing.schema_definition.is_none() && requirement.schema_definition.is_some() {
                    existing.schema_definition = requirement.schema_definition.clone();
                }
            })
            .or_insert_with(|| requirement.clone());
    }
    by_schema.into_values().collect()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MutationOperation {
    Insert,
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

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationPlan {
    pub delete: bool,
    pub table: String,
    pub where_clause: Option<Expr>,
    pub snapshot_content: Option<JsonValue>,
    pub snapshot_patch: Option<BTreeMap<String, JsonValue>>,
}

#[derive(Debug, Clone)]
pub struct PlannedStatementSet {
    pub prepared_statements: Vec<PreparedStatement>,
    pub live_table_requirements: Vec<SchemaLiveTableRequirement>,
    pub mutations: Vec<MutationRow>,
    pub update_validations: Vec<UpdateValidationPlan>,
}
