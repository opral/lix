use serde_json::Value as JsonValue;

use crate::sql::UpdateValidationPlan;

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationInputRow {
    pub entity_id: String,
    pub file_id: String,
    pub version_id: String,
    pub schema_key: String,
    pub schema_version: String,
    pub base_snapshot: JsonValue,
}

#[derive(Debug, Clone, PartialEq)]
pub struct UpdateValidationInput {
    pub plan: UpdateValidationPlan,
    pub rows: Vec<UpdateValidationInputRow>,
}
