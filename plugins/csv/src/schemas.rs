use serde_json::Value;
use std::sync::OnceLock;

pub const DOCUMENT_SCHEMA_KEY: &str = "csv_v2_document";
pub const ROW_SCHEMA_KEY: &str = "csv_v2_row";
pub const DOCUMENT_SCHEMA_PATH: &str = "schema/csv_document.json";
pub const ROW_SCHEMA_PATH: &str = "schema/csv_row.json";

const DOCUMENT_SCHEMA_JSON: &str = include_str!("../schema/csv_document.json");
const ROW_SCHEMA_JSON: &str = include_str!("../schema/csv_row.json");

const SCHEMA_JSONS: [&str; 2] = [DOCUMENT_SCHEMA_JSON, ROW_SCHEMA_JSON];

static SCHEMA_DEFINITIONS: OnceLock<Vec<Value>> = OnceLock::new();
static DOCUMENT_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();
static ROW_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();

pub fn schema_jsons() -> &'static [&'static str] {
    &SCHEMA_JSONS
}

pub fn document_schema_json() -> &'static str {
    DOCUMENT_SCHEMA_JSON
}

pub fn row_schema_json() -> &'static str {
    ROW_SCHEMA_JSON
}

pub fn schema_definitions() -> &'static Vec<Value> {
    SCHEMA_DEFINITIONS.get_or_init(|| {
        SCHEMA_JSONS
            .iter()
            .map(|raw| serde_json::from_str(raw).expect("csv schema JSON must be valid"))
            .collect()
    })
}

pub fn document_schema_definition() -> &'static Value {
    DOCUMENT_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(DOCUMENT_SCHEMA_JSON).expect("csv schema JSON must be valid")
    })
}

pub fn row_schema_definition() -> &'static Value {
    ROW_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(ROW_SCHEMA_JSON).expect("csv schema JSON must be valid")
    })
}
