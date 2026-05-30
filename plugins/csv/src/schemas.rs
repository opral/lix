use serde_json::Value;
use std::sync::OnceLock;

pub const TABLE_SCHEMA_KEY: &str = "csv_table";
pub const ROW_SCHEMA_KEY: &str = "csv_row";
pub const TABLE_SCHEMA_PATH: &str = "schema/csv_table.json";
pub const ROW_SCHEMA_PATH: &str = "schema/csv_row.json";

const TABLE_SCHEMA_JSON: &str = include_str!("../schema/csv_table.json");
const ROW_SCHEMA_JSON: &str = include_str!("../schema/csv_row.json");

const SCHEMA_JSONS: [&str; 2] = [TABLE_SCHEMA_JSON, ROW_SCHEMA_JSON];

static SCHEMA_DEFINITIONS: OnceLock<Vec<Value>> = OnceLock::new();
static TABLE_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();
static ROW_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();

pub fn schema_jsons() -> &'static [&'static str] {
    &SCHEMA_JSONS
}

pub fn table_schema_json() -> &'static str {
    TABLE_SCHEMA_JSON
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

pub fn table_schema_definition() -> &'static Value {
    TABLE_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(TABLE_SCHEMA_JSON).expect("csv schema JSON must be valid")
    })
}

pub fn row_schema_definition() -> &'static Value {
    ROW_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(ROW_SCHEMA_JSON).expect("csv schema JSON must be valid")
    })
}
