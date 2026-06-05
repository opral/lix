use serde_json::Value;
use std::sync::OnceLock;

pub const DOCUMENT_SCHEMA_KEY: &str = "text_document";
pub const LINE_SCHEMA_KEY: &str = "text_line";
pub const DOCUMENT_SCHEMA_PATH: &str = "schema/text_document.json";
pub const LINE_SCHEMA_PATH: &str = "schema/text_line.json";

const DOCUMENT_SCHEMA_JSON: &str = include_str!("../schema/text_document.json");
const LINE_SCHEMA_JSON: &str = include_str!("../schema/text_line.json");

const SCHEMA_JSONS: [&str; 2] = [DOCUMENT_SCHEMA_JSON, LINE_SCHEMA_JSON];

static SCHEMA_DEFINITIONS: OnceLock<Vec<Value>> = OnceLock::new();
static DOCUMENT_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();
static LINE_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();

pub fn schema_jsons() -> &'static [&'static str] {
    &SCHEMA_JSONS
}

pub fn document_schema_json() -> &'static str {
    DOCUMENT_SCHEMA_JSON
}

pub fn line_schema_json() -> &'static str {
    LINE_SCHEMA_JSON
}

pub fn schema_definitions() -> &'static Vec<Value> {
    SCHEMA_DEFINITIONS.get_or_init(|| {
        SCHEMA_JSONS
            .iter()
            .map(|raw| serde_json::from_str(raw).expect("text schema JSON must be valid"))
            .collect()
    })
}

pub fn document_schema_definition() -> &'static Value {
    DOCUMENT_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(DOCUMENT_SCHEMA_JSON).expect("text schema JSON must be valid")
    })
}

pub fn line_schema_definition() -> &'static Value {
    LINE_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(LINE_SCHEMA_JSON).expect("text schema JSON must be valid")
    })
}
