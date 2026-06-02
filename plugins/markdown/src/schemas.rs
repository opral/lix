use serde_json::Value;
use std::sync::OnceLock;

pub const DOCUMENT_SCHEMA_KEY: &str = "markdown_v2_document";
pub const BLOCK_SCHEMA_KEY: &str = "markdown_v2_block";
pub const DOCUMENT_SCHEMA_PATH: &str = "schema/markdown_document.json";
pub const BLOCK_SCHEMA_PATH: &str = "schema/markdown_block.json";

const DOCUMENT_SCHEMA_JSON: &str = include_str!("../schema/markdown_document.json");
const BLOCK_SCHEMA_JSON: &str = include_str!("../schema/markdown_block.json");

const SCHEMA_JSONS: [&str; 2] = [DOCUMENT_SCHEMA_JSON, BLOCK_SCHEMA_JSON];

static SCHEMA_DEFINITIONS: OnceLock<Vec<Value>> = OnceLock::new();
static DOCUMENT_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();
static BLOCK_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();

pub fn schema_jsons() -> &'static [&'static str] {
    &SCHEMA_JSONS
}

pub fn schema_definitions() -> &'static Vec<Value> {
    SCHEMA_DEFINITIONS.get_or_init(|| {
        SCHEMA_JSONS
            .iter()
            .map(|raw| serde_json::from_str(raw).expect("markdown schema JSON must be valid"))
            .collect()
    })
}

pub fn document_schema_json() -> &'static str {
    DOCUMENT_SCHEMA_JSON
}

pub fn block_schema_json() -> &'static str {
    BLOCK_SCHEMA_JSON
}

pub fn document_schema_definition() -> &'static Value {
    DOCUMENT_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(DOCUMENT_SCHEMA_JSON).expect("markdown schema JSON must be valid")
    })
}

pub fn block_schema_definition() -> &'static Value {
    BLOCK_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(BLOCK_SCHEMA_JSON).expect("markdown schema JSON must be valid")
    })
}
