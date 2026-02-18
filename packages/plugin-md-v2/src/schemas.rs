use serde_json::Value;
use std::sync::OnceLock;

pub const DOCUMENT_SCHEMA_KEY: &str = "markdown_v2_document";
pub const BLOCK_SCHEMA_KEY: &str = "markdown_v2_block";
pub const ENTITY_SCHEMA_VERSION: &str = "1";

const DOCUMENT_SCHEMA_JSON: &str = include_str!("../schema/markdown_document.json");
const BLOCK_SCHEMA_JSON: &str = include_str!("../schema/markdown_block.json");

const SCHEMA_JSONS: [&str; 2] = [DOCUMENT_SCHEMA_JSON, BLOCK_SCHEMA_JSON];

static SCHEMA_DEFINITIONS: OnceLock<Vec<Value>> = OnceLock::new();

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
