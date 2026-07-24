use serde_json::Value;
use std::sync::OnceLock;

pub const NODE_SCHEMA_KEY: &str = "markdown_node_v2";
pub const NODE_SCHEMA_PATH: &str = "schema/markdown_node_v2.json";

const NODE_SCHEMA_JSON: &str = include_str!("../schema/markdown_node_v2.json");

const SCHEMA_JSONS: [&str; 1] = [NODE_SCHEMA_JSON];

static SCHEMA_DEFINITIONS: OnceLock<Vec<Value>> = OnceLock::new();
static NODE_SCHEMA_DEFINITION: OnceLock<Value> = OnceLock::new();

pub fn schema_jsons() -> &'static [&'static str] {
    &SCHEMA_JSONS
}

pub fn node_schema_json() -> &'static str {
    NODE_SCHEMA_JSON
}

pub fn schema_definitions() -> &'static Vec<Value> {
    SCHEMA_DEFINITIONS.get_or_init(|| {
        SCHEMA_JSONS
            .iter()
            .map(|raw| serde_json::from_str(raw).expect("markdown schema JSON must be valid"))
            .collect()
    })
}

pub fn node_schema_definition() -> &'static Value {
    NODE_SCHEMA_DEFINITION.get_or_init(|| {
        serde_json::from_str(NODE_SCHEMA_JSON).expect("markdown schema JSON must be valid")
    })
}
