use jsonschema::JSONSchema;
use std::sync::Arc;

use crate::schema::SchemaKey;

pub trait CompiledSchemaCache {
    fn get_compiled_schema(&self, key: &SchemaKey) -> Option<Arc<JSONSchema>>;

    fn insert_compiled_schema(&self, key: SchemaKey, schema: Arc<JSONSchema>);
}
