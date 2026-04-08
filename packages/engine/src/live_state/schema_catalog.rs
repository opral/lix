use async_trait::async_trait;
use serde_json::Value as JsonValue;

use crate::schema::SchemaKey;
use crate::LixError;

#[async_trait(?Send)]
pub trait RegisteredSchemaCatalog {
    async fn load_schema(&mut self, key: &SchemaKey) -> Result<JsonValue, LixError>;
    async fn load_latest_schema(&mut self, schema_key: &str) -> Result<JsonValue, LixError>;
    async fn load_visible_schema_entries(
        &mut self,
    ) -> Result<Vec<(SchemaKey, JsonValue)>, LixError>;
}
