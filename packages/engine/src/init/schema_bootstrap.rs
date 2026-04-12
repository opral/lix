use crate::live_state::register_schema;
use crate::schema::builtin_schema_keys;
use crate::{LixBackend, LixError};

pub(crate) async fn init_builtin_schema_storage(backend: &dyn LixBackend) -> Result<(), LixError> {
    for schema_key in builtin_schema_keys() {
        register_schema(backend, *schema_key).await?;
    }
    Ok(())
}
