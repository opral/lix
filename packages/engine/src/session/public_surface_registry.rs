use crate::catalog::{
    dynamic_entity_surface_spec_from_schema, register_dynamic_entity_surface_spec,
    remove_dynamic_entity_surfaces_for_schema_key, SurfaceRegistry,
};
use crate::runtime::cel::shared_runtime;
use crate::schema::schema_from_registered_snapshot;
use crate::LixError;
use serde_json::Value as JsonValue;

pub(crate) fn apply_registered_schema_snapshot_to_surface_registry(
    registry: &mut SurfaceRegistry,
    snapshot: &JsonValue,
) -> Result<(), LixError> {
    let (key, schema) = schema_from_registered_snapshot(snapshot)?;
    remove_dynamic_entity_surfaces_for_schema_key(registry, &key.schema_key);
    let spec = dynamic_entity_surface_spec_from_schema(&schema, shared_runtime())?;
    register_dynamic_entity_surface_spec(registry, spec);
    Ok(())
}
