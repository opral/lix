pub(crate) mod capability;
pub(crate) mod entity_surface;
pub(crate) mod registry;
pub(crate) mod schema;
pub(crate) mod surface;

pub(crate) use capability::SurfaceCapabilities;
pub(crate) use entity_surface::{
    EntityColumnType, EntitySurfaceShape, EntitySurfaceSpec,
    derive_entity_surface_spec_from_schema, entity_surface_schema, entity_system_fields,
    schema_exposed_as_entity_history_surface, schema_exposed_as_entity_surface,
};
pub(crate) use registry::PublicCatalog;
pub(crate) use schema::PublicColumn;
pub(crate) use surface::{PublicSurfaceContract, PublicSurfaceKind};
