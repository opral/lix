pub(crate) mod capability;
pub(crate) mod registry;
pub(crate) mod schema;
pub(crate) mod schema_surface;
pub(crate) mod surface;

pub(crate) use capability::SurfaceCapabilities;
pub(crate) use registry::PublicCatalog;
pub(crate) use schema::{PublicColumn, PublicColumnInsertPolicy};
pub(crate) use schema_surface::{
    SchemaColumnType, SchemaIndexedColumn, SchemaSurfaceShape, SchemaSurfaceSpec,
    derive_schema_surface_spec_from_schema, row_visible_fields, schema_exposed_as_history_surface,
    schema_exposed_as_schema_surface, schema_surface_schema,
};
pub(crate) use surface::{
    PublicHistoryContract, PublicHistoryKind, PublicSurfaceContract, PublicSurfaceKind,
};
