pub(crate) mod capability;
pub(crate) mod registry;
pub(crate) mod schema;
pub(crate) mod surface;

pub(crate) use capability::SurfaceCapabilities;
pub(crate) use registry::PublicCatalog;
pub(crate) use schema::PublicColumn;
pub(crate) use surface::{PublicSurfaceContract, PublicSurfaceKind};
