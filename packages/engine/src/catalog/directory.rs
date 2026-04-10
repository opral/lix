//! Catalog-owned `lix_directory` declaration.
//!
//! The SQL-backed filesystem surface bridge remains the serving path for now.
//! This module only captures the declarative derived-surface shape so
//! filesystem ownership lives under `catalog/*`.

#![allow(dead_code)]

use crate::catalog::{
    CatalogDerivedRow, CatalogProjectionDefinition, CatalogProjectionInput,
    CatalogProjectionInputSpec, CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
};
use crate::LixError;

const DIRECTORY_SURFACE_NAME: &str = "lix_directory";
const DIRECTORY_BY_VERSION_SURFACE_NAME: &str = "lix_directory_by_version";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixDirectoryProjection;

impl CatalogProjectionDefinition for LixDirectoryProjection {
    fn name(&self) -> &'static str {
        DIRECTORY_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        vec![
            CatalogProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            CatalogProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![
            CatalogProjectionSurfaceSpec::new(
                DIRECTORY_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::Default,
            ),
            CatalogProjectionSurfaceSpec::new(
                DIRECTORY_BY_VERSION_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::ByVersion,
            ),
        ]
    }

    fn derive(&self, _input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        // The existing SQL bridge remains authoritative until read-time
        // filesystem derivation is extracted from that storage-aware path.
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LixDirectoryProjection, DIRECTORY_BY_VERSION_SURFACE_NAME, DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        DIRECTORY_SURFACE_NAME,
    };
    use crate::catalog::{
        CatalogProjectionDefinition, CatalogProjectionInputSpec, CatalogProjectionSurfaceSpec,
        SurfaceFamily, SurfaceVariant,
    };

    #[test]
    fn filesystem_directory_projection_stays_declarative() {
        let projection = LixDirectoryProjection;

        assert_eq!(projection.name(), DIRECTORY_SURFACE_NAME);
        assert_eq!(
            projection.inputs(),
            vec![
                CatalogProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                CatalogProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            ]
        );
        assert_eq!(
            projection.surfaces(),
            vec![
                CatalogProjectionSurfaceSpec::new(
                    DIRECTORY_SURFACE_NAME,
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::Default,
                ),
                CatalogProjectionSurfaceSpec::new(
                    DIRECTORY_BY_VERSION_SURFACE_NAME,
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::ByVersion,
                ),
            ]
        );
    }
}
