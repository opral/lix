//! Catalog-owned `lix_file` declaration.
//!
//! The SQL-backed filesystem surface bridge remains the serving path for now.
//! This module only captures the declarative derived-surface shape so
//! filesystem ownership lives under `catalog/*`.

#![allow(dead_code)]

use crate::catalog::{
    CatalogDerivedRow, CatalogProjectionDefinition, CatalogProjectionInput,
    CatalogProjectionInputSpec, CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
};
use crate::{LixError, Value};
use std::collections::BTreeMap;

const FILE_SURFACE_NAME: &str = "lix_file";
const FILE_BY_VERSION_SURFACE_NAME: &str = "lix_file_by_version";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixFileProjection;

impl CatalogProjectionDefinition for LixFileProjection {
    fn name(&self) -> &'static str {
        FILE_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        vec![
            CatalogProjectionInputSpec::tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
            CatalogProjectionInputSpec::untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
            CatalogProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            CatalogProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            CatalogProjectionInputSpec::tracked(BINARY_BLOB_REF_SCHEMA_KEY),
            CatalogProjectionInputSpec::untracked(BINARY_BLOB_REF_SCHEMA_KEY),
        ]
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![
            CatalogProjectionSurfaceSpec::new(
                FILE_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::Default,
            ),
            CatalogProjectionSurfaceSpec::new(
                FILE_BY_VERSION_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::ByVersion,
            ),
        ]
    }

    fn derive(&self, _input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        // The existing SQL bridge remains authoritative until read-time
        // filesystem derivation is extracted from that storage-aware path.
        let _ = BTreeMap::<String, Value>::new();
        Ok(Vec::new())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LixFileProjection, BINARY_BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        FILE_BY_VERSION_SURFACE_NAME, FILE_DESCRIPTOR_SCHEMA_KEY, FILE_SURFACE_NAME,
    };
    use crate::catalog::{
        CatalogProjectionDefinition, CatalogProjectionInputSpec, CatalogProjectionSurfaceSpec,
        SurfaceFamily, SurfaceVariant,
    };

    #[test]
    fn filesystem_file_projection_stays_declarative() {
        let projection = LixFileProjection;

        assert_eq!(projection.name(), FILE_SURFACE_NAME);
        assert_eq!(
            projection.inputs(),
            vec![
                CatalogProjectionInputSpec::tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                CatalogProjectionInputSpec::untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                CatalogProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                CatalogProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                CatalogProjectionInputSpec::tracked(BINARY_BLOB_REF_SCHEMA_KEY),
                CatalogProjectionInputSpec::untracked(BINARY_BLOB_REF_SCHEMA_KEY),
            ]
        );
        assert_eq!(
            projection.surfaces(),
            vec![
                CatalogProjectionSurfaceSpec::new(
                    FILE_SURFACE_NAME,
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::Default,
                ),
                CatalogProjectionSurfaceSpec::new(
                    FILE_BY_VERSION_SURFACE_NAME,
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::ByVersion,
                ),
            ]
        );
    }
}
