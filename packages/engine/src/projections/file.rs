//! Declarative `lix_file` projection definition.
//!
//! The SQL-backed filesystem surface bridge remains the serving path for now.
//! This module only captures the declarative projection shape so filesystem
//! projection ownership lives under `projections/*`.

#![allow(dead_code)]

use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};
use crate::projections::{
    DerivedRow, ProjectionInput, ProjectionInputSpec, ProjectionRegistration,
    ProjectionSurfaceSpec, ProjectionTrait,
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

pub(crate) fn builtin_lix_file_registration() -> ProjectionRegistration<LixFileProjection> {
    ProjectionRegistration::new(
        LixFileProjection,
        crate::projections::ProjectionLifecycle::ReadTime,
    )
}

impl ProjectionTrait for LixFileProjection {
    fn name(&self) -> &'static str {
        FILE_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<ProjectionInputSpec> {
        vec![
            ProjectionInputSpec::tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
            ProjectionInputSpec::untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
            ProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            ProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            ProjectionInputSpec::tracked(BINARY_BLOB_REF_SCHEMA_KEY),
            ProjectionInputSpec::untracked(BINARY_BLOB_REF_SCHEMA_KEY),
        ]
    }

    fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
        vec![
            ProjectionSurfaceSpec::new(
                FILE_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::Default,
            ),
            ProjectionSurfaceSpec::new(
                FILE_BY_VERSION_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::ByVersion,
            ),
        ]
    }

    fn derive(&self, _input: &ProjectionInput) -> Result<Vec<DerivedRow>, LixError> {
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
    use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};
    use crate::projections::{ProjectionInputSpec, ProjectionTrait};

    #[test]
    fn filesystem_file_projection_stays_declarative() {
        let projection = LixFileProjection;

        assert_eq!(projection.name(), FILE_SURFACE_NAME);
        assert_eq!(
            projection.inputs(),
            vec![
                ProjectionInputSpec::tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                ProjectionInputSpec::untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                ProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                ProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                ProjectionInputSpec::tracked(BINARY_BLOB_REF_SCHEMA_KEY),
                ProjectionInputSpec::untracked(BINARY_BLOB_REF_SCHEMA_KEY),
            ]
        );
        assert_eq!(
            projection
                .surfaces()
                .into_iter()
                .map(|surface| (
                    surface.public_name,
                    surface.surface_family,
                    surface.surface_variant
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    FILE_SURFACE_NAME.to_string(),
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::Default,
                ),
                (
                    FILE_BY_VERSION_SURFACE_NAME.to_string(),
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::ByVersion,
                ),
            ]
        );
    }
}
