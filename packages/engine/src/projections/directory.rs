//! Declarative `lix_directory` projection definition.
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
use crate::LixError;

const DIRECTORY_SURFACE_NAME: &str = "lix_directory";
const DIRECTORY_BY_VERSION_SURFACE_NAME: &str = "lix_directory_by_version";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixDirectoryProjection;

pub(crate) fn builtin_lix_directory_registration() -> ProjectionRegistration<LixDirectoryProjection>
{
    ProjectionRegistration::new(
        LixDirectoryProjection,
        crate::projections::ProjectionLifecycle::ReadTime,
    )
}

impl ProjectionTrait for LixDirectoryProjection {
    fn name(&self) -> &'static str {
        DIRECTORY_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<ProjectionInputSpec> {
        vec![
            ProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
            ProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        ]
    }

    fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
        vec![
            ProjectionSurfaceSpec::new(
                DIRECTORY_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::Default,
            ),
            ProjectionSurfaceSpec::new(
                DIRECTORY_BY_VERSION_SURFACE_NAME,
                SurfaceFamily::Filesystem,
                SurfaceVariant::ByVersion,
            ),
        ]
    }

    fn derive(&self, _input: &ProjectionInput) -> Result<Vec<DerivedRow>, LixError> {
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
    use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};
    use crate::projections::{ProjectionInputSpec, ProjectionTrait};

    #[test]
    fn filesystem_directory_projection_stays_declarative() {
        let projection = LixDirectoryProjection;

        assert_eq!(projection.name(), DIRECTORY_SURFACE_NAME);
        assert_eq!(
            projection.inputs(),
            vec![
                ProjectionInputSpec::tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                ProjectionInputSpec::untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
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
                    DIRECTORY_SURFACE_NAME.to_string(),
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::Default,
                ),
                (
                    DIRECTORY_BY_VERSION_SURFACE_NAME.to_string(),
                    SurfaceFamily::Filesystem,
                    SurfaceVariant::ByVersion,
                ),
            ]
        );
    }
}
