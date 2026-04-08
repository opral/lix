//! Declarative projection definitions.
//!
//! This module tree is the owner boundary introduced by Plan 31. It is where
//! extracted surface derivation definitions live before `live_state` consumes
//! registrations generically. Built-in registry assembly lives here, above the
//! generic `live_state` executor.

use std::sync::OnceLock;
use std::sync::Arc;

pub(crate) mod artifacts;
pub(crate) mod filesystem;
pub(crate) mod traits;
pub(crate) mod version;
pub(crate) use artifacts::{
    DerivedRow, ProjectionHydratedRow, ProjectionInput, ProjectionInputRows, ProjectionInputSpec,
    ProjectionInputVersionScope, ProjectionLifecycle, ProjectionRegistration,
    ProjectionStorageKind, ProjectionSurfaceSpec,
};
pub(crate) use traits::ProjectionTrait;

#[derive(Clone)]
pub(crate) struct RegisteredProjection {
    projection: Arc<dyn ProjectionTrait>,
    lifecycle: ProjectionLifecycle,
}

impl RegisteredProjection {
    pub(crate) fn new<P>(registration: ProjectionRegistration<P>) -> Self
    where
        P: ProjectionTrait + 'static,
    {
        let (projection, lifecycle) = registration.into_parts();
        Self {
            projection: Arc::new(projection),
            lifecycle,
        }
    }

    pub(crate) fn projection(&self) -> &dyn ProjectionTrait {
        self.projection.as_ref()
    }

    pub(crate) fn lifecycle(&self) -> ProjectionLifecycle {
        self.lifecycle
    }
}

#[derive(Clone)]
pub(crate) struct ProjectionRegistry {
    registrations: Vec<RegisteredProjection>,
}

impl ProjectionRegistry {
    pub(crate) fn new(registrations: Vec<RegisteredProjection>) -> Self {
        Self { registrations }
    }

    pub(crate) fn registrations(&self) -> &[RegisteredProjection] {
        &self.registrations
    }
}

static BUILTIN_PROJECTION_REGISTRY: OnceLock<ProjectionRegistry> = OnceLock::new();

pub(crate) fn builtin_projection_registry() -> &'static ProjectionRegistry {
    BUILTIN_PROJECTION_REGISTRY.get_or_init(|| {
        ProjectionRegistry::new(vec![RegisteredProjection::new(
            version::builtin_lix_version_registration(),
        )])
    })
}

#[cfg(test)]
mod tests {
    use super::builtin_projection_registry;
    use crate::projections::ProjectionLifecycle;

    #[test]
    fn builtin_registry_exposes_lix_version_read_time_registration() {
        let registrations = builtin_projection_registry().registrations();

        assert_eq!(registrations.len(), 1);
        assert_eq!(registrations[0].projection().name(), "lix_version");
        assert_eq!(registrations[0].lifecycle(), ProjectionLifecycle::ReadTime);
    }
}
