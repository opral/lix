//! Declarative projection definitions.
//!
//! This module tree is the owner boundary introduced by Plan 31. It is where
//! extracted surface derivation definitions live before `live_state` consumes
//! registrations generically. Built-in registry assembly lives here, above the
//! generic `live_state` executor.

use std::sync::OnceLock;

use crate::contracts::projection::{ProjectionRegistry, RegisteredProjection};

pub(crate) mod filesystem;
pub(crate) mod version;

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
    use crate::contracts::artifacts::ProjectionLifecycle;

    #[test]
    fn builtin_registry_exposes_lix_version_read_time_registration() {
        let registrations = builtin_projection_registry().registrations();

        assert_eq!(registrations.len(), 1);
        assert_eq!(registrations[0].projection().name(), "lix_version");
        assert_eq!(registrations[0].lifecycle(), ProjectionLifecycle::ReadTime);
    }
}
