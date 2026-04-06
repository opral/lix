//! Declarative projection definitions.
//!
//! This module tree is the owner boundary introduced by Plan 31. It is where
//! extracted surface derivation definitions live before `live_state` consumes
//! registrations generically. The module intentionally stays free of runtime
//! wiring beyond the internal built-in registry wrapper.

use crate::contracts::artifacts::{ProjectionLifecycle, ProjectionRegistration};
use crate::contracts::traits::ProjectionTrait;

pub(crate) mod filesystem;
pub(crate) mod version;

/// Type-erased built-in projection registration for engine-owned enumeration.
///
/// The generic `ProjectionRegistration<P>` contract stays in `contracts/`.
/// This wrapper only exists so the engine can hold heterogeneous built-in
/// projections in one internal registry.
#[allow(dead_code)]
pub(crate) struct BuiltinProjectionRegistration {
    projection: Box<dyn ProjectionTrait>,
    lifecycle: ProjectionLifecycle,
}

#[allow(dead_code)]
impl BuiltinProjectionRegistration {
    pub(crate) fn new<P>(registration: ProjectionRegistration<P>) -> Self
    where
        P: ProjectionTrait + 'static,
    {
        let (projection, lifecycle) = registration.into_parts();
        Self {
            projection: Box::new(projection),
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

#[allow(dead_code)]
pub(crate) fn builtin_projection_registrations() -> Vec<BuiltinProjectionRegistration> {
    vec![BuiltinProjectionRegistration::new(
        version::builtin_lix_version_registration(),
    )]
}

#[cfg(test)]
mod tests {
    use super::builtin_projection_registrations;
    use crate::contracts::artifacts::ProjectionLifecycle;

    #[test]
    fn builtin_registry_exposes_lix_version_read_time_registration() {
        let registrations = builtin_projection_registrations();

        assert_eq!(registrations.len(), 1);
        assert_eq!(registrations[0].projection().name(), "lix_version");
        assert_eq!(registrations[0].lifecycle(), ProjectionLifecycle::ReadTime);
    }
}
