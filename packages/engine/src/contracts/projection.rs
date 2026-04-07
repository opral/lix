use std::sync::Arc;

use crate::contracts::artifacts::{ProjectionLifecycle, ProjectionRegistration};
use crate::contracts::traits::ProjectionTrait;

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
