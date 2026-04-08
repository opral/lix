use std::collections::BTreeMap;

use crate::common::types::Value;
use crate::contracts::artifacts::{RowIdentity, TrackedRow, UntrackedRow};
use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};

/// Projection execution lifecycle.
///
/// The same projection definition can be evaluated in multiple lifecycles. The
/// lifecycle controls when projection rows are derived and maintained, not what
/// the projection means.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ProjectionLifecycle {
    /// Evaluate in the read path from tracked/untracked source state without
    /// requiring maintained persisted projection rows.
    ReadTime,
    /// Update projection rows in the same source-state mutation path that
    /// changes the projection's inputs.
    WriteTime,
    /// Update projection rows later through background catch-up/replay.
    Async,
}

/// Behavior-neutral pairing of a projection definition with the lifecycle it
/// should run under.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ProjectionRegistration<P> {
    projection: P,
    lifecycle: ProjectionLifecycle,
}

#[allow(dead_code)]
impl<P> ProjectionRegistration<P> {
    pub(crate) fn new(projection: P, lifecycle: ProjectionLifecycle) -> Self {
        Self {
            projection,
            lifecycle,
        }
    }

    pub(crate) fn projection(&self) -> &P {
        &self.projection
    }

    pub(crate) fn lifecycle(&self) -> ProjectionLifecycle {
        self.lifecycle
    }

    pub(crate) fn into_parts(self) -> (P, ProjectionLifecycle) {
        (self.projection, self.lifecycle)
    }
}

/// Storage class for a projection input.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ProjectionStorageKind {
    Tracked,
    Untracked,
}

/// Version-scope contract for one projection input.
///
/// This keeps projection definitions declarative while allowing hydration to
/// choose the right committed-state slice for global or local-lane inputs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) enum ProjectionInputVersionScope {
    SchemaDefault,
    Global,
    CurrentCommittedFrontier,
}

/// Declarative description of one schema-backed projection input.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ProjectionInputSpec {
    pub(crate) schema_key: String,
    pub(crate) storage: ProjectionStorageKind,
    pub(crate) version_scope: ProjectionInputVersionScope,
}

#[allow(dead_code)]
impl ProjectionInputSpec {
    pub(crate) fn tracked(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            storage: ProjectionStorageKind::Tracked,
            version_scope: ProjectionInputVersionScope::SchemaDefault,
        }
    }

    pub(crate) fn untracked(schema_key: impl Into<String>) -> Self {
        Self {
            schema_key: schema_key.into(),
            storage: ProjectionStorageKind::Untracked,
            version_scope: ProjectionInputVersionScope::SchemaDefault,
        }
    }

    pub(crate) fn with_version_scope(mut self, version_scope: ProjectionInputVersionScope) -> Self {
        self.version_scope = version_scope;
        self
    }
}

/// Declarative description of a public surface served by a projection.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct ProjectionSurfaceSpec {
    pub(crate) public_name: String,
    pub(crate) surface_family: SurfaceFamily,
    pub(crate) surface_variant: SurfaceVariant,
}

#[allow(dead_code)]
impl ProjectionSurfaceSpec {
    pub(crate) fn new(
        public_name: impl Into<String>,
        surface_family: SurfaceFamily,
        surface_variant: SurfaceVariant,
    ) -> Self {
        Self {
            public_name: public_name.into(),
            surface_family,
            surface_variant,
        }
    }
}

/// Hydrated source row for projection derivation.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) enum ProjectionHydratedRow {
    Tracked(TrackedRow),
    Untracked(UntrackedRow),
}

#[allow(dead_code)]
impl ProjectionHydratedRow {
    pub(crate) fn storage(&self) -> ProjectionStorageKind {
        match self {
            Self::Tracked(_) => ProjectionStorageKind::Tracked,
            Self::Untracked(_) => ProjectionStorageKind::Untracked,
        }
    }

    pub(crate) fn identity(&self) -> RowIdentity {
        match self {
            Self::Tracked(row) => RowIdentity::from_tracked_row(row),
            Self::Untracked(row) => RowIdentity::from_untracked_row(row),
        }
    }

    pub(crate) fn schema_key(&self) -> &str {
        match self {
            Self::Tracked(row) => row.schema_key.as_str(),
            Self::Untracked(row) => row.schema_key.as_str(),
        }
    }

    pub(crate) fn version_id(&self) -> &str {
        match self {
            Self::Tracked(row) => row.version_id.as_str(),
            Self::Untracked(row) => row.version_id.as_str(),
        }
    }

    pub(crate) fn values(&self) -> &BTreeMap<String, Value> {
        match self {
            Self::Tracked(row) => &row.values,
            Self::Untracked(row) => &row.values,
        }
    }
}

/// Hydrated rows for one declared projection input.
#[derive(Debug, Clone, PartialEq)]
#[allow(dead_code)]
pub(crate) struct ProjectionInputRows {
    pub(crate) spec: ProjectionInputSpec,
    pub(crate) rows: Vec<ProjectionHydratedRow>,
}

#[allow(dead_code)]
impl ProjectionInputRows {
    pub(crate) fn new(spec: ProjectionInputSpec, rows: Vec<ProjectionHydratedRow>) -> Self {
        Self { spec, rows }
    }
}

/// Fully hydrated input bundle passed to projection derivation.
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub(crate) struct ProjectionInput {
    pub(crate) inputs: Vec<ProjectionInputRows>,
}

#[allow(dead_code)]
impl ProjectionInput {
    pub(crate) fn new(inputs: Vec<ProjectionInputRows>) -> Self {
        Self { inputs }
    }

    pub(crate) fn rows_for(&self, spec: &ProjectionInputSpec) -> Option<&[ProjectionHydratedRow]> {
        self.inputs
            .iter()
            .find(|input| &input.spec == spec)
            .map(|input| input.rows.as_slice())
    }
}

/// Neutral row derived by a projection for one public surface.
#[derive(Debug, Clone, PartialEq, Default)]
#[allow(dead_code)]
pub(crate) struct DerivedRow {
    pub(crate) surface_name: String,
    pub(crate) identity: Option<RowIdentity>,
    pub(crate) values: BTreeMap<String, Value>,
}

#[allow(dead_code)]
impl DerivedRow {
    pub(crate) fn new(surface_name: impl Into<String>, values: BTreeMap<String, Value>) -> Self {
        Self {
            surface_name: surface_name.into(),
            identity: None,
            values,
        }
    }

    pub(crate) fn with_identity(mut self, identity: RowIdentity) -> Self {
        self.identity = Some(identity);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        DerivedRow, ProjectionHydratedRow, ProjectionInput, ProjectionInputRows,
        ProjectionInputSpec, ProjectionInputVersionScope, ProjectionLifecycle,
        ProjectionRegistration, ProjectionStorageKind, ProjectionSurfaceSpec,
    };
    use crate::contracts::artifacts::{RowIdentity, TrackedRow, UntrackedRow};
    use crate::contracts::surface::{SurfaceFamily, SurfaceVariant};
    use crate::Value;

    #[test]
    fn projection_registration_keeps_definition_separate_from_lifecycle() {
        let registration =
            ProjectionRegistration::new("demo_projection", ProjectionLifecycle::WriteTime);

        assert_eq!(registration.projection(), &"demo_projection");
        assert_eq!(registration.lifecycle(), ProjectionLifecycle::WriteTime);
    }

    #[test]
    fn projection_registration_round_trips_into_parts() {
        let registration = ProjectionRegistration::new(7_u32, ProjectionLifecycle::Async);

        let (projection, lifecycle) = registration.into_parts();

        assert_eq!(projection, 7);
        assert_eq!(lifecycle, ProjectionLifecycle::Async);
    }

    #[test]
    fn projection_lifecycle_freezes_read_time_name() {
        let registration = ProjectionRegistration::new((), ProjectionLifecycle::ReadTime);

        assert_eq!(registration.lifecycle(), ProjectionLifecycle::ReadTime);
    }

    #[test]
    fn projection_input_spec_tracks_storage_kind() {
        let tracked = ProjectionInputSpec::tracked("lix_version_descriptor");
        let untracked = ProjectionInputSpec::untracked("lix_version_ref");

        assert_eq!(tracked.storage, ProjectionStorageKind::Tracked);
        assert_eq!(tracked.schema_key, "lix_version_descriptor");
        assert_eq!(
            tracked.version_scope,
            ProjectionInputVersionScope::SchemaDefault
        );
        assert_eq!(untracked.storage, ProjectionStorageKind::Untracked);
        assert_eq!(untracked.schema_key, "lix_version_ref");
        assert_eq!(
            untracked.version_scope,
            ProjectionInputVersionScope::SchemaDefault
        );
    }

    #[test]
    fn projection_input_spec_can_override_version_scope() {
        let scoped = ProjectionInputSpec::tracked("demo_schema")
            .with_version_scope(ProjectionInputVersionScope::CurrentCommittedFrontier);

        assert_eq!(scoped.storage, ProjectionStorageKind::Tracked);
        assert_eq!(scoped.schema_key, "demo_schema");
        assert_eq!(
            scoped.version_scope,
            ProjectionInputVersionScope::CurrentCommittedFrontier
        );
    }

    #[test]
    fn projection_surface_spec_stays_declarative() {
        let spec = ProjectionSurfaceSpec::new(
            "lix_version",
            SurfaceFamily::Admin,
            SurfaceVariant::Default,
        );

        assert_eq!(spec.public_name, "lix_version");
        assert_eq!(spec.surface_family, SurfaceFamily::Admin);
        assert_eq!(spec.surface_variant, SurfaceVariant::Default);
    }

    #[test]
    fn projection_hydrated_row_reuses_existing_row_contracts() {
        let tracked = ProjectionHydratedRow::Tracked(sample_tracked_row());
        let untracked = ProjectionHydratedRow::Untracked(sample_untracked_row());

        assert_eq!(tracked.storage(), ProjectionStorageKind::Tracked);
        assert_eq!(tracked.schema_key(), "demo_schema");
        assert_eq!(tracked.version_id(), "v1");
        assert_eq!(
            tracked.values().get("name"),
            Some(&Value::Text("tracked".into()))
        );
        assert_eq!(
            tracked.identity(),
            RowIdentity {
                schema_key: "demo_schema".into(),
                version_id: "v1".into(),
                entity_id: "entity-1".into(),
                file_id: "file-1".into(),
            }
        );

        assert_eq!(untracked.storage(), ProjectionStorageKind::Untracked);
        assert_eq!(
            untracked.values().get("name"),
            Some(&Value::Text("untracked".into()))
        );
    }

    #[test]
    fn projection_input_groups_rows_by_declared_spec() {
        let tracked_spec = ProjectionInputSpec::tracked("lix_version_descriptor");
        let untracked_spec = ProjectionInputSpec::untracked("lix_version_ref");

        let input = ProjectionInput::new(vec![
            ProjectionInputRows::new(
                tracked_spec.clone(),
                vec![ProjectionHydratedRow::Tracked(sample_tracked_row())],
            ),
            ProjectionInputRows::new(
                untracked_spec.clone(),
                vec![ProjectionHydratedRow::Untracked(sample_untracked_row())],
            ),
        ]);

        let tracked_rows = input.rows_for(&tracked_spec).expect("tracked rows");
        let untracked_rows = input.rows_for(&untracked_spec).expect("untracked rows");

        assert_eq!(tracked_rows.len(), 1);
        assert_eq!(untracked_rows.len(), 1);
        assert_eq!(tracked_rows[0].storage(), ProjectionStorageKind::Tracked);
        assert_eq!(
            untracked_rows[0].storage(),
            ProjectionStorageKind::Untracked
        );
    }

    #[test]
    fn derived_row_keeps_surface_name_separate_from_identity() {
        let derived = DerivedRow::new(
            "lix_version",
            BTreeMap::from([
                ("id".to_string(), Value::Text("version-1".into())),
                ("name".to_string(), Value::Text("main".into())),
            ]),
        )
        .with_identity(RowIdentity {
            schema_key: "lix_version_descriptor".into(),
            version_id: "v1".into(),
            entity_id: "version-1".into(),
            file_id: "file-1".into(),
        });

        assert_eq!(derived.surface_name, "lix_version");
        assert_eq!(
            derived.identity,
            Some(RowIdentity {
                schema_key: "lix_version_descriptor".into(),
                version_id: "v1".into(),
                entity_id: "version-1".into(),
                file_id: "file-1".into(),
            })
        );
    }

    fn sample_tracked_row() -> TrackedRow {
        TrackedRow {
            entity_id: "entity-1".into(),
            schema_key: "demo_schema".into(),
            schema_version: "1".into(),
            file_id: "file-1".into(),
            version_id: "v1".into(),
            global: false,
            plugin_key: "demo".into(),
            metadata: None,
            writer_key: None,
            created_at: "2026-01-01T00:00:00.000Z".into(),
            values: BTreeMap::from([("name".to_string(), Value::Text("tracked".into()))]),
            updated_at: "2026-01-01T00:00:00.000Z".into(),
            change_id: Some("change-1".into()),
        }
    }

    fn sample_untracked_row() -> UntrackedRow {
        UntrackedRow {
            entity_id: "entity-2".into(),
            schema_key: "demo_schema".into(),
            schema_version: "1".into(),
            file_id: "file-2".into(),
            version_id: "v1".into(),
            global: false,
            plugin_key: "demo".into(),
            metadata: None,
            writer_key: None,
            created_at: "2026-01-01T00:00:00.000Z".into(),
            values: BTreeMap::from([("name".to_string(), Value::Text("untracked".into()))]),
            updated_at: "2026-01-01T00:00:00.000Z".into(),
        }
    }
}
