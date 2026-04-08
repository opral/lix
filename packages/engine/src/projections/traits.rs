use crate::common::error::LixError;
use crate::projections::{DerivedRow, ProjectionInput, ProjectionInputSpec, ProjectionSurfaceSpec};

/// Declarative projection definition boundary.
///
/// `ProjectionTrait` describes:
/// - which tracked/untracked inputs a projection needs
/// - which public surfaces it serves
/// - how hydrated source input is turned into derived rows
///
/// It does not own:
/// - storage hydration
/// - replay/catch-up
/// - readiness/progress/checkpointing
/// - runtime surface binding
///
/// Lifecycle is intentionally not part of the trait. The same projection
/// definition can be registered under different lifecycles.
#[allow(dead_code)]
pub(crate) trait ProjectionTrait: Send + Sync {
    fn name(&self) -> &'static str;

    fn inputs(&self) -> Vec<ProjectionInputSpec>;

    fn surfaces(&self) -> Vec<ProjectionSurfaceSpec>;

    fn derive(&self, input: &ProjectionInput) -> Result<Vec<DerivedRow>, LixError>;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::ProjectionTrait;
    use crate::projections::{
        DerivedRow, ProjectionHydratedRow, ProjectionInput, ProjectionInputRows,
        ProjectionInputSpec, ProjectionLifecycle, ProjectionRegistration,
        ProjectionSurfaceSpec,
    };
    use crate::Value;
    use crate::{
        contracts::artifacts::RowIdentity, contracts::artifacts::UntrackedRow,
        contracts::surface::{SurfaceFamily, SurfaceVariant},
    };

    #[test]
    fn same_projection_definition_can_be_registered_under_multiple_lifecycles() {
        let read_time = ProjectionRegistration::new(DemoProjection, ProjectionLifecycle::ReadTime);
        let write_time =
            ProjectionRegistration::new(DemoProjection, ProjectionLifecycle::WriteTime);

        assert_eq!(read_time.projection().name(), "demo_projection");
        assert_eq!(write_time.projection().name(), "demo_projection");
        assert_eq!(read_time.lifecycle(), ProjectionLifecycle::ReadTime);
        assert_eq!(write_time.lifecycle(), ProjectionLifecycle::WriteTime);
    }

    #[test]
    fn projection_trait_stays_declarative_over_inputs_and_surfaces() {
        let projection = DemoProjection;

        assert_eq!(
            projection.inputs(),
            vec![ProjectionInputSpec::untracked("lix_version_ref")]
        );
        assert_eq!(
            projection.surfaces(),
            vec![ProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        );
    }

    #[test]
    fn projection_trait_derives_rows_from_hydrated_input_without_storage_access() {
        let projection = DemoProjection;
        let input_spec = ProjectionInputSpec::untracked("lix_version_ref");
        let input = ProjectionInput::new(vec![ProjectionInputRows::new(
            input_spec,
            vec![ProjectionHydratedRow::Untracked(sample_version_ref_row())],
        )]);

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].surface_name, "lix_version");
        assert_eq!(
            derived[0].values.get("version_id"),
            Some(&Value::Text("version-1".into()))
        );
        assert_eq!(
            derived[0].identity.as_ref().map(|id| id.entity_id.as_str()),
            Some("ref-1")
        );
    }

    #[derive(Clone, Copy)]
    struct DemoProjection;

    impl ProjectionTrait for DemoProjection {
        fn name(&self) -> &'static str {
            "demo_projection"
        }

        fn inputs(&self) -> Vec<ProjectionInputSpec> {
            vec![ProjectionInputSpec::untracked("lix_version_ref")]
        }

        fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
            vec![ProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        }

        fn derive(&self, input: &ProjectionInput) -> Result<Vec<DerivedRow>, crate::LixError> {
            let Some(rows) = input.rows_for(&ProjectionInputSpec::untracked("lix_version_ref"))
            else {
                return Ok(Vec::new());
            };

            Ok(rows
                .iter()
                .filter_map(|row| match row {
                    ProjectionHydratedRow::Untracked(row) => Some(row),
                    ProjectionHydratedRow::Tracked(_) => None,
                })
                .map(|row| {
                    DerivedRow::new(
                        "lix_version",
                        BTreeMap::from([(
                            "version_id".to_string(),
                            row.values.get("version_id").cloned().unwrap_or(Value::Null),
                        )]),
                    )
                    .with_identity(RowIdentity::from_untracked_row(row))
                })
                .collect())
        }
    }

    fn sample_version_ref_row() -> UntrackedRow {
        UntrackedRow {
            entity_id: "ref-1".into(),
            schema_key: "lix_version_ref".into(),
            schema_version: "1".into(),
            file_id: "lix".into(),
            version_id: "global".into(),
            global: true,
            plugin_key: "lix".into(),
            metadata: None,
            writer_key: None,
            created_at: "2026-01-01T00:00:00.000Z".into(),
            values: BTreeMap::from([(
                "version_id".to_string(),
                Value::Text("version-1".into()),
            )]),
            updated_at: "2026-01-01T00:00:00.000Z".into(),
        }
    }
}
