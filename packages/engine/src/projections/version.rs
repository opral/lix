//! Declarative `lix_version` projection definition.
//!
//! This is the first extracted projection behind `ProjectionTrait`. It proves
//! that a public SQL surface can be derived from mixed tracked and untracked
//! hydrated input without importing storage or runtime owners.

#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::catalog::{SurfaceFamily, SurfaceVariant};
use crate::contracts::artifacts::{RowIdentity, UntrackedRow};
use crate::projections::{
    DerivedRow, ProjectionHydratedRow, ProjectionInput, ProjectionInputSpec, ProjectionLifecycle,
    ProjectionRegistration, ProjectionSurfaceSpec, ProjectionTrait,
};
use crate::{LixError, Value};

const VERSION_SURFACE_NAME: &str = "lix_version";
const VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixVersionProjection;

pub(crate) fn builtin_lix_version_registration() -> ProjectionRegistration<LixVersionProjection> {
    ProjectionRegistration::new(LixVersionProjection, ProjectionLifecycle::ReadTime)
}

impl ProjectionTrait for LixVersionProjection {
    fn name(&self) -> &'static str {
        VERSION_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<ProjectionInputSpec> {
        vec![
            ProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
            ProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
        ]
    }

    fn surfaces(&self) -> Vec<ProjectionSurfaceSpec> {
        vec![ProjectionSurfaceSpec::new(
            VERSION_SURFACE_NAME,
            SurfaceFamily::Admin,
            SurfaceVariant::Default,
        )]
    }

    fn derive(&self, input: &ProjectionInput) -> Result<Vec<DerivedRow>, LixError> {
        let ref_commit_ids = version_ref_commit_ids(input);

        Ok(input
            .rows_for(&ProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY))
            .unwrap_or(&[])
            .iter()
            .filter_map(|row| match row {
                ProjectionHydratedRow::Tracked(row) => Some(row),
                ProjectionHydratedRow::Untracked(_) => None,
            })
            .map(|descriptor| {
                let version_id = descriptor
                    .property_text("id")
                    .unwrap_or_else(|| descriptor.entity_id.clone());
                let commit_id = ref_commit_ids.get(&version_id).cloned().unwrap_or_default();

                DerivedRow::new(
                    VERSION_SURFACE_NAME,
                    BTreeMap::from([
                        ("id".to_string(), Value::Text(version_id)),
                        (
                            "name".to_string(),
                            Value::Text(descriptor.property_text("name").unwrap_or_default()),
                        ),
                        (
                            "hidden".to_string(),
                            Value::Boolean(
                                bool_value(descriptor.values.get("hidden")).unwrap_or(false),
                            ),
                        ),
                        ("commit_id".to_string(), Value::Text(commit_id)),
                    ]),
                )
                .with_identity(RowIdentity::from_tracked_row(descriptor))
            })
            .collect())
    }
}

fn version_ref_commit_ids(input: &ProjectionInput) -> BTreeMap<String, String> {
    input
        .rows_for(&ProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY))
        .unwrap_or(&[])
        .iter()
        .filter_map(|row| match row {
            ProjectionHydratedRow::Untracked(row) => Some(row),
            ProjectionHydratedRow::Tracked(_) => None,
        })
        .map(version_ref_entry)
        .collect()
}

fn version_ref_entry(row: &UntrackedRow) -> (String, String) {
    let version_id = row
        .property_text("id")
        .unwrap_or_else(|| row.entity_id.clone());
    let commit_id = row.property_text("commit_id").unwrap_or_default();
    (version_id, commit_id)
}

fn bool_value(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Boolean(value)) => Some(*value),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::{
        builtin_lix_version_registration, LixVersionProjection, VERSION_DESCRIPTOR_SCHEMA_KEY,
        VERSION_REF_SCHEMA_KEY, VERSION_SURFACE_NAME,
    };
    use crate::contracts::artifacts::{TrackedRow, UntrackedRow};
    use crate::projections::{
        ProjectionHydratedRow, ProjectionInput, ProjectionInputRows, ProjectionInputSpec,
        ProjectionLifecycle, ProjectionRegistration, ProjectionTrait,
    };
    use crate::Value;

    #[test]
    fn derives_descriptor_only_rows_with_empty_commit_id() {
        let projection = LixVersionProjection;
        let input = ProjectionInput::new(vec![ProjectionInputRows::new(
            ProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
            vec![ProjectionHydratedRow::Tracked(sample_descriptor_row(
                "main", "Main", true,
            ))],
        )]);

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 1);
        assert_eq!(derived[0].surface_name, VERSION_SURFACE_NAME);
        assert_eq!(
            derived[0].values.get("id"),
            Some(&Value::Text("main".into()))
        );
        assert_eq!(
            derived[0].values.get("name"),
            Some(&Value::Text("Main".into()))
        );
        assert_eq!(derived[0].values.get("hidden"), Some(&Value::Boolean(true)));
        assert_eq!(
            derived[0].values.get("commit_id"),
            Some(&Value::Text(String::new()))
        );
        assert_eq!(
            derived[0].identity.as_ref().map(|id| id.entity_id.as_str()),
            Some("main")
        );
    }

    #[test]
    fn derives_descriptor_and_ref_rows_with_local_commit_id() {
        let projection = LixVersionProjection;
        let input = ProjectionInput::new(vec![
            ProjectionInputRows::new(
                ProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
                vec![ProjectionHydratedRow::Tracked(sample_descriptor_row(
                    "main", "Main", false,
                ))],
            ),
            ProjectionInputRows::new(
                ProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
                vec![ProjectionHydratedRow::Untracked(sample_version_ref_row(
                    "main",
                    "commit-123",
                ))],
            ),
        ]);

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 1);
        assert_eq!(
            derived[0].values.get("commit_id"),
            Some(&Value::Text("commit-123".into()))
        );
        assert_eq!(
            derived[0].values.get("hidden"),
            Some(&Value::Boolean(false))
        );
    }

    #[test]
    fn commit_id_is_local_ref_derived_not_descriptor_derived() {
        let projection = LixVersionProjection;
        let input = ProjectionInput::new(vec![
            ProjectionInputRows::new(
                ProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
                vec![
                    ProjectionHydratedRow::Tracked(sample_descriptor_row("main", "Main", false)),
                    ProjectionHydratedRow::Tracked(sample_descriptor_row("dev", "Dev", false)),
                ],
            ),
            ProjectionInputRows::new(
                ProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
                vec![ProjectionHydratedRow::Untracked(sample_version_ref_row(
                    "main",
                    "commit-main",
                ))],
            ),
        ]);

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 2);
        let commit_ids = derived
            .iter()
            .map(|row| {
                let id = row
                    .values
                    .get("id")
                    .and_then(|value| match value {
                        Value::Text(value) => Some(value.clone()),
                        _ => None,
                    })
                    .expect("derived version id should be text");
                let commit_id = row
                    .values
                    .get("commit_id")
                    .and_then(|value| match value {
                        Value::Text(value) => Some(value.clone()),
                        _ => None,
                    })
                    .expect("derived commit id should be text");
                (id, commit_id)
            })
            .collect::<BTreeMap<_, _>>();

        assert_eq!(commit_ids.get("main"), Some(&"commit-main".to_string()));
        assert_eq!(commit_ids.get("dev"), Some(&String::new()));
    }

    #[test]
    fn same_definition_can_be_reregistered_without_changing_derivation() {
        let read_time =
            ProjectionRegistration::new(LixVersionProjection, ProjectionLifecycle::ReadTime);
        let async_registration =
            ProjectionRegistration::new(LixVersionProjection, ProjectionLifecycle::Async);
        let input = ProjectionInput::new(vec![
            ProjectionInputRows::new(
                ProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
                vec![ProjectionHydratedRow::Tracked(sample_descriptor_row(
                    "main", "Main", false,
                ))],
            ),
            ProjectionInputRows::new(
                ProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
                vec![ProjectionHydratedRow::Untracked(sample_version_ref_row(
                    "main",
                    "commit-123",
                ))],
            ),
        ]);

        let read_time_rows = read_time
            .projection()
            .derive(&input)
            .expect("read-time derivation should succeed");
        let async_rows = async_registration
            .projection()
            .derive(&input)
            .expect("async derivation should succeed");

        assert_eq!(read_time_rows, async_rows);
        assert_eq!(
            builtin_lix_version_registration().lifecycle(),
            ProjectionLifecycle::ReadTime
        );
    }

    fn sample_descriptor_row(version_id: &str, name: &str, hidden: bool) -> TrackedRow {
        TrackedRow {
            entity_id: version_id.to_string(),
            schema_key: VERSION_DESCRIPTOR_SCHEMA_KEY.to_string(),
            schema_version: "1".into(),
            file_id: "lix".into(),
            version_id: "global".into(),
            global: true,
            plugin_key: "lix".into(),
            metadata: None,
            change_id: Some(format!("change-{version_id}")),
            writer_key: None,
            created_at: "2026-04-01T00:00:00Z".into(),
            updated_at: "2026-04-01T00:00:00Z".into(),
            values: BTreeMap::from([
                ("id".to_string(), Value::Text(version_id.to_string())),
                ("name".to_string(), Value::Text(name.to_string())),
                ("hidden".to_string(), Value::Boolean(hidden)),
            ]),
        }
    }

    fn sample_version_ref_row(version_id: &str, commit_id: &str) -> UntrackedRow {
        UntrackedRow {
            entity_id: version_id.to_string(),
            schema_key: VERSION_REF_SCHEMA_KEY.to_string(),
            schema_version: "1".into(),
            file_id: "lix".into(),
            version_id: "global".into(),
            global: true,
            plugin_key: "lix".into(),
            metadata: None,
            writer_key: None,
            created_at: "2026-04-01T00:00:00Z".into(),
            updated_at: "2026-04-01T00:00:00Z".into(),
            values: BTreeMap::from([
                ("id".to_string(), Value::Text(version_id.to_string())),
                ("commit_id".to_string(), Value::Text(commit_id.to_string())),
            ]),
        }
    }
}
