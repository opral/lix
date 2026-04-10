//! Catalog-owned `lix_version` declaration.

#![allow(dead_code)]

use std::collections::BTreeMap;

use crate::catalog::{
    CatalogDerivedRow, CatalogProjectionDefinition, CatalogProjectionInput,
    CatalogProjectionInputSpec, CatalogProjectionLifecycle, CatalogProjectionRegistration,
    CatalogProjectionSourceRow, CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
};
use crate::{LixError, Value};

const VERSION_SURFACE_NAME: &str = "lix_version";
const VERSION_DESCRIPTOR_SCHEMA_KEY: &str = "lix_version_descriptor";
const VERSION_REF_SCHEMA_KEY: &str = "lix_version_ref";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixVersionProjection;

pub(crate) fn builtin_lix_version_catalog_registration(
) -> CatalogProjectionRegistration<LixVersionProjection> {
    CatalogProjectionRegistration::new(LixVersionProjection, CatalogProjectionLifecycle::ReadTime)
}

impl CatalogProjectionDefinition for LixVersionProjection {
    fn name(&self) -> &'static str {
        VERSION_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        vec![
            CatalogProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
            CatalogProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
        ]
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![CatalogProjectionSurfaceSpec::new(
            VERSION_SURFACE_NAME,
            SurfaceFamily::Admin,
            SurfaceVariant::Default,
        )]
    }

    fn derive(&self, input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        derive_lix_version_rows(input)
    }
}

fn derive_lix_version_rows(
    input: &CatalogProjectionInput,
) -> Result<Vec<CatalogDerivedRow>, LixError> {
    let ref_commit_ids = version_ref_commit_ids(input);

    Ok(input
        .rows_for(&CatalogProjectionInputSpec::tracked(
            VERSION_DESCRIPTOR_SCHEMA_KEY,
        ))
        .unwrap_or(&[])
        .iter()
        .filter(|row| {
            row.storage() == crate::catalog::CatalogProjectionStorageKind::Tracked
                && !row.is_tombstone()
        })
        .map(|descriptor| {
            let version_id = descriptor
                .property_text("id")
                .unwrap_or_else(|| descriptor.entity_id().to_string());
            let commit_id = ref_commit_ids.get(&version_id).cloned().unwrap_or_default();

            CatalogDerivedRow::new(
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
                            bool_value(descriptor.values().get("hidden")).unwrap_or(false),
                        ),
                    ),
                    ("commit_id".to_string(), Value::Text(commit_id)),
                ]),
            )
            .with_identity(descriptor.identity().clone())
        })
        .collect())
}

fn version_ref_commit_ids(input: &CatalogProjectionInput) -> BTreeMap<String, String> {
    input
        .rows_for(&CatalogProjectionInputSpec::untracked(
            VERSION_REF_SCHEMA_KEY,
        ))
        .unwrap_or(&[])
        .iter()
        .filter(|row| {
            row.storage() == crate::catalog::CatalogProjectionStorageKind::Untracked
                && !row.is_tombstone()
        })
        .map(version_ref_entry)
        .collect()
}

fn version_ref_entry(row: &CatalogProjectionSourceRow) -> (String, String) {
    let version_id = row
        .property_text("id")
        .unwrap_or_else(|| row.entity_id().to_string());
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
        LixVersionProjection, VERSION_DESCRIPTOR_SCHEMA_KEY, VERSION_REF_SCHEMA_KEY,
        VERSION_SURFACE_NAME,
    };
    use crate::catalog::{
        CatalogProjectionDefinition, CatalogProjectionInput, CatalogProjectionInputRows,
        CatalogProjectionInputSpec, CatalogProjectionLifecycle, CatalogProjectionRegistration,
        CatalogProjectionSourceRow, CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
    };
    use crate::contracts::artifacts::RowIdentity;
    use crate::Value;

    #[test]
    fn derives_descriptor_only_rows_with_empty_commit_id() {
        let projection = LixVersionProjection;
        let input = CatalogProjectionInput::new(vec![CatalogProjectionInputRows::new(
            CatalogProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
            vec![sample_descriptor_row("main", "Main", true)],
        )]);

        let derived = CatalogProjectionDefinition::derive(&projection, &input)
            .expect("derive should succeed");

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
        let input = CatalogProjectionInput::new(vec![
            CatalogProjectionInputRows::new(
                CatalogProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
                vec![sample_descriptor_row("main", "Main", false)],
            ),
            CatalogProjectionInputRows::new(
                CatalogProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
                vec![sample_version_ref_row("main", "commit-123")],
            ),
        ]);

        let derived = CatalogProjectionDefinition::derive(&projection, &input)
            .expect("derive should succeed");

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
    fn tombstoned_descriptor_rows_do_not_derive_visible_versions() {
        let projection = LixVersionProjection;
        let input = CatalogProjectionInput::new(vec![CatalogProjectionInputRows::new(
            CatalogProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
            vec![sample_descriptor_row("main", "Main", false).with_tombstone(true)],
        )]);

        let derived = CatalogProjectionDefinition::derive(&projection, &input)
            .expect("derive should succeed");

        assert!(derived.is_empty());
    }

    #[test]
    fn commit_id_is_local_ref_derived_not_descriptor_derived() {
        let projection = LixVersionProjection;
        let input = CatalogProjectionInput::new(vec![
            CatalogProjectionInputRows::new(
                CatalogProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
                vec![
                    sample_descriptor_row("main", "Main", false),
                    sample_descriptor_row("dev", "Dev", false),
                ],
            ),
            CatalogProjectionInputRows::new(
                CatalogProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
                vec![sample_version_ref_row("main", "commit-main")],
            ),
        ]);

        let derived = CatalogProjectionDefinition::derive(&projection, &input)
            .expect("derive should succeed");

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
    fn same_definition_can_be_reregistered_without_changing_catalog_derivation() {
        let read_time = CatalogProjectionRegistration::new(
            LixVersionProjection,
            CatalogProjectionLifecycle::ReadTime,
        );
        let async_registration = CatalogProjectionRegistration::new(
            LixVersionProjection,
            CatalogProjectionLifecycle::Async,
        );
        let input = CatalogProjectionInput::new(vec![
            CatalogProjectionInputRows::new(
                CatalogProjectionInputSpec::tracked(VERSION_DESCRIPTOR_SCHEMA_KEY),
                vec![sample_descriptor_row("main", "Main", false)],
            ),
            CatalogProjectionInputRows::new(
                CatalogProjectionInputSpec::untracked(VERSION_REF_SCHEMA_KEY),
                vec![sample_version_ref_row("main", "commit-main")],
            ),
        ]);

        assert_eq!(
            CatalogProjectionDefinition::derive(read_time.projection(), &input)
                .expect("read-time derive should succeed"),
            CatalogProjectionDefinition::derive(async_registration.projection(), &input)
                .expect("async derive should succeed"),
        );
    }

    #[test]
    fn catalog_definition_stays_declarative_over_inputs_and_surfaces() {
        let projection = LixVersionProjection;

        assert_eq!(
            CatalogProjectionDefinition::inputs(&projection)
                .into_iter()
                .map(|spec| spec.schema_key)
                .collect::<Vec<_>>(),
            vec![
                VERSION_DESCRIPTOR_SCHEMA_KEY.to_string(),
                VERSION_REF_SCHEMA_KEY.to_string()
            ]
        );
        assert_eq!(
            CatalogProjectionDefinition::surfaces(&projection),
            vec![CatalogProjectionSurfaceSpec::new(
                "lix_version",
                SurfaceFamily::Admin,
                SurfaceVariant::Default,
            )]
        );
    }

    #[test]
    fn catalog_registration_can_be_reregistered_without_changing_definition() {
        let read_time = CatalogProjectionRegistration::new(
            LixVersionProjection,
            CatalogProjectionLifecycle::ReadTime,
        );
        let async_registration = CatalogProjectionRegistration::new(
            LixVersionProjection,
            CatalogProjectionLifecycle::Async,
        );

        assert_eq!(
            CatalogProjectionDefinition::name(read_time.projection()),
            CatalogProjectionDefinition::name(async_registration.projection())
        );
        assert_eq!(
            CatalogProjectionDefinition::inputs(read_time.projection()),
            CatalogProjectionDefinition::inputs(async_registration.projection()),
        );
    }

    fn sample_descriptor_row(id: &str, name: &str, hidden: bool) -> CatalogProjectionSourceRow {
        CatalogProjectionSourceRow::new(
            crate::catalog::CatalogProjectionStorageKind::Tracked,
            RowIdentity {
                schema_key: VERSION_DESCRIPTOR_SCHEMA_KEY.into(),
                version_id: "global".into(),
                entity_id: id.into(),
                file_id: "lix".into(),
            },
            VERSION_DESCRIPTOR_SCHEMA_KEY,
            "global",
            BTreeMap::from([
                ("id".to_string(), Value::Text(id.to_string())),
                ("name".to_string(), Value::Text(name.to_string())),
                ("hidden".to_string(), Value::Boolean(hidden)),
            ]),
        )
    }

    fn sample_version_ref_row(id: &str, commit_id: &str) -> CatalogProjectionSourceRow {
        CatalogProjectionSourceRow::new(
            crate::catalog::CatalogProjectionStorageKind::Untracked,
            RowIdentity {
                schema_key: VERSION_REF_SCHEMA_KEY.into(),
                version_id: "global".into(),
                entity_id: id.into(),
                file_id: "lix".into(),
            },
            VERSION_REF_SCHEMA_KEY,
            "global",
            BTreeMap::from([
                ("id".to_string(), Value::Text(id.to_string())),
                ("commit_id".to_string(), Value::Text(commit_id.to_string())),
            ]),
        )
    }
}
