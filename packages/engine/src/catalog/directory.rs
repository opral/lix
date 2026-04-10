//! Catalog-owned `lix_directory` declarations.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{
    CatalogDerivedRow, CatalogProjectionDefinition, CatalogProjectionInput,
    CatalogProjectionInputSpec, CatalogProjectionInputVersionScope, CatalogProjectionLifecycle,
    CatalogProjectionRegistration, CatalogProjectionSourceRow, CatalogProjectionStorageKind,
    CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
};
use crate::{LixError, Value};

const DIRECTORY_SURFACE_NAME: &str = "lix_directory";
const DIRECTORY_BY_VERSION_SURFACE_NAME: &str = "lix_directory_by_version";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixDirectoryProjection;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixDirectoryByVersionProjection;

pub(crate) fn builtin_lix_directory_catalog_registration(
) -> CatalogProjectionRegistration<LixDirectoryProjection> {
    CatalogProjectionRegistration::new(LixDirectoryProjection, CatalogProjectionLifecycle::ReadTime)
}

pub(crate) fn builtin_lix_directory_by_version_catalog_registration(
) -> CatalogProjectionRegistration<LixDirectoryByVersionProjection> {
    CatalogProjectionRegistration::new(
        LixDirectoryByVersionProjection,
        CatalogProjectionLifecycle::ReadTime,
    )
}

impl CatalogProjectionDefinition for LixDirectoryProjection {
    fn name(&self) -> &'static str {
        DIRECTORY_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        requested_version_specs()
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![CatalogProjectionSurfaceSpec::new(
            DIRECTORY_SURFACE_NAME,
            SurfaceFamily::Filesystem,
            SurfaceVariant::Default,
        )]
    }

    fn derive(&self, input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        let Some(version_id) = input.context().requested_version_id() else {
            return Ok(Vec::new());
        };
        let commit_id = input
            .context()
            .current_head_commit_id(version_id)
            .unwrap_or_default()
            .to_string();
        let rows = derive_directory_rows_for_versions(input, &[version_id.to_string()])?;
        Ok(rows
            .into_iter()
            .map(|row| directory_row_to_surface(row, DIRECTORY_SURFACE_NAME, Some(&commit_id)))
            .collect())
    }
}

impl CatalogProjectionDefinition for LixDirectoryByVersionProjection {
    fn name(&self) -> &'static str {
        DIRECTORY_BY_VERSION_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        current_frontier_specs()
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![CatalogProjectionSurfaceSpec::new(
            DIRECTORY_BY_VERSION_SURFACE_NAME,
            SurfaceFamily::Filesystem,
            SurfaceVariant::ByVersion,
        )]
    }

    fn derive(&self, input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        let mut target_versions = input.context().current_committed_version_ids().to_vec();
        target_versions.push(crate::contracts::GLOBAL_VERSION_ID.to_string());
        target_versions.sort();
        target_versions.dedup();
        let rows = derive_directory_rows_for_versions(input, &target_versions)?;
        Ok(rows
            .into_iter()
            .map(|row| directory_row_to_surface(row, DIRECTORY_BY_VERSION_SURFACE_NAME, None))
            .collect())
    }
}

#[derive(Debug, Clone)]
struct DerivedDirectoryRow {
    identity: crate::contracts::RowIdentity,
    version_id: String,
    id: String,
    parent_id: Option<String>,
    name: String,
    path: Option<String>,
    hidden: bool,
    schema_key: String,
    schema_version: String,
    global: bool,
    change_id: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    commit_id: Option<String>,
    untracked: bool,
    lixcol_metadata: Option<String>,
}

fn derive_directory_rows_for_versions(
    input: &CatalogProjectionInput,
    target_versions: &[String],
) -> Result<Vec<DerivedDirectoryRow>, LixError> {
    let local_tracked = input
        .rows_for(&local_frontier_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY))
        .or_else(|| input.rows_for(&local_requested_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)))
        .unwrap_or(&[]);
    let local_untracked = input
        .rows_for(&local_frontier_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY))
        .or_else(|| input.rows_for(&local_requested_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY)))
        .unwrap_or(&[]);
    let global_tracked = input
        .rows_for(&global_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY))
        .unwrap_or(&[]);
    let global_untracked = input
        .rows_for(&global_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY))
        .unwrap_or(&[]);

    let mut rows = Vec::new();
    for version_id in target_versions {
        let effective_rows = resolve_effective_rows_for_version(
            version_id,
            local_tracked,
            local_untracked,
            global_tracked,
            global_untracked,
        );
        let path_rows = build_directory_rows(&effective_rows)?;
        for descriptor in effective_rows.values() {
            let Some(id) = descriptor.property_text("id") else {
                continue;
            };
            let hidden = bool_value(descriptor.values().get("hidden")).unwrap_or(false);
            let commit_id = if descriptor.storage() == CatalogProjectionStorageKind::Untracked {
                Some("untracked".to_string())
            } else {
                descriptor
                    .change_id()
                    .and_then(|change_id| input.context().commit_id_for_change(change_id))
                    .map(str::to_string)
            };
            rows.push(DerivedDirectoryRow {
                identity: descriptor.identity().clone(),
                version_id: version_id.clone(),
                id: id.clone(),
                parent_id: nullable_text(descriptor.values().get("parent_id")),
                name: descriptor.property_text("name").unwrap_or_default(),
                path: path_rows.get(&id).and_then(|row| row.path.clone()),
                hidden,
                schema_key: descriptor.schema_key.clone(),
                schema_version: descriptor.schema_version().unwrap_or_default().to_string(),
                global: descriptor.global().unwrap_or(false),
                change_id: descriptor.change_id().map(str::to_string),
                created_at: descriptor.created_at().map(str::to_string),
                updated_at: descriptor.updated_at().map(str::to_string),
                commit_id,
                untracked: descriptor.storage() == CatalogProjectionStorageKind::Untracked,
                lixcol_metadata: descriptor.metadata_text().map(str::to_string),
            });
        }
    }
    Ok(rows)
}

#[derive(Debug, Clone)]
struct DirectoryPathRow {
    path: Option<String>,
}

fn build_directory_rows(
    effective_dirs: &BTreeMap<String, &CatalogProjectionSourceRow>,
) -> Result<BTreeMap<String, DirectoryPathRow>, LixError> {
    let rows = effective_dirs
        .values()
        .filter_map(|row| {
            let id = row.property_text("id")?;
            Some((
                id.clone(),
                (
                    nullable_text(row.values().get("parent_id")),
                    row.property_text("name").unwrap_or_default(),
                ),
            ))
        })
        .collect::<BTreeMap<_, _>>();

    let mut derived = BTreeMap::new();
    for id in rows.keys() {
        let path = directory_path_for_id(id, &rows, &mut BTreeSet::new())?;
        derived.insert(id.clone(), DirectoryPathRow { path });
    }
    Ok(derived)
}

fn directory_path_for_id(
    id: &str,
    rows: &BTreeMap<String, (Option<String>, String)>,
    stack: &mut BTreeSet<String>,
) -> Result<Option<String>, LixError> {
    let Some((parent_id, name)) = rows.get(id) else {
        return Ok(None);
    };
    if !stack.insert(id.to_string()) {
        return Err(LixError::new(
            "LIX_ERROR_INVALID_ARGUMENT",
            format!("filesystem directory cycle detected at '{id}'"),
        ));
    }
    let path = match parent_id.as_deref() {
        None => Some(format!("/{name}/")),
        Some(parent_id) => {
            directory_path_for_id(parent_id, rows, stack)?.map(|parent| format!("{parent}{name}/"))
        }
    };
    stack.remove(id);
    Ok(path)
}

fn resolve_effective_rows_for_version<'a>(
    version_id: &str,
    local_tracked: &'a [CatalogProjectionSourceRow],
    local_untracked: &'a [CatalogProjectionSourceRow],
    global_tracked: &'a [CatalogProjectionSourceRow],
    global_untracked: &'a [CatalogProjectionSourceRow],
) -> BTreeMap<String, &'a CatalogProjectionSourceRow> {
    let mut entity_ids = BTreeSet::new();
    for row in local_tracked.iter().chain(local_untracked.iter()) {
        if row.version_id == version_id {
            entity_ids.insert(row.entity_id().to_string());
        }
    }
    for row in global_tracked.iter().chain(global_untracked.iter()) {
        entity_ids.insert(row.entity_id().to_string());
    }

    entity_ids
        .into_iter()
        .filter_map(|entity_id| {
            select_effective_row_for_entity(
                version_id,
                &entity_id,
                local_tracked,
                local_untracked,
                global_tracked,
                global_untracked,
            )
            .map(|row| (entity_id, row))
        })
        .collect()
}

fn select_effective_row_for_entity<'a>(
    version_id: &str,
    entity_id: &str,
    local_tracked: &'a [CatalogProjectionSourceRow],
    local_untracked: &'a [CatalogProjectionSourceRow],
    global_tracked: &'a [CatalogProjectionSourceRow],
    global_untracked: &'a [CatalogProjectionSourceRow],
) -> Option<&'a CatalogProjectionSourceRow> {
    let mut candidates = Vec::<(u8, &'a CatalogProjectionSourceRow)>::new();
    candidates.extend(
        local_untracked
            .iter()
            .filter(|row| row.version_id == version_id && row.entity_id() == entity_id)
            .map(|row| (0, row)),
    );
    candidates.extend(
        local_tracked
            .iter()
            .filter(|row| row.version_id == version_id && row.entity_id() == entity_id)
            .map(|row| (1, row)),
    );
    if version_id != crate::contracts::GLOBAL_VERSION_ID {
        candidates.extend(
            global_untracked
                .iter()
                .filter(|row| row.entity_id() == entity_id)
                .map(|row| (2, row)),
        );
        candidates.extend(
            global_tracked
                .iter()
                .filter(|row| row.entity_id() == entity_id)
                .map(|row| (3, row)),
        );
    }
    candidates.sort_by(
        |(left_precedence, left_row), (right_precedence, right_row)| {
            left_precedence
                .cmp(right_precedence)
                .then_with(|| right_row.updated_at().cmp(&left_row.updated_at()))
                .then_with(|| right_row.created_at().cmp(&left_row.created_at()))
                .then_with(|| right_row.change_id().cmp(&left_row.change_id()))
        },
    );
    candidates.into_iter().map(|(_, row)| row).next()
}

fn directory_row_to_surface(
    row: DerivedDirectoryRow,
    surface_name: &str,
    active_commit_id: Option<&str>,
) -> CatalogDerivedRow {
    let identity = row.identity.clone();
    let commit_id = active_commit_id
        .map(str::to_string)
        .or(row.commit_id)
        .unwrap_or_default();
    CatalogDerivedRow::new(
        surface_name,
        BTreeMap::from([
            ("id".to_string(), Value::Text(row.id)),
            (
                "parent_id".to_string(),
                row.parent_id.map(Value::Text).unwrap_or(Value::Null),
            ),
            ("name".to_string(), Value::Text(row.name)),
            (
                "path".to_string(),
                row.path.map(Value::Text).unwrap_or(Value::Null),
            ),
            ("hidden".to_string(), Value::Boolean(row.hidden)),
            (
                "lixcol_entity_id".to_string(),
                Value::Text(identity.entity_id.clone()),
            ),
            ("lixcol_schema_key".to_string(), Value::Text(row.schema_key)),
            (
                "lixcol_schema_version".to_string(),
                Value::Text(row.schema_version),
            ),
            ("lixcol_version_id".to_string(), Value::Text(row.version_id)),
            ("lixcol_global".to_string(), Value::Boolean(row.global)),
            (
                "lixcol_change_id".to_string(),
                row.change_id.map(Value::Text).unwrap_or(Value::Null),
            ),
            (
                "lixcol_created_at".to_string(),
                row.created_at.map(Value::Text).unwrap_or(Value::Null),
            ),
            (
                "lixcol_updated_at".to_string(),
                row.updated_at.map(Value::Text).unwrap_or(Value::Null),
            ),
            ("lixcol_commit_id".to_string(), Value::Text(commit_id)),
            (
                "lixcol_untracked".to_string(),
                Value::Boolean(row.untracked),
            ),
            (
                "lixcol_metadata".to_string(),
                row.lixcol_metadata.map(Value::Text).unwrap_or(Value::Null),
            ),
        ]),
    )
    .with_identity(identity)
}

fn requested_version_specs() -> Vec<CatalogProjectionInputSpec> {
    vec![
        local_requested_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        local_requested_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
    ]
}

fn current_frontier_specs() -> Vec<CatalogProjectionInputSpec> {
    vec![
        local_frontier_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        local_frontier_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
    ]
}

fn nullable_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Null) | None => None,
        _ => None,
    }
}

fn bool_value(value: Option<&Value>) -> Option<bool> {
    match value {
        Some(Value::Boolean(value)) => Some(*value),
        _ => None,
    }
}

fn local_requested_tracked(schema_key: &str) -> CatalogProjectionInputSpec {
    CatalogProjectionInputSpec::tracked(schema_key)
        .with_version_scope(CatalogProjectionInputVersionScope::RequestedVersion)
}

fn local_requested_untracked(schema_key: &str) -> CatalogProjectionInputSpec {
    CatalogProjectionInputSpec::untracked(schema_key)
        .with_version_scope(CatalogProjectionInputVersionScope::RequestedVersion)
}

fn local_frontier_tracked(schema_key: &str) -> CatalogProjectionInputSpec {
    CatalogProjectionInputSpec::tracked(schema_key)
        .with_version_scope(CatalogProjectionInputVersionScope::CurrentCommittedFrontier)
}

fn local_frontier_untracked(schema_key: &str) -> CatalogProjectionInputSpec {
    CatalogProjectionInputSpec::untracked(schema_key)
        .with_version_scope(CatalogProjectionInputVersionScope::CurrentCommittedFrontier)
}

fn global_tracked(schema_key: &str) -> CatalogProjectionInputSpec {
    CatalogProjectionInputSpec::tracked(schema_key)
        .with_version_scope(CatalogProjectionInputVersionScope::Global)
}

fn global_untracked(schema_key: &str) -> CatalogProjectionInputSpec {
    CatalogProjectionInputSpec::untracked(schema_key)
        .with_version_scope(CatalogProjectionInputVersionScope::Global)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogProjectionContext, CatalogProjectionInputRows};
    use crate::contracts::RowIdentity;

    #[test]
    fn default_directory_projection_derives_paths_and_active_commit_id() {
        let projection = LixDirectoryProjection;
        let input = CatalogProjectionInput::with_context(
            vec![
                CatalogProjectionInputRows::new(
                    local_requested_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                    vec![
                        sample_row(
                            "main",
                            "dir-root",
                            BTreeMap::from([
                                ("id".to_string(), Value::Text("dir-root".to_string())),
                                ("parent_id".to_string(), Value::Null),
                                ("name".to_string(), Value::Text("docs".to_string())),
                                ("hidden".to_string(), Value::Boolean(false)),
                            ]),
                        ),
                        sample_row(
                            "main",
                            "dir-child",
                            BTreeMap::from([
                                ("id".to_string(), Value::Text("dir-child".to_string())),
                                ("parent_id".to_string(), Value::Text("dir-root".to_string())),
                                ("name".to_string(), Value::Text("guides".to_string())),
                                ("hidden".to_string(), Value::Boolean(true)),
                            ]),
                        ),
                    ],
                ),
                CatalogProjectionInputRows::new(
                    local_requested_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(
                    global_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(
                    global_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                    vec![],
                ),
            ],
            CatalogProjectionContext {
                requested_version_id: Some("main".to_string()),
                current_committed_version_ids: vec!["main".to_string()],
                current_version_heads: BTreeMap::from([(
                    "main".to_string(),
                    "commit-main".to_string(),
                )]),
                change_commit_ids: BTreeMap::new(),
                blob_data_by_hash: BTreeMap::new(),
            },
        );

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 2);
        let child = derived
            .iter()
            .find(|row| row.values.get("id") == Some(&Value::Text("dir-child".to_string())))
            .expect("child directory row");
        assert_eq!(
            child.values.get("path"),
            Some(&Value::Text("/docs/guides/".to_string()))
        );
        assert_eq!(
            child.values.get("lixcol_commit_id"),
            Some(&Value::Text("commit-main".to_string()))
        );
    }

    fn sample_row(
        version_id: &str,
        entity_id: &str,
        values: BTreeMap<String, Value>,
    ) -> CatalogProjectionSourceRow {
        CatalogProjectionSourceRow::new(
            CatalogProjectionStorageKind::Tracked,
            RowIdentity {
                schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                version_id: version_id.to_string(),
                entity_id: entity_id.to_string(),
                file_id: "lix".to_string(),
            },
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            version_id,
            values,
        )
        .with_live_metadata(
            "1",
            "lix",
            None,
            None,
            None,
            false,
            Some("2026-04-10T00:00:00Z".to_string()),
            Some("2026-04-10T00:00:00Z".to_string()),
        )
    }
}
