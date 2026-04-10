//! Catalog-owned `lix_file` declarations.

#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};

use crate::catalog::{
    CatalogDerivedRow, CatalogProjectionDefinition, CatalogProjectionInput,
    CatalogProjectionInputSpec, CatalogProjectionInputVersionScope, CatalogProjectionLifecycle,
    CatalogProjectionRegistration, CatalogProjectionSourceRow, CatalogProjectionStorageKind,
    CatalogProjectionSurfaceSpec, SurfaceFamily, SurfaceVariant,
};
use crate::{LixError, Value};

const FILE_SURFACE_NAME: &str = "lix_file";
const FILE_BY_VERSION_SURFACE_NAME: &str = "lix_file_by_version";
const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";
const BINARY_BLOB_REF_SCHEMA_KEY: &str = "lix_binary_blob_ref";

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixFileProjection;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct LixFileByVersionProjection;

pub(crate) fn builtin_lix_file_catalog_registration(
) -> CatalogProjectionRegistration<LixFileProjection> {
    CatalogProjectionRegistration::new(LixFileProjection, CatalogProjectionLifecycle::ReadTime)
}

pub(crate) fn builtin_lix_file_by_version_catalog_registration(
) -> CatalogProjectionRegistration<LixFileByVersionProjection> {
    CatalogProjectionRegistration::new(
        LixFileByVersionProjection,
        CatalogProjectionLifecycle::ReadTime,
    )
}

impl CatalogProjectionDefinition for LixFileProjection {
    fn name(&self) -> &'static str {
        FILE_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        requested_version_specs()
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![CatalogProjectionSurfaceSpec::new(
            FILE_SURFACE_NAME,
            SurfaceFamily::Filesystem,
            SurfaceVariant::Default,
        )]
    }

    fn derive(&self, input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        let Some(version_id) = input.context().requested_version_id() else {
            return Ok(Vec::new());
        };
        let rows = derive_file_rows_for_versions(input, &[version_id.to_string()])?;
        let commit_id = input
            .context()
            .current_head_commit_id(version_id)
            .unwrap_or_default()
            .to_string();
        Ok(rows
            .into_iter()
            .map(|row| file_row_to_surface(row, FILE_SURFACE_NAME, Some(&commit_id)))
            .collect())
    }
}

impl CatalogProjectionDefinition for LixFileByVersionProjection {
    fn name(&self) -> &'static str {
        FILE_BY_VERSION_SURFACE_NAME
    }

    fn inputs(&self) -> Vec<CatalogProjectionInputSpec> {
        current_frontier_specs()
    }

    fn surfaces(&self) -> Vec<CatalogProjectionSurfaceSpec> {
        vec![CatalogProjectionSurfaceSpec::new(
            FILE_BY_VERSION_SURFACE_NAME,
            SurfaceFamily::Filesystem,
            SurfaceVariant::ByVersion,
        )]
    }

    fn derive(&self, input: &CatalogProjectionInput) -> Result<Vec<CatalogDerivedRow>, LixError> {
        let mut target_versions = input.context().current_committed_version_ids().to_vec();
        target_versions.push(crate::contracts::GLOBAL_VERSION_ID.to_string());
        target_versions.sort();
        target_versions.dedup();
        let rows = derive_file_rows_for_versions(input, &target_versions)?;
        Ok(rows
            .into_iter()
            .map(|row| file_row_to_surface(row, FILE_BY_VERSION_SURFACE_NAME, None))
            .collect())
    }
}

#[derive(Debug, Clone)]
struct DerivedFileRow {
    identity: crate::contracts::artifacts::RowIdentity,
    version_id: String,
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    path: Option<String>,
    data: Option<Vec<u8>>,
    metadata: Option<String>,
    hidden: bool,
    schema_key: String,
    file_id: String,
    plugin_key: String,
    schema_version: String,
    global: bool,
    change_id: Option<String>,
    created_at: Option<String>,
    updated_at: Option<String>,
    commit_id: Option<String>,
    writer_key: Option<String>,
    untracked: bool,
    lixcol_metadata: Option<String>,
}

#[derive(Debug, Clone)]
struct DerivedDirectoryRow {
    id: String,
    parent_id: Option<String>,
    name: String,
    path: Option<String>,
}

fn requested_version_specs() -> Vec<CatalogProjectionInputSpec> {
    vec![
        local_requested_tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        local_requested_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        global_tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        global_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        local_requested_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        local_requested_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        local_requested_tracked(BINARY_BLOB_REF_SCHEMA_KEY),
        local_requested_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
        global_tracked(BINARY_BLOB_REF_SCHEMA_KEY),
        global_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
    ]
}

fn current_frontier_specs() -> Vec<CatalogProjectionInputSpec> {
    vec![
        local_frontier_tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        local_frontier_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        global_tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        global_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
        local_frontier_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        local_frontier_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        global_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
        local_frontier_tracked(BINARY_BLOB_REF_SCHEMA_KEY),
        local_frontier_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
        global_tracked(BINARY_BLOB_REF_SCHEMA_KEY),
        global_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
    ]
}

fn derive_file_rows_for_versions(
    input: &CatalogProjectionInput,
    target_versions: &[String],
) -> Result<Vec<DerivedFileRow>, LixError> {
    let directory_paths = derive_directory_paths_by_version(input, target_versions)?;
    let local_tracked = input
        .rows_for(&local_frontier_tracked(FILE_DESCRIPTOR_SCHEMA_KEY))
        .or_else(|| input.rows_for(&local_requested_tracked(FILE_DESCRIPTOR_SCHEMA_KEY)))
        .unwrap_or(&[]);
    let local_untracked = input
        .rows_for(&local_frontier_untracked(FILE_DESCRIPTOR_SCHEMA_KEY))
        .or_else(|| input.rows_for(&local_requested_untracked(FILE_DESCRIPTOR_SCHEMA_KEY)))
        .unwrap_or(&[]);
    let global_file_tracked = input
        .rows_for(&global_tracked(FILE_DESCRIPTOR_SCHEMA_KEY))
        .unwrap_or(&[]);
    let global_file_untracked = input
        .rows_for(&global_untracked(FILE_DESCRIPTOR_SCHEMA_KEY))
        .unwrap_or(&[]);

    let blob_local_tracked = input
        .rows_for(&local_frontier_tracked(BINARY_BLOB_REF_SCHEMA_KEY))
        .or_else(|| input.rows_for(&local_requested_tracked(BINARY_BLOB_REF_SCHEMA_KEY)))
        .unwrap_or(&[]);
    let blob_local_untracked = input
        .rows_for(&local_frontier_untracked(BINARY_BLOB_REF_SCHEMA_KEY))
        .or_else(|| input.rows_for(&local_requested_untracked(BINARY_BLOB_REF_SCHEMA_KEY)))
        .unwrap_or(&[]);
    let blob_global_tracked = input
        .rows_for(&global_tracked(BINARY_BLOB_REF_SCHEMA_KEY))
        .unwrap_or(&[]);
    let blob_global_untracked = input
        .rows_for(&global_untracked(BINARY_BLOB_REF_SCHEMA_KEY))
        .unwrap_or(&[]);

    let mut rows = Vec::new();
    for version_id in target_versions {
        let effective_files = resolve_effective_rows_for_version(
            version_id,
            local_tracked,
            local_untracked,
            global_file_tracked,
            global_file_untracked,
        );
        let effective_blobs = resolve_effective_rows_for_version(
            version_id,
            blob_local_tracked,
            blob_local_untracked,
            blob_global_tracked,
            blob_global_untracked,
        );
        let path_map = directory_paths.get(version_id).cloned().unwrap_or_default();

        for descriptor in effective_files.values() {
            let Some(id) = descriptor.property_text("id") else {
                continue;
            };
            let directory_id = nullable_text(descriptor.values().get("directory_id"));
            let name = descriptor.property_text("name").unwrap_or_default();
            let extension = nullable_text(descriptor.values().get("extension"));
            let metadata = nullable_value_text(descriptor.values().get("metadata"));
            let hidden = bool_value(descriptor.values().get("hidden")).unwrap_or(false);
            let path = file_path(
                directory_id.as_deref(),
                &path_map,
                &name,
                extension.as_deref(),
            );
            let blob_hash = effective_blobs
                .get(&id)
                .and_then(|row| row.property_text("blob_hash"));
            let data = blob_hash
                .as_deref()
                .and_then(|hash| input.context().blob_data(hash))
                .map(|value| value.to_vec());
            let commit_id = if descriptor.storage() == CatalogProjectionStorageKind::Untracked {
                Some("untracked".to_string())
            } else {
                descriptor
                    .change_id()
                    .and_then(|change_id| input.context().commit_id_for_change(change_id))
                    .map(str::to_string)
            };

            rows.push(DerivedFileRow {
                identity: descriptor.identity().clone(),
                version_id: version_id.clone(),
                id,
                directory_id,
                name,
                extension,
                path,
                data,
                metadata,
                hidden,
                schema_key: descriptor.schema_key.clone(),
                file_id: descriptor.file_id().to_string(),
                plugin_key: descriptor.plugin_key().unwrap_or_default().to_string(),
                schema_version: descriptor.schema_version().unwrap_or_default().to_string(),
                global: descriptor.global().unwrap_or(false),
                change_id: descriptor.change_id().map(str::to_string),
                created_at: descriptor.created_at().map(str::to_string),
                updated_at: descriptor.updated_at().map(str::to_string),
                commit_id,
                writer_key: descriptor.writer_key().map(str::to_string),
                untracked: descriptor.storage() == CatalogProjectionStorageKind::Untracked,
                lixcol_metadata: descriptor.metadata_text().map(str::to_string),
            });
        }
    }
    Ok(rows)
}

fn derive_directory_paths_by_version(
    input: &CatalogProjectionInput,
    target_versions: &[String],
) -> Result<BTreeMap<String, BTreeMap<String, DerivedDirectoryRow>>, LixError> {
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

    let mut derived = BTreeMap::new();
    for version_id in target_versions {
        let effective_dirs = resolve_effective_rows_for_version(
            version_id,
            local_tracked,
            local_untracked,
            global_tracked,
            global_untracked,
        );
        let path_map = build_directory_rows(&effective_dirs)?;
        derived.insert(version_id.clone(), path_map);
    }
    Ok(derived)
}

fn build_directory_rows(
    effective_dirs: &BTreeMap<String, &CatalogProjectionSourceRow>,
) -> Result<BTreeMap<String, DerivedDirectoryRow>, LixError> {
    let mut rows = effective_dirs
        .values()
        .filter_map(|row| {
            let id = row.property_text("id")?;
            Some((
                id.clone(),
                DerivedDirectoryRow {
                    id,
                    parent_id: nullable_text(row.values().get("parent_id")),
                    name: row.property_text("name").unwrap_or_default(),
                    path: None,
                },
            ))
        })
        .collect::<BTreeMap<_, _>>();

    let ids = rows.keys().cloned().collect::<Vec<_>>();
    for id in ids {
        let path = directory_path_for_id(&id, &rows, &mut BTreeSet::new())?;
        if let Some(row) = rows.get_mut(&id) {
            row.path = path;
        }
    }
    Ok(rows)
}

fn directory_path_for_id(
    id: &str,
    rows: &BTreeMap<String, DerivedDirectoryRow>,
    stack: &mut BTreeSet<String>,
) -> Result<Option<String>, LixError> {
    let Some(row) = rows.get(id) else {
        return Ok(None);
    };
    if !stack.insert(id.to_string()) {
        return Err(LixError::new(
            "LIX_ERROR_INVALID_ARGUMENT",
            format!("filesystem directory cycle detected at '{id}'"),
        ));
    }
    let path = match row.parent_id.as_deref() {
        None => Some(format!("/{}/", row.name)),
        Some(parent_id) => directory_path_for_id(parent_id, rows, stack)?
            .map(|parent| format!("{parent}{}/", row.name)),
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

fn file_row_to_surface(
    row: DerivedFileRow,
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
                "directory_id".to_string(),
                row.directory_id.map(Value::Text).unwrap_or(Value::Null),
            ),
            ("name".to_string(), Value::Text(row.name)),
            (
                "extension".to_string(),
                row.extension.map(Value::Text).unwrap_or(Value::Null),
            ),
            (
                "path".to_string(),
                row.path.map(Value::Text).unwrap_or(Value::Null),
            ),
            (
                "data".to_string(),
                row.data.map(Value::Blob).unwrap_or(Value::Null),
            ),
            (
                "metadata".to_string(),
                row.metadata.map(Value::Text).unwrap_or(Value::Null),
            ),
            ("hidden".to_string(), Value::Boolean(row.hidden)),
            (
                "lixcol_entity_id".to_string(),
                Value::Text(identity.entity_id.clone()),
            ),
            ("lixcol_schema_key".to_string(), Value::Text(row.schema_key)),
            ("lixcol_file_id".to_string(), Value::Text(row.file_id)),
            ("lixcol_version_id".to_string(), Value::Text(row.version_id)),
            ("lixcol_plugin_key".to_string(), Value::Text(row.plugin_key)),
            (
                "lixcol_schema_version".to_string(),
                Value::Text(row.schema_version),
            ),
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
                "lixcol_writer_key".to_string(),
                row.writer_key.map(Value::Text).unwrap_or(Value::Null),
            ),
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

fn file_path(
    directory_id: Option<&str>,
    directory_paths: &BTreeMap<String, DerivedDirectoryRow>,
    name: &str,
    extension: Option<&str>,
) -> Option<String> {
    let filename = match extension {
        Some(extension) if !extension.is_empty() => format!("{name}.{extension}"),
        _ => name.to_string(),
    };
    match directory_id {
        None => Some(format!("/{filename}")),
        Some(directory_id) => directory_paths
            .get(directory_id)
            .and_then(|directory| directory.path.as_ref())
            .map(|prefix| format!("{prefix}{filename}")),
    }
}

fn nullable_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(value)) => Some(value.clone()),
        Some(Value::Null) | None => None,
        _ => None,
    }
}

fn nullable_value_text(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::Text(value)) => Some(decode_wrapped_json_string(value)),
        Some(Value::Json(serde_json::Value::String(value))) => Some(value.clone()),
        Some(Value::Json(value)) => Some(value.to_string()),
        Some(Value::Null) | None => None,
        _ => None,
    }
}

fn decode_wrapped_json_string(value: &str) -> String {
    serde_json::from_str::<String>(value).unwrap_or_else(|_| value.to_string())
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
    use crate::contracts::artifacts::RowIdentity;

    #[test]
    fn default_file_projection_derives_path_data_and_active_commit_id() {
        let projection = LixFileProjection;
        let input = CatalogProjectionInput::with_context(
            vec![
                CatalogProjectionInputRows::new(
                    local_requested_tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                    vec![sample_row(
                        CatalogProjectionStorageKind::Tracked,
                        FILE_DESCRIPTOR_SCHEMA_KEY,
                        "main",
                        "file-1",
                        "lix",
                        BTreeMap::from([
                            ("id".to_string(), Value::Text("file-1".to_string())),
                            ("directory_id".to_string(), Value::Text("dir-1".to_string())),
                            ("name".to_string(), Value::Text("hello".to_string())),
                            ("extension".to_string(), Value::Text("txt".to_string())),
                            (
                                "metadata".to_string(),
                                Value::Text("{\"owner\":\"sam\"}".to_string()),
                            ),
                            ("hidden".to_string(), Value::Boolean(false)),
                        ]),
                    )],
                ),
                CatalogProjectionInputRows::new(
                    local_requested_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                    vec![sample_row(
                        CatalogProjectionStorageKind::Tracked,
                        DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                        "main",
                        "dir-1",
                        "lix",
                        BTreeMap::from([
                            ("id".to_string(), Value::Text("dir-1".to_string())),
                            ("parent_id".to_string(), Value::Null),
                            ("name".to_string(), Value::Text("docs".to_string())),
                            ("hidden".to_string(), Value::Boolean(false)),
                        ]),
                    )],
                ),
                CatalogProjectionInputRows::new(
                    local_requested_tracked(BINARY_BLOB_REF_SCHEMA_KEY),
                    vec![sample_row(
                        CatalogProjectionStorageKind::Tracked,
                        BINARY_BLOB_REF_SCHEMA_KEY,
                        "main",
                        "file-1",
                        "lix",
                        BTreeMap::from([
                            ("id".to_string(), Value::Text("file-1".to_string())),
                            ("blob_hash".to_string(), Value::Text("hash-1".to_string())),
                            ("size_bytes".to_string(), Value::Integer(5)),
                        ]),
                    )],
                ),
                CatalogProjectionInputRows::new(global_tracked(FILE_DESCRIPTOR_SCHEMA_KEY), vec![]),
                CatalogProjectionInputRows::new(
                    global_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
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
                CatalogProjectionInputRows::new(global_tracked(BINARY_BLOB_REF_SCHEMA_KEY), vec![]),
                CatalogProjectionInputRows::new(
                    global_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
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
                blob_data_by_hash: BTreeMap::from([(
                    "hash-1".to_string(),
                    Some(b"hello".to_vec()),
                )]),
            },
        );

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 1);
        let row = &derived[0];
        assert_eq!(row.surface_name, FILE_SURFACE_NAME);
        assert_eq!(
            row.values.get("path"),
            Some(&Value::Text("/docs/hello.txt".to_string()))
        );
        assert_eq!(
            row.values.get("data"),
            Some(&Value::Blob(b"hello".to_vec()))
        );
        assert_eq!(
            row.values.get("metadata"),
            Some(&Value::Text("{\"owner\":\"sam\"}".to_string()))
        );
        assert_eq!(
            row.values.get("lixcol_commit_id"),
            Some(&Value::Text("commit-main".to_string()))
        );
    }

    #[test]
    fn by_version_file_projection_uses_change_commit_mapping() {
        let projection = LixFileByVersionProjection;
        let input = CatalogProjectionInput::with_context(
            vec![
                CatalogProjectionInputRows::new(
                    local_frontier_tracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                    vec![sample_row(
                        CatalogProjectionStorageKind::Tracked,
                        FILE_DESCRIPTOR_SCHEMA_KEY,
                        "main",
                        "file-1",
                        "lix",
                        BTreeMap::from([
                            ("id".to_string(), Value::Text("file-1".to_string())),
                            ("directory_id".to_string(), Value::Null),
                            ("name".to_string(), Value::Text("hello".to_string())),
                            ("extension".to_string(), Value::Null),
                            ("metadata".to_string(), Value::Null),
                            ("hidden".to_string(), Value::Boolean(false)),
                        ]),
                    )
                    .with_live_metadata(
                        "1",
                        "lix",
                        None,
                        Some("change-1".to_string()),
                        None,
                        false,
                        Some("2026-04-10T00:00:00Z".to_string()),
                        Some("2026-04-10T00:00:00Z".to_string()),
                    )],
                ),
                CatalogProjectionInputRows::new(
                    local_frontier_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(global_tracked(FILE_DESCRIPTOR_SCHEMA_KEY), vec![]),
                CatalogProjectionInputRows::new(
                    global_untracked(FILE_DESCRIPTOR_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(
                    local_frontier_tracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(
                    local_frontier_untracked(DIRECTORY_DESCRIPTOR_SCHEMA_KEY),
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
                CatalogProjectionInputRows::new(
                    local_frontier_tracked(BINARY_BLOB_REF_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(
                    local_frontier_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
                    vec![],
                ),
                CatalogProjectionInputRows::new(global_tracked(BINARY_BLOB_REF_SCHEMA_KEY), vec![]),
                CatalogProjectionInputRows::new(
                    global_untracked(BINARY_BLOB_REF_SCHEMA_KEY),
                    vec![],
                ),
            ],
            CatalogProjectionContext {
                requested_version_id: None,
                current_committed_version_ids: vec!["main".to_string()],
                current_version_heads: BTreeMap::new(),
                change_commit_ids: BTreeMap::from([(
                    "change-1".to_string(),
                    "commit-1".to_string(),
                )]),
                blob_data_by_hash: BTreeMap::new(),
            },
        );

        let derived = projection.derive(&input).expect("derive should succeed");

        assert_eq!(derived.len(), 1);
        assert_eq!(
            derived[0].values.get("lixcol_commit_id"),
            Some(&Value::Text("commit-1".to_string()))
        );
        assert_eq!(
            derived[0].values.get("lixcol_version_id"),
            Some(&Value::Text("main".to_string()))
        );
    }

    fn sample_row(
        storage: CatalogProjectionStorageKind,
        schema_key: &str,
        version_id: &str,
        entity_id: &str,
        file_id: &str,
        values: BTreeMap<String, Value>,
    ) -> CatalogProjectionSourceRow {
        CatalogProjectionSourceRow::new(
            storage,
            RowIdentity {
                schema_key: schema_key.to_string(),
                version_id: version_id.to_string(),
                entity_id: entity_id.to_string(),
                file_id: file_id.to_string(),
            },
            schema_key,
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
