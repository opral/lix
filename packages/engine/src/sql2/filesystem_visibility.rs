#![allow(dead_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use serde::Deserialize;

use crate::engine2::live_state::LiveStateRow;
use crate::engine2::live_state::{LiveStateContext, LiveStateFilter, LiveStateScanRequest};
use crate::LixError;

use super::filesystem_planner::{
    FilesystemRowContext, BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
    FILE_DESCRIPTOR_SCHEMA_KEY,
};

/// Transaction-visible filesystem metadata decoded from live-state rows.
///
/// The helper intentionally depends only on `LiveStateContext`. In engine2
/// write execution that context is the transaction overlay, so staged writes
/// are visible here without reaching into transaction internals.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct VisibleFilesystem {
    pub(crate) directories_by_id: BTreeMap<String, VisibleDirectory>,
    pub(crate) directory_children_by_parent_id: BTreeMap<Option<String>, BTreeSet<String>>,
    pub(crate) files_by_directory_id: BTreeMap<Option<String>, BTreeMap<String, VisibleFile>>,
    pub(crate) blob_refs_by_file_id: BTreeMap<String, VisibleBlobRef>,
}

impl VisibleFilesystem {
    /// Loads filesystem rows for a single version from transaction-visible
    /// live state and builds lookup indexes used by filesystem write planning.
    pub(crate) async fn load(
        live_state: Arc<dyn LiveStateContext>,
        version_id: &str,
    ) -> Result<Self, LixError> {
        let rows = live_state
            .scan_rows(&LiveStateScanRequest {
                filter: LiveStateFilter {
                    schema_keys: vec![
                        DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_string(),
                        FILE_DESCRIPTOR_SCHEMA_KEY.to_string(),
                        BLOB_REF_SCHEMA_KEY.to_string(),
                    ],
                    version_ids: vec![version_id.to_string()],
                    ..LiveStateFilter::default()
                },
                ..LiveStateScanRequest::default()
            })
            .await?;
        Self::from_live_rows(rows)
    }

    /// Builds filesystem lookup indexes from rows that are already known to be
    /// transaction-visible.
    pub(crate) fn from_live_rows(rows: Vec<LiveStateRow>) -> Result<Self, LixError> {
        let mut visible = Self::default();

        for row in rows {
            let Some(snapshot_content) = row.snapshot_content.as_deref() else {
                continue;
            };
            match row.schema_key.as_str() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: DirectoryDescriptorSnapshot =
                        serde_json::from_str(snapshot_content).map_err(|error| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!("invalid lix_directory_descriptor snapshot JSON: {error}"),
                            )
                        })?;
                    let directory = VisibleDirectory {
                        id: snapshot.id,
                        parent_id: snapshot.parent_id,
                        name: snapshot.name,
                        hidden: snapshot.hidden.unwrap_or(false),
                        context: filesystem_row_context(&row),
                    };
                    visible
                        .directory_children_by_parent_id
                        .entry(directory.parent_id.clone())
                        .or_default()
                        .insert(directory.id.clone());
                    visible
                        .directories_by_id
                        .insert(directory.id.clone(), directory);
                }
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    let snapshot: FileDescriptorSnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                        LixError::new(
                            "LIX_ERROR_UNKNOWN",
                            format!("invalid lix_file_descriptor snapshot JSON: {error}"),
                        )
                    })?;
                    let file = VisibleFile {
                        id: snapshot.id,
                        directory_id: snapshot.directory_id,
                        name: snapshot.name,
                        extension: snapshot.extension,
                        hidden: snapshot.hidden,
                        context: filesystem_row_context(&row),
                    };
                    visible
                        .files_by_directory_id
                        .entry(file.directory_id.clone())
                        .or_default()
                        .insert(file.id.clone(), file);
                }
                BLOB_REF_SCHEMA_KEY => {
                    let snapshot: BlobRefSnapshot = serde_json::from_str(snapshot_content)
                        .map_err(|error| {
                            LixError::new(
                                "LIX_ERROR_UNKNOWN",
                                format!("invalid lix_binary_blob_ref snapshot JSON: {error}"),
                            )
                        })?;
                    visible.blob_refs_by_file_id.insert(
                        snapshot.id.clone(),
                        VisibleBlobRef {
                            file_id: snapshot.id,
                            blob_hash: snapshot.blob_hash,
                            size_bytes: snapshot.size_bytes,
                            context: filesystem_row_context(&row),
                        },
                    );
                }
                _ => {}
            }
        }

        Ok(visible)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VisibleDirectory {
    pub(crate) id: String,
    pub(crate) parent_id: Option<String>,
    pub(crate) name: String,
    pub(crate) hidden: bool,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VisibleFile {
    pub(crate) id: String,
    pub(crate) directory_id: Option<String>,
    pub(crate) name: String,
    pub(crate) extension: Option<String>,
    pub(crate) hidden: bool,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VisibleBlobRef {
    pub(crate) file_id: String,
    pub(crate) blob_hash: String,
    pub(crate) size_bytes: Option<u64>,
    pub(crate) context: FilesystemRowContext,
}

#[derive(Debug, Deserialize)]
struct DirectoryDescriptorSnapshot {
    id: String,
    parent_id: Option<String>,
    name: String,
    hidden: Option<bool>,
}

#[derive(Debug, Deserialize)]
struct FileDescriptorSnapshot {
    id: String,
    directory_id: Option<String>,
    name: String,
    extension: Option<String>,
    hidden: bool,
}

#[derive(Debug, Deserialize)]
struct BlobRefSnapshot {
    id: String,
    blob_hash: String,
    size_bytes: Option<u64>,
}

fn filesystem_row_context(row: &LiveStateRow) -> FilesystemRowContext {
    FilesystemRowContext {
        version_id: row.version_id.clone(),
        global: row.global,
        untracked: row.untracked,
        file_id: row.file_id.clone(),
        plugin_key: row.plugin_key.clone(),
        metadata: row.metadata.clone(),
    }
}

#[cfg(test)]
mod tests {
    use async_trait::async_trait;

    use crate::engine2::live_state::LiveStateRow;
    use crate::engine2::live_state::{LiveStateContext, LiveStateRowRequest, LiveStateScanRequest};
    use crate::LixError;

    use super::{
        VisibleFilesystem, BLOB_REF_SCHEMA_KEY, DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
        FILE_DESCRIPTOR_SCHEMA_KEY,
    };

    #[tokio::test]
    async fn nested_directories_resolve_correctly() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![
                directory_row(
                    "dir-docs",
                    r#"{"id":"dir-docs","parent_id":null,"name":"docs","hidden":false}"#,
                ),
                directory_row(
                    "dir-guides",
                    r#"{"id":"dir-guides","parent_id":"dir-docs","name":"guides","hidden":false}"#,
                ),
            ]),
            "version-a",
        )
        .await
        .expect("visible filesystem should load");

        assert_eq!(
            filesystem
                .directories_by_id
                .get("dir-guides")
                .and_then(|directory| directory.parent_id.as_deref()),
            Some("dir-docs")
        );
        assert!(filesystem
            .directory_children_by_parent_id
            .get(&None)
            .is_some_and(|children| children.contains("dir-docs")));
        assert!(filesystem
            .directory_children_by_parent_id
            .get(&Some("dir-docs".to_string()))
            .is_some_and(|children| children.contains("dir-guides")));
    }

    #[tokio::test]
    async fn files_attach_to_directory_ids() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![file_row(
                "file-readme",
                r#"{"id":"file-readme","directory_id":"dir-guides","name":"readme","extension":"md","hidden":false}"#,
            )]),
            "version-a",
        )
        .await
        .expect("visible filesystem should load");

        let files = filesystem
            .files_by_directory_id
            .get(&Some("dir-guides".to_string()))
            .expect("directory should have attached files");
        let file = files
            .get("file-readme")
            .expect("file should be indexed by id inside directory");
        assert_eq!(file.name, "readme");
        assert_eq!(file.extension.as_deref(), Some("md"));
    }

    #[tokio::test]
    async fn blob_refs_attach_to_file_ids() {
        let filesystem = VisibleFilesystem::load(
            live_state(vec![blob_ref_row(
                "file-readme",
                r#"{"id":"file-readme","blob_hash":"abc123","size_bytes":5}"#,
            )]),
            "version-a",
        )
        .await
        .expect("visible filesystem should load");

        let blob_ref = filesystem
            .blob_refs_by_file_id
            .get("file-readme")
            .expect("blob ref should be indexed by file id");
        assert_eq!(blob_ref.blob_hash, "abc123");
        assert_eq!(blob_ref.size_bytes, Some(5));
    }

    fn live_state(rows: Vec<LiveStateRow>) -> std::sync::Arc<dyn LiveStateContext> {
        std::sync::Arc::new(RowsLiveStateContext { rows })
    }

    struct RowsLiveStateContext {
        rows: Vec<LiveStateRow>,
    }

    #[async_trait]
    impl LiveStateContext for RowsLiveStateContext {
        async fn scan_rows(
            &self,
            request: &LiveStateScanRequest,
        ) -> Result<Vec<LiveStateRow>, LixError> {
            Ok(self
                .rows
                .iter()
                .filter(|row| {
                    (request.filter.schema_keys.is_empty()
                        || request.filter.schema_keys.contains(&row.schema_key))
                        && (request.filter.version_ids.is_empty()
                            || request.filter.version_ids.contains(&row.version_id))
                })
                .cloned()
                .collect())
        }

        async fn load_row(
            &self,
            _request: &LiveStateRowRequest,
        ) -> Result<Option<LiveStateRow>, LixError> {
            Ok(None)
        }
    }

    fn directory_row(entity_id: &str, snapshot_content: &str) -> LiveStateRow {
        live_row(
            entity_id,
            DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
            None,
            snapshot_content,
        )
    }

    fn file_row(entity_id: &str, snapshot_content: &str) -> LiveStateRow {
        live_row(
            entity_id,
            FILE_DESCRIPTOR_SCHEMA_KEY,
            None,
            snapshot_content,
        )
    }

    fn blob_ref_row(entity_id: &str, snapshot_content: &str) -> LiveStateRow {
        live_row(
            entity_id,
            BLOB_REF_SCHEMA_KEY,
            Some(entity_id.to_string()),
            snapshot_content,
        )
    }

    fn live_row(
        entity_id: &str,
        schema_key: &str,
        file_id: Option<String>,
        snapshot_content: &str,
    ) -> LiveStateRow {
        LiveStateRow {
            entity_id: entity_id.to_string(),
            schema_key: schema_key.to_string(),
            file_id,
            plugin_key: None,
            snapshot_content: Some(snapshot_content.to_string()),
            metadata: None,
            schema_version: "1".to_string(),
            version_id: "version-a".to_string(),
            change_id: format!("change-{entity_id}"),
            commit_id: Some(format!("commit-{entity_id}")),
            global: false,
            untracked: false,
            created_at: "2026-04-23T00:00:00Z".to_string(),
            updated_at: "2026-04-23T01:00:00Z".to_string(),
        }
    }
}
