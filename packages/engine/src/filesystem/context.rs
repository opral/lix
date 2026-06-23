use std::collections::BTreeSet;
use std::sync::Arc;

use crate::LixError;
use crate::storage::{MountedFilesystem, MountedFilesystemListing, MountedFilesystemOp};
use crate::transaction::types::{TransactionFileData, TransactionWriteRow};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MountedEntryKind {
    File,
    Directory,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MountedWorkspaceTarget {
    pub(crate) id: String,
    pub(crate) path: Option<String>,
    pub(crate) branch_id: String,
    pub(crate) kind: MountedEntryKind,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MountedFileDataUpdateTarget {
    pub(crate) target: MountedWorkspaceTarget,
    pub(crate) data: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum FilesystemWriteTarget {
    Mounted(MountedFilesystemOp),
    Overlay,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct FilesystemWriteOutcome {
    pub(crate) state_rows: Vec<TransactionWriteRow>,
    pub(crate) file_data: Vec<TransactionFileData>,
    pub(crate) mounted_ops: Vec<MountedFilesystemOp>,
    pub(crate) count: u64,
}

pub(crate) struct FilesystemContext {
    active_branch_id: String,
    is_workspace_session: bool,
    mounted_listing: Option<MountedFilesystemListing>,
}

impl FilesystemContext {
    pub(crate) fn for_workspace_write(active_branch_id: &str, is_workspace_session: bool) -> Self {
        Self {
            active_branch_id: active_branch_id.to_string(),
            is_workspace_session,
            mounted_listing: None,
        }
    }

    pub(crate) async fn for_mounted_path_write(
        active_branch_id: &str,
        is_workspace_session: bool,
        mounted_filesystem: Option<Arc<dyn MountedFilesystem>>,
    ) -> Result<Self, LixError> {
        let mounted_listing = if is_workspace_session {
            match mounted_filesystem {
                Some(mounted_filesystem) => {
                    Some(mounted_filesystem.list().await.map_err(|error| {
                        LixError::new(
                            LixError::CODE_STORAGE_ERROR,
                            format!("mounted filesystem listing failed: {error}"),
                        )
                    })?)
                }
                None => None,
            }
        } else {
            None
        };
        Ok(Self {
            active_branch_id: active_branch_id.to_string(),
            is_workspace_session,
            mounted_listing,
        })
    }

    pub(crate) fn plan_file_write(
        &self,
        path: &str,
        is_plugin_path: bool,
        data: Option<Vec<u8>>,
    ) -> FilesystemWriteTarget {
        let Some(data) = data else {
            return FilesystemWriteTarget::Overlay;
        };
        let Some(listing) = self.mounted_listing.as_ref() else {
            return FilesystemWriteTarget::Overlay;
        };
        if is_plugin_path
            || mounted_write_path_is_directory(path, listing)
            || !mounted_write_path_is_managed(path, listing)
        {
            return FilesystemWriteTarget::Overlay;
        }
        FilesystemWriteTarget::Mounted(MountedFilesystemOp::WriteFile {
            path: path.to_string(),
            data,
        })
    }

    fn plan_mounted_file_delete(
        &self,
        target: &MountedWorkspaceTarget,
    ) -> Option<MountedFilesystemOp> {
        self.mounted_workspace_path(target, MountedEntryKind::File)
            .map(|path| MountedFilesystemOp::DeleteFile { path })
    }

    fn plan_mounted_file_data_update(
        &self,
        target: &MountedWorkspaceTarget,
        data: Option<Vec<u8>>,
    ) -> Option<MountedFilesystemOp> {
        let data = data?;
        self.mounted_workspace_path(target, MountedEntryKind::File)
            .map(|path| MountedFilesystemOp::WriteFile { path, data })
    }

    fn plan_mounted_directory_delete(
        &self,
        target: &MountedWorkspaceTarget,
    ) -> Option<MountedFilesystemOp> {
        self.mounted_workspace_path(target, MountedEntryKind::Directory)
            .map(|path| MountedFilesystemOp::DeleteDirectory { path })
    }

    pub(crate) fn plan_file_delete(
        &self,
        state_rows: Vec<TransactionWriteRow>,
        file_data: Vec<TransactionFileData>,
        count: u64,
        targets: Vec<MountedWorkspaceTarget>,
    ) -> Result<FilesystemWriteOutcome, LixError> {
        let mounted_ops = targets
            .iter()
            .filter_map(|target| self.plan_mounted_file_delete(target))
            .collect();
        let state_rows = self.retain_lix_owned_state_rows(state_rows, MountedEntryKind::File)?;
        let mut file_data = file_data;
        self.retain_lix_owned_file_data(&mut file_data);
        Ok(FilesystemWriteOutcome {
            state_rows,
            file_data,
            mounted_ops,
            count,
        })
    }

    pub(crate) fn plan_file_data_update(
        &self,
        state_rows: Vec<TransactionWriteRow>,
        file_data: Vec<TransactionFileData>,
        count: u64,
        targets: Vec<MountedFileDataUpdateTarget>,
    ) -> Result<FilesystemWriteOutcome, LixError> {
        let mounted_ops = targets
            .iter()
            .filter_map(|target| {
                self.plan_mounted_file_data_update(&target.target, target.data.clone())
            })
            .collect();
        let mounted_ids = targets
            .iter()
            .filter(|target| {
                target.data.is_some()
                    && self
                        .mounted_workspace_path(&target.target, MountedEntryKind::File)
                        .is_some()
            })
            .map(|target| target.target.id.as_str())
            .collect::<BTreeSet<_>>();
        let state_rows = self.retain_lix_owned_file_state_rows_by_id(state_rows, &mounted_ids)?;
        let mut file_data = file_data;
        self.retain_lix_owned_file_data_by_id(&mut file_data, &mounted_ids);
        Ok(FilesystemWriteOutcome {
            state_rows,
            file_data,
            mounted_ops,
            count,
        })
    }

    pub(crate) fn plan_directory_delete(
        &self,
        state_rows: Vec<TransactionWriteRow>,
        count: u64,
        targets: Vec<MountedWorkspaceTarget>,
    ) -> Result<FilesystemWriteOutcome, LixError> {
        let mounted_ops = targets
            .iter()
            .filter_map(|target| self.plan_mounted_directory_delete(target))
            .collect();
        let state_rows =
            self.retain_lix_owned_state_rows(state_rows, MountedEntryKind::Directory)?;
        Ok(FilesystemWriteOutcome {
            state_rows,
            file_data: Vec::new(),
            mounted_ops,
            count,
        })
    }

    fn retain_lix_owned_state_rows(
        &self,
        rows: Vec<TransactionWriteRow>,
        kind: MountedEntryKind,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let mut retained = Vec::with_capacity(rows.len());
        for row in rows {
            if !self.row_targets_workspace_mounted_id(&row, kind)? {
                retained.push(row);
            }
        }
        Ok(retained)
    }

    fn retain_lix_owned_file_data(&self, file_data: &mut Vec<TransactionFileData>) {
        file_data.retain(|write| {
            !(self.is_workspace_session
                && write.branch_id == self.active_branch_id
                && !write.global
                && mounted_id_matches(&write.file_id, MountedEntryKind::File))
        });
    }

    fn retain_lix_owned_file_state_rows_by_id(
        &self,
        rows: Vec<TransactionWriteRow>,
        mounted_ids: &BTreeSet<&str>,
    ) -> Result<Vec<TransactionWriteRow>, LixError> {
        let mut retained = Vec::with_capacity(rows.len());
        for row in rows {
            if !self.row_targets_workspace_mounted_file_id(&row, mounted_ids)? {
                retained.push(row);
            }
        }
        Ok(retained)
    }

    fn retain_lix_owned_file_data_by_id(
        &self,
        file_data: &mut Vec<TransactionFileData>,
        mounted_ids: &BTreeSet<&str>,
    ) {
        file_data.retain(|write| {
            !(self.is_workspace_session
                && write.branch_id == self.active_branch_id
                && !write.global
                && mounted_ids.contains(write.file_id.as_str()))
        });
    }

    fn mounted_workspace_path(
        &self,
        target: &MountedWorkspaceTarget,
        kind: MountedEntryKind,
    ) -> Option<String> {
        if !self.is_workspace_session
            || target.kind != kind
            || target.branch_id != self.active_branch_id
        {
            return None;
        }
        target.path.clone()
    }

    fn row_targets_workspace_mounted_id(
        &self,
        row: &TransactionWriteRow,
        kind: MountedEntryKind,
    ) -> Result<bool, LixError> {
        if !self.is_workspace_session || row.global || row.branch_id != self.active_branch_id {
            return Ok(false);
        }
        let Some(entity_pk) = row.entity_pk.as_ref() else {
            return Ok(false);
        };
        Ok(mounted_id_matches(entity_pk.as_single_string()?, kind))
    }

    fn row_targets_workspace_mounted_file_id(
        &self,
        row: &TransactionWriteRow,
        mounted_ids: &BTreeSet<&str>,
    ) -> Result<bool, LixError> {
        if !self.is_workspace_session || row.global || row.branch_id != self.active_branch_id {
            return Ok(false);
        }
        let Some(entity_pk) = row.entity_pk.as_ref() else {
            return Ok(false);
        };
        Ok(mounted_ids.contains(entity_pk.as_single_string()?))
    }
}

pub(crate) fn mounted_id_matches(id: &str, kind: MountedEntryKind) -> bool {
    match kind {
        MountedEntryKind::File => crate::filesystem::is_mounted_file_id(id),
        MountedEntryKind::Directory => crate::filesystem::is_mounted_directory_id(id),
    }
}

fn mounted_write_path_is_directory(path: &str, listing: &MountedFilesystemListing) -> bool {
    listing
        .directories
        .contains(&format!("{}/", path.trim_end_matches('/')))
}

fn mounted_write_path_is_managed(path: &str, listing: &MountedFilesystemListing) -> bool {
    if mounted_write_path_is_protected(path) || mounted_write_path_hits_unmanaged(path, listing) {
        return false;
    }
    listing.files.contains(path) || mounted_write_parent_directory_is_listed(path, listing)
}

fn mounted_write_path_is_protected(path: &str) -> bool {
    path.trim_matches('/')
        .split('/')
        .any(|segment| matches!(segment, ".git" | ".lix"))
}

fn mounted_write_path_hits_unmanaged(path: &str, listing: &MountedFilesystemListing) -> bool {
    listing.unmanaged_paths.iter().any(|unmanaged| {
        let unmanaged = unmanaged.trim_end_matches('/');
        path == unmanaged || path.starts_with(&format!("{unmanaged}/"))
    })
}

fn mounted_write_parent_directory_is_listed(
    path: &str,
    listing: &MountedFilesystemListing,
) -> bool {
    let Some((parent, _name)) = path.rsplit_once('/') else {
        return false;
    };
    let parent_directory = if parent.is_empty() {
        "/".to_string()
    } else {
        format!("{}/", parent.trim_end_matches('/'))
    };
    listing.directories.contains(&parent_directory)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::entity_pk::EntityPk;

    use super::*;

    fn context_with_listing(listing: MountedFilesystemListing) -> FilesystemContext {
        FilesystemContext {
            active_branch_id: "workspace".to_string(),
            is_workspace_session: true,
            mounted_listing: Some(listing),
        }
    }

    fn listing(
        files: &[&str],
        directories: &[&str],
        unmanaged: &[&str],
    ) -> MountedFilesystemListing {
        MountedFilesystemListing {
            files: files
                .iter()
                .map(|path| path.to_string())
                .collect::<BTreeSet<_>>(),
            directories: directories
                .iter()
                .map(|path| path.to_string())
                .collect::<BTreeSet<_>>(),
            unmanaged_paths: unmanaged
                .iter()
                .map(|path| path.to_string())
                .collect::<BTreeSet<_>>(),
        }
    }

    fn row(id: &str, branch_id: &str, global: bool) -> TransactionWriteRow {
        TransactionWriteRow {
            entity_pk: Some(EntityPk::single(id)),
            schema_key: "lix_file_descriptor".to_string(),
            file_id: None,
            snapshot: None,
            metadata: None,
            origin: None,
            created_at: None,
            updated_at: None,
            global,
            change_id: None,
            commit_id: None,
            untracked: false,
            branch_id: branch_id.to_string(),
        }
    }

    fn file_data(file_id: &str, branch_id: &str, global: bool) -> TransactionFileData {
        TransactionFileData::new(
            file_id.to_string(),
            Some(format!("/{file_id}")),
            Some(file_id.to_string()),
            branch_id.to_string(),
            global,
            false,
            b"data".to_vec(),
        )
    }

    fn file_target(path: &str, branch_id: &str) -> MountedWorkspaceTarget {
        MountedWorkspaceTarget {
            id: format!("mounted:file:{path}"),
            path: Some(path.to_string()),
            branch_id: branch_id.to_string(),
            kind: MountedEntryKind::File,
        }
    }

    fn directory_target(path: &str, branch_id: &str) -> MountedWorkspaceTarget {
        MountedWorkspaceTarget {
            id: format!("mounted:dir:{path}"),
            path: Some(path.to_string()),
            branch_id: branch_id.to_string(),
            kind: MountedEntryKind::Directory,
        }
    }

    #[test]
    fn plan_file_write_returns_mounted_op_for_managed_file() {
        let context = context_with_listing(listing(&["/docs/a.md"], &["/docs/"], &[]));

        let target = context.plan_file_write("/docs/a.md", false, Some(b"hello".to_vec()));

        assert_eq!(
            target,
            FilesystemWriteTarget::Mounted(MountedFilesystemOp::WriteFile {
                path: "/docs/a.md".to_string(),
                data: b"hello".to_vec()
            })
        );
    }

    #[test]
    fn plan_file_write_keeps_protected_and_unmanaged_paths_overlay_only() {
        let context = context_with_listing(listing(
            &["/.git/config", "/docs/a.md"],
            &["/docs/"],
            &["/docs"],
        ));

        assert_eq!(
            context.plan_file_write("/.git/config", false, Some(Vec::new())),
            FilesystemWriteTarget::Overlay
        );
        assert_eq!(
            context.plan_file_write("/docs/a.md", false, Some(Vec::new())),
            FilesystemWriteTarget::Overlay
        );
    }

    #[test]
    fn mounted_delete_ops_require_workspace_branch_and_session() {
        let context = context_with_listing(MountedFilesystemListing::default());
        let target = MountedWorkspaceTarget {
            id: "mounted:file:/docs/a.md".to_string(),
            path: Some("/docs/a.md".to_string()),
            branch_id: "workspace".to_string(),
            kind: MountedEntryKind::File,
        };
        assert_eq!(
            context.plan_mounted_file_delete(&target),
            Some(MountedFilesystemOp::DeleteFile {
                path: "/docs/a.md".to_string()
            })
        );

        let non_workspace_context = FilesystemContext {
            active_branch_id: "workspace".to_string(),
            is_workspace_session: false,
            mounted_listing: None,
        };
        assert_eq!(
            non_workspace_context.plan_mounted_file_delete(&target),
            None
        );
    }

    #[test]
    fn retain_lix_owned_state_rows_removes_only_workspace_mounted_rows() {
        let context = context_with_listing(MountedFilesystemListing::default());
        let rows = vec![
            row("mounted:file:/docs/a.md", "workspace", false),
            row("normal-file-id", "workspace", false),
            row("mounted:file:/docs/other.md", "other", false),
            row("mounted:file:/global.md", "workspace", true),
        ];

        let retained = context
            .retain_lix_owned_state_rows(rows, MountedEntryKind::File)
            .unwrap();

        let ids = retained
            .into_iter()
            .map(|row| row.entity_pk.unwrap().as_single_string_owned().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            vec![
                "normal-file-id".to_string(),
                "mounted:file:/docs/other.md".to_string(),
                "mounted:file:/global.md".to_string(),
            ]
        );
    }

    #[test]
    fn plan_file_delete_pairs_mounted_op_with_row_and_file_data_stripping() {
        let context = context_with_listing(MountedFilesystemListing::default());

        let outcome = context
            .plan_file_delete(
                vec![
                    row("mounted:file:/docs/a.md", "workspace", false),
                    row("normal-file-id", "workspace", false),
                ],
                vec![
                    file_data("mounted:file:/docs/a.md", "workspace", false),
                    file_data("normal-file-id", "workspace", false),
                ],
                2,
                vec![file_target("/docs/a.md", "workspace")],
            )
            .unwrap();

        assert_eq!(
            outcome.mounted_ops,
            vec![MountedFilesystemOp::DeleteFile {
                path: "/docs/a.md".to_string()
            }]
        );
        assert_eq!(outcome.count, 2);
        assert_eq!(outcome.state_rows.len(), 1);
        assert_eq!(
            outcome.state_rows[0]
                .entity_pk
                .as_ref()
                .unwrap()
                .as_single_string()
                .unwrap(),
            "normal-file-id"
        );
        assert_eq!(outcome.file_data.len(), 1);
        assert_eq!(outcome.file_data[0].file_id, "normal-file-id");
    }

    #[test]
    fn plan_file_data_update_pairs_write_op_with_row_and_file_data_stripping() {
        let context = context_with_listing(MountedFilesystemListing::default());

        let outcome = context
            .plan_file_data_update(
                vec![row("mounted:file:/docs/a.md", "workspace", false)],
                vec![file_data("mounted:file:/docs/a.md", "workspace", false)],
                1,
                vec![MountedFileDataUpdateTarget {
                    target: file_target("/docs/a.md", "workspace"),
                    data: Some(b"updated".to_vec()),
                }],
            )
            .unwrap();

        assert_eq!(
            outcome.mounted_ops,
            vec![MountedFilesystemOp::WriteFile {
                path: "/docs/a.md".to_string(),
                data: b"updated".to_vec()
            }]
        );
        assert!(outcome.state_rows.is_empty());
        assert!(outcome.file_data.is_empty());
        assert_eq!(outcome.count, 1);
    }

    #[test]
    fn plan_file_data_update_without_data_keeps_mounted_rows_overlay_only() {
        let context = context_with_listing(MountedFilesystemListing::default());

        let outcome = context
            .plan_file_data_update(
                vec![row("mounted:file:/docs/a.md", "workspace", false)],
                vec![file_data("mounted:file:/docs/a.md", "workspace", false)],
                1,
                vec![MountedFileDataUpdateTarget {
                    target: file_target("/docs/a.md", "workspace"),
                    data: None,
                }],
            )
            .unwrap();

        assert!(outcome.mounted_ops.is_empty());
        assert_eq!(outcome.state_rows.len(), 1);
        assert_eq!(outcome.file_data.len(), 1);
        assert_eq!(outcome.count, 1);
    }

    #[test]
    fn plan_directory_delete_pairs_mounted_op_with_row_stripping() {
        let context = context_with_listing(MountedFilesystemListing::default());

        let outcome = context
            .plan_directory_delete(
                vec![
                    row("mounted:dir:/docs/", "workspace", false),
                    row("normal-dir-id", "workspace", false),
                ],
                2,
                vec![directory_target("/docs/", "workspace")],
            )
            .unwrap();

        assert_eq!(
            outcome.mounted_ops,
            vec![MountedFilesystemOp::DeleteDirectory {
                path: "/docs/".to_string()
            }]
        );
        assert_eq!(outcome.state_rows.len(), 1);
        assert_eq!(
            outcome.state_rows[0]
                .entity_pk
                .as_ref()
                .unwrap()
                .as_single_string()
                .unwrap(),
            "normal-dir-id"
        );
        assert_eq!(outcome.count, 2);
    }

    #[test]
    fn outcome_methods_keep_non_workspace_session_overlay_only() {
        let context = FilesystemContext {
            active_branch_id: "workspace".to_string(),
            is_workspace_session: false,
            mounted_listing: None,
        };

        let outcome = context
            .plan_file_delete(
                vec![row("mounted:file:/docs/a.md", "workspace", false)],
                vec![file_data("mounted:file:/docs/a.md", "workspace", false)],
                1,
                vec![file_target("/docs/a.md", "workspace")],
            )
            .unwrap();

        assert!(outcome.mounted_ops.is_empty());
        assert_eq!(outcome.state_rows.len(), 1);
        assert_eq!(outcome.file_data.len(), 1);
    }
}
