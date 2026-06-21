mod descriptor_path;
mod keys;
mod planner;
mod read;
mod visibility;

pub(crate) use self::descriptor_path::{DirectoryPathRecord, derive_directory_paths};
pub(crate) use self::planner::directory_path_resolvers_from_state_rows;
pub(crate) use self::planner::{
    BlobRefRowInput, DirectoryDescriptorWriteIntent, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FileDescriptorWriteInput, FileDescriptorWriteIntent,
    FilesystemBlobRefKey, FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext,
    FilesystemWritePlan, blob_ref_row, blob_ref_tombstone_row,
    create_directory_path_with_leaf_id_with_resolvers, directory_descriptor_write_row,
    directory_path_resolvers_from_live_state, file_descriptor_row, file_descriptor_write_row,
    filesystem_storage_scope_key, plan_file_delete, plan_file_descriptor_write,
    plan_parsed_directory_path_update_with_resolvers, plan_parsed_file_path_update_with_resolvers,
    plan_parsed_file_path_write_with_resolvers, plan_recursive_directory_delete,
};
pub(crate) use self::read::{FilesystemIndex, filesystem_schema_keys};
pub(crate) use self::visibility::VisibleFilesystem;

/// Public identity key for matching filesystem descriptors to their stored
/// blob reference rows without duplicating filesystem storage scope rules.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct LixFilesystemBlobRefKey(FilesystemBlobRefKey);

pub fn lix_filesystem_blob_ref_key_for_active_file_descriptor(
    active_branch_id: &str,
    global: bool,
    untracked: bool,
    descriptor_id: &str,
) -> LixFilesystemBlobRefKey {
    let context = FilesystemRowContext {
        branch_id: if global {
            crate::GLOBAL_BRANCH_ID.to_string()
        } else {
            active_branch_id.to_string()
        },
        global,
        untracked,
        file_id: None,
        metadata: None,
    };
    LixFilesystemBlobRefKey(FilesystemBlobRefKey::from_context(&context, descriptor_id))
}

pub fn lix_filesystem_blob_ref_key_for_state_row(
    branch_id: &str,
    global: bool,
    untracked: bool,
    file_id: Option<String>,
    blob_ref_id: &str,
) -> LixFilesystemBlobRefKey {
    LixFilesystemBlobRefKey(FilesystemBlobRefKey::from_parts(
        branch_id,
        global,
        untracked,
        file_id,
        blob_ref_id,
    ))
}

pub fn lix_filesystem_blob_ref_key_for_active_state_row(
    active_branch_id: &str,
    global: bool,
    untracked: bool,
    file_id: Option<String>,
    blob_ref_id: &str,
) -> LixFilesystemBlobRefKey {
    lix_filesystem_blob_ref_key_for_state_row(
        if global {
            crate::GLOBAL_BRANCH_ID
        } else {
            active_branch_id
        },
        global,
        untracked,
        file_id,
        blob_ref_id,
    )
}

#[cfg(test)]
mod public_key_tests {
    use super::{
        lix_filesystem_blob_ref_key_for_active_file_descriptor,
        lix_filesystem_blob_ref_key_for_active_state_row,
        lix_filesystem_blob_ref_key_for_state_row,
    };

    #[test]
    fn active_descriptor_key_matches_file_scoped_blob_ref_key() {
        let descriptor = lix_filesystem_blob_ref_key_for_active_file_descriptor(
            "branch-a", false, false, "file-a",
        );
        let blob = lix_filesystem_blob_ref_key_for_state_row(
            "branch-a",
            false,
            false,
            Some("file-a".to_string()),
            "file-a",
        );

        assert_eq!(descriptor, blob);
    }

    #[test]
    fn active_descriptor_key_preserves_global_and_untracked_scope() {
        let global_descriptor = lix_filesystem_blob_ref_key_for_active_file_descriptor(
            "branch-a", true, false, "file-a",
        );
        let branch_blob = lix_filesystem_blob_ref_key_for_state_row(
            "branch-a",
            true,
            false,
            Some("file-a".to_string()),
            "file-a",
        );
        let global_blob = lix_filesystem_blob_ref_key_for_state_row(
            "global",
            true,
            false,
            Some("file-a".to_string()),
            "file-a",
        );
        let tracked_descriptor = lix_filesystem_blob_ref_key_for_active_file_descriptor(
            "branch-a", false, false, "file-a",
        );
        let untracked_blob = lix_filesystem_blob_ref_key_for_state_row(
            "branch-a",
            false,
            true,
            Some("file-a".to_string()),
            "file-a",
        );

        assert_ne!(global_descriptor, branch_blob);
        assert_eq!(global_descriptor, global_blob);
        assert_eq!(
            global_descriptor,
            lix_filesystem_blob_ref_key_for_active_state_row(
                "branch-a",
                true,
                false,
                Some("file-a".to_string()),
                "file-a",
            )
        );
        assert_ne!(tracked_descriptor, untracked_blob);
    }
}
