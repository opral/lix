mod descriptor_path;
mod keys;
mod path_index;
mod persistent_map;
mod planner;
mod read;
mod visibility;

pub(crate) use self::descriptor_path::{DirectoryPathRecord, derive_directory_paths};
pub(crate) use self::path_index::{
    FilesystemPathEntry, FilesystemPathIndex, FilesystemPathIndexCache, FilesystemPathIndexReader,
    FilesystemPathIndexRequest, FilesystemPathKind, FilesystemPathSelection,
    UncachedFilesystemPathIndexReader, build_path_index, load_path_index_revision,
    stage_path_index_revision,
};
#[cfg(test)]
pub(crate) use self::path_index::{full_rebuild_stats, reset_full_rebuild_stats};
pub(crate) use self::planner::directory_path_resolvers_from_state_rows;
pub(crate) use self::planner::{
    BlobRefRowInput, DirectoryDescriptorWriteIntent, DirectoryPathResolver, FileDeleteInput,
    FileDescriptorRowInput, FileDescriptorWriteInput, FileDescriptorWriteIntent,
    FilesystemBlobRefKey, FilesystemDeletePlan, FilesystemDescriptorKey, FilesystemRowContext,
    FilesystemWritePlan, blob_ref_row, blob_ref_tombstone_row,
    create_directory_path_with_leaf_id_with_resolvers, directory_descriptor_write_row,
    directory_path_resolvers_from_live_state, directory_path_resolvers_from_path_index,
    file_descriptor_row, file_descriptor_write_row, filesystem_storage_scope_key, plan_file_delete,
    plan_file_descriptor_write, plan_parsed_directory_path_update_with_resolvers,
    plan_parsed_file_path_update_with_resolvers, plan_parsed_file_path_write_with_resolvers,
    plan_recursive_directory_delete,
};
pub(crate) use self::read::{FilesystemIndex, filesystem_schema_keys};
pub(crate) use self::visibility::VisibleFilesystem;
