use crate::sql::logical_plan::public_ir::{
    PlannedFilesystemDescriptor, PlannedFilesystemFile, PlannedFilesystemState,
};
use crate::write_runtime::filesystem::runtime::{
    FilesystemDescriptorState, FilesystemTransactionFileState, FilesystemTransactionState,
};

pub(crate) fn filesystem_transaction_state_from_planned(
    state: &PlannedFilesystemState,
) -> FilesystemTransactionState {
    FilesystemTransactionState {
        files: state
            .files
            .iter()
            .map(|(key, file)| {
                (
                    key.clone(),
                    filesystem_transaction_file_state_from_planned(file),
                )
            })
            .collect(),
    }
}

fn filesystem_transaction_file_state_from_planned(
    file: &PlannedFilesystemFile,
) -> FilesystemTransactionFileState {
    FilesystemTransactionFileState {
        file_id: file.file_id.clone(),
        version_id: file.version_id.clone(),
        untracked: file.untracked,
        descriptor: file
            .descriptor
            .as_ref()
            .map(filesystem_descriptor_state_from_planned),
        metadata_patch: file.metadata_patch.clone(),
        data: file.data.clone(),
        deleted: file.deleted,
    }
}

fn filesystem_descriptor_state_from_planned(
    descriptor: &PlannedFilesystemDescriptor,
) -> FilesystemDescriptorState {
    FilesystemDescriptorState {
        directory_id: descriptor.directory_id.clone(),
        name: descriptor.name.clone(),
        extension: descriptor.extension.clone(),
        metadata: descriptor.metadata.clone(),
        hidden: descriptor.hidden,
    }
}
