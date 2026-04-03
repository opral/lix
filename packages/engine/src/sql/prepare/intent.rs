use std::collections::BTreeSet;

use crate::filesystem::runtime::{
    BinaryBlobWrite, FilesystemDescriptorState, FilesystemTransactionFileState,
    FilesystemTransactionState,
};
use crate::sql::logical_plan::public_ir::{
    PlannedFilesystemDescriptor, PlannedFilesystemFile, PlannedFilesystemState,
};
use crate::sql::prepare::contracts::requirements::PlanRequirements;
use crate::{LixBackend, LixError, Value};
use sqlparser::ast::Statement;

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntentCollectionPolicy {
    pub(crate) skip_side_effect_collection: bool,
}

pub(crate) struct ExecutionIntent {
    pub(crate) filesystem_state: FilesystemTransactionState,
}

pub(crate) async fn collect_execution_intent_with_backend(
    _backend: &dyn LixBackend,
    _statements: &[Statement],
    _params: &[Value],
    _active_version_id: &str,
    _writer_key: Option<&str>,
    requirements: &PlanRequirements,
    policy: IntentCollectionPolicy,
) -> Result<ExecutionIntent, LixError> {
    let filesystem_state = if policy.skip_side_effect_collection || requirements.read_only_query {
        FilesystemTransactionState::default()
    } else {
        // Raw SQL intent collection no longer stages filesystem ops through a separate
        // event stream. Public and transaction-local filesystem writes are carried by the
        // typed filesystem state built during write planning.
        FilesystemTransactionState::default()
    };

    Ok(ExecutionIntent { filesystem_state })
}

pub(crate) fn authoritative_binary_blob_write_targets(
    writes: &[BinaryBlobWrite],
) -> BTreeSet<(String, String)> {
    writes
        .iter()
        .filter_map(|write| {
            write
                .file_id
                .as_ref()
                .map(|file_id| (file_id.clone(), write.version_id.clone()))
        })
        .collect()
}

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
