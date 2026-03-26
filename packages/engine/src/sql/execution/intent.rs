use std::collections::BTreeSet;

use crate::filesystem::runtime::{BinaryBlobWrite, FilesystemTransactionState};
use crate::sql::execution::contracts::requirements::PlanRequirements;
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
    _engine: &crate::engine::Engine,
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
