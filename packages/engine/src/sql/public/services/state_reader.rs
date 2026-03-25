pub(crate) use crate::canonical::readers::{
    CommitQueryExecutor, ExactCommittedStateRow, ExactCommittedStateRowRequest,
};
use crate::canonical::readers::{
    load_committed_version_head_commit_id_from_live_state,
    load_exact_committed_state_row_from_live_state,
};
pub(crate) use crate::live_state::raw::{RawRow, RawStorage};
use crate::live_state::raw::{
    load_exact_row_with_backend, scan_rows_with_backend, scan_rows_with_executor, snapshot_json,
};
pub(crate) use crate::live_state::system::VersionRefRow;
pub(crate) use crate::live_state::tracked::{
    ExactTrackedRowRequest, TrackedScanRequest, TrackedTombstoneMarker,
};
use crate::live_state::system::load_version_ref_with_backend;
use crate::live_state::tracked::{load_exact_tombstone_with_executor, scan_tombstones_with_executor};
use crate::live_state::{
    is_untracked_live_table, load_live_row_access_for_table_name, load_live_row_access_with_backend,
    logical_snapshot_from_projected_row, normalized_live_column_values,
};
use crate::{LixBackend, LixError, Value};
pub(crate) use crate::live_state::LiveRowAccess;

pub(crate) async fn load_committed_version_head_commit_id(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<String>, LixError> {
    let mut executor = backend;
    load_committed_version_head_commit_id_from_live_state(&mut executor, version_id).await
}

pub(crate) async fn load_exact_committed_state_row(
    backend: &dyn LixBackend,
    request: &ExactCommittedStateRowRequest,
) -> Result<Option<ExactCommittedStateRow>, LixError> {
    load_exact_committed_state_row_from_live_state(backend, request).await
}

pub(crate) async fn scan_live_rows(
    backend: &dyn LixBackend,
    storage: RawStorage,
    schema_key: &str,
    version_id: &str,
    constraints: &[crate::live_state::constraints::ScanConstraint],
    required_columns: &[String],
) -> Result<Vec<RawRow>, LixError> {
    scan_rows_with_backend(
        backend,
        storage,
        schema_key,
        version_id,
        constraints,
        required_columns,
    )
    .await
}

pub(crate) async fn scan_live_rows_with_executor_ref(
    executor: &mut dyn CommitQueryExecutor,
    storage: RawStorage,
    schema_key: &str,
    version_id: &str,
    constraints: &[crate::live_state::constraints::ScanConstraint],
    required_columns: &[String],
) -> Result<Vec<RawRow>, LixError> {
    scan_rows_with_executor(
        executor,
        storage,
        schema_key,
        version_id,
        constraints,
        required_columns,
    )
    .await
}

pub(crate) async fn load_exact_live_row(
    backend: &dyn LixBackend,
    storage: RawStorage,
    schema_key: &str,
    version_id: &str,
    entity_id: &str,
    file_id: Option<&str>,
) -> Result<Option<RawRow>, LixError> {
    load_exact_row_with_backend(backend, storage, schema_key, version_id, entity_id, file_id).await
}

pub(crate) async fn load_version_ref(
    backend: &dyn LixBackend,
    version_id: &str,
) -> Result<Option<VersionRefRow>, LixError> {
    load_version_ref_with_backend(backend, version_id).await
}

pub(crate) async fn load_exact_tombstone(
    executor: &mut dyn CommitQueryExecutor,
    request: &ExactTrackedRowRequest,
) -> Result<Option<TrackedTombstoneMarker>, LixError> {
    load_exact_tombstone_with_executor(executor, request).await
}

pub(crate) async fn scan_tombstones(
    executor: &mut dyn CommitQueryExecutor,
    request: &TrackedScanRequest,
) -> Result<Vec<TrackedTombstoneMarker>, LixError> {
    scan_tombstones_with_executor(executor, request).await
}

pub(crate) async fn load_live_row_access(
    backend: &dyn LixBackend,
    schema_key: &str,
) -> Result<LiveRowAccess, LixError> {
    load_live_row_access_with_backend(backend, schema_key).await
}

pub(crate) async fn load_live_row_access_for_table(
    backend: &dyn LixBackend,
    table_name: &str,
) -> Result<Option<LiveRowAccess>, LixError> {
    load_live_row_access_for_table_name(backend, table_name).await
}

pub(crate) fn normalized_values_from_snapshot(
    access: &LiveRowAccess,
    snapshot_content: Option<&str>,
) -> Result<std::collections::BTreeMap<String, Value>, LixError> {
    normalized_live_column_values(access.layout(), snapshot_content)
}

pub(crate) fn projected_row_snapshot_json(
    access: Option<&LiveRowAccess>,
    schema_key: &str,
    row: &[Value],
    first_projected_column: usize,
    raw_snapshot_index: usize,
) -> Result<serde_json::Value, LixError> {
    logical_snapshot_from_projected_row(
        access,
        schema_key,
        row,
        first_projected_column,
        raw_snapshot_index,
    )
    .and_then(|value| {
        value.ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_UNKNOWN",
                format!(
                    "projected row for schema '{}' did not contain a logical snapshot",
                    schema_key
                ),
            )
        })
    })
}

pub(crate) fn snapshot_json_from_row(
    access: &LiveRowAccess,
    row: &RawRow,
) -> Result<serde_json::Value, LixError> {
    snapshot_json(access, row)
}

pub(crate) fn is_untracked_live_table_name(table_name: &str) -> bool {
    is_untracked_live_table(table_name)
}
