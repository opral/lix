use std::sync::Arc;

use crate::changelog::{CanonicalChange, MaterializedCanonicalChange};
use crate::json_store::JsonStoreContext;
use crate::live_state::{LiveStateRow, MaterializedLiveStateRow};
use crate::storage::StorageContext;
use crate::storage::StorageWriteSet;
use crate::tracked_state::{MaterializedTrackedStateRow, TrackedStateContext, TrackedStateRow};
use crate::transaction::prepare_version_ref_row;
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRow,
};
use crate::version::VersionContext;
use crate::GLOBAL_VERSION_ID;

pub(crate) const TEST_EMPTY_ROOT_COMMIT_ID: &str = "test-empty-root";
const TEST_TIMESTAMP: &str = "1970-01-01T00:00:00.000Z";

/// Seeds a version head and matching tracked root for unit tests.
///
/// A version ref that points at a commit without a tracked root is invalid for
/// the serving projection. This helper keeps that invariant in one place while
/// still letting low-level tests use synthetic commit ids.
pub(crate) async fn seed_version_head(storage: StorageContext, version_id: &str, commit_id: &str) {
    seed_version_head_with_rows(storage, version_id, commit_id, &[]).await;
}

/// Seeds the global version head to an empty tracked root for unit tests.
pub(crate) async fn seed_global_version_head(storage: StorageContext) {
    seed_version_head(storage, GLOBAL_VERSION_ID, TEST_EMPTY_ROOT_COMMIT_ID).await;
}

/// Seeds a version head and writes the tracked root contents for its commit.
pub(crate) async fn seed_version_head_with_rows(
    storage: StorageContext,
    version_id: &str,
    commit_id: &str,
    rows: &[MaterializedTrackedStateRow],
) {
    let mut transaction = storage
        .begin_write_transaction()
        .await
        .expect("seed transaction should open");
    let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
    let mut writes = StorageWriteSet::new();
    let canonical_row = {
        let mut json_writer = JsonStoreContext::new().writer();
        prepare_version_ref_row(
            &mut writes,
            &mut json_writer,
            version_id,
            commit_id,
            TEST_TIMESTAMP,
        )
        .expect("version ref should canonicalize")
    };
    version_ctx
        .stage_canonical_ref_rows(&mut writes, &[canonical_row])
        .expect("version ref should stage");
    writes
        .apply(&mut transaction.as_mut())
        .await
        .expect("version ref should write");
    let mut writes = StorageWriteSet::new();
    {
        let mut json_writer = JsonStoreContext::new().writer();
        let canonical_rows =
            tracked_state_rows_from_materialized(&mut writes, &mut json_writer, rows)
                .expect("tracked rows should canonicalize");
        TrackedStateContext::new()
            .writer()
            .stage_root(
                &mut transaction.as_mut(),
                &mut writes,
                commit_id,
                None,
                &canonical_rows,
            )
            .await
            .expect("tracked root should write");
    }
    writes
        .apply(&mut transaction.as_mut())
        .await
        .expect("tracked root should write");
    transaction.commit().await.expect("seed should commit");
}

pub(crate) fn tracked_state_rows_from_materialized(
    writes: &mut StorageWriteSet,
    json_writer: &mut crate::json_store::JsonStoreWriter,
    rows: &[MaterializedTrackedStateRow],
) -> Result<Vec<TrackedStateRow>, crate::LixError> {
    rows.iter()
        .map(|row| tracked_state_row_from_materialized(writes, json_writer, row))
        .collect()
}

pub(crate) fn tracked_state_row_from_materialized(
    writes: &mut StorageWriteSet,
    json_writer: &mut crate::json_store::JsonStoreWriter,
    row: &MaterializedTrackedStateRow,
) -> Result<TrackedStateRow, crate::LixError> {
    Ok(TrackedStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row
            .snapshot_content
            .as_deref()
            .map(|value| json_writer.stage_bytes(writes, value.as_bytes()))
            .transpose()?,
        metadata_ref: row
            .metadata
            .as_ref()
            .map(|value| {
                let serialized = crate::serialize_row_metadata(value);
                json_writer.stage_bytes(writes, serialized.as_bytes())
            })
            .transpose()?,
        schema_version: row.schema_version.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        change_id: row.change_id.clone(),
        commit_id: row.commit_id.clone(),
    })
}

pub(crate) fn untracked_state_row_from_materialized(
    writes: &mut StorageWriteSet,
    json_writer: &mut crate::json_store::JsonStoreWriter,
    row: &MaterializedUntrackedStateRow,
) -> Result<UntrackedStateRow, crate::LixError> {
    Ok(UntrackedStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row
            .snapshot_content
            .as_deref()
            .map(|value| json_writer.stage_bytes(writes, value.as_bytes()))
            .transpose()?,
        metadata_ref: row
            .metadata
            .as_ref()
            .map(|value| {
                let serialized = crate::serialize_row_metadata(value);
                json_writer.stage_bytes(writes, serialized.as_bytes())
            })
            .transpose()?,
        schema_version: row.schema_version.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        version_id: row.version_id.clone(),
    })
}

pub(crate) fn live_state_row_from_materialized(
    writes: &mut StorageWriteSet,
    json_writer: &mut crate::json_store::JsonStoreWriter,
    row: &MaterializedLiveStateRow,
) -> Result<LiveStateRow, crate::LixError> {
    Ok(LiveStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row
            .snapshot_content
            .as_deref()
            .map(|value| json_writer.stage_bytes(writes, value.as_bytes()))
            .transpose()?,
        metadata_ref: row
            .metadata
            .as_ref()
            .map(|value| {
                let serialized = crate::serialize_row_metadata(value);
                json_writer.stage_bytes(writes, serialized.as_bytes())
            })
            .transpose()?,
        schema_version: row.schema_version.clone(),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        change_id: row.change_id.clone(),
        commit_id: row.commit_id.clone(),
        untracked: row.untracked,
        version_id: row.version_id.clone(),
    })
}

pub(crate) fn canonical_change_from_materialized(
    writes: &mut StorageWriteSet,
    json_writer: &mut crate::json_store::JsonStoreWriter,
    change: &MaterializedCanonicalChange,
) -> Result<CanonicalChange, crate::LixError> {
    Ok(CanonicalChange {
        id: change.id.clone(),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        schema_version: change.schema_version.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref: change
            .snapshot_content
            .as_deref()
            .map(|value| json_writer.stage_bytes(writes, value.as_bytes()))
            .transpose()?,
        metadata_ref: change
            .metadata
            .as_ref()
            .map(|value| {
                let serialized = crate::serialize_row_metadata(value);
                json_writer.stage_bytes(writes, serialized.as_bytes())
            })
            .transpose()?,
        created_at: change.created_at.clone(),
    })
}
