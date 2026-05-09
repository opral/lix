use std::sync::Arc;

use crate::commit_store::{Change, CommitDraftRef, CommitStoreContext};
use crate::json_store::{JsonStoreContext, NormalizedJson};
use crate::storage::StorageContext;
use crate::storage::StorageWriteSet;
use crate::storage::StorageWriteTransaction;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateDeltaRef,
};
use crate::transaction::prepare_version_ref_row;
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRow,
};
use crate::version::VersionContext;

fn prepare_json_ref(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    value: &str,
) -> Result<crate::json_store::JsonRef, crate::LixError> {
    json_writer.prepare_json(NormalizedJson::from_arc_unchecked(Arc::from(value)))
}
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
        let row = prepare_version_ref_row(&mut json_writer, version_id, commit_id, TEST_TIMESTAMP)
            .expect("version ref should canonicalize");
        json_writer.flush_into(&mut writes);
        row
    };
    version_ctx
        .stage_canonical_ref_rows(&mut writes, &[canonical_row])
        .expect("version ref should stage");
    writes
        .apply(&mut transaction.as_mut())
        .await
        .expect("version ref should write");
    stage_tracked_root_from_materialized(
        transaction.as_mut(),
        &TrackedStateContext::new(),
        commit_id,
        None,
        rows,
    )
    .await
    .expect("tracked root should write");
    transaction.commit().await.expect("seed should commit");
}

pub(crate) async fn stage_tracked_root_from_materialized(
    transaction: &mut dyn StorageWriteTransaction,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let mut writes = StorageWriteSet::new();
    let mut json_writer = JsonStoreContext::new().writer();
    let changes = rows
        .iter()
        .map(|row| tracked_change_from_materialized(&mut json_writer, row))
        .collect::<Result<Vec<_>, _>>()?;
    json_writer.flush_into(&mut writes);

    let parent_ids = parent_commit_id
        .map(|parent| vec![parent.to_string()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id}:commit");
    let commit = CommitDraftRef {
        id: commit_id,
        change_id: &commit_change_id,
        parent_ids: &parent_ids,
        author_account_ids: &[],
        created_at: rows
            .first()
            .map(|row| row.updated_at.as_str())
            .unwrap_or(TEST_TIMESTAMP),
    };
    let commit_store = CommitStoreContext::new();
    let change_ids = changes
        .iter()
        .map(|change| change.id.clone())
        .collect::<Vec<_>>();
    let existing_changes = commit_store
        .reader(&mut *transaction)
        .load_change_index_entries(&change_ids)
        .await?;
    let mut authored_changes = Vec::new();
    let mut authored_created_at = Vec::new();
    let mut authored_updated_at = Vec::new();
    let mut adopted_changes = Vec::new();
    let mut adopted_created_at = Vec::new();
    let mut adopted_updated_at = Vec::new();
    for ((change, row), existing) in changes.iter().zip(rows).zip(existing_changes) {
        if existing.is_some() {
            adopted_changes.push(change.as_ref());
            adopted_created_at.push(row.created_at.as_str());
            adopted_updated_at.push(row.updated_at.as_str());
        } else {
            authored_changes.push(change.as_ref());
            authored_created_at.push(row.created_at.as_str());
            authored_updated_at.push(row.updated_at.as_str());
        }
    }
    let staged = commit_store
        .writer(&mut *transaction, &mut writes)
        .stage_commit_draft(commit, authored_changes.clone(), adopted_changes.clone())
        .await?;
    let mut deltas = Vec::with_capacity(changes.len());
    deltas.extend(
        authored_changes
            .iter()
            .zip(&staged.authored_locators)
            .zip(authored_created_at)
            .zip(authored_updated_at)
            .map(
                |(((change, locator), created_at), updated_at)| TrackedStateDeltaRef {
                    change: *change,
                    locator: locator.as_ref(),
                    created_at,
                    updated_at,
                },
            ),
    );
    deltas.extend(
        adopted_changes
            .iter()
            .zip(&staged.adopted_locators)
            .zip(adopted_created_at)
            .zip(adopted_updated_at)
            .map(
                |(((change, locator), created_at), updated_at)| TrackedStateDeltaRef {
                    change: *change,
                    locator: locator.as_ref(),
                    created_at,
                    updated_at,
                },
            ),
    );
    tracked_state
        .writer(&mut *transaction, &mut writes)
        .stage_delta(commit_id, parent_commit_id, deltas)
        .await?;
    writes.apply(&mut *transaction).await.map(|_| ())
}

pub(crate) fn tracked_change_from_materialized(
    json_writer: &mut crate::json_store::JsonStoreWriter,
    row: &MaterializedTrackedStateRow,
) -> Result<Change, crate::LixError> {
    Ok(Change {
        id: row.change_id.clone(),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row
            .snapshot_content
            .as_deref()
            .map(|value| prepare_json_ref(json_writer, value))
            .transpose()?,
        metadata_ref: row
            .metadata
            .as_ref()
            .map(|value| {
                let serialized = crate::serialize_row_metadata(value);
                prepare_json_ref(json_writer, &serialized)
            })
            .transpose()?,
        created_at: row.created_at.clone(),
    })
}

pub(crate) fn untracked_state_row_from_materialized(
    writes: &mut StorageWriteSet,
    json_writer: &mut crate::json_store::JsonStoreWriter,
    row: &MaterializedUntrackedStateRow,
) -> Result<UntrackedStateRow, crate::LixError> {
    let row = UntrackedStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row
            .snapshot_content
            .as_deref()
            .map(|value| prepare_json_ref(json_writer, value))
            .transpose()?,
        metadata_ref: row
            .metadata
            .as_ref()
            .map(|value| {
                let serialized = crate::serialize_row_metadata(value);
                prepare_json_ref(json_writer, &serialized)
            })
            .transpose()?,
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        version_id: row.version_id.clone(),
    };
    json_writer.flush_into(writes);
    Ok(row)
}
