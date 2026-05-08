use crate::binary_cas::BinaryCasContext;
use crate::changelog::{CanonicalChange, CanonicalChangeRef, ChangelogContext};
use crate::functions::FunctionContext;
#[cfg(test)]
use crate::json_store::{JsonStoreContext, JsonStoreWriter, NormalizedJson};
use crate::live_state::{LiveStateContext, LiveStateTrackedRowRef, LiveStateWriter};
use crate::storage::{StorageReader, StorageWriteSet, StorageWriteTransaction};
use crate::tracked_state::{TrackedStateKeyRef, TrackedStateRowRef, TrackedStateValueRef};
use crate::transaction::prepare_version_ref_row;
use crate::transaction::staging::PreparedWriteSet;
use crate::transaction::types::{
    PreparedAdoptedStateRow, PreparedStateRow, StageJson, StagedCommitMembers, TransactionJson,
};
use crate::untracked_state::{UntrackedStateIdentityRef, UntrackedStateRowRef};
use crate::version::{VersionContext, VersionRefReader};
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};

const COMMIT_SCHEMA_KEY: &str = "lix_commit";

type RowIndex = usize;
type AdoptedRowIndex = usize;

/// Commits prepared transaction rows into durable tracked and untracked stores.
///
/// Providers decode DataFusion DML into hydrated `PreparedStateRow`s. Untracked
/// rows are durable local overlay state and bypass changelog/commit rows.
/// Tracked rows receive normal `lix_commit` rows, append canonical changelog
/// facts, then update the live-state serving projection. The tracked side of
/// that projection is a prolly root keyed by the new commit id.
pub(crate) async fn commit_prepared_writes(
    binary_cas: &BinaryCasContext,
    changelog: &ChangelogContext,
    live_state: &LiveStateContext,
    version_ctx: &VersionContext,
    runtime_functions: Option<&FunctionContext>,
    transaction: &mut (impl StorageWriteTransaction + ?Sized),
    prepared_writes: PreparedWriteSet,
) -> Result<(), LixError> {
    let mut writes = StorageWriteSet::new();
    let mut json_writer = prepared_writes.json_writer;

    if !prepared_writes.file_data_writes.is_empty() {
        let mut blob_writer = binary_cas.writer(&mut writes);
        for write in &prepared_writes.file_data_writes {
            blob_writer.stage_bytes(&write.data)?;
        }
    }

    let state_rows = prepared_writes.state_rows;
    let adopted_rows = prepared_writes.adopted_rows;
    let finalized = finalize_commit_rows(
        prepared_writes.commit_members_by_version,
        prepared_writes.extra_commit_parents_by_version,
        version_ctx,
        transaction,
        &mut json_writer,
    )
    .await?;
    let commit_rows = finalized.commit_rows;
    let version_heads = finalized.version_heads;
    let tracked_roots = finalized.tracked_roots;
    let row_index = index_prepared_rows(&state_rows)?;
    let adopted_index = index_adopted_rows(&adopted_rows);

    if let Some(runtime_functions) = runtime_functions {
        let mut writer = live_state.writer(&mut *transaction);
        runtime_functions
            .stage_persist_if_needed(&mut writer, &mut writes, &mut json_writer)
            .await?;
    }

    if state_rows.is_empty()
        && adopted_rows.is_empty()
        && commit_rows.is_empty()
        && version_heads.is_empty()
        && writes.is_empty()
    {
        return Ok(());
    }

    if !row_index.changelog_row_indices.is_empty() || !commit_rows.is_empty() {
        validate_new_canonical_changes(
            changelog,
            transaction,
            &state_rows,
            &row_index.changelog_row_indices,
            &commit_rows,
        )
        .await?;
        {
            let mut writer = changelog.writer(&mut writes);
            for &row_index in &row_index.changelog_row_indices {
                writer.stage_changes(std::iter::once(canonical_change_ref_from_state_row(
                    &state_rows[row_index],
                )?))?;
            }
            for commit_row in &commit_rows {
                writer.stage_changes(std::iter::once(canonical_change_ref_from_commit_row(
                    commit_row,
                )))?;
            }
        }
    }
    if !adopted_rows.is_empty() {
        validate_adopted_canonical_changes(changelog, transaction, &adopted_rows).await?;
    }

    // The serving projection is updated in the same backend transaction as the
    // changelog append. Tracked rows become prolly mutations under their owning
    // commit root; untracked rows remain in the separate local overlay store.
    {
        let mut writer = live_state.writer(&mut *transaction);
        writer.stage_untracked_rows(
            &mut writes,
            row_index
                .untracked_row_indices
                .iter()
                .map(|&row_index| untracked_row_ref_from_state_row(&state_rows[row_index])),
        )?;
        writer.stage_delete_untracked_rows(
            &mut writes,
            row_index
                .changelog_row_indices
                .iter()
                .map(|&row_index| untracked_identity_ref_from_state_row(&state_rows[row_index]))
                .chain(
                    adopted_rows
                        .iter()
                        .map(untracked_identity_ref_from_adopted_row),
                ),
        );
        stage_tracked_roots(
            &mut writer,
            &mut writes,
            &state_rows,
            row_index.tracked_row_indices_by_commit,
            &adopted_rows,
            adopted_index.tracked_row_indices_by_commit,
            tracked_roots,
        )
        .await?;
    }

    for version_head in version_heads {
        let canonical_row = prepare_version_ref_row(
            &mut json_writer,
            &version_head.version_id,
            &version_head.commit_id,
            &version_head.timestamp,
        )?;
        version_ctx.stage_canonical_ref_rows(&mut writes, &[canonical_row])?;
    }

    json_writer.flush_into(&mut writes);
    writes.apply(transaction).await?;
    Ok(())
}

struct PreparedRowIndex {
    changelog_row_indices: Vec<RowIndex>,
    untracked_row_indices: Vec<RowIndex>,
    tracked_row_indices_by_commit: BTreeMap<String, Vec<RowIndex>>,
}

struct PreparedAdoptedRowIndex {
    tracked_row_indices_by_commit: BTreeMap<String, Vec<AdoptedRowIndex>>,
}

fn index_prepared_rows(rows: &[PreparedStateRow]) -> Result<PreparedRowIndex, LixError> {
    let mut changelog_row_indices = Vec::new();
    let mut untracked_row_indices = Vec::new();
    let mut tracked_row_indices_by_commit = BTreeMap::<String, Vec<RowIndex>>::new();

    for (row_index, row) in rows.iter().enumerate() {
        if row.untracked {
            untracked_row_indices.push(row_index);
            continue;
        }
        let Some(commit_id) = row.commit_id.as_ref() else {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked prepared row is missing commit_id before commit indexing",
            ));
        };
        changelog_row_indices.push(row_index);
        tracked_row_indices_by_commit
            .entry(commit_id.clone())
            .or_default()
            .push(row_index);
    }

    Ok(PreparedRowIndex {
        changelog_row_indices,
        untracked_row_indices,
        tracked_row_indices_by_commit,
    })
}

fn index_adopted_rows(rows: &[PreparedAdoptedStateRow]) -> PreparedAdoptedRowIndex {
    let mut tracked_row_indices_by_commit = BTreeMap::<String, Vec<AdoptedRowIndex>>::new();
    for (row_index, row) in rows.iter().enumerate() {
        tracked_row_indices_by_commit
            .entry(row.commit_id.clone())
            .or_default()
            .push(row_index);
    }
    PreparedAdoptedRowIndex {
        tracked_row_indices_by_commit,
    }
}

async fn validate_new_canonical_changes(
    changelog: &ChangelogContext,
    transaction: &mut (impl StorageReader + ?Sized),
    rows: &[PreparedStateRow],
    row_indices: &[RowIndex],
    commit_rows: &[FinalizedCommitRow],
) -> Result<(), LixError> {
    let reader = changelog.reader(&mut *transaction);
    let mut change_ids = Vec::with_capacity(row_indices.len() + commit_rows.len());
    let mut seen_change_ids = BTreeSet::new();
    for &row_index in row_indices {
        let change = canonical_change_ref_from_state_row(&rows[row_index])?;
        if !seen_change_ids.insert(change.id) {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "canonical change id '{}' appears more than once in the same transaction",
                    change.id
                ),
            ));
        }
        change_ids.push(change.id.to_string());
    }
    for commit_row in commit_rows {
        let change = canonical_change_ref_from_commit_row(commit_row);
        if !seen_change_ids.insert(change.id) {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "canonical change id '{}' appears more than once in the same transaction",
                    change.id
                ),
            ));
        }
        change_ids.push(change.id.to_string());
    }
    let existing_changes = reader.load_changes(&change_ids).await?;
    for (change_id, existing) in change_ids.iter().zip(existing_changes) {
        let Some(existing) = existing else {
            continue;
        };
        let entity_id = existing
            .entity_id
            .as_json_array_text()
            .unwrap_or_else(|_| "<invalid entity_id>".to_string());
        return Err(LixError::new(
            LixError::CODE_UNIQUE,
            format!(
                "canonical change id '{}' already exists with different content for schema '{}' entity '{}'",
                change_id, existing.schema_key, entity_id
            ),
        ));
    }
    Ok(())
}

async fn validate_adopted_canonical_changes(
    changelog: &ChangelogContext,
    transaction: &mut (impl StorageReader + ?Sized),
    rows: &[PreparedAdoptedStateRow],
) -> Result<(), LixError> {
    let mut change_ids = Vec::with_capacity(rows.len());
    let mut seen_change_ids = BTreeSet::new();
    for row in rows {
        let expected = canonical_change_ref_from_adopted_row(row);
        if !seen_change_ids.insert(expected.id) {
            return Err(LixError::new(
                LixError::CODE_UNIQUE,
                format!(
                    "adopted canonical change id '{}' appears more than once in the same transaction",
                    expected.id
                ),
            ));
        }
        change_ids.push(expected.id.to_string());
    }
    let reader = changelog.reader(&mut *transaction);
    let existing_changes = reader.load_changes(&change_ids).await?;
    for (expected, existing) in rows
        .iter()
        .map(canonical_change_ref_from_adopted_row)
        .zip(existing_changes)
    {
        match existing {
            Some(existing) if canonical_change_matches_ref(&existing, expected) => {}
            Some(existing) => {
                let entity_id = existing
                    .entity_id
                    .as_json_array_text()
                    .unwrap_or_else(|_| "<invalid entity_id>".to_string());
                return Err(LixError::new(
                    LixError::CODE_UNIQUE,
                    format!(
                        "adopted canonical change id '{}' exists with different content for schema '{}' entity '{}'",
                        expected.id, existing.schema_key, entity_id
                    ),
                ));
            }
            None => {
                return Err(LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!(
                        "adopted canonical change id '{}' does not exist in the changelog",
                        expected.id
                    ),
                ));
            }
        }
    }
    Ok(())
}

async fn stage_tracked_roots<S>(
    writer: &mut LiveStateWriter<S>,
    writes: &mut StorageWriteSet,
    state_rows: &[PreparedStateRow],
    mut tracked_row_indices_by_commit: BTreeMap<String, Vec<RowIndex>>,
    adopted_rows: &[PreparedAdoptedStateRow],
    mut adopted_row_indices_by_commit: BTreeMap<String, Vec<AdoptedRowIndex>>,
    tracked_roots: Vec<PendingTrackedRoot>,
) -> Result<(), LixError>
where
    S: StorageReader,
{
    for root in tracked_roots {
        let state_row_indices = tracked_row_indices_by_commit
            .remove(&root.commit_id)
            .unwrap_or_default();
        let adopted_row_indices = adopted_row_indices_by_commit
            .remove(&root.commit_id)
            .unwrap_or_default();
        let mut root_rows = Vec::with_capacity(state_row_indices.len() + adopted_row_indices.len());
        for row_index in state_row_indices {
            if let Some(row) = tracked_state_row_ref_from_state_row(&state_rows[row_index])? {
                root_rows.push(row);
            }
        }
        for row_index in adopted_row_indices {
            root_rows.push(tracked_state_row_ref_from_adopted_row(
                &adopted_rows[row_index],
            ));
        }
        writer
            .stage_tracked_root(
                writes,
                &root.version_id,
                &root.commit_id,
                root.parent_commit_id.as_deref(),
                root_rows,
            )
            .await?;
    }
    if !tracked_row_indices_by_commit.is_empty() || !adopted_row_indices_by_commit.is_empty() {
        let mut commit_ids = tracked_row_indices_by_commit
            .keys()
            .chain(adopted_row_indices_by_commit.keys())
            .cloned()
            .collect::<Vec<_>>();
        commit_ids.sort();
        commit_ids.dedup();
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked live_state rows have no finalized root metadata for commit ids: {}",
                commit_ids.join(", ")
            ),
        ));
    }
    Ok(())
}

fn canonical_change_matches_ref(
    change: &CanonicalChange,
    expected: CanonicalChangeRef<'_>,
) -> bool {
    change.id == expected.id
        && &change.entity_id == expected.entity_id
        && change.schema_key == expected.schema_key
        && change.file_id.as_deref() == expected.file_id
        && change.snapshot_ref.as_ref() == expected.snapshot_ref
        && change.metadata_ref.as_ref() == expected.metadata_ref
        && change.created_at == expected.created_at
}

fn canonical_change_ref_from_state_row(
    row: &PreparedStateRow,
) -> Result<CanonicalChangeRef<'_>, LixError> {
    let Some(change_id) = row.change_id.as_deref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked staged row is missing change_id before changelog append",
        ));
    };

    Ok(CanonicalChangeRef {
        id: change_id,
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        created_at: &row.created_at,
    })
}

fn canonical_change_ref_from_adopted_row(row: &PreparedAdoptedStateRow) -> CanonicalChangeRef<'_> {
    CanonicalChangeRef {
        id: &row.change_id,
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        created_at: &row.created_at,
    }
}

fn canonical_change_ref_from_commit_row(row: &FinalizedCommitRow) -> CanonicalChangeRef<'_> {
    CanonicalChangeRef {
        id: &row.change_id,
        entity_id: &row.entity_id,
        schema_key: COMMIT_SCHEMA_KEY,
        file_id: None,
        snapshot_ref: Some(&row.snapshot.json_ref),
        metadata_ref: None,
        created_at: &row.created_at,
    }
}

fn untracked_row_ref_from_state_row(row: &PreparedStateRow) -> UntrackedStateRowRef<'_> {
    UntrackedStateRowRef {
        entity_id: &row.entity_id,
        schema_key: &row.schema_key,
        file_id: row.file_id.as_deref(),
        snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
        metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
        created_at: &row.created_at,
        updated_at: &row.updated_at,
        global: row.global,
        version_id: &row.version_id,
    }
}

fn untracked_identity_ref_from_state_row(row: &PreparedStateRow) -> UntrackedStateIdentityRef<'_> {
    UntrackedStateIdentityRef {
        version_id: &row.version_id,
        schema_key: &row.schema_key,
        entity_id: &row.entity_id,
        file_id: row.file_id.as_deref(),
    }
}

fn untracked_identity_ref_from_adopted_row(
    row: &PreparedAdoptedStateRow,
) -> UntrackedStateIdentityRef<'_> {
    UntrackedStateIdentityRef {
        version_id: &row.version_id,
        schema_key: &row.schema_key,
        entity_id: &row.entity_id,
        file_id: row.file_id.as_deref(),
    }
}

fn tracked_state_row_ref_from_state_row(
    row: &PreparedStateRow,
) -> Result<Option<LiveStateTrackedRowRef<'_>>, LixError> {
    if row.schema_key == COMMIT_SCHEMA_KEY {
        return Ok(None);
    }
    let Some(change_id) = row.change_id.as_deref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked staged row is missing change_id before tracked_state write",
        ));
    };
    let Some(commit_id) = row.commit_id.as_deref() else {
        return Err(LixError::new(
            "LIX_ERROR_UNKNOWN",
            "tracked staged row is missing commit_id before tracked_state write",
        ));
    };
    Ok(Some(LiveStateTrackedRowRef {
        global: row.global,
        version_id: &row.version_id,
        row: TrackedStateRowRef {
            key: TrackedStateKeyRef {
                schema_key: &row.schema_key,
                file_id: row.file_id.as_deref(),
                entity_id: &row.entity_id,
            },
            value: TrackedStateValueRef {
                snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
                metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
                created_at: &row.created_at,
                updated_at: &row.updated_at,
                change_id,
                commit_id,
                deleted: row.snapshot.is_none(),
            },
        },
    }))
}

fn tracked_state_row_ref_from_adopted_row(
    row: &PreparedAdoptedStateRow,
) -> LiveStateTrackedRowRef<'_> {
    LiveStateTrackedRowRef {
        global: row.global,
        version_id: &row.version_id,
        row: TrackedStateRowRef {
            key: TrackedStateKeyRef {
                schema_key: &row.schema_key,
                file_id: row.file_id.as_deref(),
                entity_id: &row.entity_id,
            },
            value: TrackedStateValueRef {
                snapshot_ref: row.snapshot.as_ref().map(|snapshot| &snapshot.json_ref),
                metadata_ref: row.metadata.as_ref().map(|metadata| &metadata.json_ref),
                created_at: &row.created_at,
                updated_at: &row.updated_at,
                change_id: &row.change_id,
                commit_id: &row.commit_id,
                deleted: row.snapshot.is_none(),
            },
        },
    }
}

/// Materializes tracked staged membership into `lix_commit` rows.
///
/// Staging only accumulates `version_id -> change_ids` because commit ids,
/// parent heads, and commit-row timestamps belong to transaction finalization.
/// The `change_ids` list is the ordered set of canonical changes whose effects
/// the commit introduces relative to its first parent; merge commits may later
/// populate this list with existing source-parent changes instead of copied
/// changelog facts.
/// This function turns those membership sets into finalized commit rows for the
/// changelog. Commit rows are graph facts, not normalized state rows, so they
/// do not carry transaction schema-plan ids.
///
/// Commit finalization output split by durability target.
///
/// `commit_rows` are ordinary changelog facts. live_state later projects them
/// from commit_graph; tracked_state roots do not store commit graph facts.
///
/// `version_heads` are moving refs. They are written through `VersionContext`
/// and must never be appended to changelog.
struct FinalizedCommitRows {
    commit_rows: Vec<FinalizedCommitRow>,
    version_heads: Vec<PendingVersionHead>,
    tracked_roots: Vec<PendingTrackedRoot>,
}

struct FinalizedCommitRow {
    entity_id: crate::entity_identity::EntityIdentity,
    snapshot: StageJson,
    created_at: String,
    change_id: String,
}

struct PendingVersionHead {
    version_id: String,
    commit_id: String,
    timestamp: String,
}

struct PendingTrackedRoot {
    version_id: String,
    commit_id: String,
    parent_commit_id: Option<String>,
}

async fn finalize_commit_rows(
    commit_members_by_version: BTreeMap<String, StagedCommitMembers>,
    extra_commit_parents_by_version: BTreeMap<String, Vec<String>>,
    version_ctx: &VersionContext,
    transaction: &mut (impl StorageReader + ?Sized),
    json_writer: &mut crate::json_store::JsonStoreWriter,
) -> Result<FinalizedCommitRows, LixError> {
    let mut commit_rows = Vec::new();
    let mut version_heads = Vec::new();
    let mut tracked_roots = Vec::new();

    for (version_id, members) in commit_members_by_version {
        if members.is_empty() && !members.allow_empty {
            continue;
        }

        let commit_id = members.commit_id;
        let commit_change_id = members.commit_change_id;
        let change_set_id = members.change_set_id;
        let timestamp = members.created_at;
        let change_ids = members.change_ids.into_iter().collect::<Vec<_>>();
        let parent_commit_ids = version_ctx
            .ref_reader(&mut *transaction)
            .load_head_commit_id(&version_id)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        let parent_commit_ids = merge_parent_commit_ids(
            parent_commit_ids,
            extra_commit_parents_by_version
                .get(&version_id)
                .cloned()
                .unwrap_or_default(),
        );
        let parent_commit_id = parent_commit_ids.first().cloned();
        let snapshot = crate::transaction::types::stage_json_from_value(
            json_writer,
            TransactionJson::from_value(
                serde_json::json!({
                    "id": commit_id,
                    "change_set_id": change_set_id,
                    "change_ids": change_ids,
                    "author_account_ids": [],
                    "parent_commit_ids": parent_commit_ids,
                }),
                "engine commit row snapshot_content",
            )?,
            "engine commit row snapshot_content",
        )?;

        commit_rows.push(FinalizedCommitRow {
            entity_id: crate::entity_identity::EntityIdentity::single(&commit_id),
            snapshot,
            created_at: timestamp.clone(),
            change_id: commit_change_id,
        });
        version_heads.push(PendingVersionHead {
            version_id: version_id.clone(),
            commit_id: commit_id.clone(),
            timestamp,
        });
        tracked_roots.push(PendingTrackedRoot {
            version_id,
            commit_id,
            parent_commit_id,
        });
    }

    Ok(FinalizedCommitRows {
        commit_rows,
        version_heads,
        tracked_roots,
    })
}

fn merge_parent_commit_ids(mut base: Vec<String>, extra: Vec<String>) -> Vec<String> {
    for parent in extra {
        if !base.contains(&parent) {
            base.push(parent);
        }
    }
    base
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    };

    use async_trait::async_trait;
    use serde_json::Value as JsonValue;

    use super::*;
    use crate::backend::{
        testing::UnitTestBackend, Backend, BackendKvEntryPage, BackendKvExistsBatch,
        BackendKvGetRequest, BackendKvKeyPage, BackendKvScanRequest, BackendKvValueBatch,
        BackendKvValuePage, BackendKvWriteBatch, BackendKvWriteStats, BackendReadTransaction,
        BackendWriteTransaction,
    };
    use crate::changelog::ChangelogContext;
    use crate::live_state::{LiveStateContext, LiveStateRowRequest};
    use crate::schema_catalog::SchemaPlanId;
    use crate::storage::StorageContext;
    use crate::transaction::types::PreparedRowFacts;
    use crate::untracked_state::{
        MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRowRequest,
    };
    use crate::version::VersionContext;
    use crate::NullableKeyFilter;
    use crate::GLOBAL_VERSION_ID;

    const DETERMINISTIC_MODE_KEY: &str = "lix_deterministic_mode";
    const DETERMINISTIC_SEQUENCE_KEY: &str = "lix_deterministic_sequence_number";

    fn live_state_context() -> LiveStateContext {
        LiveStateContext::new(
            crate::tracked_state::TrackedStateContext::new(),
            crate::untracked_state::UntrackedStateContext::new(),
            crate::commit_graph::CommitGraphContext::new(crate::changelog::ChangelogContext::new()),
        )
    }

    #[tokio::test]
    async fn commit_staged_writes_appends_changelog_and_updates_serving_projection() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let mut json_writer = JsonStoreContext::new().writer();
        let state_rows = vec![tracked_global_row(&mut json_writer, "change-1")];
        commit_prepared_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-1"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
                json_writer,
            },
        )
        .await
        .expect("commit should flush staged rows");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = {
            let reader = changelog.reader(storage.clone());
            reader
                .scan_changes(&crate::changelog::ChangelogScanRequest::default())
                .await
        }
        .expect("changelog scan should succeed");
        let change_ids = changes
            .iter()
            .map(|change| change.id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(change_ids, vec!["change-1", "test-uuid-2"]);
        assert!(changes
            .iter()
            .any(|change| change.schema_key == "lix_commit"));
        assert!(!changes
            .iter()
            .any(|change| change.schema_key == "lix_version_ref"));

        let loaded_head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn commit_with_only_untracked_writes_does_not_create_lix_commit() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let untracked_state = UntrackedStateContext::new();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let mut json_writer = JsonStoreContext::new().writer();
        let state_rows = vec![untracked_global_row(&mut json_writer, "change-untracked")];
        commit_prepared_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::new(),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
                json_writer,
            },
        )
        .await
        .expect("commit should flush untracked row");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = {
            let reader = changelog.reader(storage.clone());
            reader
                .scan_changes(&crate::changelog::ChangelogScanRequest::default())
                .await
        }
        .expect("changelog scan should succeed");
        assert!(changes.is_empty());

        let loaded = {
            let mut untracked_reader = untracked_state.reader(storage.clone());
            untracked_reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "test_schema".to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("untracked row load should succeed")
        .expect("untracked row should be persisted");
        assert_eq!(
            loaded.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );
    }

    #[tokio::test]
    async fn tracked_write_deletes_matching_untracked_overlay() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let untracked_state = UntrackedStateContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));

        let mut seed_transaction = storage
            .begin_write_transaction()
            .await
            .expect("seed transaction should open");
        let mut writes = StorageWriteSet::new();
        let canonical_row = {
            let mut json_writer = JsonStoreContext::new().writer();
            let staged_row = untracked_global_row(&mut json_writer, "change-untracked");
            let canonical_row = crate::test_support::untracked_state_row_from_materialized(
                &mut writes,
                &mut json_writer,
                &MaterializedUntrackedStateRow::from(staged_row),
            )
            .expect("untracked seed should canonicalize");
            json_writer.flush_into(&mut writes);
            canonical_row
        };
        untracked_state
            .writer(&mut writes)
            .stage_rows(std::iter::once(canonical_row.as_ref()))
            .expect("untracked seed should write");
        writes
            .apply(&mut seed_transaction.as_mut())
            .await
            .expect("untracked seed should apply");
        seed_transaction
            .commit()
            .await
            .expect("seed transaction should persist");

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut json_writer = JsonStoreContext::new().writer();
        let state_rows = vec![tracked_global_row(&mut json_writer, "change-tracked")];
        commit_prepared_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-tracked"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
                json_writer,
            },
        )
        .await
        .expect("tracked commit should flush");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let untracked = {
            let mut untracked_reader = untracked_state.reader(storage.clone());
            untracked_reader.load_row(&untracked_request()).await
        }
        .expect("untracked load should succeed");
        assert_eq!(untracked, None);

        let visible = live_state
            .reader(storage.clone())
            .load_row(&live_state_request())
            .await
            .expect("live-state load should succeed")
            .expect("tracked row should be visible");
        assert!(!visible.untracked);
        assert_eq!(visible.change_id.as_deref(), Some("change-tracked"));
        assert_eq!(visible.snapshot_content.as_deref(), Some("{\"value\":1}"));
    }

    #[tokio::test]
    async fn commit_staged_writes_applies_cross_subsystem_rows_as_one_backend_batch() {
        let counting_backend = Arc::new(CountingBackend::new());
        let write_batches = counting_backend.write_batches();
        let backend: Arc<dyn Backend + Send + Sync> = counting_backend;
        let storage = StorageContext::new(backend);
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let untracked_state = UntrackedStateContext::new();
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_global_version_head(storage.clone()).await;
        {
            let mut seed_transaction = storage
                .begin_write_transaction()
                .await
                .expect("seed transaction should open");
            let mut writes = StorageWriteSet::new();
            let mut json_writer = JsonStoreContext::new().writer();
            let mode_snapshot = serde_json::to_string(&serde_json::json!({
                "key": DETERMINISTIC_MODE_KEY,
                "value": { "enabled": true },
            }))
            .expect("mode snapshot should serialize");
            let mode_snapshot_ref = json_writer
                .prepare_json(NormalizedJson::from_arc_unchecked(Arc::from(
                    mode_snapshot.as_str(),
                )))
                .expect("deterministic mode snapshot should stage");
            {
                let row = crate::untracked_state::UntrackedStateRow {
                    entity_id: crate::entity_identity::EntityIdentity::single(
                        DETERMINISTIC_MODE_KEY,
                    ),
                    schema_key: "lix_key_value".to_string(),
                    file_id: None,
                    snapshot_ref: Some(mode_snapshot_ref),
                    metadata_ref: None,
                    created_at: "2026-01-01T00:00:00Z".to_string(),
                    updated_at: "2026-01-01T00:00:00Z".to_string(),
                    global: true,
                    version_id: GLOBAL_VERSION_ID.to_string(),
                };
                let mut writer = live_state.writer(seed_transaction.as_mut());
                writer
                    .stage_untracked_rows(&mut writes, std::iter::once(row.as_ref()))
                    .expect("deterministic mode should stage");
            }
            json_writer.flush_into(&mut writes);
            writes
                .apply(&mut seed_transaction.as_mut())
                .await
                .expect("deterministic mode should apply");
            seed_transaction
                .commit()
                .await
                .expect("seed transaction should persist");
        }
        write_batches.store(0, Ordering::SeqCst);
        let runtime_functions = {
            let reader = live_state.reader(storage.clone());
            FunctionContext::prepare(&reader)
                .await
                .expect("runtime context should prepare")
        };
        runtime_functions.provider().call_uuid_v7();
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");

        let mut json_writer = JsonStoreContext::new().writer();
        let tracked_row = tracked_global_row(&mut json_writer, "change-tracked");
        let mut untracked_row = untracked_global_row(&mut json_writer, "change-untracked");
        untracked_row.entity_id = crate::entity_identity::EntityIdentity::single("entity-2");

        commit_prepared_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            Some(&runtime_functions),
            transaction.as_mut(),
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows: vec![tracked_row, untracked_row],
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    GLOBAL_VERSION_ID.to_string(),
                    members(["change-tracked"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
                json_writer,
            },
        )
        .await
        .expect("cross-subsystem commit should stage and apply");

        assert_eq!(
            write_batches.load(Ordering::SeqCst),
            1,
            "tracked, json, untracked, changelog, and version refs must apply as one backend write batch"
        );

        transaction
            .commit()
            .await
            .expect("commit should persist kv");
        assert_eq!(write_batches.load(Ordering::SeqCst), 1);

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&crate::changelog::ChangelogScanRequest::default())
            .await
            .expect("changelog scan should succeed");
        assert!(changes.iter().any(|change| change.id == "change-tracked"));
        assert!(changes
            .iter()
            .any(|change| change.schema_key == "lix_commit"));

        let loaded_head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("version ref load should succeed");
        assert_eq!(loaded_head.as_deref(), Some("test-uuid-1"));

        let untracked = {
            let mut untracked_reader = untracked_state.reader(storage.clone());
            untracked_reader
                .load_row(&UntrackedStateRowRequest {
                    schema_key: "test_schema".to_string(),
                    version_id: GLOBAL_VERSION_ID.to_string(),
                    entity_id: crate::entity_identity::EntityIdentity::single("entity-2"),
                    file_id: NullableKeyFilter::Null,
                })
                .await
        }
        .expect("untracked row load should succeed")
        .expect("untracked row should persist");
        assert_eq!(
            untracked.snapshot_content.as_deref(),
            Some("{\"value\":\"untracked\"}")
        );

        let sequence_row = live_state
            .reader(storage.clone())
            .load_row(&LiveStateRowRequest {
                schema_key: "lix_key_value".to_string(),
                version_id: GLOBAL_VERSION_ID.to_string(),
                entity_id: crate::entity_identity::EntityIdentity::single(
                    DETERMINISTIC_SEQUENCE_KEY,
                ),
                file_id: NullableKeyFilter::Null,
            })
            .await
            .expect("deterministic sequence should load")
            .expect("deterministic sequence should persist");
        assert_eq!(
            sequence_row.snapshot_content.as_deref(),
            Some("{\"key\":\"lix_deterministic_sequence_number\",\"value\":0}")
        );
    }

    #[tokio::test]
    async fn non_global_tracked_write_creates_one_commit_and_advances_only_touched_version() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let binary_cas = BinaryCasContext::new();
        let changelog = ChangelogContext::new();
        let live_state = Arc::new(live_state_context());
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), GLOBAL_VERSION_ID, "global-before")
            .await;
        crate::test_support::seed_version_head(storage.clone(), "version-a", "version-a-before")
            .await;

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut json_writer = JsonStoreContext::new().writer();
        let state_rows = vec![tracked_version_row(
            &mut json_writer,
            "version-a",
            "change-version-a",
        )];
        commit_prepared_writes(
            &binary_cas,
            &changelog,
            live_state.as_ref(),
            &version_ctx,
            None,
            transaction.as_mut(),
            PreparedWriteSet {
                insert_identities: BTreeMap::new(),
                state_rows,
                adopted_rows: Vec::new(),
                commit_members_by_version: BTreeMap::from([(
                    "version-a".to_string(),
                    members(["change-version-a"]),
                )]),
                extra_commit_parents_by_version: BTreeMap::new(),
                file_data_writes: Vec::new(),
                json_writer,
            },
        )
        .await
        .expect("version commit should flush");
        transaction
            .commit()
            .await
            .expect("commit should persist kv");

        let changes = changelog
            .reader(storage.clone())
            .scan_changes(&crate::changelog::ChangelogScanRequest::default())
            .await
            .expect("changelog scan should succeed");
        let commit_changes = changes
            .iter()
            .filter(|change| change.schema_key == "lix_commit")
            .collect::<Vec<_>>();
        assert_eq!(
            commit_changes.len(),
            1,
            "a write to one non-global version must create exactly one commit"
        );
        assert_eq!(
            commit_changes[0]
                .entity_id
                .as_single_string_owned()
                .expect("commit entity id should project"),
            "test-uuid-1"
        );
        assert!(changes.iter().any(|change| change.id == "change-version-a"));
        assert!(!changes
            .iter()
            .any(|change| change.schema_key == "lix_version_ref"));

        let global_head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id(GLOBAL_VERSION_ID)
            .await
            .expect("global head should load");
        let version_head = version_ctx
            .ref_reader(storage.clone())
            .load_head_commit_id("version-a")
            .await
            .expect("version head should load");
        assert_eq!(global_head.as_deref(), Some("global-before"));
        assert_eq!(version_head.as_deref(), Some("test-uuid-1"));
    }

    #[tokio::test]
    async fn finalize_commit_rows_parents_global_commit_to_existing_version_ref() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(
            storage.clone(),
            GLOBAL_VERSION_ID,
            "initial-commit",
        )
        .await;

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut json_writer = JsonStoreContext::new().writer();
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                members(["change-a", "change-b"]),
            )]),
            BTreeMap::new(),
            &version_ctx,
            transaction.as_mut(),
            &mut json_writer,
        )
        .await
        .expect("global commit row should finalize");

        assert_eq!(rows.commit_rows.len(), 1);
        assert_eq!(rows.version_heads.len(), 1);
        let row = &rows.commit_rows[0];
        assert_eq!(
            row.entity_id.as_single_string_owned().as_deref(),
            Ok("test-uuid-1")
        );
        assert_eq!(row.change_id, "test-uuid-2");
        assert_eq!(row.created_at, "test-timestamp-1");

        let snapshot = serde_json::from_str::<JsonValue>(row.snapshot.normalized.as_ref())
            .expect("commit snapshot should be JSON");
        assert_eq!(
            snapshot.get("id").and_then(JsonValue::as_str),
            Some("test-uuid-1")
        );
        assert_eq!(
            snapshot
                .get("change_ids")
                .and_then(JsonValue::as_array)
                .expect("change_ids should be array")
                .iter()
                .map(|value| value.as_str().expect("change id should be string"))
                .collect::<Vec<_>>(),
            vec!["change-a", "change-b"]
        );
        assert_eq!(
            snapshot
                .get("parent_commit_ids")
                .and_then(JsonValue::as_array)
                .expect("parent_commit_ids should be array")
                .iter()
                .map(|value| value.as_str().expect("parent id should be string"))
                .collect::<Vec<_>>(),
            vec!["initial-commit"]
        );

        let version_head = &rows.version_heads[0];
        assert_eq!(version_head.version_id, GLOBAL_VERSION_ID);
        assert_eq!(version_head.commit_id, "test-uuid-1");
    }

    #[tokio::test]
    async fn finalize_commit_rows_skips_empty_members() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut json_writer = JsonStoreContext::new().writer();
        let rows = finalize_commit_rows(
            BTreeMap::from([(
                GLOBAL_VERSION_ID.to_string(),
                StagedCommitMembers::default(),
            )]),
            BTreeMap::new(),
            &version_ctx,
            transaction.as_mut(),
            &mut json_writer,
        )
        .await
        .expect("empty members should be ignored");

        assert!(rows.commit_rows.is_empty());
        assert!(rows.version_heads.is_empty());
    }

    #[tokio::test]
    async fn finalize_commit_rows_uses_existing_version_ref_as_parent() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), GLOBAL_VERSION_ID, "global-before")
            .await;
        crate::test_support::seed_version_head(storage.clone(), "version-a", "previous-commit")
            .await;

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut json_writer = JsonStoreContext::new().writer();
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            BTreeMap::new(),
            &version_ctx,
            transaction.as_mut(),
            &mut json_writer,
        )
        .await
        .expect("active-version commit finalization should resolve parent");

        let snapshot =
            serde_json::from_str::<JsonValue>(rows.commit_rows[0].snapshot.normalized.as_ref())
                .expect("commit snapshot should be JSON");
        assert_eq!(
            snapshot
                .get("parent_commit_ids")
                .and_then(JsonValue::as_array)
                .expect("parent_commit_ids should be array")
                .iter()
                .map(|value| value.as_str().expect("parent id should be text"))
                .collect::<Vec<_>>(),
            vec!["previous-commit"]
        );
        assert_eq!(rows.version_heads[0].version_id, "version-a");
    }

    #[tokio::test]
    async fn finalize_commit_rows_appends_extra_merge_parent_after_target_head() {
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        let storage = StorageContext::new(Arc::clone(&backend));
        let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
        crate::test_support::seed_version_head(storage.clone(), "version-a", "target-head").await;

        let mut transaction = storage
            .begin_write_transaction()
            .await
            .expect("transaction should open");
        let mut json_writer = JsonStoreContext::new().writer();
        let rows = finalize_commit_rows(
            BTreeMap::from([("version-a".to_string(), members(["change-a"]))]),
            BTreeMap::from([("version-a".to_string(), vec!["source-head".to_string()])]),
            &version_ctx,
            transaction.as_mut(),
            &mut json_writer,
        )
        .await
        .expect("merge commit finalization should resolve parents");

        let snapshot =
            serde_json::from_str::<JsonValue>(rows.commit_rows[0].snapshot.normalized.as_ref())
                .expect("commit snapshot should be JSON");
        assert_eq!(
            snapshot
                .get("parent_commit_ids")
                .and_then(JsonValue::as_array)
                .expect("parent_commit_ids should be array")
                .iter()
                .map(|value| value.as_str().expect("parent id should be text"))
                .collect::<Vec<_>>(),
            vec!["target-head", "source-head"]
        );
    }

    fn members<const N: usize>(change_ids: [&str; N]) -> StagedCommitMembers {
        let mut members = StagedCommitMembers::new(
            "test-uuid-1".to_string(),
            "test-uuid-2".to_string(),
            "test-uuid-3".to_string(),
            "test-timestamp-1".to_string(),
        );
        for change_id in change_ids {
            members.add_change_id(change_id.to_string());
        }
        members
    }

    fn tracked_global_row(json_writer: &mut JsonStoreWriter, change_id: &str) -> PreparedStateRow {
        tracked_version_row(json_writer, GLOBAL_VERSION_ID, change_id)
    }

    fn tracked_version_row(
        json_writer: &mut JsonStoreWriter,
        version_id: &str,
        change_id: &str,
    ) -> PreparedStateRow {
        PreparedStateRow {
            schema_plan_id: SchemaPlanId::for_test(0),
            facts: PreparedRowFacts::default(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            schema_key: "test_schema".to_string(),
            file_id: None,
            snapshot: Some(
                crate::transaction::types::stage_json_from_value(
                    json_writer,
                    crate::transaction::types::TransactionJson::from_value_for_test(
                        serde_json::json!({ "value": 1 }),
                    ),
                    "test tracked row snapshot",
                )
                .expect("test snapshot should stage"),
            ),
            metadata: None,
            origin: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-01-01T00:00:00Z".to_string(),
            global: version_id == GLOBAL_VERSION_ID,
            change_id: Some(change_id.to_string()),
            commit_id: Some("test-uuid-1".to_string()),
            untracked: false,
            version_id: version_id.to_string(),
        }
    }

    fn untracked_global_row(
        json_writer: &mut JsonStoreWriter,
        change_id: &str,
    ) -> PreparedStateRow {
        let mut row = tracked_global_row(json_writer, change_id);
        row.snapshot = Some(
            crate::transaction::types::stage_json_from_value(
                json_writer,
                crate::transaction::types::TransactionJson::from_value_for_test(
                    serde_json::json!({ "value": "untracked" }),
                ),
                "test untracked row snapshot",
            )
            .expect("test snapshot should stage"),
        );
        PreparedStateRow {
            change_id: None,
            commit_id: None,
            untracked: true,
            ..row
        }
    }

    fn untracked_request() -> UntrackedStateRowRequest {
        UntrackedStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }

    fn live_state_request() -> LiveStateRowRequest {
        LiveStateRowRequest {
            schema_key: "test_schema".to_string(),
            version_id: GLOBAL_VERSION_ID.to_string(),
            entity_id: crate::entity_identity::EntityIdentity::single("entity-1"),
            file_id: NullableKeyFilter::Null,
        }
    }

    struct CountingBackend {
        inner: UnitTestBackend,
        write_batches: Arc<AtomicUsize>,
    }

    impl CountingBackend {
        fn new() -> Self {
            Self {
                inner: UnitTestBackend::new(),
                write_batches: Arc::new(AtomicUsize::new(0)),
            }
        }

        fn write_batches(&self) -> Arc<AtomicUsize> {
            Arc::clone(&self.write_batches)
        }
    }

    #[async_trait]
    impl Backend for CountingBackend {
        async fn begin_read_transaction(
            &self,
        ) -> Result<Box<dyn BackendReadTransaction + Send + Sync + 'static>, LixError> {
            self.inner.begin_read_transaction().await
        }

        async fn begin_write_transaction(
            &self,
        ) -> Result<Box<dyn BackendWriteTransaction + Send + Sync + 'static>, LixError> {
            Ok(Box::new(CountingWriteTransaction {
                inner: self.inner.begin_write_transaction().await?,
                write_batches: Arc::clone(&self.write_batches),
            }))
        }
    }

    struct CountingWriteTransaction {
        inner: Box<dyn BackendWriteTransaction + Send + Sync + 'static>,
        write_batches: Arc<AtomicUsize>,
    }

    #[async_trait]
    impl BackendReadTransaction for CountingWriteTransaction {
        async fn get_values(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvValueBatch, LixError> {
            self.inner.get_values(request).await
        }

        async fn exists_many(
            &mut self,
            request: BackendKvGetRequest,
        ) -> Result<BackendKvExistsBatch, LixError> {
            self.inner.exists_many(request).await
        }

        async fn scan_keys(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvKeyPage, LixError> {
            self.inner.scan_keys(request).await
        }

        async fn scan_values(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvValuePage, LixError> {
            self.inner.scan_values(request).await
        }

        async fn scan_entries(
            &mut self,
            request: BackendKvScanRequest,
        ) -> Result<BackendKvEntryPage, LixError> {
            self.inner.scan_entries(request).await
        }

        async fn rollback(self: Box<Self>) -> Result<(), LixError> {
            let Self { inner, .. } = *self;
            inner.rollback().await
        }
    }

    #[async_trait]
    impl BackendWriteTransaction for CountingWriteTransaction {
        async fn write_kv_batch(
            &mut self,
            batch: BackendKvWriteBatch,
        ) -> Result<BackendKvWriteStats, LixError> {
            self.write_batches.fetch_add(1, Ordering::SeqCst);
            self.inner.write_kv_batch(batch).await
        }

        async fn commit(self: Box<Self>) -> Result<(), LixError> {
            let Self { inner, .. } = *self;
            inner.commit().await
        }
    }
}
