use std::sync::Arc;

use std::collections::BTreeMap;

use crate::changelog::{
    Change, ChangeLoadEntry, ChangeLoadRequest, ChangeLocator as ChangelogChangeLocator,
    ChangeProjection, ChangeRef as ChangelogChangeRef, ChangeVisibilityMode, ChangelogContext,
    CommitBody, CommitHeader, MembershipRecord, MembershipRole, Segment, SegmentChange,
    SegmentChangeDirectory, SegmentCommit, SegmentCommitDirectory, SegmentDirectory, SegmentHeader,
    SegmentInlinePayload, StateRowIdentity,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::storage::StorageContext;
use crate::storage::StorageRead;
use crate::storage::StorageWriteSet;
use crate::tracked_state::{
    MaterializedTrackedStateRow, TrackedStateContext, TrackedStateDeltaRef,
};
use crate::transaction::prepare_version_ref_row;
use crate::untracked_state::{
    MaterializedUntrackedStateRow, UntrackedStateContext, UntrackedStateRow,
};
use crate::version::VersionContext;

fn prepare_json_ref(value: &str) -> crate::json_store::JsonRef {
    crate::json_store::JsonRef::for_content(value.as_bytes())
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
    let mut read = storage
        .begin_read(crate::storage::StorageReadOptions::default())
        .expect("seed read should open");
    let version_ctx = VersionContext::new(Arc::new(UntrackedStateContext::new()));
    let mut writes = StorageWriteSet::new();
    let canonical_row = prepare_version_ref_row(version_id, commit_id, TEST_TIMESTAMP)
        .expect("version ref should canonicalize");
    version_ctx
        .stage_canonical_ref_rows(&mut writes, &[canonical_row.row])
        .expect("version ref should stage");
    stage_tracked_root_from_materialized(
        &mut read,
        &mut writes,
        &TrackedStateContext::new(),
        commit_id,
        None,
        rows,
    )
    .await
    .expect("tracked root should write");
    storage
        .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
        .expect("seed should commit");
}

pub(crate) async fn stage_tracked_root_from_materialized(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let parent_ids = parent_commit_id
        .map(|parent| vec![parent.to_string()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id}:commit");
    let staged = stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        &parent_ids,
        rows,
        &changes,
    )
    .await?;
    let deltas = staged
        .locators
        .iter()
        .map(|(row_index, locator)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                change: ChangelogChangeRef {
                    id: &change.id,
                    authored_commit_id: Some(&locator.commit_id),
                    entity_id: &change.entity_id,
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    snapshot_ref: change.snapshot_ref.as_ref(),
                    metadata_ref: change.metadata_ref.as_ref(),
                    created_at: &change.created_at,
                },
                locator: locator.as_ref(),
                created_at: &row.created_at,
                updated_at: &row.updated_at,
            }
        })
        .collect::<Vec<_>>();
    tracked_state
        .writer(read, writes)
        .stage_projection_root(commit_id, parent_commit_id, deltas)
        .await?;
    Ok(())
}

pub(crate) async fn stage_tracked_root_from_materialized_with_parents(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    tracked_state: &TrackedStateContext,
    commit_id: &str,
    parent_ids: &[String],
    projection_parent_commit_id: Option<&str>,
    rows: &[MaterializedTrackedStateRow],
) -> Result<(), crate::LixError> {
    let changes = rows
        .iter()
        .map(tracked_change_from_materialized)
        .collect::<Result<Vec<_>, _>>()?;
    let commit_change_id = format!("{commit_id}:commit");
    let staged = stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        parent_ids,
        rows,
        &changes,
    )
    .await?;
    let deltas = staged
        .locators
        .iter()
        .map(|(row_index, locator)| {
            let change = &changes[*row_index];
            let row = &rows[*row_index];
            TrackedStateDeltaRef {
                change: ChangelogChangeRef {
                    id: &change.id,
                    authored_commit_id: Some(&locator.commit_id),
                    entity_id: &change.entity_id,
                    schema_key: &change.schema_key,
                    file_id: change.file_id.as_deref(),
                    snapshot_ref: change.snapshot_ref.as_ref(),
                    metadata_ref: change.metadata_ref.as_ref(),
                    created_at: &change.created_at,
                },
                locator: locator.as_ref(),
                created_at: &row.created_at,
                updated_at: &row.updated_at,
            }
        })
        .collect::<Vec<_>>();
    tracked_state
        .writer(read, writes)
        .stage_projection_root(commit_id, projection_parent_commit_id, deltas)
        .await?;
    Ok(())
}

pub(crate) async fn stage_empty_changelog_commit(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_commit_id: Option<&str>,
) -> Result<(), crate::LixError> {
    let parent_ids = parent_commit_id
        .map(|parent| vec![parent.to_string()])
        .unwrap_or_default();
    let commit_change_id = format!("{commit_id}:commit");
    stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        &parent_ids,
        &[],
        &[],
    )
    .await?;
    Ok(())
}

pub(crate) async fn stage_empty_changelog_commit_with_parents(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    parent_ids: &[String],
) -> Result<(), crate::LixError> {
    let commit_change_id = format!("{commit_id}:commit");
    stage_test_changelog_commit(
        read,
        writes,
        commit_id,
        &commit_change_id,
        parent_ids,
        &[],
        &[],
    )
    .await?;
    Ok(())
}

async fn stage_test_changelog_commit(
    mut read: &mut (impl StorageRead + Send + Sync + ?Sized),
    writes: &mut StorageWriteSet,
    commit_id: &str,
    commit_change_id: &str,
    parent_ids: &[String],
    rows: &[MaterializedTrackedStateRow],
    changes: &[Change],
) -> Result<TestStagedChangelogCommit, crate::LixError> {
    let winner_indices = final_state_row_winner_indices(rows)?;
    let winner_change_ids = winner_indices
        .iter()
        .map(|&index| changes[index].id.clone())
        .collect::<Vec<_>>();
    let existing_locators = load_existing_changelog_locators(read, &winner_change_ids).await?;
    let mut segment_changes = Vec::new();
    let mut membership = Vec::new();
    let mut membership_ordinals = Vec::new();
    let mut state_row_identities = Vec::new();
    let mut adopted_locators = Vec::new();
    for &row_index in &winner_indices {
        let row = &rows[row_index];
        let change = &changes[row_index];
        let (role, source_parent_ordinal) = if let Some(locator) = existing_locators.get(&change.id)
        {
            adopted_locators.push((row_index, locator.clone()));
            (
                MembershipRole::Adopted,
                Some(source_parent_ordinal_for_existing_change(
                    parent_ids,
                    &locator.commit_id,
                    &change.id,
                    commit_id,
                )?),
            )
        } else {
            segment_changes.push(segment_change_from_materialized(row, change, commit_id)?);
            (MembershipRole::Authored, None)
        };
        let ordinal = membership.len() as u32;
        membership.push(MembershipRecord {
            member_change_id: change.id.clone(),
            role,
            source_parent_ordinal,
        });
        membership_ordinals.push((change.id.clone(), ordinal));
        state_row_identities.push((
            state_row_identity_from_materialized(row)?,
            change.id.clone(),
        ));
    }
    let created_at = rows
        .first()
        .map(|row| row.created_at.clone())
        .unwrap_or_else(|| TEST_TIMESTAMP.to_string());
    let segment = Segment {
        header: SegmentHeader {
            segment_id: format!("test-seed-{commit_id}"),
            format_version: 0,
            commit_count: 0,
            change_count: 0,
            byte_count: 0,
            payload_count: 0,
            checksum: String::new(),
        },
        directory: SegmentDirectory::default(),
        commits: vec![SegmentCommit {
            header: CommitHeader {
                id: commit_id.to_string(),
                parent_commit_ids: parent_ids.to_vec(),
                derivable_change_id: commit_change_id.to_string(),
                author_account_ids: Vec::new(),
                created_at,
                membership_count: 0,
            },
            body: CommitBody { membership },
            directory: SegmentCommitDirectory {
                state_row_identities,
                membership_ordinals,
            },
            checksum: String::new(),
        }],
        changes: segment_changes,
    };
    let mut writer = ChangelogContext::new().writer(&mut read, writes);
    let report = writer.stage_segment(segment).await?;
    writer.stage_publish_commit(commit_id).await?;
    let staged_locations = report
        .change_locations
        .into_iter()
        .collect::<BTreeMap<_, _>>();
    let mut locators = adopted_locators;
    for &row_index in &winner_indices {
        let change = &changes[row_index];
        if existing_locators.contains_key(&change.id) {
            continue;
        }
        locators.push((
            row_index,
            ChangelogChangeLocator {
                change_id: change.id.clone(),
                commit_id: commit_id.to_string(),
                location: staged_locations.get(&change.id).cloned().ok_or_else(|| {
                    crate::LixError::new(
                        crate::LixError::CODE_INTERNAL_ERROR,
                        format!(
                            "test changelog segment report is missing change '{}'",
                            change.id
                        ),
                    )
                })?,
            },
        ));
    }
    locators.sort_by_key(|(row_index, _)| *row_index);
    Ok(TestStagedChangelogCommit { locators })
}

struct TestStagedChangelogCommit {
    locators: Vec<(usize, ChangelogChangeLocator)>,
}

fn source_parent_ordinal_for_existing_change(
    parent_ids: &[String],
    source_commit_id: &str,
    change_id: &str,
    commit_id: &str,
) -> Result<u32, crate::LixError> {
    if let Some(index) = parent_ids
        .iter()
        .position(|parent_id| parent_id == source_commit_id)
    {
        return Ok(index as u32);
    }
    if !parent_ids.is_empty() {
        return Ok(0);
    }
    Err(crate::LixError::new(
        crate::LixError::CODE_INTERNAL_ERROR,
        format!(
            "test changelog commit '{commit_id}' cannot adopt existing change '{change_id}' without a source parent"
        ),
    ))
}

async fn load_existing_changelog_locators(
    read: &mut (impl StorageRead + Send + Sync + ?Sized),
    change_ids: &[String],
) -> Result<BTreeMap<String, ChangelogChangeLocator>, crate::LixError> {
    if change_ids.is_empty() {
        return Ok(BTreeMap::new());
    }
    let mut unique = change_ids.to_vec();
    unique.sort();
    unique.dedup();
    let mut reader = ChangelogContext::new().reader(&mut *read);
    let physical = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &unique,
            projection: ChangeProjection::PhysicalLocation,
            visibility: ChangeVisibilityMode::PhysicalOnly,
        })
        .await?;
    let segment = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &unique,
            projection: ChangeProjection::Segment,
            visibility: ChangeVisibilityMode::PhysicalOnly,
        })
        .await?;
    let mut out = BTreeMap::new();
    for ((change_id, physical_entry), segment_entry) in unique
        .into_iter()
        .zip(physical.entries)
        .zip(segment.entries)
    {
        let Some(ChangeLoadEntry::PhysicalLocation(location)) = physical_entry else {
            continue;
        };
        let Some(ChangeLoadEntry::Segment(change)) = segment_entry else {
            return Err(crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                format!("changelog segment projection missing existing change '{change_id}'"),
            ));
        };
        let Some(commit_id) = change.authored_commit_id else {
            return Err(crate::LixError::new(
                crate::LixError::CODE_INTERNAL_ERROR,
                format!("existing changelog change '{change_id}' has no authored commit"),
            ));
        };
        out.insert(
            change_id.clone(),
            ChangelogChangeLocator {
                change_id,
                commit_id,
                location,
            },
        );
    }
    Ok(out)
}

fn final_state_row_winner_indices(
    rows: &[MaterializedTrackedStateRow],
) -> Result<Vec<usize>, crate::LixError> {
    let mut winners = BTreeMap::<StateRowIdentity, usize>::new();
    for (index, row) in rows.iter().enumerate() {
        winners.insert(state_row_identity_from_materialized(row)?, index);
    }
    let mut indices = winners.into_values().collect::<Vec<_>>();
    indices.sort_unstable();
    Ok(indices)
}

fn segment_change_from_materialized(
    row: &MaterializedTrackedStateRow,
    change: &Change,
    commit_id: &str,
) -> Result<SegmentChange, crate::LixError> {
    let mut inline_payloads = Vec::new();
    if let Some(snapshot) = row.snapshot_content.as_deref() {
        inline_payloads.push(SegmentInlinePayload {
            json_ref: prepare_json_ref(snapshot),
            bytes: snapshot.as_bytes().to_vec(),
        });
    }
    if let Some(metadata) = row.metadata.as_ref() {
        let serialized = crate::serialize_row_metadata(metadata);
        inline_payloads.push(SegmentInlinePayload {
            json_ref: prepare_json_ref(&serialized),
            bytes: serialized.into_bytes(),
        });
    }
    Ok(SegmentChange {
        id: change.id.clone(),
        authored_commit_id: Some(commit_id.to_string()),
        entity_id: change.entity_id.clone(),
        schema_key: change.schema_key.clone(),
        file_id: change.file_id.clone(),
        snapshot_ref: change.snapshot_ref,
        metadata_ref: change.metadata_ref,
        created_at: change.created_at.clone(),
        inline_payloads,
        directory: SegmentChangeDirectory::default(),
    })
}

fn state_row_identity_from_materialized(
    row: &MaterializedTrackedStateRow,
) -> Result<StateRowIdentity, crate::LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(row.schema_key.clone())?,
        file_id: FileId::new(
            row.file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: EntityId::new(row.entity_id.as_json_array_text()?)?,
    })
}

pub(crate) fn tracked_change_from_materialized(
    row: &MaterializedTrackedStateRow,
) -> Result<Change, crate::LixError> {
    Ok(Change {
        id: row.change_id.clone(),
        authored_commit_id: Some(row.commit_id.clone()),
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_ref: row.snapshot_content.as_deref().map(prepare_json_ref),
        metadata_ref: row.metadata.as_ref().map(|value| {
            let serialized = crate::serialize_row_metadata(value);
            prepare_json_ref(&serialized)
        }),
        created_at: row.updated_at.clone(),
    })
}

pub(crate) fn untracked_state_row_from_materialized(
    _writes: &mut StorageWriteSet,
    row: &MaterializedUntrackedStateRow,
) -> Result<UntrackedStateRow, crate::LixError> {
    Ok(UntrackedStateRow {
        entity_id: row.entity_id.clone(),
        schema_key: row.schema_key.clone(),
        file_id: row.file_id.clone(),
        snapshot_content: row.snapshot_content.clone(),
        metadata: row.metadata.as_ref().map(crate::serialize_row_metadata),
        created_at: row.created_at.clone(),
        updated_at: row.updated_at.clone(),
        global: row.global,
        version_id: row.version_id.clone(),
    })
}
