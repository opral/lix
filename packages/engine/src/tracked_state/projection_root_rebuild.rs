use crate::changelog::{
    Change, ChangeLoadEntry, ChangeLoadRequest, ChangeLocator as ChangelogChangeLocator,
    ChangeProjection, ChangeRef as ChangelogChangeRef, ChangeVisibilityMode, ChangelogContext,
    CommitBody, CommitHeader, CommitLoadEntry, CommitLoadRequest, CommitProjection,
    CommitVisibilityMode, StateRowIdentity,
};
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::storage::StorageRead;
use crate::tracked_state::context::{TrackedStateRootRebuilder, TrackedStateWriteReport};
use crate::tracked_state::storage;
use crate::tracked_state::tree::TrackedStateTree;
use crate::tracked_state::types::{TrackedStateIndexValue, TrackedStateKey, TrackedStateRootId};
use crate::tracked_state::TrackedStateDeltaRef;
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};

/// Owned delta used only by explicit projection-root rebuild.
///
/// Normal transaction commits already have borrowed `ChangeRef` and
/// changelog change references and locations available while staging.
/// Rebuild loads those facts back from storage, so it owns the decoded data
/// internally and immediately passes a borrowed view into the same tracked-state
/// root writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionRootRebuildDelta {
    pub(crate) change: Change,
    pub(crate) locator: ChangelogChangeLocator,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

impl ProjectionRootRebuildDelta {
    pub(crate) fn as_ref(&self) -> TrackedStateDeltaRef<'_> {
        let change = ChangelogChangeRef {
            id: &self.change.id,
            authored_commit_id: Some(&self.locator.commit_id),
            entity_id: &self.change.entity_id,
            schema_key: &self.change.schema_key,
            file_id: self.change.file_id.as_deref(),
            snapshot_ref: self.change.snapshot_ref.as_ref(),
            metadata_ref: self.change.metadata_ref.as_ref(),
            created_at: &self.change.created_at,
        };
        TrackedStateDeltaRef {
            change,
            locator: self.locator.as_ref(),
            created_at: &self.created_at,
            updated_at: &self.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionRootRebuildInput {
    pub(crate) commit_id: String,
    pub(crate) parent_commit_id: Option<String>,
    pub(crate) deltas: Vec<ProjectionRootRebuildDelta>,
    pub(crate) replayed_commits: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ProjectionRootEnsureReport {
    pub(crate) commit_id: String,
    pub(crate) root_id: TrackedStateRootId,
    pub(crate) repaired: bool,
    pub(crate) parent_commit_id: Option<String>,
    pub(crate) replayed_commits: usize,
    pub(crate) replayed_changes: usize,
}

struct LocatedChange {
    locator: ChangelogChangeLocator,
    change: Change,
}

/// Explicit projection-root rebuild over changelog.
///
/// Normal transaction commits stage projection roots with already prepared
/// changelog refs. This path exists for deliberate root repair only.
pub(crate) async fn rebuild_projection_root_at<S>(
    rebuilder: &mut TrackedStateRootRebuilder<'_, S>,
    commit_id: &str,
) -> Result<TrackedStateWriteReport, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    // Explicit rebuilds keep the legacy incremental behavior: they may reuse
    // the nearest first-parent projection root without validating its rows.
    // Missing-root repair goes through ensure_projection_root, which enables
    // parent validation before inheriting created_at values.
    let input = build_incremental_projection_root_rebuild_input_with_parent_validation(
        rebuilder.store,
        commit_id,
        false,
    )
    .await?;
    let delta_refs = input
        .deltas
        .iter()
        .map(ProjectionRootRebuildDelta::as_ref)
        .collect::<Vec<_>>();
    rebuilder
        .tracked_state
        .writer(rebuilder.store, rebuilder.writes)
        .stage_projection_root(
            &input.commit_id,
            input.parent_commit_id.as_deref(),
            delta_refs,
        )
        .await
}

pub(crate) async fn ensure_projection_root<S>(
    rebuilder: &mut TrackedStateRootRebuilder<'_, S>,
    commit_id: &str,
) -> Result<ProjectionRootEnsureReport, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    if let Some(root_id) = storage::load_root(rebuilder.store, commit_id).await? {
        return Ok(ProjectionRootEnsureReport {
            commit_id: commit_id.to_string(),
            root_id,
            repaired: false,
            parent_commit_id: None,
            replayed_commits: 0,
            replayed_changes: 0,
        });
    }

    let input = build_incremental_projection_root_rebuild_input(rebuilder.store, commit_id).await?;
    let parent_commit_id = input.parent_commit_id.clone();
    let replayed_commits = input.replayed_commits;
    let replayed_changes = input.deltas.len();
    let report = stage_projection_root_rebuild_input(rebuilder, input).await?;

    Ok(ProjectionRootEnsureReport {
        commit_id: commit_id.to_string(),
        root_id: report.root_id,
        repaired: true,
        parent_commit_id,
        replayed_commits,
        replayed_changes,
    })
}

pub(super) async fn build_projection_root_rebuild_input<S>(
    store: &S,
    commit_id: &str,
) -> Result<ProjectionRootRebuildInput, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let lineage = load_first_parent_lineage(store, commit_id).await?;
    let mut located_changes = Vec::new();
    for commit in &lineage {
        located_changes.append(&mut load_commit_located_changes(store, commit).await?);
    }
    let deltas = project_projection_root_rebuild_deltas(located_changes);

    Ok(ProjectionRootRebuildInput {
        commit_id: commit_id.to_string(),
        parent_commit_id: None,
        deltas,
        replayed_commits: lineage.len(),
    })
}

pub(super) async fn build_incremental_projection_root_rebuild_input<S>(
    store: &S,
    commit_id: &str,
) -> Result<ProjectionRootRebuildInput, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    build_incremental_projection_root_rebuild_input_with_parent_validation(store, commit_id, true)
        .await
}

async fn build_incremental_projection_root_rebuild_input_with_parent_validation<S>(
    store: &S,
    commit_id: &str,
    validate_parent_values: bool,
) -> Result<ProjectionRootRebuildInput, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let mut reverse_replay = Vec::new();
    let mut seen = BTreeSet::new();
    let mut current = Some(commit_id.to_string());
    let mut parent = None;
    while let Some(current_id) = current {
        if !seen.insert(current_id.clone()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state materialization found first-parent cycle at commit '{current_id}'"
                ),
            ));
        }
        let commit = load_visible_commit(store, &current_id).await?;
        let first_parent = commit.header.parent_commit_ids.first().cloned();
        reverse_replay.push(commit);
        let Some(parent_id) = first_parent else {
            break;
        };
        if let Some(root_id) = storage::load_root(store, &parent_id).await? {
            parent = Some((parent_id, root_id));
            break;
        }
        current = Some(parent_id);
    }

    reverse_replay.reverse();
    let mut located_changes = Vec::new();
    for commit in &reverse_replay {
        located_changes.append(&mut load_commit_located_changes(store, commit).await?);
    }
    let (parent_commit_id, parent_root_id) = match parent {
        Some((commit_id, root_id)) => (Some(commit_id), Some(root_id)),
        None => (None, None),
    };
    let parent = parent_commit_id.as_deref().zip(parent_root_id.as_ref());
    let deltas = project_projection_root_rebuild_deltas_with_parent(
        store,
        parent,
        validate_parent_values,
        located_changes,
    )
    .await?;

    Ok(ProjectionRootRebuildInput {
        commit_id: commit_id.to_string(),
        parent_commit_id,
        deltas,
        replayed_commits: reverse_replay.len(),
    })
}

async fn stage_projection_root_rebuild_input<S>(
    rebuilder: &mut TrackedStateRootRebuilder<'_, S>,
    input: ProjectionRootRebuildInput,
) -> Result<TrackedStateWriteReport, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let delta_refs = input
        .deltas
        .iter()
        .map(ProjectionRootRebuildDelta::as_ref)
        .collect::<Vec<_>>();
    rebuilder
        .tracked_state
        .writer(rebuilder.store, rebuilder.writes)
        .stage_projection_root(
            &input.commit_id,
            input.parent_commit_id.as_deref(),
            delta_refs,
        )
        .await
}

async fn load_first_parent_lineage<S>(
    store: &S,
    commit_id: &str,
) -> Result<Vec<LoadedCommit>, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let mut lineage = Vec::new();
    let mut seen = BTreeSet::new();
    let mut current = Some(commit_id.to_string());
    while let Some(current_id) = current {
        if !seen.insert(current_id.clone()) {
            return Err(LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state materialization found first-parent cycle at commit '{current_id}'"
                ),
            ));
        }
        let commit = load_visible_commit(store, &current_id).await?;
        current = commit.header.parent_commit_ids.first().cloned();
        lineage.push(commit);
    }
    lineage.reverse();
    Ok(lineage)
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct LoadedCommit {
    header: CommitHeader,
    body: CommitBody,
}

async fn load_visible_commit<S>(store: &S, commit_id: &str) -> Result<LoadedCommit, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let mut reader = ChangelogContext::new().reader(store);
    let batch = reader
        .load_commits(CommitLoadRequest {
            commit_ids: &[commit_id.to_string()],
            projection: CommitProjection::Full,
            visibility: CommitVisibilityMode::RequireVisible,
        })
        .await?;
    let Some(entry) = batch.entries.into_iter().next().flatten() else {
        return Err(missing_commit_error(commit_id));
    };
    match entry {
        CommitLoadEntry::Full { header, body } => Ok(LoadedCommit { header, body }),
        _ => Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog full commit projection returned non-full entry",
        )),
    }
}

async fn load_commit_located_changes<S>(
    store: &S,
    commit: &LoadedCommit,
) -> Result<Vec<LocatedChange>, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let change_ids = commit
        .body
        .membership
        .iter()
        .map(|membership| membership.member_change_id.clone())
        .collect::<Vec<_>>();
    if change_ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut reader = ChangelogContext::new().reader(store);
    let logical = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &change_ids,
            projection: ChangeProjection::Logical,
            visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
        })
        .await?;
    let physical = reader
        .load_changes(ChangeLoadRequest {
            change_ids: &change_ids,
            projection: ChangeProjection::PhysicalLocation,
            visibility: ChangeVisibilityMode::RequireReachableFromVisibleCommit,
        })
        .await?;
    change_ids
        .into_iter()
        .zip(logical.entries)
        .zip(physical.entries)
        .map(|((change_id, logical), physical)| {
            let Some(ChangeLoadEntry::Logical(change)) = logical else {
                return Err(missing_change_error(&commit.header.id, &change_id));
            };
            let Some(ChangeLoadEntry::PhysicalLocation(location)) = physical else {
                return Err(missing_change_error(&commit.header.id, &change_id));
            };
            let authored_commit_id = change.authored_commit_id.clone().ok_or_else(|| {
                LixError::new(
                    LixError::CODE_INTERNAL_ERROR,
                    format!("changelog change '{change_id}' has no authored commit"),
                )
            })?;
            Ok(LocatedChange {
                locator: ChangelogChangeLocator {
                    change_id,
                    commit_id: authored_commit_id,
                    location,
                },
                change,
            })
        })
        .collect()
}

fn project_projection_root_rebuild_deltas(
    changes: impl IntoIterator<Item = LocatedChange>,
) -> Vec<ProjectionRootRebuildDelta> {
    let mut projected = BTreeMap::<TrackedStateKey, ProjectionRootRebuildDelta>::new();
    for LocatedChange { locator, change } in changes {
        let key = TrackedStateKey {
            schema_key: change.schema_key.clone(),
            file_id: change.file_id.clone(),
            entity_id: change.entity_id.clone(),
        };
        let created_at = projected
            .get(&key)
            .map(|delta| delta.created_at.clone())
            .unwrap_or_else(|| change.created_at.clone());
        let updated_at = change.created_at.clone();
        projected.insert(
            key,
            ProjectionRootRebuildDelta {
                change,
                locator,
                created_at,
                updated_at,
            },
        );
    }
    projected.into_values().collect()
}

async fn project_projection_root_rebuild_deltas_with_parent<S>(
    store: &S,
    parent: Option<(&str, &TrackedStateRootId)>,
    validate_parent_values: bool,
    changes: impl IntoIterator<Item = LocatedChange>,
) -> Result<Vec<ProjectionRootRebuildDelta>, LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let mut deltas = project_projection_root_rebuild_deltas(changes);
    let Some((parent_commit_id, parent_root_id)) = parent else {
        return Ok(deltas);
    };
    let keys = deltas
        .iter()
        .map(|delta| TrackedStateKey {
            schema_key: delta.change.schema_key.clone(),
            file_id: delta.change.file_id.clone(),
            entity_id: delta.change.entity_id.clone(),
        })
        .collect::<Vec<_>>();
    let parent_values = TrackedStateTree::new()
        .get_many(store, parent_root_id, &keys)
        .await?;
    if validate_parent_values {
        validate_parent_values_against_changelog(store, parent_commit_id, &keys, &parent_values)
            .await?;
    }
    for (delta, parent_value) in deltas.iter_mut().zip(parent_values) {
        if let Some(TrackedStateIndexValue { created_at, .. }) = parent_value {
            delta.created_at = created_at;
        }
    }
    Ok(deltas)
}

async fn validate_parent_values_against_changelog<S>(
    store: &S,
    parent_commit_id: &str,
    keys: &[TrackedStateKey],
    values: &[Option<TrackedStateIndexValue>],
) -> Result<(), LixError>
where
    S: StorageRead + Send + Sync + ?Sized,
{
    let identities = keys
        .iter()
        .map(state_row_identity_from_key)
        .collect::<Result<Vec<_>, _>>()?;
    let mut reader = ChangelogContext::new().reader(store);
    let facts = reader
        .load_first_parent_winner_facts_for_visible_commit(parent_commit_id, &identities)
        .await?;
    for (identity, value) in identities.into_iter().zip(values) {
        let fact = facts.get(&identity);
        let Some(value) = value else {
            if fact.is_some() {
                return Err(LixError::unknown(format!(
                    "tracked_state materialization parent root for commit '{}' is missing changelog first-parent winner for identity {:?}",
                    parent_commit_id, identity
                )));
            }
            continue;
        };
        let Some(fact) = fact else {
            return Err(LixError::unknown(format!(
                "tracked_state materialization parent root for commit '{}' contains non-winner identity {:?}",
                parent_commit_id, identity
            )));
        };
        if fact.change_id != value.change_locator.change_id
            || fact.created_at != value.created_at
            || fact.updated_at != value.updated_at
            || fact.deleted != value.deleted
        {
            return Err(LixError::unknown(format!(
                "tracked_state materialization parent root for commit '{}' does not match changelog first-parent winner for identity {:?}",
                parent_commit_id, identity
            )));
        }
    }
    Ok(())
}

fn state_row_identity_from_key(key: &TrackedStateKey) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(key.schema_key.clone())?,
        file_id: FileId::new(
            key.file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: EntityId::new(key.entity_id.as_json_array_text()?)?,
    })
}

fn missing_change_error(commit_id: &str, change_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state materialization missing changelog change '{change_id}' for commit '{commit_id}'"),
    )
}

fn missing_commit_error(commit_id: &str) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state materialization missing commit '{commit_id}'"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::SegmentObjectLocation;
    use crate::entity_identity::EntityIdentity;
    use crate::tracked_state::tree::TrackedStateTree;
    use crate::tracked_state::types::TrackedStateIndexValue;

    #[test]
    fn projection_root_rebuild_delta_ref_borrows_owned_facts() {
        let delta = ProjectionRootRebuildDelta {
            change: Change {
                id: "change-1".to_string(),
                authored_commit_id: Some("commit-1".to_string()),
                entity_id: EntityIdentity::single("entity-1"),
                schema_key: "schema".to_string(),
                file_id: Some("file".to_string()),
                snapshot_ref: None,
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
            },
            locator: ChangelogChangeLocator {
                change_id: "change-1".to_string(),
                commit_id: "commit-1".to_string(),
                location: SegmentObjectLocation {
                    segment_id: "segment-1".to_string(),
                    offset: 7,
                    len: 11,
                    checksum: "checksum-1".to_string(),
                },
            },
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-02-01T00:00:00Z".to_string(),
        };

        let delta_ref = delta.as_ref();

        assert_eq!(delta_ref.change.id, "change-1");
        assert_eq!(delta_ref.change.schema_key, "schema");
        assert_eq!(delta_ref.change.file_id, Some("file"));
        assert_eq!(delta_ref.locator.commit_id, "commit-1");
        assert_eq!(delta_ref.locator.location.segment_id, "segment-1");
        assert_eq!(delta_ref.locator.location.offset, 7);
        assert_eq!(delta_ref.locator.location.len, 11);
        assert_eq!(delta_ref.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(delta_ref.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn project_projection_root_rebuild_deltas_keeps_first_seen_created_at_and_latest_updated_at() {
        let deltas = project_projection_root_rebuild_deltas(vec![
            located_change(
                "commit-1",
                0,
                "change-create",
                "entity-1",
                "2026-01-01T00:00:00Z",
            ),
            located_change(
                "commit-2",
                0,
                "change-update",
                "entity-1",
                "2026-02-01T00:00:00Z",
            ),
        ]);

        assert_eq!(deltas.len(), 1);
        let delta = &deltas[0];
        assert_eq!(delta.change.id, "change-update");
        assert_eq!(delta.locator.commit_id, "commit-2");
        assert_eq!(delta.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(delta.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn project_projection_root_rebuild_deltas_uses_adopted_change_time_not_target_commit_time() {
        let deltas = project_projection_root_rebuild_deltas(vec![located_change(
            "source-commit",
            0,
            "adopted-change",
            "entity-1",
            "2026-01-01T00:00:00Z",
        )]);

        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].created_at, "2026-01-01T00:00:00Z");
        assert_eq!(deltas[0].updated_at, "2026-01-01T00:00:00Z");
    }

    #[test]
    fn project_projection_root_rebuild_deltas_tracks_entities_independently() {
        let deltas = project_projection_root_rebuild_deltas(vec![
            located_change(
                "commit-1",
                0,
                "entity-a-create",
                "entity-a",
                "2026-01-01T00:00:00Z",
            ),
            located_change(
                "commit-1",
                1,
                "entity-b-create",
                "entity-b",
                "2026-01-02T00:00:00Z",
            ),
            located_change(
                "commit-2",
                0,
                "entity-a-update",
                "entity-a",
                "2026-02-01T00:00:00Z",
            ),
        ]);

        let entity_a = deltas
            .iter()
            .find(|delta| delta.change.entity_id == EntityIdentity::single("entity-a"))
            .expect("entity-a delta");
        let entity_b = deltas
            .iter()
            .find(|delta| delta.change.entity_id == EntityIdentity::single("entity-b"))
            .expect("entity-b delta");
        assert_eq!(entity_a.change.id, "entity-a-update");
        assert_eq!(entity_a.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(entity_a.updated_at, "2026-02-01T00:00:00Z");
        assert_eq!(entity_b.change.id, "entity-b-create");
        assert_eq!(entity_b.created_at, "2026-01-02T00:00:00Z");
        assert_eq!(entity_b.updated_at, "2026-01-02T00:00:00Z");
    }

    #[tokio::test]
    async fn incremental_projection_rebuild_preserves_parent_created_at() {
        let storage =
            crate::storage::StorageContext::new(crate::storage::InMemoryStorageBackend::new());
        let tracked_state = crate::tracked_state::TrackedStateContext::new();
        let parent_row = materialized_row(
            "entity-1",
            "change-parent",
            "parent",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_materialized_root(&storage, &tracked_state, "parent", None, &[parent_row])
            .await
            .expect("parent root should stage");
        let child_row = materialized_row(
            "entity-1",
            "change-child",
            "child",
            "child",
            "2026-01-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        );
        stage_materialized_root(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[child_row],
        )
        .await
        .expect("child root should stage");

        let input = build_incremental_projection_root_rebuild_input(
            &storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .expect("read should open"),
            "child",
        )
        .await
        .expect("rebuild input should load");

        assert_eq!(input.parent_commit_id.as_deref(), Some("parent"));
        assert_eq!(input.replayed_commits, 1);
        assert_eq!(input.deltas.len(), 1);
        assert_eq!(input.deltas[0].created_at, "2026-01-01T00:00:00Z");
        assert_eq!(input.deltas[0].updated_at, "2026-02-01T00:00:00Z");
    }

    #[tokio::test]
    async fn incremental_projection_rebuild_rejects_corrupt_parent_root_created_at() {
        let storage =
            crate::storage::StorageContext::new(crate::storage::InMemoryStorageBackend::new());
        let tracked_state = crate::tracked_state::TrackedStateContext::new();
        let parent_row = materialized_row(
            "entity-1",
            "change-parent",
            "parent",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_materialized_root(&storage, &tracked_state, "parent", None, &[parent_row])
            .await
            .expect("parent root should stage");
        let child_row = materialized_row(
            "entity-1",
            "change-child",
            "child",
            "child",
            "2026-01-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        );
        stage_materialized_root(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[child_row],
        )
        .await
        .expect("child root should stage");

        let parent_key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_id: EntityIdentity::single("entity-1"),
        };
        let parent_root = storage::load_root(
            &storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .expect("read should open"),
            "parent",
        )
        .await
        .expect("parent root should load")
        .expect("parent root should exist");
        let mut corrupt_parent_value = TrackedStateTree::new()
            .get(
                &storage
                    .begin_read(crate::storage::StorageReadOptions::default())
                    .expect("read should open"),
                &parent_root,
                &parent_key,
            )
            .await
            .expect("parent value should load")
            .expect("parent value should exist");
        corrupt_parent_value.created_at = "1999-01-01T00:00:00Z".to_string();
        let forged_root = {
            let read = storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            let result = TrackedStateTree::new()
                .apply_mutations(
                    &read,
                    &mut writes,
                    None,
                    vec![
                        crate::tracked_state::types::TrackedStateMutation::put_encoded(
                            crate::tracked_state::codec::encode_key(&parent_key),
                            crate::tracked_state::codec::encode_value(&corrupt_parent_value),
                        ),
                    ],
                    Some("forged-parent"),
                )
                .await
                .expect("forged root should write");
            storage::stage_projection_metadata(
                &mut writes,
                &crate::tracked_state::types::TrackedStateProjectionMetadata {
                    commit_id: "parent".to_string(),
                    root_id: result.root_id.clone(),
                    parent_roots: Vec::new(),
                    changed_key_count: 1,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                    primary_chunk_count: result.chunk_count as u64,
                    primary_chunk_bytes: result.chunk_bytes as u64,
                },
            )
            .expect("corrupt parent metadata should encode");
            writes.delete(
                storage::TRACKED_STATE_PROJECTION_SPACE,
                crate::storage::StorageKey(bytes::Bytes::copy_from_slice(b"child")),
            );
            storage
                .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
                .expect("corruption should commit");
            result.root_id
        };

        assert_eq!(
            storage::load_root(
                &storage
                    .begin_read(crate::storage::StorageReadOptions::default())
                    .expect("read should open"),
                "parent",
            )
            .await
            .expect("corrupt parent root should load"),
            Some(forged_root)
        );

        assert_child_repair_rejects(
            &storage,
            &tracked_state,
            "does not match changelog first-parent winner",
        )
        .await;
    }

    #[tokio::test]
    async fn explicit_projection_rebuild_allows_corrupt_parent_created_at_for_legacy_rebuild_mode() {
        let storage =
            crate::storage::StorageContext::new(crate::storage::InMemoryStorageBackend::new());
        let tracked_state = crate::tracked_state::TrackedStateContext::new();
        let parent_row = materialized_row(
            "entity-1",
            "change-parent",
            "parent",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_materialized_root(&storage, &tracked_state, "parent", None, &[parent_row])
            .await
            .expect("parent root should stage");
        let child_row = materialized_row(
            "entity-1",
            "change-child",
            "child",
            "child",
            "2026-01-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        );
        stage_materialized_root(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[child_row],
        )
        .await
        .expect("child root should stage");
        let parent_key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_id: EntityIdentity::single("entity-1"),
        };
        let parent_root = storage::load_root(
            &storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .expect("read should open"),
            "parent",
        )
        .await
        .expect("parent root should load")
        .expect("parent root should exist");
        let mut corrupt_parent_value = TrackedStateTree::new()
            .get(
                &storage
                    .begin_read(crate::storage::StorageReadOptions::default())
                    .expect("read should open"),
                &parent_root,
                &parent_key,
            )
            .await
            .expect("parent value should load")
            .expect("parent value should exist");
        corrupt_parent_value.created_at = "1999-01-01T00:00:00Z".to_string();
        stage_parent_projection_root_and_delete_child(
            &storage,
            "parent",
            "child",
            vec![(parent_key, corrupt_parent_value)],
        )
        .await
        .expect("corruption should stage");

        let mut read = storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let report = tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .rebuild_projection_root_at("child")
            .await
            .expect("explicit rebuild should preserve legacy unvalidated parent behavior");
        assert_eq!(report.commit_id, "child");
        storage
            .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
            .expect("explicit rebuild should commit");
        let input = build_incremental_projection_root_rebuild_input_with_parent_validation(
            &storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .expect("read should open"),
            "child",
            false,
        )
        .await
        .expect("legacy rebuild input should load");
        assert_eq!(input.parent_commit_id.as_deref(), Some("parent"));
        assert_eq!(input.deltas[0].created_at, "1999-01-01T00:00:00Z");
    }

    #[tokio::test]
    async fn incremental_projection_rebuild_rejects_parent_root_missing_winner_row() {
        let storage =
            crate::storage::StorageContext::new(crate::storage::InMemoryStorageBackend::new());
        let tracked_state = crate::tracked_state::TrackedStateContext::new();
        let parent_row = materialized_row(
            "entity-1",
            "change-parent",
            "parent",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_materialized_root(&storage, &tracked_state, "parent", None, &[parent_row])
            .await
            .expect("parent root should stage");
        let child_row = materialized_row(
            "entity-1",
            "change-child",
            "child",
            "child",
            "2026-01-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        );
        stage_materialized_root(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[child_row],
        )
        .await
        .expect("child root should stage");

        {
            let read = storage
                .begin_read(crate::storage::StorageReadOptions::default())
                .expect("read should open");
            let mut writes = storage.new_write_set();
            let result = TrackedStateTree::new()
                .apply_mutations(&read, &mut writes, None, Vec::new(), Some("empty-parent"))
                .await
                .expect("empty root should write");
            storage::stage_projection_metadata(
                &mut writes,
                &crate::tracked_state::types::TrackedStateProjectionMetadata {
                    commit_id: "parent".to_string(),
                    root_id: result.root_id.clone(),
                    parent_roots: Vec::new(),
                    changed_key_count: 0,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                    primary_chunk_count: result.chunk_count as u64,
                    primary_chunk_bytes: result.chunk_bytes as u64,
                },
            )
            .expect("corrupt parent metadata should encode");
            writes.delete(
                storage::TRACKED_STATE_PROJECTION_SPACE,
                crate::storage::StorageKey(bytes::Bytes::copy_from_slice(b"child")),
            );
            storage
                .commit_write_set(writes, crate::storage::StorageWriteOptions::default())
                .expect("corruption should commit");
        }

        assert_child_repair_rejects(
            &storage,
            &tracked_state,
            "is missing changelog first-parent winner",
        )
        .await;
    }

    #[tokio::test]
    async fn incremental_projection_rebuild_rejects_parent_root_non_winner_row() {
        let storage =
            crate::storage::StorageContext::new(crate::storage::InMemoryStorageBackend::new());
        let tracked_state = crate::tracked_state::TrackedStateContext::new();
        stage_materialized_root(&storage, &tracked_state, "parent", None, &[])
            .await
            .expect("parent root should stage");
        let child_row = materialized_row(
            "entity-1",
            "change-child",
            "child",
            "child",
            "2026-02-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        );
        stage_materialized_root(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[child_row],
        )
        .await
        .expect("child root should stage");

        let forged_key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_id: EntityIdentity::single("entity-1"),
        };
        let forged_value = tracked_value(
            "change-forged",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_parent_projection_root_and_delete_child(
            &storage,
            "parent",
            "child",
            vec![(forged_key, forged_value)],
        )
        .await
        .expect("corruption should stage");

        assert_child_repair_rejects(&storage, &tracked_state, "contains non-winner identity").await;
    }

    #[tokio::test]
    async fn incremental_projection_rebuild_rejects_parent_root_wrong_winner_change_id() {
        let storage =
            crate::storage::StorageContext::new(crate::storage::InMemoryStorageBackend::new());
        let tracked_state = crate::tracked_state::TrackedStateContext::new();
        let parent_row = materialized_row(
            "entity-1",
            "change-parent",
            "parent",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_materialized_root(&storage, &tracked_state, "parent", None, &[parent_row])
            .await
            .expect("parent root should stage");
        let child_row = materialized_row(
            "entity-1",
            "change-child",
            "child",
            "child",
            "2026-01-01T00:00:00Z",
            "2026-02-01T00:00:00Z",
        );
        stage_materialized_root(
            &storage,
            &tracked_state,
            "child",
            Some("parent"),
            &[child_row],
        )
        .await
        .expect("child root should stage");

        let forged_key = TrackedStateKey {
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            entity_id: EntityIdentity::single("entity-1"),
        };
        let forged_value = tracked_value(
            "change-forged",
            "parent",
            "2026-01-01T00:00:00Z",
            "2026-01-01T00:00:00Z",
        );
        stage_parent_projection_root_and_delete_child(
            &storage,
            "parent",
            "child",
            vec![(forged_key, forged_value)],
        )
        .await
        .expect("corruption should stage");

        assert_child_repair_rejects(
            &storage,
            &tracked_state,
            "does not match changelog first-parent winner",
        )
        .await;
    }

    async fn assert_child_repair_rejects(
        storage: &crate::storage::StorageContext,
        tracked_state: &crate::tracked_state::TrackedStateContext,
        expected_error: &str,
    ) {
        let mut read = storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let error = tracked_state
            .root_rebuilder(&mut read, &mut writes)
            .ensure_projection_root("child")
            .await
            .expect_err("repair must reject corrupt parent projection root");
        assert!(
            error.message.contains(expected_error),
            "unexpected error: {error}"
        );
    }

    async fn stage_parent_projection_root_and_delete_child(
        storage: &crate::storage::StorageContext,
        parent_commit_id: &str,
        child_commit_id: &str,
        entries: Vec<(TrackedStateKey, TrackedStateIndexValue)>,
    ) -> Result<(), LixError> {
        let read = storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        let mutations = entries
            .into_iter()
            .map(|(key, value)| {
                crate::tracked_state::types::TrackedStateMutation::put_encoded(
                    crate::tracked_state::codec::encode_key(&key),
                    crate::tracked_state::codec::encode_value(&value),
                )
            })
            .collect::<Vec<_>>();
        let changed_key_count = mutations.len();
        let result = TrackedStateTree::new()
            .apply_mutations(&read, &mut writes, None, mutations, Some("forged-parent"))
            .await?;
        storage::stage_projection_metadata(
            &mut writes,
            &crate::tracked_state::types::TrackedStateProjectionMetadata {
                commit_id: parent_commit_id.to_string(),
                root_id: result.root_id,
                parent_roots: Vec::new(),
                changed_key_count: changed_key_count as u64,
                row_count_estimate: result.row_count as u64,
                tree_height: result.tree_height as u32,
                primary_chunk_count: result.chunk_count as u64,
                primary_chunk_bytes: result.chunk_bytes as u64,
            },
        )?;
        writes.delete(
            storage::TRACKED_STATE_PROJECTION_SPACE,
            crate::storage::StorageKey(bytes::Bytes::copy_from_slice(child_commit_id.as_bytes())),
        );
        storage.commit_write_set(writes, crate::storage::StorageWriteOptions::default())?;
        Ok(())
    }

    async fn stage_materialized_root(
        storage: &crate::storage::StorageContext,
        tracked_state: &crate::tracked_state::TrackedStateContext,
        commit_id: &str,
        parent_commit_id: Option<&str>,
        rows: &[crate::tracked_state::MaterializedTrackedStateRow],
    ) -> Result<(), LixError> {
        let mut read = storage
            .begin_read(crate::storage::StorageReadOptions::default())
            .expect("read should open");
        let mut writes = storage.new_write_set();
        crate::test_support::stage_tracked_root_from_materialized(
            &mut read,
            &mut writes,
            tracked_state,
            commit_id,
            parent_commit_id,
            rows,
        )
        .await?;
        storage.commit_write_set(writes, crate::storage::StorageWriteOptions::default())?;
        Ok(())
    }

    fn materialized_row(
        entity_id: &str,
        change_id: &str,
        commit_id: &str,
        value: &str,
        created_at: &str,
        updated_at: &str,
    ) -> crate::tracked_state::MaterializedTrackedStateRow {
        crate::tracked_state::MaterializedTrackedStateRow {
            entity_id: EntityIdentity::single(entity_id),
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            snapshot_content: Some(format!("{{\"value\":\"{value}\"}}")),
            metadata: None,
            deleted: false,
            created_at: created_at.to_string(),
            updated_at: updated_at.to_string(),
            change_id: change_id.to_string(),
            commit_id: commit_id.to_string(),
        }
    }

    fn tracked_value(
        change_id: &str,
        commit_id: &str,
        created_at: &str,
        updated_at: &str,
    ) -> TrackedStateIndexValue {
        TrackedStateIndexValue {
            change_locator: ChangelogChangeLocator {
                change_id: change_id.to_string(),
                commit_id: commit_id.to_string(),
                location: SegmentObjectLocation {
                    segment_id: format!("segment-{commit_id}"),
                    offset: 0,
                    len: 1,
                    checksum: change_id.to_string(),
                },
            },
            deleted: false,
            snapshot_ref: None,
            metadata_ref: None,
            created_at: created_at.to_string(),
            updated_at: updated_at.to_string(),
        }
    }

    fn located_change(
        commit_id: &str,
        offset: u64,
        change_id: &str,
        entity_id: &str,
        created_at: &str,
    ) -> LocatedChange {
        LocatedChange {
            locator: ChangelogChangeLocator {
                change_id: change_id.to_string(),
                commit_id: commit_id.to_string(),
                location: SegmentObjectLocation {
                    segment_id: format!("segment-{commit_id}"),
                    offset,
                    len: 1,
                    checksum: change_id.to_string(),
                },
            },
            change: Change {
                id: change_id.to_string(),
                authored_commit_id: Some(commit_id.to_string()),
                entity_id: EntityIdentity::single(entity_id),
                schema_key: "schema".to_string(),
                file_id: Some("file".to_string()),
                snapshot_ref: None,
                metadata_ref: None,
                created_at: created_at.to_string(),
            },
        }
    }
}
