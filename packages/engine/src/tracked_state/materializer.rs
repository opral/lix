use crate::commit_store::{Change, ChangeLocator, Commit, CommitStoreContext};
use crate::storage::StorageReader;
use crate::tracked_state::context::{TrackedStateMaterializer, TrackedStateWriteReport};
use crate::tracked_state::types::TrackedStateKey;
use crate::tracked_state::TrackedStateDeltaRef;
use crate::LixError;
use std::collections::{BTreeMap, BTreeSet};

/// Owned materialization delta used only by explicit projection-root hydration.
///
/// Normal transaction commits already have borrowed `ChangeRef` and
/// `ChangeLocatorRef` values available while staging commit_store.
/// Materialization loads those facts back from storage, so it owns the decoded
/// data internally and immediately passes a borrowed view into the same
/// tracked-state root writer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializationDelta {
    pub(crate) change: Change,
    pub(crate) locator: ChangeLocator,
    pub(crate) created_at: String,
    pub(crate) updated_at: String,
}

impl MaterializationDelta {
    pub(crate) fn as_ref(&self) -> TrackedStateDeltaRef<'_> {
        TrackedStateDeltaRef {
            change: self.change.as_ref(),
            locator: self.locator.as_ref(),
            created_at: &self.created_at,
            updated_at: &self.updated_at,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MaterializationInput {
    pub(crate) commit_id: String,
    pub(crate) parent_commit_id: Option<String>,
    pub(crate) deltas: Vec<MaterializationDelta>,
}

struct LocatedChange {
    locator: ChangeLocator,
    change: Change,
}

/// Explicit projection-root materialization over commit_store.
///
/// Normal transaction commits must use `TrackedStateWriter::stage_delta` with
/// already prepared commit_store refs. This path exists for deliberate
/// materialization only.
pub(crate) async fn materialize_root_at<S>(
    materializer: &mut TrackedStateMaterializer<'_, S>,
    commit_id: &str,
) -> Result<TrackedStateWriteReport, LixError>
where
    S: StorageReader + ?Sized,
{
    let input =
        build_materialization_input(materializer.store, materializer.commit_store, commit_id)
            .await?;
    let delta_refs = input
        .deltas
        .iter()
        .map(MaterializationDelta::as_ref)
        .collect::<Vec<_>>();
    materializer
        .tracked_state
        .writer(materializer.store, materializer.writes)
        .stage_projection_root(
            &input.commit_id,
            input.parent_commit_id.as_deref(),
            delta_refs,
        )
        .await
}

async fn build_materialization_input<S>(
    store: &mut S,
    commit_store: &CommitStoreContext,
    commit_id: &str,
) -> Result<MaterializationInput, LixError>
where
    S: StorageReader + ?Sized,
{
    let lineage = load_first_parent_lineage(store, commit_store, commit_id).await?;
    let mut located_changes = Vec::new();
    for commit in lineage {
        located_changes
            .append(&mut load_commit_located_changes(store, commit_store, &commit).await?);
    }
    let deltas = project_materialization_deltas(located_changes);

    Ok(MaterializationInput {
        commit_id: commit_id.to_string(),
        parent_commit_id: None,
        deltas,
    })
}

async fn load_first_parent_lineage<S>(
    store: &mut S,
    commit_store: &CommitStoreContext,
    commit_id: &str,
) -> Result<Vec<Commit>, LixError>
where
    S: StorageReader + ?Sized,
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
        let commit = commit_store
            .load_commit_from(store, &current_id)
            .await?
            .ok_or_else(|| missing_commit_error(&current_id))?;
        current = commit.parent_ids.first().cloned();
        lineage.push(commit);
    }
    lineage.reverse();
    Ok(lineage)
}

async fn load_commit_located_changes<S>(
    store: &mut S,
    commit_store: &CommitStoreContext,
    commit: &Commit,
) -> Result<Vec<LocatedChange>, LixError>
where
    S: StorageReader + ?Sized,
{
    let mut located_changes = Vec::new();
    for pack_id in 0..commit.change_pack_count {
        let changes = commit_store
            .load_change_pack_from(store, &commit.id, pack_id)
            .await?
            .ok_or_else(|| missing_pack_error("change", &commit.id, pack_id))?;
        for (source_ordinal, change) in changes.into_iter().enumerate() {
            let locator = ChangeLocator {
                source_commit_id: commit.id.clone(),
                source_pack_id: pack_id,
                source_ordinal: u32::try_from(source_ordinal).map_err(|_| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        "tracked_state materialization change pack ordinal exceeds u32",
                    )
                })?,
                change_id: change.id.clone(),
            };
            located_changes.push(LocatedChange { locator, change });
        }
    }

    let mut adopted_locators = Vec::new();
    for pack_id in 0..commit.membership_pack_count {
        let mut locators = commit_store
            .load_membership_pack_from(store, &commit.id, pack_id)
            .await?
            .ok_or_else(|| missing_pack_error("membership", &commit.id, pack_id))?;
        adopted_locators.append(&mut locators);
    }
    let adopted_changes = load_changes_by_locators(store, commit_store, &adopted_locators).await?;
    located_changes.extend(
        adopted_locators
            .into_iter()
            .zip(adopted_changes)
            .map(|(locator, change)| LocatedChange { locator, change }),
    );
    Ok(located_changes)
}

fn project_materialization_deltas(
    changes: impl IntoIterator<Item = LocatedChange>,
) -> Vec<MaterializationDelta> {
    let mut projected = BTreeMap::<TrackedStateKey, MaterializationDelta>::new();
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
            MaterializationDelta {
                change,
                locator,
                created_at,
                updated_at,
            },
        );
    }
    projected.into_values().collect()
}

async fn load_changes_by_locators(
    store: &mut (impl StorageReader + ?Sized),
    commit_store: &CommitStoreContext,
    locators: &[ChangeLocator],
) -> Result<Vec<Change>, LixError> {
    let mut packs = BTreeMap::<(String, u32), Vec<Change>>::new();
    for locator in locators {
        let key = (locator.source_commit_id.clone(), locator.source_pack_id);
        if packs.contains_key(&key) {
            continue;
        }
        let changes = commit_store
            .load_change_pack_from(store, &locator.source_commit_id, locator.source_pack_id)
            .await?
            .ok_or_else(|| {
                missing_pack_error("change", &locator.source_commit_id, locator.source_pack_id)
            })?;
        packs.insert(key, changes);
    }

    locators
        .iter()
        .map(|locator| change_from_loaded_packs(&packs, locator))
        .collect()
}

fn change_from_loaded_packs(
    packs: &BTreeMap<(String, u32), Vec<Change>>,
    locator: &ChangeLocator,
) -> Result<Change, LixError> {
    let key = (locator.source_commit_id.clone(), locator.source_pack_id);
    let changes = packs.get(&key).ok_or_else(|| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state materialization lost loaded change pack ({}, {})",
                locator.source_commit_id, locator.source_pack_id
            ),
        )
    })?;
    let change = changes
        .get(usize::try_from(locator.source_ordinal).map_err(|_| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                "tracked_state materialization locator ordinal does not fit usize",
            )
        })?)
        .ok_or_else(|| {
            LixError::new(
                LixError::CODE_INTERNAL_ERROR,
                format!(
                    "tracked_state materialization locator for '{}' points past pack ({}, {})",
                    locator.change_id, locator.source_commit_id, locator.source_pack_id
                ),
            )
        })?;
    if change.id != locator.change_id {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!(
                "tracked_state materialization locator expected '{}' but found '{}'",
                locator.change_id, change.id
            ),
        ));
    }
    Ok(change.clone())
}

fn missing_pack_error(label: &str, commit_id: &str, pack_id: u32) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("tracked_state materialization missing {label} pack ({commit_id}, {pack_id})"),
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
    use crate::commit_store::ChangeLocator;
    use crate::entity_identity::EntityIdentity;

    #[test]
    fn materialization_delta_ref_borrows_owned_facts() {
        let delta = MaterializationDelta {
            change: Change {
                id: "change-1".to_string(),
                entity_id: EntityIdentity::single("entity-1"),
                schema_key: "schema".to_string(),
                file_id: Some("file".to_string()),
                snapshot_ref: None,
                metadata_ref: None,
                created_at: "2026-01-01T00:00:00Z".to_string(),
            },
            locator: ChangeLocator {
                source_commit_id: "commit-1".to_string(),
                source_pack_id: 7,
                source_ordinal: 11,
                change_id: "change-1".to_string(),
            },
            created_at: "2026-01-01T00:00:00Z".to_string(),
            updated_at: "2026-02-01T00:00:00Z".to_string(),
        };

        let delta_ref = delta.as_ref();

        assert_eq!(delta_ref.change.id, "change-1");
        assert_eq!(delta_ref.change.schema_key, "schema");
        assert_eq!(delta_ref.change.file_id, Some("file"));
        assert_eq!(delta_ref.locator.source_commit_id, "commit-1");
        assert_eq!(delta_ref.locator.source_pack_id, 7);
        assert_eq!(delta_ref.locator.source_ordinal, 11);
        assert_eq!(delta_ref.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(delta_ref.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn change_from_loaded_packs_resolves_locator_by_pack_and_ordinal() {
        let mut packs = BTreeMap::new();
        packs.insert(
            ("source-commit".to_string(), 3),
            vec![change("change-0"), change("change-1"), change("change-2")],
        );
        let locator = ChangeLocator {
            source_commit_id: "source-commit".to_string(),
            source_pack_id: 3,
            source_ordinal: 1,
            change_id: "change-1".to_string(),
        };

        let resolved = change_from_loaded_packs(&packs, &locator).expect("locator should resolve");

        assert_eq!(resolved.id, "change-1");
    }

    #[test]
    fn change_from_loaded_packs_rejects_locator_change_id_mismatch() {
        let mut packs = BTreeMap::new();
        packs.insert(("source-commit".to_string(), 3), vec![change("actual")]);
        let locator = ChangeLocator {
            source_commit_id: "source-commit".to_string(),
            source_pack_id: 3,
            source_ordinal: 0,
            change_id: "expected".to_string(),
        };

        let error =
            change_from_loaded_packs(&packs, &locator).expect_err("mismatched locator should fail");

        assert!(error.message.contains("expected"));
        assert!(error.message.contains("actual"));
    }

    #[test]
    fn project_materialization_deltas_keeps_first_seen_created_at_and_latest_updated_at() {
        let deltas = project_materialization_deltas(vec![
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
        assert_eq!(delta.locator.source_commit_id, "commit-2");
        assert_eq!(delta.created_at, "2026-01-01T00:00:00Z");
        assert_eq!(delta.updated_at, "2026-02-01T00:00:00Z");
    }

    #[test]
    fn project_materialization_deltas_uses_adopted_change_time_not_target_commit_time() {
        let deltas = project_materialization_deltas(vec![located_change(
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
    fn project_materialization_deltas_tracks_entities_independently() {
        let deltas = project_materialization_deltas(vec![
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

    fn change(id: &str) -> Change {
        Change {
            id: id.to_string(),
            entity_id: EntityIdentity::single("entity-1"),
            schema_key: "schema".to_string(),
            file_id: Some("file".to_string()),
            snapshot_ref: None,
            metadata_ref: None,
            created_at: "2026-01-01T00:00:00Z".to_string(),
        }
    }

    fn located_change(
        commit_id: &str,
        source_ordinal: u32,
        change_id: &str,
        entity_id: &str,
        created_at: &str,
    ) -> LocatedChange {
        LocatedChange {
            locator: ChangeLocator {
                source_commit_id: commit_id.to_string(),
                source_pack_id: 0,
                source_ordinal,
                change_id: change_id.to_string(),
            },
            change: Change {
                id: change_id.to_string(),
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
