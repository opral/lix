use std::collections::{HashMap, HashSet};

use super::context::ChangelogStorageRead;
use super::store::{
    by_change_index_value, by_change_key, by_change_membership_ids_from_key,
    by_change_membership_index_value, by_change_membership_key, by_commit_index_value,
    by_commit_key, commit_visibility_key, commit_visibility_value, segment_key,
    visible_change_proof_key, visible_change_proof_value, BY_CHANGE_INDEX_SPACE,
    BY_CHANGE_MEMBERSHIP_INDEX_SPACE, BY_COMMIT_INDEX_SPACE, COMMIT_VISIBILITY_SPACE,
    SEGMENT_SPACE, VISIBLE_CHANGE_PROOF_SPACE,
};
use super::truth::{compute_retained_primary_closure, load_segment_truth_index};
use super::types::{
    ByChangeEntry, ByCommitEntry, CommitVisibility, GcLiveSet, GcPlan, GcRepairSet, GcRoot,
    GcSweepSet, MembershipRole, Segment, SegmentChange, SegmentCommit, SegmentObjectLocation,
    StateRowIdentity,
};
use crate::changelog::decode_commit_visibility;
use crate::common::{CanonicalSchemaKey, EntityId, FileId};
use crate::json_store::{self, JsonRef};
use crate::storage::{StorageCoreProjection, StorageSpace, StorageWriteSet};
use crate::LixError;

pub(super) async fn plan_gc<S>(store: &mut S, roots: &[GcRoot]) -> Result<GcPlan, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let truth = load_segment_truth_index(store).await?;
    let all_commit_visibility = scan_utf8_keys(store, COMMIT_VISIBILITY_SPACE).await?;
    let all_by_commit = scan_utf8_keys(store, BY_COMMIT_INDEX_SPACE).await?;
    let all_by_change = scan_utf8_keys(store, BY_CHANGE_INDEX_SPACE).await?;
    let all_by_change_membership = scan_by_change_membership_entries(store).await?;
    let all_visible_change_proof = scan_utf8_keys(store, VISIBLE_CHANGE_PROOF_SPACE).await?;
    let all_json_payloads = scan_json_payload_keys(store).await?;

    let mut live = GcLiveSet::default();
    let mut pending_commits = Vec::new();
    for root in roots {
        pending_commits.push(gc_root_commit_id(root).to_string());
    }

    let mut commit_index: HashMap<String, (SegmentObjectLocation, SegmentCommit)> = HashMap::new();
    let mut change_index: HashMap<String, (SegmentObjectLocation, SegmentChange)> = HashMap::new();
    let mut source_parent_facts = HashMap::<String, GcSourceParentFacts>::new();

    while let Some(commit_id) = pending_commits.pop() {
        if live.commits.contains(&commit_id) {
            continue;
        }
        let Some((commit_location, commit)) = truth.commits.get(&commit_id).cloned() else {
            return Err(LixError::unknown(format!(
                "changelog GC root/ancestor commit '{commit_id}' was not found in changelog segments"
            )));
        };
        if commit_index
            .insert(commit_id.clone(), (commit_location.clone(), commit.clone()))
            .is_some()
        {
            return Err(LixError::unknown(format!(
                "changelog GC found duplicate live commit id '{commit_id}'"
            )));
        }

        push_unique(&mut live.commits, commit_id.clone());
        push_unique(&mut live.segments, commit_location.segment_id.clone());

        for parent_id in &commit.header.parent_commit_ids {
            if !live.commits.contains(parent_id) {
                pending_commits.push(parent_id.clone());
            }
        }

        for membership in &commit.body.membership {
            let change_id = &membership.member_change_id;
            let Some((change_location, change)) = truth.changes.get(change_id).cloned() else {
                return Err(LixError::unknown(format!(
                    "changelog GC live commit '{}' references missing change '{}'",
                    commit.header.id, change_id
                )));
            };
            if let Some((existing_location, existing_change)) =
                change_index.insert(change_id.clone(), (change_location.clone(), change.clone()))
            {
                if existing_location != change_location || existing_change != change {
                    return Err(LixError::unknown(format!(
                        "changelog GC found duplicate live change id '{change_id}'"
                    )));
                }
            }
            push_unique(&mut live.changes, change_id.clone());
            push_unique(&mut live.segments, change_location.segment_id.clone());
            mark_change_payloads(&mut live.payloads, &change);
        }
    }
    let mut visiting_commits = HashSet::new();
    let mut checked_commits = HashSet::new();
    for root in roots {
        validate_reachable_commit_graph_acyclic(
            gc_root_commit_id(root),
            &commit_index,
            &mut visiting_commits,
            &mut checked_commits,
        )?;
    }
    validate_gc_adopted_memberships(&commit_index, &change_index, &mut source_parent_facts)?;

    let live_commits: HashSet<_> = live.commits.iter().cloned().collect();
    let retained =
        compute_retained_primary_closure(&truth, live.segments.iter().cloned().collect())?;
    let mut retained_memberships = HashSet::new();
    for (commit_id, (_, commit)) in &truth.commits {
        if !retained.commits.contains(commit_id) {
            continue;
        }
        for membership in &commit.body.membership {
            retained_memberships.insert((membership.member_change_id.clone(), commit_id.clone()));
        }
    }
    let mut retained_payloads = Vec::new();
    for (change_id, (_, change)) in &truth.changes {
        if retained.changes.contains(change_id) {
            mark_change_payloads(&mut retained_payloads, change);
        }
    }
    let retained_payload_hashes = retained_payloads
        .iter()
        .map(|json_ref| *json_ref.as_hash_array())
        .collect::<HashSet<_>>();
    let expected_commit_visibility = expected_commit_visibilities(&commit_index, &live_commits);
    let retained_commit_order = truth
        .segment_ids
        .iter()
        .flat_map(|segment_id| {
            truth
                .commits
                .iter()
                .filter(move |(_, (location, _))| &location.segment_id == segment_id)
                .map(|(commit_id, _)| commit_id.clone())
        })
        .filter(|commit_id| retained.commits.contains(commit_id))
        .collect::<Vec<_>>();
    let retained_change_order = truth
        .segment_ids
        .iter()
        .flat_map(|segment_id| {
            truth
                .changes
                .iter()
                .filter(move |(_, (location, _))| &location.segment_id == segment_id)
                .map(|(change_id, _)| change_id.clone())
        })
        .filter(|change_id| retained.changes.contains(change_id))
        .collect::<Vec<_>>();
    let retained_commit_index = truth
        .commits
        .iter()
        .filter(|(commit_id, _)| retained.commits.contains(*commit_id))
        .map(|(commit_id, value)| (commit_id.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let retained_change_index = truth
        .changes
        .iter()
        .filter(|(change_id, _)| retained.changes.contains(*change_id))
        .map(|(change_id, value)| (change_id.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let expected_retained_by_commit =
        expected_live_by_commit_entries(&retained_commit_index, &retained_commit_order)?;
    let expected_retained_by_change =
        expected_live_by_change_entries(&retained_change_index, &retained_change_order);
    let expected_visible_change_proof =
        expected_visible_change_proofs(&commit_index, &live.commits, &live_commits);
    let live_commit_visibility_entries = load_commit_visibilities(
        store,
        &all_commit_visibility
            .iter()
            .filter(|commit_id| live_commits.contains(*commit_id))
            .cloned()
            .collect::<Vec<_>>(),
    )
    .await?;
    for (commit_id, expected) in &expected_commit_visibility {
        if let Some(actual) = live_commit_visibility_entries.get(commit_id) {
            if actual != expected {
                return Err(LixError::unknown(format!(
                    "changelog GC live commit_visibility entry for '{commit_id}' does not match segment truth"
                )));
            }
        }
    }

    let sweep = GcSweepSet {
        segments: truth
            .segment_ids
            .into_iter()
            .filter(|segment_id| !retained.segments.contains(segment_id))
            .collect(),
        commit_visibility: all_commit_visibility
            .into_iter()
            .filter(|commit_id| {
                !live_commits.contains(commit_id)
                    || live_commit_visibility_entries.get(commit_id)
                        != expected_commit_visibility.get(commit_id)
            })
            .collect(),
        by_commit: all_by_commit
            .into_iter()
            .filter(|commit_id| !retained.commits.contains(commit_id))
            .collect(),
        by_change: all_by_change
            .into_iter()
            .filter(|change_id| !retained.changes.contains(change_id))
            .collect(),
        by_change_membership: all_by_change_membership
            .keys()
            .filter(|membership| !retained_memberships.contains(*membership))
            .cloned()
            .collect(),
        visible_change_proof: all_visible_change_proof
            .into_iter()
            .filter(|change_id| !expected_visible_change_proof.contains_key(change_id))
            .collect(),
        json_payloads: all_json_payloads
            .into_iter()
            .filter(|json_ref| !retained_payload_hashes.contains(json_ref.as_hash_array()))
            .collect(),
    };
    let repair = GcRepairSet {
        by_commit: retained_commit_order
            .iter()
            .filter_map(|commit_id| expected_retained_by_commit.get(commit_id).cloned())
            .collect(),
        by_change: retained_change_order
            .iter()
            .filter_map(|change_id| expected_retained_by_change.get(change_id).cloned())
            .collect(),
        by_change_membership: retained_memberships.into_iter().collect(),
        visible_change_proof: expected_visible_change_proof.into_iter().collect(),
    };

    Ok(GcPlan {
        roots: roots.to_vec(),
        live,
        sweep,
        repair,
    })
}

#[derive(Default)]
struct GcSourceParentFacts {
    reachable_memberships: HashSet<String>,
    first_parent_winners: HashMap<StateRowIdentity, String>,
}

fn gc_source_parent_facts(
    root_commit_id: &str,
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
) -> Result<GcSourceParentFacts, LixError> {
    let mut facts = GcSourceParentFacts::default();
    let mut stack = vec![root_commit_id.to_string()];
    let mut visited = HashSet::new();
    while let Some(commit_id) = stack.pop() {
        if !visited.insert(commit_id.clone()) {
            continue;
        }
        let Some((_, commit)) = commit_index.get(&commit_id) else {
            continue;
        };
        facts.reachable_memberships.extend(
            commit
                .body
                .membership
                .iter()
                .map(|membership| membership.member_change_id.clone()),
        );
        stack.extend(commit.header.parent_commit_ids.iter().cloned());
    }

    let mut next_commit_id = Some(root_commit_id.to_string());
    let mut visited = HashSet::new();
    while let Some(commit_id) = next_commit_id.take() {
        if !visited.insert(commit_id.clone()) {
            return Err(LixError::unknown(format!(
                "changelog GC cannot resolve source parent facts because first-parent history contains cycle at commit '{}'",
                commit_id
            )));
        }
        let Some((_, commit)) = commit_index.get(&commit_id) else {
            break;
        };
        for (identity, change_id) in &commit.directory.state_row_identities {
            facts
                .first_parent_winners
                .entry(identity.clone())
                .or_insert_with(|| change_id.clone());
        }
        next_commit_id = commit.header.parent_commit_ids.first().cloned();
    }
    Ok(facts)
}

fn state_row_identity_for_change(change: &SegmentChange) -> Result<StateRowIdentity, LixError> {
    Ok(StateRowIdentity {
        schema_key: CanonicalSchemaKey::new(change.schema_key.clone())?,
        file_id: FileId::new(
            change
                .file_id
                .clone()
                .unwrap_or_else(|| "__global__".to_string()),
        )?,
        entity_id: EntityId::new(change.entity_id.as_json_array_text()?)?,
    })
}

fn validate_reachable_commit_graph_acyclic(
    commit_id: &str,
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    visiting: &mut HashSet<String>,
    checked: &mut HashSet<String>,
) -> Result<(), LixError> {
    if checked.contains(commit_id) {
        return Ok(());
    }
    if !visiting.insert(commit_id.to_string()) {
        return Err(LixError::unknown(format!(
            "changelog GC found parent cycle at commit '{commit_id}'"
        )));
    }
    let Some((_, commit)) = commit_index.get(commit_id) else {
        return Err(LixError::unknown(format!(
            "changelog GC root/ancestor commit '{commit_id}' was not found in changelog segments"
        )));
    };
    for parent_id in &commit.header.parent_commit_ids {
        validate_reachable_commit_graph_acyclic(parent_id, commit_index, visiting, checked)?;
    }
    visiting.remove(commit_id);
    checked.insert(commit_id.to_string());
    Ok(())
}

pub(super) async fn collect_garbage<S>(
    store: &mut S,
    writes: &mut StorageWriteSet,
    roots: &[GcRoot],
) -> Result<GcPlan, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let plan = plan_gc(store, roots).await?;
    stage_gc_sweep(writes, &plan)?;
    Ok(plan)
}

pub(super) fn stage_gc_sweep(writes: &mut StorageWriteSet, plan: &GcPlan) -> Result<(), LixError> {
    for segment_id in &plan.sweep.segments {
        writes.delete(SEGMENT_SPACE, segment_key(segment_id));
    }
    for commit_id in &plan.sweep.commit_visibility {
        writes.delete(COMMIT_VISIBILITY_SPACE, commit_visibility_key(commit_id));
    }
    for commit_id in &plan.sweep.by_commit {
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key(commit_id));
    }
    for change_id in &plan.sweep.by_change {
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key(change_id));
    }
    for (change_id, commit_id) in &plan.sweep.by_change_membership {
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key(change_id, commit_id),
        );
    }
    for change_id in &plan.sweep.visible_change_proof {
        writes.delete(
            VISIBLE_CHANGE_PROOF_SPACE,
            visible_change_proof_key(change_id),
        );
    }
    for json_ref in &plan.sweep.json_payloads {
        json_store::stage_direct_json_payload_delete(writes, json_ref);
    }
    for entry in &plan.repair.by_commit {
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key(&entry.commit_id),
            by_commit_index_value(entry)?,
        );
    }
    for entry in &plan.repair.by_change {
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key(&entry.change_id),
            by_change_index_value(entry)?,
        );
    }
    for (change_id, commit_id) in &plan.repair.by_change_membership {
        writes.put(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key(change_id, commit_id),
            by_change_membership_index_value(),
        );
    }
    for (change_id, commit_id) in &plan.repair.visible_change_proof {
        writes.put(
            VISIBLE_CHANGE_PROOF_SPACE,
            visible_change_proof_key(change_id),
            visible_change_proof_value(commit_id),
        );
    }
    Ok(())
}

fn validate_gc_adopted_memberships(
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    change_index: &HashMap<String, (SegmentObjectLocation, SegmentChange)>,
    source_parent_facts: &mut HashMap<String, GcSourceParentFacts>,
) -> Result<(), LixError> {
    for commit in commit_index.values().map(|(_, commit)| commit) {
        for membership in &commit.body.membership {
            if membership.role != MembershipRole::Adopted {
                let change_id = &membership.member_change_id;
                let Some((_, change)) = change_index.get(change_id) else {
                    continue;
                };
                if change.authored_commit_id.as_deref() != Some(commit.header.id.as_str()) {
                    return Err(LixError::unknown(format!(
                        "changelog GC live commit '{}' authored membership change '{}' has mismatched authored_commit_id",
                        commit.header.id, change_id
                    )));
                }
                continue;
            };
            let change_id = &membership.member_change_id;
            let Some((_, change)) = change_index.get(change_id) else {
                continue;
            };
            if change.authored_commit_id.as_deref() == Some(commit.header.id.as_str()) {
                return Err(LixError::unknown(format!(
                    "changelog GC live commit '{}' adopted membership change '{}' is authored by the same commit",
                    commit.header.id, change_id
                )));
            }
            let source_parent_ordinal = membership.source_parent_ordinal.ok_or_else(|| {
                LixError::unknown(format!(
                    "changelog GC live commit '{}' adopted membership change '{}' is missing source_parent_ordinal",
                    commit.header.id, change_id
                ))
            })?;
            let parent_id = commit
                .header
                .parent_commit_ids
                .get(source_parent_ordinal as usize)
                .ok_or_else(|| {
                    LixError::unknown(format!(
                        "changelog GC live commit '{}' adopted membership change '{}' source_parent_ordinal {} is out of bounds",
                        commit.header.id, change_id, source_parent_ordinal
                    ))
                })?;
            if !source_parent_facts.contains_key(parent_id) {
                let facts = gc_source_parent_facts(parent_id, commit_index)?;
                source_parent_facts.insert(parent_id.clone(), facts);
            }
            let facts = source_parent_facts
                .get(parent_id)
                .expect("source parent facts should be cached");
            if !facts.reachable_memberships.contains(change_id) {
                return Err(LixError::unknown(format!(
                    "changelog GC live commit '{}' adopted membership change '{}' is not reachable from source parent '{}'",
                    commit.header.id, change_id, parent_id
                )));
            }
            let identity = state_row_identity_for_change(change)?;
            if facts.first_parent_winners.get(&identity) != Some(change_id) {
                return Err(LixError::unknown(format!(
                    "changelog GC live commit '{}' adopted membership change '{}' is not the source parent '{}' winner for {:?}",
                    commit.header.id, change_id, parent_id, identity
                )));
            }
        }
    }
    Ok(())
}

async fn scan_utf8_keys<S>(store: &mut S, space: StorageSpace) -> Result<Vec<String>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut after = None;
    let mut out = Vec::new();
    loop {
        let page = store
            .changelog_scan(
                space,
                Vec::new(),
                after,
                256,
                StorageCoreProjection::KeyOnly,
            )
            .await?;
        for index in 0..page.keys.len() {
            let Some(key) = page.keys.get(index) else {
                continue;
            };
            out.push(
                std::str::from_utf8(key)
                    .map_err(|error| {
                        LixError::unknown(format!(
                            "changelog GC found invalid UTF-8 key in namespace '{}': {error}",
                            space.name
                        ))
                    })?
                    .to_string(),
            );
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(out)
}

async fn scan_by_change_membership_entries<S>(
    store: &mut S,
) -> Result<HashMap<(String, String), Vec<u8>>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut after = None;
    let mut out = HashMap::new();
    loop {
        let page = store
            .changelog_scan(
                BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                Vec::new(),
                after,
                256,
                StorageCoreProjection::FullValue,
            )
            .await?;
        for index in 0..page.len() {
            let Some(key) = page.key(index) else {
                continue;
            };
            let Some(value) = page.value(index) else {
                return Err(LixError::unknown(
                    "changelog GC by_change_membership scan returned key without value".to_string(),
                ));
            };
            out.insert(by_change_membership_ids_from_key(key)?, value.to_vec());
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(out)
}

fn expected_commit_visibilities(
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    live_commits: &HashSet<String>,
) -> HashMap<String, CommitVisibility> {
    let mut out = HashMap::new();
    for (commit_id, (location, commit)) in commit_index {
        if live_commits.contains(commit_id) {
            out.insert(
                commit_id.clone(),
                CommitVisibility {
                    commit_id: commit.header.id.clone(),
                    checksum: location.checksum.clone(),
                    location: location.clone(),
                },
            );
        }
    }
    out
}

fn expected_visible_change_proofs(
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    live_commit_order: &[String],
    live_commits: &HashSet<String>,
) -> HashMap<String, String> {
    let mut out = HashMap::new();
    for commit_id in live_commit_order {
        let Some((_, commit)) = commit_index.get(commit_id) else {
            continue;
        };
        if !live_commits.contains(commit_id) {
            continue;
        }
        for membership in &commit.body.membership {
            out.insert(membership.member_change_id.clone(), commit_id.clone());
        }
    }
    out
}

fn expected_live_by_commit_entries(
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    live_commit_order: &[String],
) -> Result<HashMap<String, ByCommitEntry>, LixError> {
    let mut generations = HashMap::<String, u64>::new();
    for commit_id in live_commit_order {
        expected_live_commit_generation(commit_id, commit_index, &mut generations)?;
    }
    let mut out = HashMap::new();
    for commit_id in live_commit_order {
        let Some((location, commit)) = commit_index.get(commit_id) else {
            continue;
        };
        let generation = generations.get(commit_id).copied().unwrap_or_default();
        out.insert(
            commit_id.clone(),
            ByCommitEntry {
                commit_id: commit.header.id.clone(),
                location: location.clone(),
                parent_commit_ids: commit.header.parent_commit_ids.clone(),
                generation,
            },
        );
    }
    Ok(out)
}

fn expected_live_commit_generation(
    commit_id: &str,
    commit_index: &HashMap<String, (SegmentObjectLocation, SegmentCommit)>,
    generations: &mut HashMap<String, u64>,
) -> Result<u64, LixError> {
    if let Some(generation) = generations.get(commit_id) {
        return Ok(*generation);
    }
    let Some((_, commit)) = commit_index.get(commit_id) else {
        return Ok(0);
    };
    let mut generation = 0;
    for parent_id in &commit.header.parent_commit_ids {
        let parent_generation = if commit_index.contains_key(parent_id) {
            expected_live_commit_generation(parent_id, commit_index, generations)?
        } else {
            0
        };
        generation = generation.max(parent_generation.saturating_add(1));
    }
    generations.insert(commit_id.to_string(), generation);
    Ok(generation)
}

fn expected_live_by_change_entries(
    change_index: &HashMap<String, (SegmentObjectLocation, SegmentChange)>,
    live_change_order: &[String],
) -> HashMap<String, ByChangeEntry> {
    let mut out = HashMap::new();
    for change_id in live_change_order {
        let Some((location, change)) = change_index.get(change_id) else {
            continue;
        };
        out.insert(
            change_id.clone(),
            ByChangeEntry {
                change_id: change.id.clone(),
                location: location.clone(),
            },
        );
    }
    out
}

async fn load_commit_visibilities<S>(
    store: &mut S,
    commit_ids: &[String],
) -> Result<HashMap<String, CommitVisibility>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let values = store
        .changelog_get_many(
            COMMIT_VISIBILITY_SPACE,
            commit_ids
                .iter()
                .map(|commit_id| commit_visibility_key(commit_id))
                .collect(),
        )
        .await?;
    let mut out = HashMap::new();
    for (commit_id, value) in commit_ids.iter().zip(values.into_iter()) {
        let Some(bytes) = value else {
            continue;
        };
        let visibility = decode_commit_visibility(&bytes)?;
        if visibility.commit_id != *commit_id {
            return Err(LixError::unknown(format!(
                "commit visibility key for '{commit_id}' contains commit_id '{}'",
                visibility.commit_id
            )));
        }
        out.insert(commit_id.clone(), visibility);
    }
    Ok(out)
}

async fn scan_json_payload_keys<S>(store: &mut S) -> Result<Vec<JsonRef>, LixError>
where
    S: ChangelogStorageRead + ?Sized,
{
    let mut after = None;
    let mut out = Vec::new();
    loop {
        let page = store
            .changelog_scan(
                json_store::store::JSON_SPACE,
                Vec::new(),
                after,
                256,
                StorageCoreProjection::KeyOnly,
            )
            .await?;
        for index in 0..page.keys.len() {
            let Some(key) = page.keys.get(index) else {
                continue;
            };
            out.push(json_store::direct_json_payload_ref_from_key(key)?);
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(out)
}

fn push_unique(values: &mut Vec<String>, value: String) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn push_unique_json_ref(values: &mut Vec<JsonRef>, value: JsonRef) {
    if !values.iter().any(|existing| existing == &value) {
        values.push(value);
    }
}

fn gc_root_commit_id(root: &GcRoot) -> &str {
    match root {
        GcRoot::VersionHead(commit_id)
        | GcRoot::PinnedCommit(commit_id)
        | GcRoot::RemoteRef(commit_id) => commit_id,
    }
}

fn mark_change_payloads(payloads: &mut Vec<JsonRef>, change: &SegmentChange) {
    if let Some(snapshot_ref) = &change.snapshot_ref {
        push_unique_json_ref(payloads, snapshot_ref.clone());
    }
    if let Some(metadata_ref) = &change.metadata_ref {
        push_unique_json_ref(payloads, metadata_ref.clone());
    }
    for inline_payload in &change.inline_payloads {
        push_unique_json_ref(payloads, inline_payload.json_ref.clone());
    }
    for payload_location in &change.directory.payloads {
        push_unique_json_ref(payloads, payload_location.json_ref.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::segment::canonicalize_segment;
    use crate::changelog::test_support::commit_visibility_from_segment;
    use crate::changelog::{
        decode_segment, encode_segment, ChangelogContext, CommitBody, CommitHeader,
        MembershipRecord, MembershipRole, RebuildIndexStats, Segment, SegmentChange,
        SegmentChangeDirectory, SegmentCommit, SegmentCommitDirectory, SegmentDirectory,
        SegmentHeader, SegmentInlinePayload, SegmentPayloadLocation,
    };
    use crate::common::{CanonicalSchemaKey, EntityId, FileId};
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::JsonRef;
    use crate::storage::InMemoryStorageBackend;
    use crate::storage::{StorageContext, StorageWriteSet};

    #[tokio::test]
    async fn gc_plan_marks_live_commit_membership_changes_payloads_and_segments() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let snapshot_ref = JsonRef::from_hash_bytes([7; 32]);
        let metadata_ref = JsonRef::from_hash_bytes([8; 32]);
        let live_segment = single_commit_segment_with_payloads(
            "segment-1",
            "commit-1",
            "change-1",
            Some(snapshot_ref),
            Some(metadata_ref),
        );
        let dead_segment = single_commit_segment("segment-dead", "commit-dead", "change-dead");

        write_segments(
            &storage,
            &context,
            vec![live_segment, dead_segment],
            &["commit-1"],
        )
        .await;

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-1".to_string())])
            .await
            .unwrap();

        assert_eq!(plan.live.commits, vec!["commit-1"]);
        assert_eq!(plan.live.changes, vec!["change-1"]);
        assert!(plan
            .live
            .payloads
            .contains(&JsonRef::from_hash_bytes([7; 32])));
        assert!(plan
            .live
            .payloads
            .contains(&JsonRef::from_hash_bytes([8; 32])));
        assert_eq!(plan.live.segments, vec!["segment-1"]);
        assert_eq!(plan.sweep.segments, vec!["segment-dead"]);
        assert_eq!(plan.sweep.by_commit, vec!["commit-dead"]);
        assert_eq!(plan.sweep.by_change, vec!["change-dead"]);
        assert_eq!(
            plan.sweep.by_change_membership,
            vec![("change-dead".to_string(), "commit-dead".to_string())]
        );
    }

    #[tokio::test]
    async fn gc_sweeps_unrooted_segment_even_when_rebuildable_indexes_exist() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let segment = single_commit_segment("segment-1", "commit-1", "change-1");

        write_segments(&storage, &context, vec![segment], &[]).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.collect_garbage(&[]).await.unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert!(plan.live.commits.is_empty());
        assert_eq!(plan.sweep.segments, vec!["segment-1"]);
        assert_eq!(plan.sweep.by_commit, vec!["commit-1"]);
        assert_eq!(plan.sweep.by_change, vec!["change-1"]);

        assert_missing(
            &storage,
            vec![
                (SEGMENT_SPACE, segment_key("segment-1")),
                (BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1")),
                (BY_CHANGE_INDEX_SPACE, by_change_key("change-1")),
                (
                    BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                    by_change_membership_key("change-1", "commit-1"),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn collect_garbage_preserves_visible_reads_and_removes_dead_physical_reads() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let mut live_segment = single_commit_segment("segment-live", "commit-live", "change-live");
        live_segment.commits[0].header.membership_count = 1;
        let dead_segment = single_commit_segment("segment-dead", "commit-dead", "change-dead");

        write_segments(
            &storage,
            &context,
            vec![live_segment.clone(), dead_segment],
            &["commit-live"],
        )
        .await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-live".to_string())])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(plan.live.commits, vec!["commit-live"]);
        assert_eq!(plan.live.changes, vec!["change-live"]);
        assert_eq!(plan.sweep.segments, vec!["segment-dead"]);

        let mut reader = context.reader(storage.clone());
        let visible_commit = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["commit-live".to_string()],
                projection: crate::changelog::CommitProjection::Full,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
            })
            .await
            .unwrap();
        assert_eq!(
            visible_commit.entries,
            vec![Some(crate::changelog::CommitLoadEntry::Full {
                header: live_segment.commits[0].header.clone(),
                body: live_segment.commits[0].body.clone(),
            })]
        );

        let mut reader = context.reader(storage.clone());
        let visible_change = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-live".to_string()],
                projection: crate::changelog::ChangeProjection::Segment,
                visibility:
                    crate::changelog::ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();
        assert_eq!(
            visible_change.entries,
            vec![Some(crate::changelog::ChangeLoadEntry::Segment(
                live_segment.changes[0].clone()
            ))]
        );

        let mut reader = context.reader(storage.clone());
        let dead_commit = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["commit-dead".to_string()],
                projection: crate::changelog::CommitProjection::Full,
                visibility: crate::changelog::CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert_eq!(dead_commit.entries, vec![None]);

        let mut reader = context.reader(storage.clone());
        let dead_change = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-dead".to_string()],
                projection: crate::changelog::ChangeProjection::Segment,
                visibility: crate::changelog::ChangeVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert_eq!(dead_change.entries, vec![None]);
    }

    #[tokio::test]
    async fn gc_sweeps_stale_indexes_without_treating_them_as_roots() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let stale_location = SegmentObjectLocation {
            segment_id: "missing-segment".to_string(),
            offset: 0,
            len: 0,
            checksum: "stale-checksum".to_string(),
        };

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("stale-commit"),
            by_commit_index_value(&ByCommitEntry {
                commit_id: "stale-commit".to_string(),
                location: stale_location.clone(),
                parent_commit_ids: Vec::new(),
                generation: 0,
            })
            .unwrap(),
        );
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("stale-change"),
            by_change_index_value(&ByChangeEntry {
                change_id: "stale-change".to_string(),
                location: stale_location,
            })
            .unwrap(),
        );
        writes.put(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("stale-change", "stale-commit"),
            by_change_membership_index_value(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.collect_garbage(&[]).await.unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert!(plan.live.commits.is_empty());
        assert!(plan.live.changes.is_empty());
        assert_eq!(plan.sweep.by_commit, vec!["stale-commit"]);
        assert_eq!(plan.sweep.by_change, vec!["stale-change"]);
        assert_eq!(
            plan.sweep.by_change_membership,
            vec![("stale-change".to_string(), "stale-commit".to_string())]
        );

        assert_missing(
            &storage,
            vec![
                (BY_COMMIT_INDEX_SPACE, by_commit_key("stale-commit")),
                (BY_CHANGE_INDEX_SPACE, by_change_key("stale-change")),
                (
                    BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                    by_change_membership_key("stale-change", "stale-commit"),
                ),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn gc_sweeps_stale_membership_row_even_when_change_and_commit_are_live() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let change_segment =
            single_commit_segment("segment-change", "commit-change", "change-live");
        let commit_segment = single_commit_segment("segment-commit", "commit-live", "change-other");

        write_segments(
            &storage,
            &context,
            vec![change_segment, commit_segment],
            &["commit-change", "commit-live"],
        )
        .await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-live", "commit-live"),
            by_change_membership_index_value(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[
                    GcRoot::VersionHead("commit-change".to_string()),
                    GcRoot::PinnedCommit("commit-live".to_string()),
                ])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            plan.sweep.by_change_membership,
            vec![("change-live".to_string(), "commit-live".to_string())]
        );
        assert_missing(
            &storage,
            vec![(
                BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                by_change_membership_key("change-live", "commit-live"),
            )],
        )
        .await;
    }

    #[tokio::test]
    async fn gc_repairs_corrupt_live_membership_row() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = single_commit_segment("segment-live", "commit-live", "change-live");

        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-live", "commit-live"),
            b"not empty".to_vec(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-live".to_string())])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(
            plan.repair.by_change_membership,
            vec![("change-live".to_string(), "commit-live".to_string())]
        );
        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(
                BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
                vec![by_change_membership_key("change-live", "commit-live")],
            )],
        );
        assert_eq!(result[0][0].as_deref(), Some(&[][..]));
    }

    #[tokio::test]
    async fn gc_sweeps_stale_commit_visibility_and_retains_live_visibility() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = single_commit_segment("segment-live", "commit-live", "change-live");
        let stale_location = SegmentObjectLocation {
            segment_id: "missing-segment".to_string(),
            offset: 0,
            len: 0,
            checksum: "stale-checksum".to_string(),
        };

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(live_segment).await.unwrap();
            writer.stage_publish_commit("commit-live").await.unwrap();
        }
        writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key("stale-commit"),
            commit_visibility_value(&CommitVisibility {
                commit_id: "stale-commit".to_string(),
                location: stale_location.clone(),
                checksum: stale_location.checksum.clone(),
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-live".to_string())])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(plan.live.commits, vec!["commit-live"]);
        assert_eq!(plan.sweep.commit_visibility, vec!["stale-commit"]);

        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(
                COMMIT_VISIBILITY_SPACE,
                vec![
                    commit_visibility_key("commit-live"),
                    commit_visibility_key("stale-commit"),
                ],
            )],
        );
        assert!(result[0][0].is_some());
        assert_eq!(result[0][1], None);
    }

    #[tokio::test]
    async fn gc_rejects_stale_live_locator_indexes() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = single_commit_segment("segment-live", "commit-live", "change-live");
        let stale_location = SegmentObjectLocation {
            segment_id: "missing-segment".to_string(),
            offset: 0,
            len: 0,
            checksum: "stale-checksum".to_string(),
        };

        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key("commit-live"),
            commit_visibility_value(&CommitVisibility {
                commit_id: "commit-live".to_string(),
                location: stale_location.clone(),
                checksum: stale_location.checksum.clone(),
            })
            .unwrap(),
        );
        writes.put(
            BY_COMMIT_INDEX_SPACE,
            by_commit_key("commit-live"),
            by_commit_index_value(&ByCommitEntry {
                commit_id: "commit-live".to_string(),
                location: stale_location.clone(),
                parent_commit_ids: Vec::new(),
                generation: 0,
            })
            .unwrap(),
        );
        writes.put(
            BY_CHANGE_INDEX_SPACE,
            by_change_key("change-live"),
            by_change_index_value(&ByChangeEntry {
                change_id: "change-live".to_string(),
                location: stale_location,
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage);
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-live".to_string())])
            .await
            .expect_err("live locator index drift must fail closed");

        assert!(
            error.message.contains("does not match segment truth"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn collect_garbage_rejects_stale_live_locator_indexes_without_sweeping() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = single_commit_segment("segment-live", "commit-live", "change-live");
        let dead_segment = canonicalize_segment(single_commit_segment(
            "segment-dead",
            "commit-dead",
            "change-dead",
        ))
        .unwrap();
        let stale_location = SegmentObjectLocation {
            segment_id: "missing-segment".to_string(),
            offset: 0,
            len: 0,
            checksum: "stale-checksum".to_string(),
        };

        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;
        write_raw_segment(&storage, &dead_segment).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key("commit-live"),
            commit_visibility_value(&CommitVisibility {
                commit_id: "commit-live".to_string(),
                location: stale_location.clone(),
                checksum: stale_location.checksum.clone(),
            })
            .unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-live".to_string())])
                .await
                .expect_err("live locator drift must abort GC before sweeping")
        };
        assert!(
            error.message.contains("does not match segment truth"),
            "unexpected error: {error}"
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(SEGMENT_SPACE, vec![segment_key("segment-dead")])],
        );
        assert!(result[0][0].is_some());
    }

    #[tokio::test]
    async fn gc_sweeps_dead_segment_with_missing_parent() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = single_commit_segment("segment-live", "commit-live", "change-live");
        let mut dead_segment = single_commit_segment("segment-dead", "commit-dead", "change-dead");
        dead_segment.commits[0]
            .header
            .parent_commit_ids
            .push("missing-parent".to_string());
        dead_segment = canonicalize_segment(dead_segment).unwrap();

        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;
        write_raw_segment(&storage, &dead_segment).await;

        let mut reader = context.reader(storage);
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-live".to_string())])
            .await
            .unwrap();

        assert_eq!(plan.sweep.segments, vec!["segment-dead"]);
    }

    #[tokio::test]
    async fn gc_rejects_duplicate_live_commit_even_when_by_commit_exists() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let segment_1 = single_commit_segment("segment-1", "commit-1", "change-1");
        write_segments(&storage, &context, vec![segment_1], &["commit-1"]).await;

        let segment_2 =
            canonicalize_segment(single_commit_segment("segment-2", "commit-1", "change-2"))
                .unwrap();
        write_raw_segment(&storage, &segment_2).await;

        let mut reader = context.reader(storage);
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-1".to_string())])
            .await
            .expect_err("GC must reject duplicate commit ids even when by_commit exists");

        assert!(
            error.message.contains("duplicate commit id 'commit-1'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_rejects_duplicate_live_change_even_when_by_change_exists() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let segment_1 = single_commit_segment("segment-1", "commit-1", "change-1");
        write_segments(&storage, &context, vec![segment_1], &["commit-1"]).await;

        let segment_2 =
            canonicalize_segment(single_commit_segment("segment-2", "commit-2", "change-1"))
                .unwrap();
        write_raw_segment(&storage, &segment_2).await;

        let mut reader = context.reader(storage);
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-1".to_string())])
            .await
            .expect_err("GC must reject duplicate change ids even when by_change exists");

        assert!(
            error.message.contains("duplicate change id 'change-1'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_sweeps_stale_visible_change_proof_even_when_change_and_commit_are_live() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let change_segment =
            single_commit_segment("segment-change", "commit-change", "change-live");
        let commit_segment = canonicalize_segment(single_commit_segment(
            "segment-commit",
            "commit-live",
            "change-other",
        ))
        .unwrap();
        let stale_visibility = commit_visibility_from_segment(&commit_segment, "commit-live");

        write_segments(
            &storage,
            &context,
            vec![change_segment, commit_segment],
            &["commit-change", "commit-live"],
        )
        .await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            VISIBLE_CHANGE_PROOF_SPACE,
            visible_change_proof_key("change-live"),
            visible_change_proof_value(&stale_visibility.commit_id),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[
                    GcRoot::VersionHead("commit-change".to_string()),
                    GcRoot::PinnedCommit("commit-live".to_string()),
                ])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert!(plan.sweep.visible_change_proof.is_empty());
        assert!(plan
            .repair
            .visible_change_proof
            .iter()
            .any(|(change_id, _)| change_id == "change-live"));
        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(
                VISIBLE_CHANGE_PROOF_SPACE,
                vec![visible_change_proof_key("change-live")],
            )],
        );
        assert!(result[0][0].is_some());
    }

    #[tokio::test]
    async fn gc_repairs_visible_change_proof_that_points_to_wrong_commit() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = canonicalize_segment(single_commit_segment(
            "segment-live",
            "commit-live",
            "change-live",
        ))
        .unwrap();
        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            VISIBLE_CHANGE_PROOF_SPACE,
            visible_change_proof_key("change-live"),
            visible_change_proof_value(&"wrong-commit".to_string()),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-live".to_string())])
            .await
            .unwrap();

        assert!(plan.sweep.visible_change_proof.is_empty());
        assert_eq!(plan.repair.visible_change_proof[0].0, "change-live");
    }

    #[tokio::test]
    async fn gc_rejects_stale_commit_visibility_even_when_visible_change_points_to_commit() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = canonicalize_segment(single_commit_segment(
            "segment-live",
            "commit-live",
            "change-live",
        ))
        .unwrap();
        let mut stale_visibility = commit_visibility_from_segment(&live_segment, "commit-live");
        stale_visibility.checksum = "stale-checksum".to_string();

        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key("commit-live"),
            commit_visibility_value(&stale_visibility).unwrap(),
        );
        writes.put(
            VISIBLE_CHANGE_PROOF_SPACE,
            visible_change_proof_key("change-live"),
            visible_change_proof_value(&"commit-live".to_string()),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-live".to_string())])
            .await
            .expect_err("live visibility drift must fail closed");

        assert!(
            error.message.contains("does not match segment truth"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_repairs_stale_live_visible_change_proof() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_segment = single_commit_segment("segment-live", "commit-live", "change-live");

        write_segments(&storage, &context, vec![live_segment], &["commit-live"]).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            VISIBLE_CHANGE_PROOF_SPACE,
            visible_change_proof_key("change-live"),
            b"missing-commit".to_vec(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-live".to_string())])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert!(plan.sweep.visible_change_proof.is_empty());
        assert_eq!(plan.repair.visible_change_proof[0].0, "change-live");
        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(
                VISIBLE_CHANGE_PROOF_SPACE,
                vec![visible_change_proof_key("change-live")],
            )],
        );
        assert!(result[0][0].is_some());
    }

    #[tokio::test]
    async fn gc_marks_parent_commits_reachable_from_rooted_child() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let parent_segment =
            single_commit_segment("segment-parent", "commit-parent", "change-parent");
        let mut child_segment =
            single_commit_segment("segment-child", "commit-child", "change-child");
        child_segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-parent".to_string());

        write_segments(
            &storage,
            &context,
            vec![parent_segment, child_segment],
            &["commit-parent", "commit-child"],
        )
        .await;

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-child".to_string())])
            .await
            .unwrap();

        assert!(plan.live.commits.contains(&"commit-child".to_string()));
        assert!(plan.live.commits.contains(&"commit-parent".to_string()));
        assert!(plan.live.changes.contains(&"change-child".to_string()));
        assert!(plan.live.changes.contains(&"change-parent".to_string()));
        assert!(plan.live.segments.contains(&"segment-child".to_string()));
        assert!(plan.live.segments.contains(&"segment-parent".to_string()));
        assert!(plan.sweep.segments.is_empty());
        assert!(plan.sweep.by_commit.is_empty());
        assert!(plan.sweep.by_change.is_empty());
    }

    #[tokio::test]
    async fn gc_errors_when_rooted_child_references_missing_parent() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let published_child =
            single_commit_segment("segment-child", "commit-child", "change-child");
        write_segments(&storage, &context, vec![published_child], &["commit-child"]).await;
        let mut child_segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-child", "commit-child", "change-child"),
        )
        .await;
        child_segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-missing-parent".to_string());
        let child_segment = canonicalize_segment(child_segment).unwrap();
        write_raw_segment(&storage, &child_segment).await;
        write_raw_visibility(&storage, &child_segment, "commit-child").await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-child".to_string())])
            .await
            .expect_err("missing parent commit must be corruption");
        assert!(error.message.contains(
            "changelog GC root/ancestor commit 'commit-missing-parent' was not found in changelog segments"
        ));

        let mut reader = context.reader(storage.clone());
        let child_commit = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["commit-child".to_string()],
                projection: crate::changelog::CommitProjection::Full,
                visibility: crate::changelog::CommitVisibilityMode::RequireVisible,
            })
            .await
            .unwrap();
        assert!(child_commit.entries[0].is_some());
    }

    #[tokio::test]
    async fn gc_errors_when_reachable_commit_graph_has_cycle() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let published_root = single_commit_segment("segment-cycle", "commit-root", "change-root");
        write_segments(&storage, &context, vec![published_root], &["commit-root"]).await;

        let mut segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-cycle", "commit-root", "change-root"),
        )
        .await;
        let mut child = single_commit_segment("segment-cycle", "commit-child", "change-child")
            .commits
            .remove(0);
        child.header.parent_commit_ids = vec!["commit-root".to_string()];
        child.body.membership.clear();
        child.directory.state_row_identities.clear();
        child.directory.membership_ordinals.clear();
        child.header.membership_count = 0;
        segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-child".to_string());
        segment.commits.push(child);
        let segment = canonicalize_segment(segment).unwrap();
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-root".to_string())])
            .await
            .expect_err("reachable commit graph cycle must be corruption");
        assert!(
            error.message.contains("parent cycle"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_keeps_adopted_change_and_source_parent_commit() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let author_segment = single_commit_segment("segment-author", "commit-author", "change-1");
        let mut adopting_segment =
            adopting_commit_segment("segment-adopter", "commit-adopter", "change-1");
        adopting_segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-author".to_string());

        write_segments(
            &storage,
            &context,
            vec![author_segment, adopting_segment],
            &["commit-author", "commit-adopter"],
        )
        .await;

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-adopter".to_string())])
            .await
            .unwrap();

        assert_eq!(plan.live.commits, vec!["commit-adopter", "commit-author"]);
        assert_eq!(plan.live.changes, vec!["change-1"]);
        assert!(plan.live.segments.contains(&"segment-author".to_string()));
        assert!(plan.live.segments.contains(&"segment-adopter".to_string()));
        assert_eq!(plan.sweep.commit_visibility, Vec::<String>::new());
        assert!(plan.sweep.segments.is_empty());
    }

    #[tokio::test]
    async fn gc_retained_segment_closure_keeps_external_adopted_change_segment() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let author_segment = canonicalize_segment(single_commit_segment(
            "segment-author",
            "commit-author",
            "change-adopted",
        ))
        .unwrap();
        let mut retained_segment =
            single_commit_segment("segment-retained", "commit-live", "change-live");
        let mut adopted =
            adopting_commit_segment("segment-retained", "commit-adopter", "change-adopted")
                .commits
                .remove(0);
        adopted
            .header
            .parent_commit_ids
            .push("commit-author".to_string());
        retained_segment.commits.push(adopted);
        let retained_segment = canonicalize_segment(retained_segment).unwrap();

        write_raw_segment(&storage, &author_segment).await;
        write_raw_segment(&storage, &retained_segment).await;

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-live".to_string())])
            .await
            .unwrap();

        assert_eq!(plan.live.commits, vec!["commit-live"]);
        assert_eq!(plan.live.changes, vec!["change-live"]);
        assert!(plan.live.segments.contains(&"segment-retained".to_string()));
        assert!(!plan.live.segments.contains(&"segment-author".to_string()));
        assert!(plan.sweep.segments.is_empty());
        assert!(plan
            .repair
            .by_change
            .iter()
            .any(|entry| entry.change_id == "change-adopted"));
        assert!(plan
            .repair
            .by_change_membership
            .contains(&("change-adopted".to_string(), "commit-adopter".to_string())));
    }

    #[tokio::test]
    async fn gc_rejects_cross_segment_adopted_membership_authored_by_same_commit() {
        let storage = test_storage();
        let mut change_segment =
            single_commit_segment("change-segment", "commit-author", "change-1");
        change_segment.commits.clear();
        change_segment.directory.commits.clear();
        change_segment.changes[0].authored_commit_id = Some("commit-adopter".to_string());
        let change_segment = canonicalize_segment(change_segment).unwrap();
        write_raw_segment(&storage, &change_segment).await;

        let mut adopt_segment =
            adopting_commit_segment("adopt-segment", "commit-adopter", "change-1");
        adopt_segment.commits[0]
            .header
            .parent_commit_ids
            .push("missing-parent".to_string());
        let adopt_segment = canonicalize_segment(adopt_segment).unwrap();
        write_raw_segment(&storage, &adopt_segment).await;
        let parent_segment = canonicalize_segment(single_commit_segment(
            "parent-segment",
            "missing-parent",
            "parent-change",
        ))
        .unwrap();
        write_raw_segment(&storage, &parent_segment).await;

        let context = ChangelogContext::new();
        let mut reader = context.reader(storage);
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-adopter".to_string())])
            .await
            .expect_err("GC must reject self-authored adopted membership");

        assert!(
            error.message.contains("is authored by the same commit"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_sweeps_dead_direct_json_payloads_and_retains_live_payloads() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let live_ref = JsonRef::from_hash_bytes([7; 32]);
        let dead_ref = JsonRef::from_hash_bytes([9; 32]);
        let live_segment = single_commit_segment_with_payloads(
            "segment-1",
            "commit-1",
            "change-1",
            Some(live_ref),
            None,
        );

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(live_segment).await.unwrap();
            writer.stage_publish_commit("commit-1").await.unwrap();
        }
        json_store::stage_direct_json_payload_put(&mut writes, &live_ref, b"live".to_vec());
        json_store::stage_direct_json_payload_put(&mut writes, &dead_ref, b"dead".to_vec());
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-1".to_string())])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert_eq!(plan.sweep.json_payloads, vec![dead_ref]);

        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(
                json_store::store::JSON_SPACE,
                vec![
                    live_ref.as_hash_bytes().to_vec(),
                    dead_ref.as_hash_bytes().to_vec(),
                ],
            )],
        );
        assert_eq!(result[0][0].as_deref(), Some(&b"live"[..]));
        assert_eq!(result[0][1], None);
    }

    #[tokio::test]
    async fn gc_errors_when_live_membership_references_missing_change() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let segment = single_commit_segment("segment-1", "commit-1", "change-1");

        write_segments(&storage, &context, vec![segment.clone()], &["commit-1"]).await;
        let mut segment = segment;
        segment.changes.clear();
        segment.directory.changes.clear();
        let segment = canonicalize_segment(segment).unwrap();
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[GcRoot::VersionHead("commit-1".to_string())])
            .await
            .expect_err("missing membership change must be corruption");
        assert!(
            error
                .message
                .contains("references missing change 'change-1'")
                || error
                    .message
                    .contains("references missing authored change 'change-1'"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn collect_garbage_errors_without_sweeping_when_parent_is_missing() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let dead_segment = single_commit_segment("segment-dead", "commit-dead", "change-dead");

        let published_child =
            single_commit_segment("segment-child", "commit-child", "change-child");
        write_segments(
            &storage,
            &context,
            vec![published_child, dead_segment],
            &["commit-child"],
        )
        .await;
        let mut child_segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-child", "commit-child", "change-child"),
        )
        .await;
        child_segment.commits[0]
            .header
            .parent_commit_ids
            .push("commit-missing-parent".to_string());
        let child_segment = canonicalize_segment(child_segment).unwrap();
        write_raw_segment(&storage, &child_segment).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-child".to_string())])
                .await
                .expect_err("missing parent commit must abort collect_garbage")
        };
        assert!(error.message.contains(
            "changelog GC root/ancestor commit 'commit-missing-parent' was not found in changelog segments"
        ));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(SEGMENT_SPACE, vec![segment_key("segment-dead")])],
        );
        assert!(
            result[0][0].is_some(),
            "collect_garbage must stage no sweep deletes after graph corruption"
        );
    }

    #[tokio::test]
    async fn collect_garbage_errors_without_sweeping_when_membership_change_is_missing() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let mut corrupt_segment = single_commit_segment("segment-corrupt", "commit-1", "change-1");
        let dead_segment = single_commit_segment("segment-dead", "commit-dead", "change-dead");

        write_segments(
            &storage,
            &context,
            vec![corrupt_segment.clone(), dead_segment],
            &["commit-1"],
        )
        .await;
        corrupt_segment.changes.clear();
        corrupt_segment.directory.changes.clear();
        let corrupt_segment = canonicalize_segment(corrupt_segment).unwrap();
        write_raw_segment(&storage, &corrupt_segment).await;

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let error = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-1".to_string())])
                .await
                .expect_err("missing membership change must abort collect_garbage")
        };
        assert!(
            error
                .message
                .contains("references missing change 'change-1'")
                || error
                    .message
                    .contains("references missing authored change 'change-1'"),
            "unexpected error: {error}"
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(SEGMENT_SPACE, vec![segment_key("segment-dead")])],
        );
        assert!(
            result[0][0].is_some(),
            "collect_garbage must stage no sweep deletes after graph corruption"
        );
    }

    #[tokio::test]
    async fn gc_errors_when_segment_contains_duplicate_commit_ids() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let mut segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-1", "commit-1", "change-1"),
        )
        .await;
        let mut duplicate = segment.commits[0].clone();
        duplicate.body.membership.clear();
        duplicate.directory.state_row_identities.clear();
        duplicate.directory.membership_ordinals.clear();
        duplicate.header.membership_count = 0;
        segment.commits.push(duplicate);
        segment.header.commit_count = segment.commits.len() as u32;
        let segment = canonicalize_segment(segment).unwrap();
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("duplicate commit ids must be invalid segment input");
        assert!(
            error
                .message
                .contains("contains duplicate commit 'commit-1'")
                || error
                    .message
                    .contains("contains duplicate commit directory entry 'commit-1'")
                || error.message.contains(
                    "commit 'commit-1' locator offset/len does not match encoded byte range"
                ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_errors_when_segment_contains_duplicate_change_ids() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let mut segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-1", "commit-1", "change-1"),
        )
        .await;
        segment.changes.push(segment.changes[0].clone());
        segment.header.change_count = segment.changes.len() as u32;
        let segment = canonicalize_segment(segment).unwrap();
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("duplicate change ids must be invalid segment input");
        assert!(
            error
                .message
                .contains("contains duplicate change 'change-1'")
                || error
                    .message
                    .contains("contains duplicate change directory entry 'change-1'")
                || error.message.contains(
                    "change 'change-1' locator offset/len does not match encoded byte range"
                ),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn gc_errors_when_segment_commit_membership_count_drifts() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let mut segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-1", "commit-1", "change-1"),
        )
        .await;
        segment.commits[0].header.membership_count = 0;
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("membership_count drift must be invalid segment input");
        assert!(error
            .message
            .contains("membership_count 0 does not match 1"));
    }

    #[tokio::test]
    async fn gc_errors_when_segment_commit_directory_membership_drifts() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let mut segment = stored_segment(
            &storage,
            &context,
            single_commit_segment("segment-1", "commit-1", "change-1"),
        )
        .await;
        segment.commits[0].directory.membership_ordinals.clear();
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("membership directory drift must be invalid segment input");
        assert!(error
            .message
            .contains("is missing membership ordinal for change 'change-1'"));
    }

    #[tokio::test]
    async fn gc_errors_when_segment_change_payload_directory_drifts() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let payload_ref = JsonRef::for_content(b"payload");
        let mut segment = single_commit_segment("segment-1", "commit-1", "change-1");
        segment.changes[0]
            .inline_payloads
            .push(SegmentInlinePayload {
                json_ref: payload_ref,
                bytes: b"payload".to_vec(),
            });
        let mut segment = stored_segment(&storage, &context, segment).await;
        segment.changes[0].directory.payloads = vec![SegmentPayloadLocation {
            json_ref: payload_ref,
            offset: 0,
            len: 999,
        }];
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("payload directory drift must be invalid segment input");
        assert!(error
            .message
            .contains("payload directory entry does not match inline payload"));
    }

    #[tokio::test]
    async fn gc_retains_mixed_live_dead_segment_whole_and_its_indexes() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let segment = mixed_segment();

        write_segments(&storage, &context, vec![segment], &["commit-live"]).await;

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-live".to_string())])
            .await
            .unwrap();

        assert_eq!(plan.live.segments, vec!["segment-mixed"]);
        assert!(plan.sweep.segments.is_empty());
        assert!(plan.sweep.by_commit.is_empty());
        assert!(plan.sweep.by_change.is_empty());
        assert!(plan.sweep.by_change_membership.is_empty());
    }

    #[tokio::test]
    async fn gc_retains_payloads_for_dead_changes_in_retained_segments() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let dead_ref = JsonRef::from_hash_bytes([9; 32]);
        let mut segment = single_commit_segment("segment-mixed", "commit-live", "change-live");
        let dead = single_commit_segment_with_payloads(
            "segment-mixed",
            "commit-dead",
            "change-dead",
            Some(dead_ref),
            None,
        );
        segment.commits.push(dead.commits[0].clone());
        segment.changes.push(dead.changes[0].clone());

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.stage_segment(segment).await.unwrap();
            writer.stage_publish_commit("commit-live").await.unwrap();
        }
        json_store::stage_direct_json_payload_put(&mut writes, &dead_ref, b"dead".to_vec());
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let plan = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer
                .collect_garbage(&[GcRoot::VersionHead("commit-live".to_string())])
                .await
                .unwrap()
        };
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        assert!(plan.sweep.segments.is_empty());
        assert!(plan.sweep.json_payloads.is_empty());
        let result = crate::changelog::test_support::read_test_value_groups(
            &storage,
            vec![(
                json_store::store::JSON_SPACE,
                vec![dead_ref.as_hash_bytes().to_vec()],
            )],
        );
        assert_eq!(result[0][0].as_deref(), Some(&b"dead"[..]));
    }

    #[tokio::test]
    async fn gc_plan_is_equivalent_after_rebuilding_mandatory_indexes() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let segment = single_commit_segment("segment-1", "commit-1", "change-1");

        write_segments(&storage, &context, vec![segment.clone()], &["commit-1"]).await;

        let mut reader = context.reader(storage.clone());
        let before = reader
            .plan_gc(&[GcRoot::VersionHead("commit-1".to_string())])
            .await
            .unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.delete(BY_COMMIT_INDEX_SPACE, by_commit_key("commit-1"));
        writes.delete(BY_CHANGE_INDEX_SPACE, by_change_key("change-1"));
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_SPACE,
            by_change_membership_key("change-1", "commit-1"),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        let stats = {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            writer.rebuild_mandatory_indexes().await.unwrap()
        };
        assert_eq!(
            stats,
            RebuildIndexStats {
                expected: 4,
                put: 3,
                deleted: 0,
                unchanged: 1
            }
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let after = reader
            .plan_gc(&[GcRoot::VersionHead("commit-1".to_string())])
            .await
            .unwrap();
        assert_eq!(after.live, before.live);
        assert_eq!(after.sweep, before.sweep);

        let mut reader = context.reader(storage);
        let visible_change = reader
            .load_changes(crate::changelog::ChangeLoadRequest {
                change_ids: &["change-1".to_string()],
                projection: crate::changelog::ChangeProjection::Segment,
                visibility:
                    crate::changelog::ChangeVisibilityMode::RequireReachableFromVisibleCommit,
            })
            .await
            .unwrap();
        assert_eq!(
            visible_change.entries,
            vec![Some(crate::changelog::ChangeLoadEntry::Segment(
                segment.changes[0].clone()
            ))]
        );
    }

    fn test_storage() -> StorageContext {
        StorageContext::new(InMemoryStorageBackend::new())
    }

    async fn write_segments(
        storage: &StorageContext,
        context: &ChangelogContext,
        segments: Vec<Segment>,
        published_commits: &[&str],
    ) {
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        {
            let mut writer = context.writer(&mut *transaction, &mut writes);
            for segment in segments {
                writer.stage_segment(segment).await.unwrap();
            }
            for commit_id in published_commits {
                writer.stage_publish_commit(commit_id).await.unwrap();
            }
        }
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();
    }

    async fn stored_segment(
        storage: &StorageContext,
        context: &ChangelogContext,
        segment: Segment,
    ) -> Segment {
        let segment_id = segment.header.segment_id.clone();
        let existing = crate::changelog::test_support::read_test_value_groups(
            storage,
            vec![(SEGMENT_SPACE, vec![segment_key(&segment_id)])],
        )[0][0]
            .clone();
        if existing.is_none() {
            write_segments(storage, context, vec![segment], &[]).await;
        }
        let result = crate::changelog::test_support::read_test_value_groups(
            storage,
            vec![(SEGMENT_SPACE, vec![segment_key(&segment_id)])],
        );
        let bytes = result[0][0].as_deref().expect("stored segment bytes");
        let segment = decode_segment(bytes).unwrap();
        segment
    }

    async fn write_raw_segment(storage: &StorageContext, segment: &Segment) {
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            SEGMENT_SPACE,
            segment_key(&segment.header.segment_id),
            encode_segment(segment).unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();
    }

    async fn write_raw_visibility(storage: &StorageContext, segment: &Segment, commit_id: &str) {
        let location = segment
            .directory
            .commits
            .iter()
            .find_map(|(candidate, location)| {
                if candidate == commit_id {
                    Some(location.clone())
                } else {
                    None
                }
            })
            .expect("commit location");
        let visibility = CommitVisibility {
            commit_id: commit_id.to_string(),
            checksum: location.checksum.clone(),
            location,
        };
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            COMMIT_VISIBILITY_SPACE,
            commit_visibility_key(commit_id),
            commit_visibility_value(&visibility).unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();
    }

    async fn assert_missing(storage: &StorageContext, keys: Vec<(StorageSpace, Vec<u8>)>) {
        let result = crate::changelog::test_support::read_test_value_groups(
            storage,
            keys.into_iter()
                .map(|(space, key)| (space, vec![key]))
                .collect(),
        );
        for group in result {
            assert_eq!(group[0], None);
        }
    }

    fn single_commit_segment(segment_id: &str, commit_id: &str, change_id: &str) -> Segment {
        single_commit_segment_with_payloads(segment_id, commit_id, change_id, None, None)
    }

    fn single_commit_segment_with_payloads(
        segment_id: &str,
        commit_id: &str,
        change_id: &str,
        snapshot_ref: Option<JsonRef>,
        metadata_ref: Option<JsonRef>,
    ) -> Segment {
        Segment {
            header: SegmentHeader {
                segment_id: segment_id.to_string(),
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
                    parent_commit_ids: Vec::new(),
                    derivable_change_id: format!("{commit_id}-derivable"),
                    author_account_ids: vec!["account-1".to_string()],
                    created_at: "2026-05-12T00:00:00Z".to_string(),
                    membership_count: 0,
                },
                body: CommitBody {
                    membership: vec![MembershipRecord {
                        member_change_id: change_id.to_string(),
                        role: MembershipRole::Authored,
                        source_parent_ordinal: None,
                    }],
                },
                directory: SegmentCommitDirectory {
                    state_row_identities: vec![(
                        state_row_identity(change_id),
                        change_id.to_string(),
                    )],
                    membership_ordinals: vec![(change_id.to_string(), 0)],
                },
                checksum: String::new(),
            }],
            changes: vec![SegmentChange {
                id: change_id.to_string(),
                authored_commit_id: Some(commit_id.to_string()),
                entity_id: EntityIdentity::single(change_id),
                schema_key: "message".to_string(),
                file_id: Some("file-1".to_string()),
                snapshot_ref,
                metadata_ref,
                created_at: "2026-05-12T00:00:00Z".to_string(),
                inline_payloads: Vec::new(),
                directory: SegmentChangeDirectory::default(),
            }],
        }
    }

    fn adopting_commit_segment(segment_id: &str, commit_id: &str, change_id: &str) -> Segment {
        let mut segment = single_commit_segment(segment_id, commit_id, change_id);
        segment.commits[0].body.membership[0].role = MembershipRole::Adopted;
        segment.commits[0].body.membership[0].source_parent_ordinal = Some(0);
        segment.changes.clear();
        segment
    }

    fn mixed_segment() -> Segment {
        let mut segment = single_commit_segment("segment-mixed", "commit-live", "change-live");
        let dead = single_commit_segment("segment-mixed", "commit-dead", "change-dead");
        segment.commits.push(dead.commits[0].clone());
        segment.changes.push(dead.changes[0].clone());
        segment
    }

    fn state_row_identity(entity_id: &str) -> crate::changelog::StateRowIdentity {
        crate::changelog::StateRowIdentity {
            schema_key: CanonicalSchemaKey::new("message").unwrap(),
            file_id: FileId::new("file-1").unwrap(),
            entity_id: EntityId::new(
                EntityIdentity::single(entity_id)
                    .as_json_array_text()
                    .unwrap(),
            )
            .unwrap(),
        }
    }
}
