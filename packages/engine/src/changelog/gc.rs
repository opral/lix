use std::collections::{HashMap, HashSet};

use super::segment::validate_segment_shape;
use super::store::{
    by_change_index_value, by_change_key, by_change_membership_ids_from_key,
    by_change_membership_index_value, by_change_membership_key, by_commit_index_value,
    by_commit_key, commit_visibility_key, commit_visibility_value, segment_key,
    BY_CHANGE_INDEX_NAMESPACE, BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE, BY_COMMIT_INDEX_NAMESPACE,
    COMMIT_VISIBILITY_NAMESPACE, SEGMENT_NAMESPACE,
};
use super::types::{
    ByChangeEntry, ByCommitEntry, CommitVisibility, GcLiveSet, GcPlan, GcRoot, GcSweepSet, Segment,
    SegmentChange, SegmentCommit, SegmentObjectLocation,
};
use crate::changelog::decode_segment;
use crate::json_store::{store::JSON_NAMESPACE, JsonRef};
use crate::storage::{KvScanRange, KvScanRequest, StorageReader, StorageWriteSet};
use crate::LixError;

pub(super) async fn plan_gc<S>(store: &mut S, roots: &[GcRoot]) -> Result<GcPlan, LixError>
where
    S: StorageReader + ?Sized,
{
    let segments = scan_all_segments(store).await?;
    let all_commit_visibility = scan_utf8_keys(store, COMMIT_VISIBILITY_NAMESPACE).await?;
    let all_by_commit = scan_utf8_keys(store, BY_COMMIT_INDEX_NAMESPACE).await?;
    let all_by_change = scan_utf8_keys(store, BY_CHANGE_INDEX_NAMESPACE).await?;
    let all_by_change_membership = scan_by_change_membership_keys(store).await?;
    let all_json_payloads = scan_json_payload_keys(store).await?;

    let mut commit_index: HashMap<String, (String, SegmentCommit)> = HashMap::new();
    let mut change_index: HashMap<String, (String, SegmentChange)> = HashMap::new();
    let mut all_segment_ids = Vec::new();

    for segment in &segments {
        push_unique(&mut all_segment_ids, segment.header.segment_id.clone());
        for commit in &segment.commits {
            if commit_index
                .insert(
                    commit.header.id.clone(),
                    (segment.header.segment_id.clone(), commit.clone()),
                )
                .is_some()
            {
                return Err(LixError::unknown(format!(
                    "changelog GC found duplicate commit id '{}'",
                    commit.header.id
                )));
            }
        }
        for change in &segment.changes {
            if change_index
                .insert(
                    change.id.clone(),
                    (segment.header.segment_id.clone(), change.clone()),
                )
                .is_some()
            {
                return Err(LixError::unknown(format!(
                    "changelog GC found duplicate change id '{}'",
                    change.id
                )));
            }
        }
    }

    let mut live = GcLiveSet::default();
    let mut pending_commits = Vec::new();
    for root in roots {
        pending_commits.push(gc_root_commit_id(root).to_string());
    }
    let mut visiting_commits = HashSet::new();
    let mut checked_commits = HashSet::new();
    for commit_id in &pending_commits {
        validate_reachable_commit_graph_acyclic(
            commit_id,
            &commit_index,
            &mut visiting_commits,
            &mut checked_commits,
        )?;
    }

    while let Some(commit_id) = pending_commits.pop() {
        if live.commits.contains(&commit_id) {
            continue;
        }
        let Some((segment_id, commit)) = commit_index.get(&commit_id).cloned() else {
            return Err(LixError::unknown(format!(
                "changelog GC root/ancestor commit '{commit_id}' was not found in changelog segments"
            )));
        };

        push_unique(&mut live.commits, commit_id.clone());
        push_unique(&mut live.segments, segment_id);

        for parent_id in &commit.header.parent_commit_ids {
            if !live.commits.contains(parent_id) {
                pending_commits.push(parent_id.clone());
            }
        }

        for membership in &commit.body.membership {
            let change_id = &membership.member_change_id;
            let Some((change_segment_id, change)) = change_index.get(change_id).cloned() else {
                return Err(LixError::unknown(format!(
                    "changelog GC live commit '{}' references missing change '{}'",
                    commit.header.id, change_id
                )));
            };
            push_unique(&mut live.changes, change_id.clone());
            push_unique(&mut live.segments, change_segment_id);
            mark_change_payloads(&mut live.payloads, &change);
        }
    }

    let live_commits: HashSet<_> = live.commits.iter().cloned().collect();
    let live_changes: HashSet<_> = live.changes.iter().cloned().collect();

    let sweep = GcSweepSet {
        segments: all_segment_ids
            .into_iter()
            .filter(|segment_id| !live.segments.contains(segment_id))
            .collect(),
        commit_visibility: all_commit_visibility
            .into_iter()
            .filter(|commit_id| !live_commits.contains(commit_id))
            .collect(),
        by_commit: all_by_commit
            .into_iter()
            .filter(|commit_id| !live_commits.contains(commit_id))
            .collect(),
        by_change: all_by_change
            .into_iter()
            .filter(|change_id| !live_changes.contains(change_id))
            .collect(),
        by_change_membership: all_by_change_membership
            .into_iter()
            .filter(|(change_id, commit_id)| {
                !live_changes.contains(change_id) || !live_commits.contains(commit_id)
            })
            .collect(),
        json_payloads: all_json_payloads
            .into_iter()
            .filter(|json_ref| !live.payloads.contains(json_ref))
            .collect(),
    };

    Ok(GcPlan {
        roots: roots.to_vec(),
        live,
        sweep,
    })
}

fn validate_reachable_commit_graph_acyclic(
    commit_id: &str,
    commit_index: &HashMap<String, (String, SegmentCommit)>,
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
    S: StorageReader + ?Sized,
{
    let plan = plan_gc(store, roots).await?;
    stage_gc_sweep(writes, &plan)?;
    Ok(plan)
}

pub(super) fn stage_gc_sweep(writes: &mut StorageWriteSet, plan: &GcPlan) -> Result<(), LixError> {
    for segment_id in &plan.sweep.segments {
        writes.delete(SEGMENT_NAMESPACE, segment_key(segment_id));
    }
    for commit_id in &plan.sweep.commit_visibility {
        writes.delete(
            COMMIT_VISIBILITY_NAMESPACE,
            commit_visibility_key(commit_id),
        );
    }
    for commit_id in &plan.sweep.by_commit {
        writes.delete(BY_COMMIT_INDEX_NAMESPACE, by_commit_key(commit_id));
    }
    for change_id in &plan.sweep.by_change {
        writes.delete(BY_CHANGE_INDEX_NAMESPACE, by_change_key(change_id));
    }
    for (change_id, commit_id) in &plan.sweep.by_change_membership {
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
            by_change_membership_key(change_id, commit_id),
        );
    }
    for json_ref in &plan.sweep.json_payloads {
        writes.delete(JSON_NAMESPACE, json_ref.as_hash_bytes().to_vec());
    }
    Ok(())
}

async fn scan_all_segments<S>(store: &mut S) -> Result<Vec<Segment>, LixError>
where
    S: StorageReader + ?Sized,
{
    let mut after = None;
    let mut segments = Vec::new();
    loop {
        let page = store
            .scan_entries(KvScanRequest {
                namespace: SEGMENT_NAMESPACE.to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after,
                limit: 64,
            })
            .await?;
        for index in 0..page.len() {
            let Some(bytes) = page.value(index) else {
                continue;
            };
            let segment = decode_segment(bytes)?;
            validate_segment_shape(&segment)?;
            segments.push(segment);
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(segments)
}

async fn scan_utf8_keys<S>(store: &mut S, namespace: &str) -> Result<Vec<String>, LixError>
where
    S: StorageReader + ?Sized,
{
    let mut after = None;
    let mut out = Vec::new();
    loop {
        let page = store
            .scan_keys(KvScanRequest {
                namespace: namespace.to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after,
                limit: 256,
            })
            .await?;
        for index in 0..page.keys.len() {
            let Some(key) = page.keys.get(index) else {
                continue;
            };
            out.push(
                std::str::from_utf8(key)
                    .map_err(|error| {
                        LixError::unknown(format!(
                            "changelog GC found invalid UTF-8 key in namespace '{namespace}': {error}"
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

async fn scan_by_change_membership_keys<S>(store: &mut S) -> Result<Vec<(String, String)>, LixError>
where
    S: StorageReader + ?Sized,
{
    let mut after = None;
    let mut out = Vec::new();
    loop {
        let page = store
            .scan_keys(KvScanRequest {
                namespace: BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE.to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after,
                limit: 256,
            })
            .await?;
        for index in 0..page.keys.len() {
            let Some(key) = page.keys.get(index) else {
                continue;
            };
            out.push(by_change_membership_ids_from_key(key)?);
        }
        let Some(next_after) = page.resume_after else {
            break;
        };
        after = Some(next_after);
    }
    Ok(out)
}

async fn scan_json_payload_keys<S>(store: &mut S) -> Result<Vec<JsonRef>, LixError>
where
    S: StorageReader + ?Sized,
{
    let mut after = None;
    let mut out = Vec::new();
    loop {
        let page = store
            .scan_keys(KvScanRequest {
                namespace: JSON_NAMESPACE.to_string(),
                range: KvScanRange::prefix(Vec::new()),
                after,
                limit: 256,
            })
            .await?;
        for index in 0..page.keys.len() {
            let Some(key) = page.keys.get(index) else {
                continue;
            };
            let hash: [u8; 32] = key.try_into().map_err(|_| {
                LixError::unknown(format!(
                    "changelog GC found json_store.json key with {} bytes, expected 32",
                    key.len()
                ))
            })?;
            out.push(JsonRef::from_hash_bytes(hash));
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
    use std::sync::Arc;

    use super::*;
    use crate::backend::{testing::UnitTestBackend, Backend};
    use crate::changelog::segment::canonicalize_segment;
    use crate::changelog::{
        encode_segment, ChangelogContext, CommitBody, CommitHeader, MembershipRecord,
        MembershipRole, RebuildIndexStats, Segment, SegmentChange, SegmentChangeDirectory,
        SegmentCommit, SegmentCommitDirectory, SegmentDirectory, SegmentHeader,
        SegmentInlinePayload, SegmentPayloadLocation,
    };
    use crate::common::{CanonicalSchemaKey, EntityId, FileId};
    use crate::entity_identity::EntityIdentity;
    use crate::json_store::JsonRef;
    use crate::storage::{
        KvGetGroup, KvGetRequest, StorageContext, StorageReader, StorageWriteSet,
    };

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
                (SEGMENT_NAMESPACE, segment_key("segment-1")),
                (BY_COMMIT_INDEX_NAMESPACE, by_commit_key("commit-1")),
                (BY_CHANGE_INDEX_NAMESPACE, by_change_key("change-1")),
                (
                    BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
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
            BY_COMMIT_INDEX_NAMESPACE,
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
            BY_CHANGE_INDEX_NAMESPACE,
            by_change_key("stale-change"),
            by_change_index_value(&ByChangeEntry {
                change_id: "stale-change".to_string(),
                location: stale_location,
            })
            .unwrap(),
        );
        writes.put(
            BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
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
                (BY_COMMIT_INDEX_NAMESPACE, by_commit_key("stale-commit")),
                (BY_CHANGE_INDEX_NAMESPACE, by_change_key("stale-change")),
                (
                    BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
                    by_change_membership_key("stale-change", "stale-commit"),
                ),
            ],
        )
        .await;
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
            COMMIT_VISIBILITY_NAMESPACE,
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

        let mut transaction = storage.begin_read_transaction().await.unwrap();
        let result = transaction
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: COMMIT_VISIBILITY_NAMESPACE.to_string(),
                    keys: vec![
                        commit_visibility_key("commit-live"),
                        commit_visibility_key("stale-commit"),
                    ],
                }],
            })
            .await
            .unwrap();
        assert!(result.groups[0].value(0).unwrap().is_some());
        assert_eq!(result.groups[0].value(1), Some(None));
        transaction.rollback().await.unwrap();
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
    async fn gc_keeps_adopted_change_without_marking_authoring_commit() {
        let storage = test_storage();
        let context = ChangelogContext::new();
        let author_segment = single_commit_segment("segment-author", "commit-author", "change-1");
        let adopting_segment =
            adopting_commit_segment("segment-adopter", "commit-adopter", "change-1");

        write_segments(
            &storage,
            &context,
            vec![author_segment, adopting_segment],
            &["commit-adopter"],
        )
        .await;

        let mut reader = context.reader(storage.clone());
        let plan = reader
            .plan_gc(&[GcRoot::VersionHead("commit-adopter".to_string())])
            .await
            .unwrap();

        assert_eq!(plan.live.commits, vec!["commit-adopter"]);
        assert_eq!(plan.live.changes, vec!["change-1"]);
        assert!(plan.live.segments.contains(&"segment-author".to_string()));
        assert!(plan.live.segments.contains(&"segment-adopter".to_string()));
        assert_eq!(plan.sweep.by_commit, vec!["commit-author"]);
        assert_eq!(plan.sweep.commit_visibility, Vec::<String>::new());
        assert!(plan.sweep.segments.is_empty());
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
        writes.put(
            JSON_NAMESPACE,
            live_ref.as_hash_bytes().to_vec(),
            b"live".to_vec(),
        );
        writes.put(
            JSON_NAMESPACE,
            dead_ref.as_hash_bytes().to_vec(),
            b"dead".to_vec(),
        );
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

        let mut transaction = storage.begin_read_transaction().await.unwrap();
        let result = transaction
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: JSON_NAMESPACE.to_string(),
                    keys: vec![
                        live_ref.as_hash_bytes().to_vec(),
                        dead_ref.as_hash_bytes().to_vec(),
                    ],
                }],
            })
            .await
            .unwrap();
        assert_eq!(result.groups[0].value(0), Some(Some(&b"live"[..])));
        assert_eq!(result.groups[0].value(1), Some(None));
        transaction.rollback().await.unwrap();
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
        assert!(error
            .message
            .contains("references missing change 'change-1'"));
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

        let mut reader = context.reader(storage.clone());
        let dead_commit = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["commit-dead".to_string()],
                projection: crate::changelog::CommitProjection::Full,
                visibility: crate::changelog::CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert!(
            dead_commit.entries[0].is_some(),
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
        assert!(error
            .message
            .contains("references missing change 'change-1'"));
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();

        let mut reader = context.reader(storage.clone());
        let dead_commit = reader
            .load_commits(crate::changelog::CommitLoadRequest {
                commit_ids: &["commit-dead".to_string()],
                projection: crate::changelog::CommitProjection::Full,
                visibility: crate::changelog::CommitVisibilityMode::PhysicalOnly,
            })
            .await
            .unwrap();
        assert!(
            dead_commit.entries[0].is_some(),
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
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("duplicate commit ids must be invalid segment input");
        assert!(error
            .message
            .contains("contains duplicate commit 'commit-1'"));
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
        write_raw_segment(&storage, &segment).await;

        let mut reader = context.reader(storage.clone());
        let error = reader
            .plan_gc(&[])
            .await
            .expect_err("duplicate change ids must be invalid segment input");
        assert!(error
            .message
            .contains("contains duplicate change 'change-1'"));
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
        let payload_ref = JsonRef::from_hash_bytes([11; 32]);
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
    async fn gc_retains_mixed_live_dead_segment_whole_but_sweeps_dead_indexes() {
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
        assert_eq!(plan.sweep.by_commit, vec!["commit-dead"]);
        assert_eq!(plan.sweep.by_change, vec!["change-dead"]);
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
        writes.delete(BY_COMMIT_INDEX_NAMESPACE, by_commit_key("commit-1"));
        writes.delete(BY_CHANGE_INDEX_NAMESPACE, by_change_key("change-1"));
        writes.delete(
            BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
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
                expected: 3,
                put: 3,
                deleted: 0,
                unchanged: 0
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
        let backend: Arc<dyn Backend + Send + Sync> = Arc::new(UnitTestBackend::new());
        StorageContext::new(backend)
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
        let mut transaction = storage.begin_read_transaction().await.unwrap();
        let existing = transaction
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: SEGMENT_NAMESPACE.to_string(),
                    keys: vec![segment_key(&segment_id)],
                }],
            })
            .await
            .unwrap();
        let existing = existing.groups[0]
            .value(0)
            .unwrap()
            .map(|bytes| bytes.to_vec());
        transaction.rollback().await.unwrap();
        if existing.is_none() {
            write_segments(storage, context, vec![segment], &[]).await;
        }
        let mut transaction = storage.begin_read_transaction().await.unwrap();
        let result = transaction
            .get_values(KvGetRequest {
                groups: vec![KvGetGroup {
                    namespace: SEGMENT_NAMESPACE.to_string(),
                    keys: vec![segment_key(&segment_id)],
                }],
            })
            .await
            .unwrap();
        let bytes = result.groups[0]
            .value(0)
            .unwrap()
            .expect("stored segment bytes");
        let segment = decode_segment(bytes).unwrap();
        transaction.rollback().await.unwrap();
        segment
    }

    async fn write_raw_segment(storage: &StorageContext, segment: &Segment) {
        let mut transaction = storage.begin_write_transaction().await.unwrap();
        let mut writes = StorageWriteSet::new();
        writes.put(
            SEGMENT_NAMESPACE,
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
            COMMIT_VISIBILITY_NAMESPACE,
            commit_visibility_key(commit_id),
            commit_visibility_value(&visibility).unwrap(),
        );
        writes.apply(&mut *transaction).await.unwrap();
        transaction.commit().await.unwrap();
    }

    async fn assert_missing(storage: &StorageContext, keys: Vec<(&'static str, Vec<u8>)>) {
        let mut transaction = storage.begin_read_transaction().await.unwrap();
        let result = transaction
            .get_values(KvGetRequest {
                groups: keys
                    .into_iter()
                    .map(|(namespace, key)| KvGetGroup {
                        namespace: namespace.to_string(),
                        keys: vec![key],
                    })
                    .collect(),
            })
            .await
            .unwrap();
        for group in result.groups {
            assert_eq!(group.value(0), Some(None));
        }
        transaction.rollback().await.unwrap();
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
            entity_id: EntityId::new(entity_id).unwrap(),
        }
    }
}
