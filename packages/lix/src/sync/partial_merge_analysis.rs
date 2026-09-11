//! Read-only changed-identity reconciliation. No branch/control publication.
use super::partial_state::PartialReplicaState;
use crate::changelog::{
    ChangelogContext, ChangelogReader, CommitId, CommitLoadRequest, CommitRecord,
};
use crate::storage_adapter::{StorageAdapterRead, StorageKey, StoragePrecondition};
use crate::tracked_state::{
    NativeMetadataRef, TrackedStateContext, TrackedStateDiffRequest, TrackedStateFilter,
    TrackedStateKey, TrackedStateMergePlan,
};
use crate::{LixError, NullableKeyFilter};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Copy, Debug)]
pub(crate) struct PartialMergeBudget {
    pub max_local_commits: usize,
    pub max_local_members: usize,
    pub max_local_payload_bytes: usize,
    pub max_remote_graph_records: usize,
}
pub(super) struct PartialMergeAnalysis {
    pub base: CommitId,
    pub remote: CommitId,
    pub local: CommitId,
    /// Original canonical local commits in parent-first order. These must be
    /// body-acknowledged before an authority can accept the merge's second parent.
    pub local_commits: Vec<CommitRecord>,
    pub groups: Vec<TrackedStateMergePlan>,
    pub already_in_authority: bool,
    pub preconditions: Vec<StoragePrecondition>,
}
fn blocked(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_SCOPE_UNSUPPORTED", message)
}
fn limited(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_BUDGET_EXCEEDED", message)
}
fn id(text: &str) -> Result<CommitId, LixError> {
    CommitId::parse_lix(text, "partial merge coordinate")
}
pub(super) async fn record(
    read: &(impl StorageAdapterRead + ?Sized),
    key: CommitId,
    local: bool,
) -> Result<CommitRecord, LixError> {
    let result = ChangelogContext::new()
        .reader(read)
        .load_commits(CommitLoadRequest { commit_ids: &[key] })
        .await?
        .into_iter()
        .next()
        .and_then(|(_, value)| value);
    result.ok_or_else(|| {
        if local {
            blocked("locally authored graph record is absent")
        } else {
            NativeMetadataRef::CommitGraphRecord(key.to_string())
                .annotate_missing(blocked("remote graph record must be hydrated"))
        }
    })
}
/// Bounded causal DAG proof. Every parent is considered, including the second
/// parent of an earlier authority merge. A budget/missing input is an error,
/// never evidence that the ancestor is absent. No inventory scan is used.
pub(super) async fn bounded_ancestor(
    read: &(impl StorageAdapterRead + ?Sized),
    ancestor: &CommitRecord,
    descendant: CommitId,
    cache: &mut BTreeMap<CommitId, CommitRecord>,
    limit: usize,
) -> Result<bool, LixError> {
    let mut pending = vec![(descendant, None)];
    let mut visited = BTreeSet::new();
    while let Some((current, child_generation)) = pending.pop() {
        let node = if current == ancestor.commit_id {
            ancestor.clone()
        } else if let Some(node) = cache.get(&current) {
            node.clone()
        } else {
            if cache.len() >= limit {
                return Err(limited("remote causal ancestry budget exceeded"));
            }
            let node = record(read, current, false).await?;
            cache.insert(current, node.clone());
            node
        };
        if child_generation.is_some_and(|generation| node.generation >= generation) {
            return Err(blocked(
                "causal parent generation does not precede its child",
            ));
        }
        if current == ancestor.commit_id {
            return Ok(true);
        }
        if !visited.insert(current) || node.generation <= ancestor.generation {
            continue;
        }
        if node.parent_commit_ids.len() > limit
            || pending.len().saturating_add(node.parent_commit_ids.len()) > limit
        {
            return Err(limited("remote causal frontier budget exceeded"));
        }
        for parent in node.parent_commit_ids {
            pending.push((parent, Some(node.generation)));
        }
    }
    Ok(false)
}

pub(super) async fn prepare_partial_merge_analysis(
    read: &(impl StorageAdapterRead + ?Sized),
    old: &PartialReplicaState,
    candidate: &PartialReplicaState,
    budget: PartialMergeBudget,
) -> Result<PartialMergeAnalysis, LixError> {
    if budget.max_local_commits == 0
        || budget.max_local_commits > 1024
        || budget.max_local_members == 0
        || budget.max_local_members > 65536
        || budget.max_remote_graph_records == 0
        || budget.max_remote_graph_records > 1024
        || budget.max_local_payload_bytes == 0
    {
        return Err(limited("invalid partial merge work budget"));
    }
    if old.repository_id() != candidate.repository_id()
        || old.epoch_id() != candidate.epoch_id()
        || old.remote_id() != candidate.remote_id()
        || old.active_account_id() != candidate.active_account_id()
        || old.descriptor().selected_branch.branch_id
            != candidate.descriptor().selected_branch.branch_id
        || candidate.descriptor().cursor < old.descriptor().cursor
    {
        return Err(blocked("candidate belongs to another admission"));
    }
    let branch = &old.descriptor().selected_branch.branch_id;
    if branch == crate::GLOBAL_BRANCH_ID {
        return Err(blocked(
            "first merge slice requires a separate selected branch",
        ));
    }
    let (push, push_raw, receipt_guard) =
        super::partial_push_state::load_partial_push_state(read, old, branch).await?;
    let (global, global_raw, _) =
        super::partial_push_state::load_partial_push_state(read, old, crate::GLOBAL_BRANCH_ID)
            .await?;
    if push.prepared.is_some() || global.prepared.is_some() {
        return Err(blocked(
            "recover the exact prepared upload attempt before merge analysis",
        ));
    }
    let ids = [branch.clone(), crate::GLOBAL_BRANCH_ID.to_owned()];
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(read)
        .load_observed(&ids)
        .await?;
    let local_control = observed[0]
        .control
        .as_ref()
        .ok_or_else(|| blocked("selected control disappeared"))?;
    let global_control = observed[1]
        .control
        .as_ref()
        .ok_or_else(|| blocked("global control disappeared"))?;
    if candidate.descriptor().global_branch.head.commit_id != global.confirmed.head
        || candidate.descriptor().global_branch.checkpoint.commit_id != global.confirmed.checkpoint
        || global_control.head_commit_id != global.confirmed.head
        || global_control
            .working_diff_checkpoint_commit_id
            .map(|key| key.to_string())
            .as_ref()
            != Some(&global.confirmed.checkpoint)
        || candidate.descriptor().selected_branch.checkpoint.commit_id != push.confirmed.checkpoint
        || local_control
            .working_diff_checkpoint_commit_id
            .map(|key| key.to_string())
            .as_ref()
            != Some(&push.confirmed.checkpoint)
    {
        return Err(blocked(
            "catalog/global/checkpoint reconciliation must precede selected ordinary merge",
        ));
    }
    let base = id(&push.confirmed.head)?;
    let remote = id(&candidate.descriptor().selected_branch.head.commit_id)?;
    let local = local_control.head_commit_id;
    if local == base || remote == base {
        return Err(blocked("merge analysis requires two changed heads"));
    }
    let native = analyze_native_kv_divergence(
        read,
        base,
        remote,
        local,
        old.active_account_id(),
        id(&global.confirmed.head)?,
        budget,
    )
    .await?;
    let mut guards = vec![receipt_guard];
    for (branch, raw) in [(ids[0].as_str(), push_raw), (ids[1].as_str(), global_raw)] {
        let uuid =
            uuid::Uuid::parse_str(branch).map_err(|_| blocked("malformed push branch UUID"))?;
        guards.push(StoragePrecondition::KeyValueEquals {
            space: super::partial_push_state::PARTIAL_BRANCH_PUSH_SPACE,
            key: StorageKey(bytes::Bytes::copy_from_slice(uuid.as_bytes())),
            expected: raw,
        });
    }
    for (branch, observation) in ids.iter().zip(observed) {
        guards.push(crate::branch::branch_head_control_precondition(
            branch,
            observation.raw_token,
        )?);
    }
    Ok(PartialMergeAnalysis {
        base,
        remote,
        local,
        local_commits: native.local_commits,
        groups: native.groups,
        already_in_authority: native.already_in_authority,
        preconditions: guards,
    })
}

pub(super) struct NativeKvMergeAnalysis {
    pub local_commits: Vec<CommitRecord>,
    pub groups: Vec<TrackedStateMergePlan>,
    pub already_in_authority: bool,
}
pub(super) async fn analyze_native_kv_divergence(
    read: &(impl StorageAdapterRead + ?Sized),
    base: CommitId,
    remote: CommitId,
    local: CommitId,
    account: &str,
    global_head: CommitId,
    budget: PartialMergeBudget,
) -> Result<NativeKvMergeAnalysis, LixError> {
    if budget.max_local_commits == 0
        || budget.max_local_commits > 1024
        || budget.max_local_members == 0
        || budget.max_local_members > 65536
        || budget.max_remote_graph_records == 0
        || budget.max_remote_graph_records > 1024
        || budget.max_local_payload_bytes == 0
        || budget.max_local_payload_bytes > 64 * 1024 * 1024
    {
        return Err(limited("invalid bounded native merge budget"));
    }
    if base == local {
        return Err(blocked("native merge has no local suffix"));
    }
    let base_record = record(read, base, true).await?;
    let mut commits = Vec::new();
    let mut keys = BTreeSet::<TrackedStateKey>::new();
    let mut seen = BTreeSet::new();
    let mut cursor = local;
    let mut member_count = 0usize;
    let mut payload_bytes = 0usize;
    while cursor != base {
        if commits.len() >= budget.max_local_commits {
            return Err(limited("local suffix commit budget exceeded"));
        }
        if !seen.insert(cursor) {
            return Err(blocked("local suffix contains a cycle"));
        }
        let node = record(read, cursor, true).await?;
        if node.is_checkpoint
            || node.parent_commit_ids.len() != 1
            || node.account_id != account
            || node
                .base_commit_id
                .is_some_and(|key| key != global_head)
        {
            return Err(blocked(
                "local suffix is not ordinary selected state at the confirmed catalog",
            ));
        }
        let header = crate::tracked_state::load_published_commit_state_topology(read, cursor)
            .await?
            .ok_or_else(|| blocked("local commit state header is absent"))?;
        member_count = member_count
            .checked_add(header.mutation_member_count() as usize)
            .ok_or_else(|| limited("local member count overflow"))?;
        if member_count > budget.max_local_members {
            return Err(limited(
                "local member budget exceeded before payload loading",
            ));
        }
        let members =
            crate::tracked_state::load_local_commit_delta_members_with_payloads(read, cursor)
                .await?;
        for member in members {
            if !member.authored
                || member.key.schema_key != "lix_key_value"
                || member.key.file_id.is_some()
            {
                return Err(blocked(
                    "first merge slice supports only authored unfiled key/value rows",
                ));
            }
            payload_bytes = payload_bytes
                .checked_add(member.change.snapshot.as_ref().map_or(0, Vec::len))
                .ok_or_else(|| limited("local payload byte count overflow"))?;
            if payload_bytes > budget.max_local_payload_bytes {
                return Err(limited("local payload budget exceeded"));
            }
            keys.insert(member.key);
        }
        cursor = node.parent_commit_ids[0];
        commits.push(node);
    }
    commits.reverse();
    let local_record = commits
        .last()
        .ok_or_else(|| blocked("local suffix is empty"))?;
    let mut graph = BTreeMap::new();
    if !bounded_ancestor(
        read,
        &base_record,
        remote,
        &mut graph,
        budget.max_remote_graph_records,
    )
    .await?
    {
        return Err(blocked(
            "authority no longer descends from the confirmed merge base",
        ));
    }
    let included = bounded_ancestor(
        read,
        local_record,
        remote,
        &mut graph,
        budget.max_remote_graph_records,
    )
    .await?;
    let mut groups = Vec::new();
    if !included {
        let mut correlated = BTreeMap::<(String, Option<String>), Vec<crate::row_pk::RowPk>>::new();
        for key in keys {
            correlated
                .entry((key.schema_key, key.file_id))
                .or_default()
                .push(key.row_pk);
        }
        let mut reader = TrackedStateContext::new().reader(read);
        for ((schema, file), row_pks) in correlated {
            let request = TrackedStateDiffRequest {
                filter: TrackedStateFilter {
                    schema_keys: vec![schema],
                    file_ids: vec![file.map_or(NullableKeyFilter::Null, NullableKeyFilter::Value)],
                    row_pks,
                    include_tombstones: true,
                    ..Default::default()
                },
                retain_payloads: true,
            };
            let local_diff = reader
                .diff_commit_members(&base.to_string(), &local.to_string(), &request)
                .await?;
            let remote_diff = reader
                .diff_commit_members(&base.to_string(), &remote.to_string(), &request)
                .await?;
            let fallback =
                crate::tracked_state::merge_payload_fallback_ids(&remote_diff, &local_diff)?;
            let payloads = reader.load_change_payloads(&fallback).await?;
            groups.push(crate::tracked_state::plan_merge(
                &remote_diff,
                &local_diff,
                &payloads,
            )?);
        }
    }
    Ok(NativeKvMergeAnalysis {
        local_commits: commits,
        groups,
        already_in_authority: included,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::StorageWriteOptions;
    use crate::{Lix, Memory, open_lix};

    fn budget() -> PartialMergeBudget {
        PartialMergeBudget {
            max_local_commits: 32,
            max_local_members: 128,
            max_local_payload_bytes: 1024 * 1024,
            max_remote_graph_records: 64,
        }
    }

    // Complete native stores deliberately isolate reconciliation semantics from
    // transport. The receipt below is test-only bookkeeping, never opened as a
    // partial engine or advertised as a complete-state certificate.
    async fn divergent_fixture(
        same_identity: bool,
    ) -> (Lix<Memory>, PartialReplicaState, PartialReplicaState) {
        let memory = Memory::new();
        let authority = open_lix().with_storage(memory.clone()).await.unwrap();
        authority
            .set_sync_role(crate::sync::SyncRole::Authority)
            .unwrap();
        authority.execute("INSERT INTO lix_key_value (key,value) VALUES ('merge-a','base'),('merge-b','base')",&[]).await.unwrap();
        let old = PartialReplicaState::new(
            format!("https://example.test/lix/{}", authority.lix_id()),
            authority.active_account_id().into(),
            uuid::Uuid::now_v7().to_string(),
            authority.partial_replica_descriptor(None).await.unwrap(),
        )
        .unwrap();
        let local = open_lix()
            .with_storage(memory.fork().unwrap())
            .await
            .unwrap();
        local
            .execute(
                "UPDATE lix_key_value SET value='local' WHERE key='merge-a'",
                &[],
            )
            .await
            .unwrap();
        let remote_sql = if same_identity {
            "UPDATE lix_key_value SET value='remote' WHERE key='merge-a'"
        } else {
            "UPDATE lix_key_value SET value='remote' WHERE key='merge-b'"
        };
        authority.execute(remote_sql, &[]).await.unwrap();
        let candidate = old
            .with_descriptor_and_fresh_generations(
                authority.partial_replica_descriptor(None).await.unwrap(),
            )
            .unwrap();
        let commit = crate::sync::export_sync_commit(
            &authority,
            &candidate.descriptor().selected_branch.head.commit_id,
        )
        .await
        .unwrap()
        .unwrap();
        local
            .push_sync_repository_for_account(
                &crate::sync::SyncPushRequest {
                    commits: vec![commit],
                    ref_updates: vec![],
                    inline_blobs: vec![],
                },
                old.active_account_id(),
            )
            .await
            .unwrap();
        let storage = local.storage_adapter();
        let mut writes = storage.new_write_set();
        let mut preconditions =
            super::super::partial_push_state::stage_initial_partial_push_states(&mut writes, &old)
                .unwrap();
        preconditions.push(
            super::super::partial_state::stage_partial_replica_state(&mut writes, &old, None)
                .unwrap(),
        );
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        (local, old, candidate)
    }

    #[tokio::test]
    async fn changed_identity_merge_analysis_preserves_both_heads_and_conflicts() {
        for same_identity in [false, true] {
            let (local, old, candidate) = divergent_fixture(same_identity).await;
            let storage = local.storage_adapter();
            let read = storage.begin_read(Default::default()).await.unwrap();
            let before = crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load_observed(&[old.descriptor().selected_branch.branch_id.clone()])
                .await
                .unwrap();
            let result = prepare_partial_merge_analysis(&read, &old, &candidate, budget())
                .await
                .unwrap();
            assert_eq!(result.local_commits.len(), 1);
            assert!(!result.already_in_authority);
            assert_eq!(result.groups.len(), 1);
            assert_eq!(result.groups[0].conflicts.len(), usize::from(same_identity));
            assert_eq!(result.groups[0].picks.len(), usize::from(!same_identity));
            assert_eq!(
                result.local,
                before[0].control.as_ref().unwrap().head_commit_id
            );
            assert_eq!(
                result.base.to_string(),
                old.descriptor().selected_branch.head.commit_id
            );
            assert_eq!(
                result.remote.to_string(),
                candidate.descriptor().selected_branch.head.commit_id
            );
            assert_eq!(result.preconditions.len(), 5);
            let after = crate::branch::BranchHeadControlContext::new()
                .reader(&read)
                .load_observed(&[old.descriptor().selected_branch.branch_id.clone()])
                .await
                .unwrap();
            assert_eq!(before[0].raw_token, after[0].raw_token);
        }
    }

    #[tokio::test]
    async fn merge_analysis_rejects_budget_without_mutating_state() {
        let (local, old, candidate) = divergent_fixture(false).await;
        let storage = local.storage_adapter();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let mut tiny = budget();
        tiny.max_local_payload_bytes = 1;
        let error = prepare_partial_merge_analysis(&read, &old, &candidate, tiny)
            .await
            .err()
            .unwrap();
        assert_eq!(error.code, "LIX_PARTIAL_MERGE_BUDGET_EXCEEDED");
        let (push, _, _) = super::super::partial_push_state::load_partial_push_state(
            &read,
            &old,
            &old.descriptor().selected_branch.branch_id,
        )
        .await
        .unwrap();
        assert_eq!(
            push.confirmed.head,
            old.descriptor().selected_branch.head.commit_id
        );
        assert!(push.prepared.is_none());
    }
    #[tokio::test]
    async fn causal_ancestry_finds_second_parent_and_never_calls_missing_absent() {
        let (local, old, _candidate) = divergent_fixture(false).await;
        let storage = local.storage_adapter();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let base = record(
            &read,
            id(&old.descriptor().selected_branch.head.commit_id).unwrap(),
            true,
        )
        .await
        .unwrap();
        let mut merge = base.clone();
        merge.commit_id = id("00000000-0000-7000-8000-000000005001").unwrap();
        merge.generation = base.generation + 1;
        merge.parent_commit_ids = vec![
            id("00000000-0000-7000-8000-000000005002").unwrap(),
            base.commit_id,
        ];
        let mut cache = BTreeMap::from([(merge.commit_id, merge.clone())]);
        assert!(
            bounded_ancestor(&read, &base, merge.commit_id, &mut cache, 4)
                .await
                .unwrap()
        );
        merge.parent_commit_ids.reverse();
        cache.insert(merge.commit_id, merge.clone());
        // The missing first visited parent cannot be silently pruned. A later
        // replay may prove ancestry once that exact graph input is available.
        let error = bounded_ancestor(&read, &base, merge.commit_id, &mut cache, 4)
            .await
            .unwrap_err();
        assert!(
            NativeMetadataRef::from_missing_error(&error)
                .unwrap()
                .is_some()
        );
    }
}
