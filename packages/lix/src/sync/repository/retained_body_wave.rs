//! Child of sync::repository. Only the native importer can construct this proof.
//! Construct after all body validation/staging succeeds, before atomic commit.
use crate::sync::{PartialMergeRequest, SyncPushRequest};
use crate::tracked_state::StagedCommitStateManifest;
use crate::{LixError, changelog::CommitId};
use std::collections::{BTreeMap, BTreeSet};

pub(crate) struct VerifiedRetainedBodyWave {
    repository_id: String,
    account_id: String,
    branch_id: String,
    attempt_id: String,
    base: CommitId,
    commit_count: usize,
    initial: bool,
    anchor_guards: Vec<crate::storage_adapter::StoragePrecondition>,
    binding_digest: [u8; 32],
    previous: CommitId,
    tip: CommitId,
    anchors: BTreeSet<CommitId>,
}
impl VerifiedRetainedBodyWave {
    /// `existing_complete` contains only bodies successfully checked by the
    /// ordinary import's canonical load_sync_commit equality path. Deferred
    /// history headers and incomplete boundary rows are never included.
    pub(super) async fn from_validated_import(
        read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
        initial: bool,
        repository_id: &str,
        merge: &PartialMergeRequest,
        request: &SyncPushRequest,
        account: &str,
        previous: CommitId,
        staged: &BTreeMap<CommitId, StagedCommitStateManifest>,
        existing_complete: &BTreeSet<CommitId>,
    ) -> Result<Self, LixError> {
        let invalid = || {
            LixError::new(
                "LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID",
                "retained wave is not a complete contiguous ordinary KV suffix",
            )
        };
        merge.validate()?;
        if !request.ref_updates.is_empty()
            || !request.inline_blobs.is_empty()
            || request.commits.is_empty()
            || request.commits.len() > 32
        {
            return Err(invalid());
        }
        let mut tip = previous;
        let mut seen = BTreeSet::new();
        for commit in &request.commits {
            let key = CommitId::parse_lix(&commit.commit_id, "retained body")?;
            if commit.is_checkpoint
                || commit.global_scope
                || commit.account_id != account
                || commit.parent_commit_ids != vec![tip.to_string()]
                || commit.state_alias.is_some()
                || commit.selected_source_commit_id.is_some()
                || !seen.insert(key)
                || (!staged.contains_key(&key) && !existing_complete.contains(&key))
                || commit
                    .base_commit_id
                    .as_ref()
                    .is_some_and(|base| base != &merge.global_head_commit_id)
                || commit.members.iter().any(|member| {
                    !member.authored
                        || member.schema_key != "lix_key_value"
                        || member.file_id.is_some()
                })
            {
                return Err(invalid());
            }
            tip = key;
        }
        let anchors = [
            &merge.expected_authority_head_commit_id,
            &merge.checkpoint_commit_id,
            &merge.global_head_commit_id,
            &merge.global_checkpoint_commit_id,
        ]
        .into_iter()
        .map(|value| CommitId::parse_lix(value, "upload attempt anchor"))
        .collect::<Result<BTreeSet<_>, _>>()?;
        let mut anchor_guards = Vec::new();
        if initial {
            let branches = [merge.branch_id.clone(), crate::GLOBAL_BRANCH_ID.to_owned()];
            let observed = crate::branch::BranchHeadControlContext::new()
                .reader(read)
                .load_observed(&branches)
                .await?;
            for (index, (head, checkpoint)) in [
                (
                    &merge.expected_authority_head_commit_id,
                    &merge.checkpoint_commit_id,
                ),
                (
                    &merge.global_head_commit_id,
                    &merge.global_checkpoint_commit_id,
                ),
            ]
            .into_iter()
            .enumerate()
            {
                let control = observed[index].control.as_ref().ok_or_else(invalid)?;
                if control.head_commit_id != head.as_str()
                    || control
                        .working_diff_checkpoint_commit_id
                        .map(|id| id.to_string())
                        .as_ref()
                        != Some(checkpoint)
                {
                    return Err(invalid());
                }
            }
            let base = crate::sync::partial_merge_analysis::record(
                read,
                CommitId::parse_lix(&merge.base_commit_id, "retention base")?,
                false,
            )
            .await?;
            let remote = CommitId::parse_lix(
                &merge.expected_authority_head_commit_id,
                "retention authority head",
            )?;
            if !crate::sync::partial_merge_analysis::bounded_ancestor(
                read,
                &base,
                remote,
                &mut BTreeMap::new(),
                1024,
            )
            .await?
            {
                return Err(invalid());
            }
            for (branch, observation) in branches.iter().zip(observed) {
                anchor_guards.push(crate::branch::branch_head_control_precondition(
                    branch,
                    observation.raw_token,
                )?);
            }
        }
        let bytes = serde_json::to_vec(merge).map_err(|_| invalid())?;
        Ok(Self {
            initial,
            anchor_guards,
            repository_id: repository_id.into(),
            account_id: account.into(),
            branch_id: merge.branch_id.clone(),
            attempt_id: merge.attempt_id.clone(),
            base: CommitId::parse_lix(&merge.base_commit_id, "upload base")?,
            commit_count: request.commits.len(),
            binding_digest: *blake3::hash(&bytes).as_bytes(),
            previous,
            tip,
            anchors,
        })
    }
    pub(crate) fn initial(&self) -> bool {
        self.initial
    }
    pub(crate) fn anchor_guards(&self) -> &[crate::storage_adapter::StoragePrecondition] {
        &self.anchor_guards
    }
    pub(crate) fn matches_identity(
        &self,
        repository: &str,
        account: &str,
        branch: &str,
        attempt: &str,
    ) -> bool {
        self.repository_id == repository
            && self.account_id == account
            && self.branch_id == branch
            && self.attempt_id == attempt
    }
    pub(crate) fn base(&self) -> CommitId {
        self.base
    }
    pub(crate) fn commit_count(&self) -> usize {
        self.commit_count
    }
    pub(crate) fn binding_digest(&self) -> [u8; 32] {
        self.binding_digest
    }
    pub(crate) fn previous(&self) -> CommitId {
        self.previous
    }
    pub(crate) fn tip(&self) -> CommitId {
        self.tip
    }
    pub(crate) fn anchors(&self) -> &BTreeSet<CommitId> {
        &self.anchors
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn incomplete_native_body_closure_cannot_mint_a_retention_root() {
        let lix = crate::open_lix().await.unwrap();
        let before = lix.partial_replica_descriptor(None).await.unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('wave-key','value')",
            &[],
        )
        .await
        .unwrap();
        let after = lix.partial_replica_descriptor(None).await.unwrap();
        let commit = crate::sync::export_sync_commit(&lix, &after.selected_branch.head.commit_id)
            .await
            .unwrap()
            .unwrap();
        let merge = PartialMergeRequest {
            attempt_id: uuid::Uuid::now_v7().to_string(),
            branch_id: before.selected_branch.branch_id,
            base_commit_id: before.selected_branch.head.commit_id.clone(),
            expected_authority_head_commit_id: after.selected_branch.head.commit_id.clone(),
            captured_local_head_commit_id: after.selected_branch.head.commit_id,
            checkpoint_commit_id: before.selected_branch.checkpoint.commit_id,
            global_head_commit_id: before.global_branch.head.commit_id,
            global_checkpoint_commit_id: before.global_branch.checkpoint.commit_id,
        };
        let storage = lix.storage_adapter();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let request = SyncPushRequest {
            commits: vec![commit],
            ref_updates: vec![],
            inline_blobs: vec![],
        };
        let error = VerifiedRetainedBodyWave::from_validated_import(
            &read,
            false,
            lix.lix_id(),
            &merge,
            &request,
            lix.active_account_id(),
            CommitId::parse_lix(&merge.base_commit_id, "test base").unwrap(),
            &BTreeMap::new(),
            &BTreeSet::new(),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.code, "LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID");
    }
}
