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
    body_roots: BTreeSet<CommitId>,
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
                "retained wave is not a complete contiguous ordinary selected suffix",
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
        let anchors = [
            &merge.expected_authority_head_commit_id,
            &merge.checkpoint_commit_id,
            &merge.expected_authority_checkpoint_commit_id,
            &merge.global_head_commit_id,
            &merge.global_checkpoint_commit_id,
        ]
        .into_iter()
        .map(|value| CommitId::parse_lix(value, "upload attempt anchor"))
        .collect::<Result<BTreeSet<_>, _>>()?;
        let mut known = anchors.clone();
        known.insert(CommitId::parse_lix(&merge.base_commit_id, "retained base")?);
        known.insert(previous);
        if let Some((attempt, _)) = crate::gc::load_native_upload_attempt(
            read,
            &crate::gc::NativeUploadAttemptIdentity {
                repository_id: repository_id.into(),
                account_id: account.into(),
                branch_id: merge.branch_id.clone(),
                attempt_id: merge.attempt_id.clone(),
            },
        )
        .await?
        {
            known.extend(attempt.retained_body_roots()?);
        }
        let mut tip = previous;
        let mut seen = BTreeSet::new();
        let captured_catalog =
            CommitId::parse_lix(&merge.global_head_commit_id, "captured catalog")?;
        let mut catalog_bases = BTreeSet::from([captured_catalog]);
        for commit in &request.commits {
            let key = CommitId::parse_lix(&commit.commit_id, "retained body")?;
            let mut dependencies = commit
                .parent_commit_ids
                .iter()
                .map(|id| CommitId::parse_lix(id, "retained parent"))
                .collect::<Result<BTreeSet<_>, _>>()?;
            if let Some(alias) = &commit.state_alias {
                dependencies.insert(CommitId::parse_lix(
                    &alias.source_commit_id,
                    "retained alias",
                )?);
            }
            if let Some(source) = &commit.selected_source_commit_id {
                dependencies.insert(CommitId::parse_lix(source, "retained source")?);
            }
            if let Some(source) = &commit.complete_incorporation_source_commit_id {
                dependencies.insert(CommitId::parse_lix(
                    source,
                    "retained incorporation source",
                )?);
            }
            if commit.global_scope
                || commit.account_id != account
                || !seen.insert(key)
                || (!staged.contains_key(&key) && !existing_complete.contains(&key))
                || dependencies.is_empty()
                || !dependencies.is_subset(&known)
            {
                return Err(invalid());
            }
            if let Some(base) = &commit.base_commit_id {
                let base = CommitId::parse_lix(base, "retained catalog base")?;
                if !catalog_bases.contains(&base) {
                    if !crate::sync::partial_merge_analysis::catalog_contains(
                        read,
                        base,
                        captured_catalog,
                        1024,
                    )
                    .await?
                    {
                        return Err(invalid());
                    }
                    catalog_bases.insert(base);
                }
            }
            known.insert(key);
            tip = key;
        }
        let mut anchor_guards = Vec::new();
        if initial {
            let branches = [merge.branch_id.clone(), crate::GLOBAL_BRANCH_ID.to_owned()];
            let observed = crate::branch::BranchHeadControlContext::new()
                .reader(read)
                .load_observed(&branches)
                .await?;
            // Both authority frontiers may advance while immutable bodies travel.
            // The original GLOBAL coordinate remains a retained body dependency.
            let global = observed[1].control.as_ref().ok_or_else(invalid)?;
            if !crate::sync::partial_merge_analysis::catalog_contains(
                read,
                CommitId::parse_lix(&merge.global_head_commit_id, "captured catalog")?,
                global.head_commit_id,
                1024,
            )
            .await?
            {
                return Err(invalid());
            }
            let selected = observed[0].control.as_ref().ok_or_else(invalid)?;
            let captured_remote = crate::sync::partial_merge_analysis::record(
                read,
                CommitId::parse_lix(
                    &merge.expected_authority_head_commit_id,
                    "captured authority",
                )?,
                false,
            )
            .await?;
            if !crate::sync::partial_merge_analysis::incorporated(
                read,
                &captured_remote,
                selected.head_commit_id,
                &mut BTreeMap::new(),
                1024,
            )
            .await?
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID",
                    "retention authority no longer incorporates the captured selected frontier",
                ));
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
            if !crate::sync::partial_merge_analysis::incorporated(
                read,
                &base,
                remote,
                &mut BTreeMap::new(),
                1024,
            )
            .await?
            {
                return Err(LixError::new(
                    "LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID",
                    "retention authority does not incorporate the captured merge base",
                ));
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
            body_roots: seen,
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
    pub(crate) fn body_roots(&self) -> &BTreeSet<CommitId> {
        &self.body_roots
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
        initial_anchor_case(false, false, false).await;
    }

    #[tokio::test]
    async fn authority_advancement_preserves_retention_of_complete_native_bodies() {
        initial_anchor_case(true, false, true).await;
        initial_anchor_case(true, true, true).await;
        initial_anchor_case(true, true, false).await;
    }

    async fn initial_anchor_case(complete: bool, selected_stale: bool, global_stale: bool) {
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
            expected_authority_head_commit_id: if selected_stale {
                before.selected_branch.head.commit_id.clone()
            } else {
                after.selected_branch.head.commit_id.clone()
            },
            captured_local_head_commit_id: after.selected_branch.head.commit_id,
            expected_authority_checkpoint_commit_id: before
                .selected_branch
                .checkpoint
                .commit_id
                .clone(),
            captured_local_checkpoint_commit_id: before
                .selected_branch
                .checkpoint
                .commit_id
                .clone(),
            checkpoint_commit_id: before.selected_branch.checkpoint.commit_id,
            global_head_commit_id: before.global_branch.head.commit_id,
            global_checkpoint_commit_id: before.global_branch.checkpoint.commit_id,
        };
        if global_stale {
            let global = lix
                .open_another_session()
                .with_branch(crate::GLOBAL_BRANCH_ID)
                .await
                .unwrap();
            global
                .execute(
                    "INSERT INTO lix_key_value(key,value) VALUES('global-race','changed')",
                    &[],
                )
                .await
                .unwrap();
        }
        let storage = lix.storage_adapter();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let request = SyncPushRequest {
            commits: vec![commit],
            ref_updates: vec![],
            inline_blobs: vec![],
        };
        let result = VerifiedRetainedBodyWave::from_validated_import(
            &read,
            true,
            lix.lix_id(),
            &merge,
            &request,
            lix.active_account_id(),
            CommitId::parse_lix(&merge.base_commit_id, "test base").unwrap(),
            &BTreeMap::new(),
            &if complete {
                BTreeSet::from([CommitId::parse_lix(
                    &merge.captured_local_head_commit_id,
                    "test complete body",
                )
                .unwrap()])
            } else {
                BTreeSet::new()
            },
        )
        .await;
        if complete {
            assert!(
                result.is_ok(),
                "authority advancement must retain the same request"
            );
        } else {
            assert_eq!(
                result.err().unwrap().code,
                "LIX_PARTIAL_UPLOAD_ATTEMPT_INVALID"
            );
        }
    }
}
