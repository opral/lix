//! Repository importer-owned native completeness proof for explicit migration.
use crate::storage_adapter::{StorageAdapterRead, StoragePrecondition};
use crate::sync::{NativeGlobalBodyWaveRequest, NativeGlobalMigrationRequest};
use crate::tracked_state::StagedCommitStateManifest;
use crate::{GLOBAL_BRANCH_ID, LixError, changelog::CommitId};
use std::collections::{BTreeMap, BTreeSet};
pub(crate) struct VerifiedGlobalMigrationBody {
    repository: String,
    account: String,
    request: NativeGlobalMigrationRequest,
    branch: String,
    previous: String,
    tip: String,
    guards: Vec<StoragePrecondition>,
}
fn invalid() -> LixError {
    LixError::new(
        "LIX_MIGRATION_GLOBAL_BODY_INVALID",
        "migration body wave must have complete canonical native dependencies and its exact authenticated scope",
    )
}
impl VerifiedGlobalMigrationBody {
    pub(super) async fn from_validated_import(
        read: &(impl StorageAdapterRead + ?Sized),
        repository: &str,
        account: &str,
        wave: &NativeGlobalBodyWaveRequest,
        staged: &BTreeMap<CommitId, StagedCommitStateManifest>,
        existing: &BTreeSet<CommitId>,
    ) -> Result<Self, LixError> {
        wave.validate()?;
        let control = crate::branch::BranchHeadControlContext::new()
            .reader(read)
            .load_observed(&[GLOBAL_BRANCH_ID.to_owned()])
            .await?
            .into_iter()
            .next()
            .ok_or_else(invalid)?;
        let current = control.control.as_ref().ok_or_else(invalid)?;
        if current.head_commit_id != wave.request.expected_authority_head_commit_id
            || current
                .working_diff_checkpoint_commit_id
                .map(|c| c.to_string())
                .as_ref()
                != Some(&wave.request.checkpoint_commit_id)
        {
            return Err(invalid());
        }
        let previous = CommitId::parse_lix(&wave.previous_commit_id, "migration previous")?;
        // An initial boundary is an actual complete authority object, never a
        // fabricated new-branch merge base or a header-only receipt.
        let boundary = crate::sync::commit::load_sync_commit(read, previous)
            .await?
            .ok_or_else(invalid)?;
        if boundary.global_scope != (wave.branch_id == GLOBAL_BRANCH_ID) {
            return Err(invalid());
        }
        let mut tip = previous;
        let mut seen = BTreeSet::new();
        for commit in &wave.bodies.commits {
            let key = CommitId::parse_lix(&commit.commit_id, "migration body")?;
            if !seen.insert(key)
                || commit.is_checkpoint
                || commit.account_id != account
                || commit.global_scope != (wave.branch_id == GLOBAL_BRANCH_ID)
                || commit.parent_commit_ids != vec![tip.to_string()]
                || commit.state_alias.is_some()
                || commit.selected_source_commit_id.is_some()
                || (!staged.contains_key(&key) && !existing.contains(&key))
            {
                return Err(invalid());
            }
            if commit.global_scope {
                if commit.base_commit_id.is_some()
                    || commit.members.iter().any(|m| {
                        !m.authored
                            || m.schema_key != crate::branch::BRANCH_DESCRIPTOR_SCHEMA_KEY
                            || m.file_id.is_some()
                    })
                {
                    return Err(invalid());
                }
            } else if commit.base_commit_id.is_none() {
                return Err(invalid());
            }
            tip = key;
        }
        if wave.bodies.commits.is_empty() {
            let branch = wave
                .request
                .new_branches
                .iter()
                .find(|b| b.branch_id == wave.branch_id)
                .ok_or_else(invalid)?;
            if wave.previous_commit_id != branch.head_commit_id {
                return Err(invalid());
            }
        }
        Ok(Self {
            repository: repository.into(),
            account: account.into(),
            request: wave.request.clone(),
            branch: wave.branch_id.clone(),
            previous: wave.previous_commit_id.clone(),
            tip: tip.to_string(),
            guards: vec![crate::branch::branch_head_control_precondition(
                GLOBAL_BRANCH_ID,
                control.raw_token,
            )?],
        })
    }
    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn request(&self) -> &NativeGlobalMigrationRequest {
        &self.request
    }
    pub(crate) fn branch(&self) -> &str {
        &self.branch
    }
    pub(crate) fn previous(&self) -> &str {
        &self.previous
    }
    pub(crate) fn tip(&self) -> &str {
        &self.tip
    }
    pub(crate) fn anchor_guards(&self) -> &[StoragePrecondition] {
        &self.guards
    }
}
