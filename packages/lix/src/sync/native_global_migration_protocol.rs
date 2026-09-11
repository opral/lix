//! Closed-storage migration only: exact global merge and newly created refs.
use crate::{GLOBAL_BRANCH_ID, LixError};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
fn invalid() -> LixError {
    LixError::new(
        "LIX_MIGRATION_GLOBAL_SCOPE_UNSUPPORTED",
        "invalid descriptor-only global migration coordinates",
    )
}
fn uuid(s: &str) -> Result<(), LixError> {
    crate::storage_codec::id_string::uuid_bytes_from_canonical(s)
        .map(|_| ())
        .ok_or_else(invalid)
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeNewBranchCoordinate {
    pub branch_id: String,
    pub head_commit_id: String,
    pub checkpoint_commit_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeGlobalMigrationRequest {
    pub attempt_id: String,
    pub base_commit_id: String,
    pub expected_authority_head_commit_id: String,
    pub captured_local_head_commit_id: String,
    pub checkpoint_commit_id: String,
    /// Canonical ascending branch-ID order is part of the durable request identity.
    pub new_branches: Vec<NativeNewBranchCoordinate>,
}
impl NativeGlobalMigrationRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        for s in [
            &self.attempt_id,
            &self.base_commit_id,
            &self.expected_authority_head_commit_id,
            &self.captured_local_head_commit_id,
            &self.checkpoint_commit_id,
        ] {
            uuid(s)?;
        }
        if self.new_branches.is_empty()
            || self.new_branches.len() > 1024
            || self.base_commit_id == self.captured_local_head_commit_id
            || self.expected_authority_head_commit_id == self.captured_local_head_commit_id
        {
            return Err(invalid());
        }
        let mut previous: Option<&str> = None;
        for branch in &self.new_branches {
            for s in [
                &branch.branch_id,
                &branch.head_commit_id,
                &branch.checkpoint_commit_id,
            ] {
                uuid(s)?;
            }
            if branch.branch_id == GLOBAL_BRANCH_ID
                || previous.is_some_and(|old| old >= branch.branch_id.as_str())
            {
                return Err(invalid());
            }
            previous = Some(&branch.branch_id);
        }
        Ok(())
    }
    pub(crate) fn branch_ids(&self) -> BTreeSet<String> {
        self.new_branches
            .iter()
            .map(|v| v.branch_id.clone())
            .collect()
    }
    pub(crate) fn binding_digest(&self) -> Result<[u8; 32], LixError> {
        self.validate()?;
        let bytes = serde_json::to_vec(self).map_err(|_| invalid())?;
        Ok(*blake3::hash(&bytes).as_bytes())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeGlobalMigrationReceipt {
    pub request: NativeGlobalMigrationRequest,
    pub merge_commit_id: String,
}
impl NativeGlobalMigrationReceipt {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        uuid(&self.merge_commit_id)?;
        if [
            &self.request.base_commit_id,
            &self.request.expected_authority_head_commit_id,
            &self.request.captured_local_head_commit_id,
        ]
        .contains(&&self.merge_commit_id)
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeGlobalBodyWaveRequest {
    pub request: NativeGlobalMigrationRequest,
    /// Global or one exact new branch ID; each independent chain is retained.
    pub branch_id: String,
    pub previous_commit_id: String,
    pub bodies: super::SyncPushRequest,
}
impl NativeGlobalBodyWaveRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        uuid(&self.branch_id)?;
        uuid(&self.previous_commit_id)?;
        if self.branch_id != GLOBAL_BRANCH_ID
            && !self
                .request
                .new_branches
                .iter()
                .any(|b| b.branch_id == self.branch_id)
        {
            return Err(invalid());
        }
        if !self.bodies.ref_updates.is_empty()
            || !self.bodies.inline_blobs.is_empty()
            || (self.bodies.commits.is_empty() && self.branch_id == GLOBAL_BRANCH_ID)
            || self.bodies.commits.len() > 32
        {
            return Err(invalid());
        }
        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    fn id(n: u128) -> String {
        uuid::Uuid::from_u128(n).to_string()
    }
    fn request() -> NativeGlobalMigrationRequest {
        NativeGlobalMigrationRequest {
            attempt_id: id(1),
            base_commit_id: id(2),
            expected_authority_head_commit_id: id(3),
            captured_local_head_commit_id: id(4),
            checkpoint_commit_id: id(5),
            new_branches: vec![NativeNewBranchCoordinate {
                branch_id: id(6),
                head_commit_id: id(7),
                checkpoint_commit_id: id(8),
            }],
        }
    }
    #[test]
    fn request_identity_binds_every_new_ref() {
        let a = request();
        a.validate().unwrap();
        for field in 0..3 {
            let mut b = a.clone();
            match field {
                0 => b.new_branches[0].branch_id = id(9),
                1 => b.new_branches[0].head_commit_id = id(9),
                _ => b.new_branches[0].checkpoint_commit_id = id(9),
            }
            assert_ne!(a.binding_digest().unwrap(), b.binding_digest().unwrap());
        }
    }
    #[test]
    fn duplicate_and_unsorted_targets_rejected() {
        let mut a = request();
        a.new_branches.push(a.new_branches[0].clone());
        assert!(a.validate().is_err());
        a.new_branches[1].branch_id = id(1);
        assert!(a.validate().is_err());
    }
    #[test]
    fn receipt_cannot_hide_missing_global_merge() {
        let a = request();
        let receipt = NativeGlobalMigrationReceipt {
            merge_commit_id: a.captured_local_head_commit_id.clone(),
            request: a,
        };
        assert!(receipt.validate().is_err());
    }
}
