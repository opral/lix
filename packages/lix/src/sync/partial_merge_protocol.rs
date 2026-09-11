//! Native background merge coordinates; clients never supply selected rows.
use crate::LixError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialMergeRequest {
    pub attempt_id: String,
    pub branch_id: String,
    pub base_commit_id: String,
    pub expected_authority_head_commit_id: String,
    pub captured_local_head_commit_id: String,
    pub checkpoint_commit_id: String,
    pub global_head_commit_id: String,
    pub global_checkpoint_commit_id: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct PartialMergeReceipt {
    pub request: PartialMergeRequest,
    pub merge_commit_id: String,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_PROTOCOL_INVALID", message)
}
impl PartialMergeRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        for value in [
            &self.attempt_id,
            &self.branch_id,
            &self.base_commit_id,
            &self.expected_authority_head_commit_id,
            &self.captured_local_head_commit_id,
            &self.checkpoint_commit_id,
            &self.global_head_commit_id,
            &self.global_checkpoint_commit_id,
        ] {
            if crate::storage_codec::id_string::uuid_bytes_from_canonical(value).is_none() {
                return Err(invalid("merge coordinates require canonical UUIDs"));
            }
        }
        if self.branch_id == crate::GLOBAL_BRANCH_ID {
            return Err(invalid("selected merge cannot target the global branch"));
        }
        if self.captured_local_head_commit_id == self.base_commit_id {
            return Err(invalid("merge requires a captured local suffix"));
        }
        Ok(())
    }
}
impl PartialMergeReceipt {
    pub(crate) fn validate_for(&self, request: &PartialMergeRequest) -> Result<(), LixError> {
        request.validate()?;
        if &self.request != request {
            return Err(invalid(
                "authority receipt does not echo the exact merge attempt",
            ));
        }
        if crate::storage_codec::id_string::uuid_bytes_from_canonical(&self.merge_commit_id)
            .is_none()
        {
            return Err(invalid(
                "authority merge receipt has an invalid result commit",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetainedBodyWaveRequest {
    pub request: PartialMergeRequest,
    pub expected_previous_commit_id: String,
    pub bodies: super::SyncPushRequest,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct RetainedBodyWaveResponse {
    pub push: super::SyncPushResponse,
    pub accepted_tip: String,
    pub expires_at_ms: u64,
}
impl RetainedBodyWaveRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        if crate::storage_codec::id_string::uuid_bytes_from_canonical(
            &self.expected_previous_commit_id,
        )
        .is_none()
            || self.bodies.commits.is_empty()
            || self.bodies.commits.len() > 32
            || !self.bodies.ref_updates.is_empty()
            || !self.bodies.inline_blobs.is_empty()
        {
            return Err(invalid(
                "retained body wave must contain at most32 complete commits and no refs/blobs",
            ));
        }
        Ok(())
    }
}
