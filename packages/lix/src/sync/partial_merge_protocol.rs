//! Native background merge coordinates; clients never supply selected rows.
use crate::LixError;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "PartialMergeRequestWire", into = "PartialMergeRequestWire")]
pub(crate) struct PartialMergeRequest {
    pub attempt_id: String,
    pub branch_id: String,
    pub base_commit_id: String,
    pub expected_authority_head_commit_id: String,
    pub captured_local_head_commit_id: String,
    pub checkpoint_commit_id: String,
    pub expected_authority_checkpoint_commit_id: String,
    pub captured_local_checkpoint_commit_id: String,
    pub global_head_commit_id: String,
    pub global_checkpoint_commit_id: String,
}
// Equal checkpoint coordinates have one canonical representation: omit the
// redundant overrides. This preserves existing request bytes and retention
// digests, including already accepted immutable receipts.
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PartialMergeRequestWire {
    attempt_id: String,
    branch_id: String,
    base_commit_id: String,
    expected_authority_head_commit_id: String,
    captured_local_head_commit_id: String,
    checkpoint_commit_id: String,
    global_head_commit_id: String,
    global_checkpoint_commit_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    expected_authority_checkpoint_commit_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    captured_local_checkpoint_commit_id: Option<String>,
}
impl From<PartialMergeRequestWire> for PartialMergeRequest {
    fn from(wire: PartialMergeRequestWire) -> Self {
        Self {
            expected_authority_checkpoint_commit_id: wire
                .expected_authority_checkpoint_commit_id
                .unwrap_or_else(|| wire.checkpoint_commit_id.clone()),
            captured_local_checkpoint_commit_id: wire
                .captured_local_checkpoint_commit_id
                .unwrap_or_else(|| wire.checkpoint_commit_id.clone()),
            attempt_id: wire.attempt_id,
            branch_id: wire.branch_id,
            base_commit_id: wire.base_commit_id,
            expected_authority_head_commit_id: wire.expected_authority_head_commit_id,
            captured_local_head_commit_id: wire.captured_local_head_commit_id,
            checkpoint_commit_id: wire.checkpoint_commit_id,
            global_head_commit_id: wire.global_head_commit_id,
            global_checkpoint_commit_id: wire.global_checkpoint_commit_id,
        }
    }
}
impl From<PartialMergeRequest> for PartialMergeRequestWire {
    fn from(request: PartialMergeRequest) -> Self {
        Self {
            expected_authority_checkpoint_commit_id: (request
                .expected_authority_checkpoint_commit_id
                != request.checkpoint_commit_id)
                .then_some(request.expected_authority_checkpoint_commit_id),
            captured_local_checkpoint_commit_id: (request.captured_local_checkpoint_commit_id
                != request.checkpoint_commit_id)
                .then_some(request.captured_local_checkpoint_commit_id),
            attempt_id: request.attempt_id,
            branch_id: request.branch_id,
            base_commit_id: request.base_commit_id,
            expected_authority_head_commit_id: request.expected_authority_head_commit_id,
            captured_local_head_commit_id: request.captured_local_head_commit_id,
            checkpoint_commit_id: request.checkpoint_commit_id,
            global_head_commit_id: request.global_head_commit_id,
            global_checkpoint_commit_id: request.global_checkpoint_commit_id,
        }
    }
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
    /// A checkpoint is a changed field relative to the captured base, just as
    /// row columns are. An unchanged local value must not erase newer authority work.
    pub(crate) fn accepted_checkpoint_commit_id(&self) -> &str {
        if self.captured_local_checkpoint_commit_id == self.checkpoint_commit_id {
            &self.expected_authority_checkpoint_commit_id
        } else {
            &self.captured_local_checkpoint_commit_id
        }
    }
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        for value in [
            &self.attempt_id,
            &self.branch_id,
            &self.base_commit_id,
            &self.expected_authority_head_commit_id,
            &self.captured_local_head_commit_id,
            &self.checkpoint_commit_id,
            &self.expected_authority_checkpoint_commit_id,
            &self.captured_local_checkpoint_commit_id,
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

#[cfg(test)]
mod checkpoint_encoding_tests {
    use super::*;

    fn old_request_bytes() -> Vec<u8> {
        let fields = [
            "attemptId",
            "branchId",
            "baseCommitId",
            "expectedAuthorityHeadCommitId",
            "capturedLocalHeadCommitId",
            "checkpointCommitId",
            "globalHeadCommitId",
            "globalCheckpointCommitId",
        ];
        format!(
            "{{{}}}",
            fields
                .iter()
                .enumerate()
                .map(|(index, field)| format!(
                    "\"{field}\":\"{}\"",
                    uuid::Uuid::from_u128(index as u128 + 1)
                ))
                .collect::<Vec<_>>()
                .join(",")
        )
        .into_bytes()
    }

    #[test]
    fn existing_equal_checkpoint_request_preserves_canonical_bytes_and_retention_digest() {
        let old = old_request_bytes();
        let request: PartialMergeRequest = serde_json::from_slice(&old).unwrap();
        request.validate().unwrap();
        assert_eq!(
            request.expected_authority_checkpoint_commit_id,
            request.checkpoint_commit_id
        );
        assert_eq!(
            request.captured_local_checkpoint_commit_id,
            request.checkpoint_commit_id
        );
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_eq!(encoded, old);
        assert_eq!(blake3::hash(&encoded), blake3::hash(&old));
    }

    #[test]
    fn unequal_checkpoints_are_bound_to_request_and_receipt_identity() {
        let mut request: PartialMergeRequest =
            serde_json::from_slice(&old_request_bytes()).unwrap();
        request.expected_authority_checkpoint_commit_id = uuid::Uuid::from_u128(20).to_string();
        request.captured_local_checkpoint_commit_id = uuid::Uuid::from_u128(21).to_string();
        request.validate().unwrap();
        let encoded = serde_json::to_vec(&request).unwrap();
        assert_ne!(blake3::hash(&encoded), blake3::hash(&old_request_bytes()));
        assert_eq!(
            serde_json::from_slice::<PartialMergeRequest>(&encoded).unwrap(),
            request
        );
        let receipt = PartialMergeReceipt {
            request: request.clone(),
            merge_commit_id: uuid::Uuid::from_u128(22).to_string(),
        };
        request.captured_local_checkpoint_commit_id = request.checkpoint_commit_id.clone();
        assert!(receipt.validate_for(&request).is_err());
        request.expected_authority_checkpoint_commit_id = "invalid".into();
        assert!(request.validate().is_err());
    }
}
