use super::codec;
use super::types::{
    ByChangeEntry, ByCommitEntry, ChangeLoadBatch, ChangeLoadRequest, CommitId, CommitLoadBatch,
    CommitLoadRequest, CommitVisibility, GcPlan, GcRoot, RebuildIndexStats, Segment,
    SegmentStageReport,
};
use crate::common::LixError;
use crate::storage::{StorageSpace, StorageSpaceId};
use async_trait::async_trait;

pub(crate) const SEGMENT_NAMESPACE: &str = "changelog.segment";
pub(crate) const COMMIT_VISIBILITY_NAMESPACE: &str = "changelog.commit_visibility";
pub(crate) const BY_COMMIT_INDEX_NAMESPACE: &str = "changelog.index.by_commit";
pub(crate) const BY_CHANGE_INDEX_NAMESPACE: &str = "changelog.index.by_change";
pub(crate) const BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE: &str =
    "changelog.index.by_change_membership";
pub(crate) const VISIBLE_CHANGE_PROOF_NAMESPACE: &str = "changelog.index.visible_change";

pub const SEGMENT_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0001), SEGMENT_NAMESPACE);
pub const COMMIT_VISIBILITY_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0002), COMMIT_VISIBILITY_NAMESPACE);
pub(crate) const BY_COMMIT_INDEX_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0003), BY_COMMIT_INDEX_NAMESPACE);
pub(crate) const BY_CHANGE_INDEX_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0004), BY_CHANGE_INDEX_NAMESPACE);
pub(crate) const BY_CHANGE_MEMBERSHIP_INDEX_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0006_0005),
    BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
);
pub(crate) const VISIBLE_CHANGE_PROOF_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0006), VISIBLE_CHANGE_PROOF_NAMESPACE);

pub(crate) fn segment_key(segment_id: &str) -> Vec<u8> {
    identity_key(segment_id)
}

pub(crate) fn commit_visibility_key(commit_id: &str) -> Vec<u8> {
    identity_key(commit_id)
}

pub(crate) fn by_commit_key(commit_id: &str) -> Vec<u8> {
    identity_key(commit_id)
}

pub(crate) fn by_change_key(change_id: &str) -> Vec<u8> {
    identity_key(change_id)
}

pub(crate) fn visible_change_proof_key(change_id: &str) -> Vec<u8> {
    identity_key(change_id)
}

pub(crate) fn by_change_membership_prefix(change_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    push_ordered_str(&mut key, change_id);
    key
}

pub(crate) fn by_change_membership_key(change_id: &str, commit_id: &str) -> Vec<u8> {
    let mut key = by_change_membership_prefix(change_id);
    push_ordered_str(&mut key, commit_id);
    key
}

pub(crate) fn by_change_membership_commit_id_from_key(
    change_id: &str,
    key: &[u8],
) -> Result<Option<String>, LixError> {
    let prefix = by_change_membership_prefix(change_id);
    let Some(rest) = key.strip_prefix(prefix.as_slice()) else {
        return Ok(None);
    };
    let (commit_id, consumed) = read_ordered_str(rest)?;
    if consumed != rest.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog by_change_membership key has trailing bytes",
        ));
    }
    Ok(Some(commit_id))
}

pub(crate) fn by_change_membership_ids_from_key(key: &[u8]) -> Result<(String, String), LixError> {
    let (change_id, consumed_change) = read_ordered_str(key)?;
    let (commit_id, consumed_commit) = read_ordered_str(&key[consumed_change..])?;
    if consumed_change + consumed_commit != key.len() {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog by_change_membership key has trailing bytes",
        ));
    }
    Ok((change_id, commit_id))
}

pub(crate) fn segment_value(segment: &Segment) -> Result<Vec<u8>, LixError> {
    codec::encode_segment(segment)
}

pub(crate) fn commit_visibility_value(visibility: &CommitVisibility) -> Result<Vec<u8>, LixError> {
    codec::encode_commit_visibility(visibility)
}

pub(crate) fn visible_change_proof_value(commit_id: &CommitId) -> Vec<u8> {
    identity_key(commit_id)
}

pub(crate) fn visible_change_proof_commit_id_from_value(
    bytes: &[u8],
) -> Result<CommitId, LixError> {
    String::from_utf8(bytes.to_vec()).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("visible_change proof value contains invalid UTF-8 commit id: {error}"),
        )
    })
}

pub(crate) fn by_commit_index_value(entry: &ByCommitEntry) -> Result<Vec<u8>, LixError> {
    codec::encode_by_commit_entry(entry)
}

pub(crate) fn by_change_index_value(entry: &ByChangeEntry) -> Result<Vec<u8>, LixError> {
    codec::encode_by_change_entry(entry)
}

pub(crate) fn by_change_membership_index_value() -> Vec<u8> {
    codec::encode_empty_index_value()
}

fn identity_key(id: &str) -> Vec<u8> {
    id.as_bytes().to_vec()
}

fn push_ordered_str(out: &mut Vec<u8>, value: &str) {
    for byte in value.as_bytes() {
        out.push(*byte);
        if *byte == 0 {
            out.push(0xff);
        }
    }
    out.push(0);
}

fn read_ordered_str(bytes: &[u8]) -> Result<(String, usize), LixError> {
    let mut out = Vec::new();
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            0 => {
                if bytes.get(index + 1) == Some(&0xff) {
                    out.push(0);
                    index += 2;
                    continue;
                }
                let value = String::from_utf8(out).map_err(|error| {
                    LixError::new(
                        LixError::CODE_INTERNAL_ERROR,
                        format!("changelog ordered key contains invalid UTF-8: {error}"),
                    )
                })?;
                return Ok((value, index + 1));
            }
            byte => {
                out.push(byte);
                index += 1;
            }
        }
    }
    Err(LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        "changelog ordered key component is missing terminator",
    ))
}

#[async_trait]
pub(crate) trait ChangelogReader {
    async fn plan_gc(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError>;

    async fn load_commits(
        &mut self,
        request: CommitLoadRequest<'_>,
    ) -> Result<CommitLoadBatch, LixError>;

    async fn load_changes(
        &mut self,
        request: ChangeLoadRequest<'_>,
    ) -> Result<ChangeLoadBatch, LixError>;
}

#[async_trait]
pub(crate) trait ChangelogWriter {
    async fn stage_segment(&mut self, segment: Segment) -> Result<SegmentStageReport, LixError>;

    async fn stage_publish_commit(&mut self, commit_id: &str) -> Result<(), LixError>;

    async fn collect_garbage(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError>;

    async fn rebuild_mandatory_indexes(&mut self) -> Result<RebuildIndexStats, LixError>;

    async fn rebuild_by_commit_index(&mut self) -> Result<RebuildIndexStats, LixError>;

    async fn rebuild_by_change_index(&mut self) -> Result<RebuildIndexStats, LixError>;

    async fn rebuild_by_change_membership_index(&mut self) -> Result<RebuildIndexStats, LixError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_are_stable() {
        assert_eq!(SEGMENT_NAMESPACE, "changelog.segment");
        assert_eq!(COMMIT_VISIBILITY_NAMESPACE, "changelog.commit_visibility");
        assert_eq!(BY_COMMIT_INDEX_NAMESPACE, "changelog.index.by_commit");
        assert_eq!(BY_CHANGE_INDEX_NAMESPACE, "changelog.index.by_change");
        assert_eq!(
            BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE,
            "changelog.index.by_change_membership"
        );
        assert_eq!(
            VISIBLE_CHANGE_PROOF_NAMESPACE,
            "changelog.index.visible_change"
        );
    }

    #[test]
    fn identity_keys_use_utf8_bytes() {
        assert_eq!(segment_key("segment-1"), b"segment-1".to_vec());
        assert_eq!(commit_visibility_key("commit-1"), b"commit-1".to_vec());
        assert_eq!(by_commit_key("commit-1"), b"commit-1".to_vec());
        assert_eq!(by_change_key("change-1"), b"change-1".to_vec());
        assert_eq!(visible_change_proof_key("change-1"), b"change-1".to_vec());
    }

    #[test]
    fn by_change_membership_keys_are_prefixed_by_change_id() {
        let prefix = by_change_membership_prefix("change-1");
        let key = by_change_membership_key("change-1", "commit-1");
        assert!(key.starts_with(&prefix));
        assert!(!by_change_membership_key("change-10", "commit-1").starts_with(&prefix));
        assert_eq!(
            by_change_membership_commit_id_from_key("change-1", &key).unwrap(),
            Some("commit-1".to_string())
        );
        assert_eq!(
            by_change_membership_commit_id_from_key(
                "change-2",
                &by_change_membership_key("change-1", "commit-1")
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn by_change_membership_index_values_are_empty() {
        assert!(by_change_membership_index_value().is_empty());
    }
}
