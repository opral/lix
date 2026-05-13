use super::codec;
use super::types::{
    ByChangeEntry, ByCommitEntry, ChangeLoadBatch, ChangeLoadRequest, CommitLoadBatch,
    CommitLoadRequest, CommitVisibility, GcPlan, GcRoot, RebuildIndexStats, Segment,
    StateRowIdentity,
};
use crate::common::LixError;
use async_trait::async_trait;

pub(crate) const SEGMENT_NAMESPACE: &str = "changelog.segment";
pub(crate) const COMMIT_VISIBILITY_NAMESPACE: &str = "changelog.commit_visibility";
pub(crate) const BY_COMMIT_INDEX_NAMESPACE: &str = "changelog.index.by_commit";
pub(crate) const BY_CHANGE_INDEX_NAMESPACE: &str = "changelog.index.by_change";
pub(crate) const BY_CHANGE_MEMBERSHIP_INDEX_NAMESPACE: &str =
    "changelog.index.by_change_membership";
pub(crate) const BY_KEY_VALUE_INDEX_NAMESPACE: &str = "changelog.index.by_key_value";
pub(crate) const BY_KEY_COMMIT_INDEX_NAMESPACE: &str = "changelog.index.by_key_commit";

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

pub(crate) fn by_key_value_prefix(identity: &StateRowIdentity) -> Vec<u8> {
    state_row_identity_tuple(identity)
}

pub(crate) fn by_key_value_key(identity: &StateRowIdentity, change_id: &str) -> Vec<u8> {
    let mut key = by_key_value_prefix(identity);
    push_ordered_str(&mut key, change_id);
    key
}

pub(crate) fn by_key_commit_prefix(identity: &StateRowIdentity) -> Vec<u8> {
    state_row_identity_tuple(identity)
}

pub(crate) fn by_key_commit_key(
    identity: &StateRowIdentity,
    commit_id: &str,
    member_change_id: &str,
) -> Vec<u8> {
    let mut key = by_key_commit_prefix(identity);
    push_ordered_str(&mut key, commit_id);
    push_ordered_str(&mut key, member_change_id);
    key
}

pub(crate) fn segment_value(segment: &Segment) -> Result<Vec<u8>, LixError> {
    codec::encode_segment(segment)
}

pub(crate) fn commit_visibility_value(visibility: &CommitVisibility) -> Result<Vec<u8>, LixError> {
    codec::encode_commit_visibility(visibility)
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

pub(crate) fn by_key_value_index_value() -> Vec<u8> {
    codec::encode_empty_index_value()
}

pub(crate) fn by_key_commit_index_value() -> Vec<u8> {
    codec::encode_empty_index_value()
}

fn identity_key(id: &str) -> Vec<u8> {
    id.as_bytes().to_vec()
}

fn state_row_identity_tuple(identity: &StateRowIdentity) -> Vec<u8> {
    let mut key = Vec::new();
    push_ordered_str(&mut key, identity.schema_key.as_str());
    push_ordered_str(&mut key, identity.file_id.as_str());
    push_ordered_str(&mut key, identity.entity_id.as_str());
    key
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
    async fn stage_segment(&mut self, segment: Segment) -> Result<(), LixError>;

    async fn stage_publish_commit(&mut self, commit_id: &str) -> Result<(), LixError>;

    async fn collect_garbage(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError>;

    async fn stage_gc_sweep(&mut self, plan: &GcPlan) -> Result<(), LixError>;

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
        assert_eq!(BY_KEY_VALUE_INDEX_NAMESPACE, "changelog.index.by_key_value");
        assert_eq!(
            BY_KEY_COMMIT_INDEX_NAMESPACE,
            "changelog.index.by_key_commit"
        );
    }

    #[test]
    fn identity_keys_use_utf8_bytes() {
        assert_eq!(segment_key("segment-1"), b"segment-1".to_vec());
        assert_eq!(commit_visibility_key("commit-1"), b"commit-1".to_vec());
        assert_eq!(by_commit_key("commit-1"), b"commit-1".to_vec());
        assert_eq!(by_change_key("change-1"), b"change-1".to_vec());
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
    fn by_key_indexes_are_prefixed_by_state_row_identity() {
        let identity = state_row_identity("message", "app/en.json", "title");

        let value_prefix = by_key_value_prefix(&identity);
        let value_key = by_key_value_key(&identity, "change-1");
        assert!(value_key.starts_with(&value_prefix));

        let commit_prefix = by_key_commit_prefix(&identity);
        let commit_key = by_key_commit_key(&identity, "commit-1", "change-1");
        assert!(commit_key.starts_with(&commit_prefix));

        assert_eq!(value_prefix, commit_prefix);
    }

    #[test]
    fn ordered_string_encoding_preserves_component_boundaries() {
        let a = state_row_identity("message", "file", "a");
        let aa = state_row_identity("message", "file", "aa");
        let b = state_row_identity("message", "file", "b");

        assert!(by_key_value_prefix(&a) < by_key_value_prefix(&aa));
        assert!(by_key_value_prefix(&aa) < by_key_value_prefix(&b));

        let key = by_key_value_key(&a, "change-1");
        let neighboring_identity = state_row_identity("message", "file", "a-sibling");
        assert!(!by_key_value_key(&neighboring_identity, "change-1")
            .starts_with(&by_key_value_prefix(&a)));
        assert!(key.starts_with(&by_key_value_prefix(&a)));
    }

    #[test]
    fn by_key_index_values_are_empty() {
        assert!(by_change_membership_index_value().is_empty());
        assert!(by_key_value_index_value().is_empty());
        assert!(by_key_commit_index_value().is_empty());
    }

    fn state_row_identity(schema_key: &str, file_id: &str, entity_id: &str) -> StateRowIdentity {
        StateRowIdentity {
            schema_key: schema_key.try_into().unwrap(),
            file_id: file_id.try_into().unwrap(),
            entity_id: entity_id.try_into().unwrap(),
        }
    }
}
