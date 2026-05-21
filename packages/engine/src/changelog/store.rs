use super::types::{
    ChangeLoadBatch, ChangeLoadRequest, ChangeScanBatch, ChangeScanRequest, ChangelogAppend,
    CommitLoadBatch, CommitLoadRequest, CommitScanBatch, CommitScanRequest, GcPlan, GcRoot,
};
use crate::common::LixError;
use crate::storage::{StorageSpace, StorageSpaceId};
use async_trait::async_trait;

pub(crate) const COMMIT_NAMESPACE: &str = "changelog.commit";
pub(crate) const CHANGE_NAMESPACE: &str = "changelog.change";
pub(crate) const COMMIT_CHANGE_REF_CHUNK_NAMESPACE: &str = "changelog.commit_change_ref_chunk";

pub(crate) const COMMIT_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0001), COMMIT_NAMESPACE);
pub(crate) const CHANGE_SPACE: StorageSpace =
    StorageSpace::new(StorageSpaceId(0x0006_0002), CHANGE_NAMESPACE);
pub(crate) const COMMIT_CHANGE_REF_CHUNK_SPACE: StorageSpace = StorageSpace::new(
    StorageSpaceId(0x0006_0003),
    COMMIT_CHANGE_REF_CHUNK_NAMESPACE,
);

pub(crate) fn commit_key(commit_id: &str) -> Vec<u8> {
    identity_key(commit_id)
}

pub(crate) fn change_key(change_id: &str) -> Vec<u8> {
    identity_key(change_id)
}

pub(crate) fn commit_change_ref_chunk_prefix(commit_id: &str) -> Vec<u8> {
    let mut key = Vec::new();
    push_ordered_str(&mut key, commit_id);
    key
}

pub(crate) fn commit_change_ref_chunk_key(commit_id: &str, chunk_no: u32) -> Vec<u8> {
    let mut key = commit_change_ref_chunk_prefix(commit_id);
    key.extend_from_slice(&chunk_no.to_be_bytes());
    key
}

pub(crate) fn commit_change_ref_chunk_ids_from_key(key: &[u8]) -> Result<(String, u32), LixError> {
    let (commit_id, consumed_commit) = read_ordered_str(key)?;
    let rest = &key[consumed_commit..];
    if rest.len() != 4 {
        return Err(LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            "changelog commit_change_ref_chunk key has invalid chunk number",
        ));
    }
    let chunk_no = u32::from_be_bytes([rest[0], rest[1], rest[2], rest[3]]);
    Ok((commit_id, chunk_no))
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

    async fn scan_commits(
        &mut self,
        request: CommitScanRequest<'_>,
    ) -> Result<CommitScanBatch, LixError>;

    async fn load_changes(
        &mut self,
        request: ChangeLoadRequest<'_>,
    ) -> Result<ChangeLoadBatch, LixError>;

    async fn scan_changes(
        &mut self,
        request: ChangeScanRequest<'_>,
    ) -> Result<ChangeScanBatch, LixError>;
}

#[async_trait]
pub(crate) trait ChangelogWriter {
    async fn stage_append(&mut self, append: ChangelogAppend) -> Result<(), LixError>;

    async fn collect_garbage(&mut self, roots: &[GcRoot]) -> Result<GcPlan, LixError>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn namespaces_are_stable() {
        assert_eq!(COMMIT_NAMESPACE, "changelog.commit");
        assert_eq!(CHANGE_NAMESPACE, "changelog.change");
        assert_eq!(
            COMMIT_CHANGE_REF_CHUNK_NAMESPACE,
            "changelog.commit_change_ref_chunk"
        );
    }

    #[test]
    fn identity_keys_use_utf8_bytes() {
        assert_eq!(commit_key("commit-1"), b"commit-1".to_vec());
        assert_eq!(change_key("change-1"), b"change-1".to_vec());
    }

    #[test]
    fn commit_change_ref_chunk_keys_are_prefixed_by_commit_id() {
        let prefix = commit_change_ref_chunk_prefix("commit-1");
        let key = commit_change_ref_chunk_key("commit-1", 42);
        assert!(key.starts_with(&prefix));
        assert!(!commit_change_ref_chunk_key("commit-10", 0).starts_with(&prefix));
        assert_eq!(
            commit_change_ref_chunk_ids_from_key(&key).unwrap(),
            ("commit-1".to_string(), 42)
        );
    }
}
