use super::types::{
    ChangeId, ChangeLoadBatch, ChangeLoadRequest, ChangeScanBatch, ChangeScanRequest,
    ChangelogAppend, CommitId, CommitLoadBatch, CommitLoadRequest, CommitScanBatch,
    CommitScanRequest, GcPlan, GcRoot,
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

// Identity keys are the raw 16 UUID bytes. UUIDv7's big-endian byte order
// matches the lexicographic order of its lowercase hyphenated text, so range
// scans and resume tokens behave identically to the former text keys at
// 20 fewer bytes per key.
pub(crate) fn commit_key(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

pub(crate) fn change_key(change_id: ChangeId) -> Vec<u8> {
    change_id.as_uuid().as_bytes().to_vec()
}

pub(crate) fn commit_change_ref_chunk_prefix(commit_id: CommitId) -> Vec<u8> {
    commit_id.as_uuid().as_bytes().to_vec()
}

pub(crate) fn commit_change_ref_chunk_key(commit_id: CommitId, chunk_no: u32) -> Vec<u8> {
    let mut key = commit_change_ref_chunk_prefix(commit_id);
    key.extend_from_slice(&chunk_no.to_be_bytes());
    key
}

pub(crate) fn commit_id_from_key(key: &[u8]) -> Result<CommitId, LixError> {
    uuid_from_key(key, "commit").map(CommitId::new)
}

pub(crate) fn change_id_from_key(key: &[u8]) -> Result<ChangeId, LixError> {
    uuid_from_key(key, "change").map(ChangeId::new)
}

fn uuid_from_key(key: &[u8], kind: &str) -> Result<uuid::Uuid, LixError> {
    uuid::Uuid::from_slice(key).map_err(|error| {
        LixError::new(
            LixError::CODE_INTERNAL_ERROR,
            format!("changelog {kind} key is not a 16-byte uuid: {error}"),
        )
    })
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
    fn identity_keys_use_raw_uuid_bytes() {
        let commit_id = CommitId::for_test_label("commit-1");
        let change_id = ChangeId::for_test_label("change-1");
        assert_eq!(
            commit_key(commit_id),
            commit_id.as_uuid().as_bytes().to_vec()
        );
        assert_eq!(
            change_key(change_id),
            change_id.as_uuid().as_bytes().to_vec()
        );
        assert_eq!(commit_key(commit_id).len(), 16);
    }

    #[test]
    fn identity_key_order_matches_text_order() {
        let mut ids = (0..32)
            .map(|index| CommitId::for_test_label(&format!("commit-{index}")))
            .collect::<Vec<_>>();
        ids.sort_by_key(|id| commit_key(*id));
        let text_sorted = {
            let mut text = ids.iter().map(CommitId::to_string).collect::<Vec<_>>();
            text.sort();
            text
        };
        assert_eq!(
            ids.iter().map(CommitId::to_string).collect::<Vec<_>>(),
            text_sorted,
            "binary key order must match hyphenated text order"
        );
    }

    #[test]
    fn commit_change_ref_chunk_keys_are_prefixed_by_commit_id() {
        let commit_id = CommitId::for_test_label("commit-1");
        let other_commit_id = CommitId::for_test_label("commit-10");
        let prefix = commit_change_ref_chunk_prefix(commit_id);
        let key = commit_change_ref_chunk_key(commit_id, 42);
        assert!(key.starts_with(&prefix));
        assert!(!commit_change_ref_chunk_key(other_commit_id, 0).starts_with(&prefix));
        assert_eq!(&key[..16], commit_id.as_uuid().as_bytes());
        assert_eq!(&key[16..], 42u32.to_be_bytes());
    }
}
