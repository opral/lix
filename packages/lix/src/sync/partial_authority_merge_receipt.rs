use super::partial_merge_protocol::{PartialMergeReceipt, PartialMergeRequest};
// Immutable authority acknowledgment of one exact reconciliation attempt.
// Staged with M and its expected-R branch CAS, never after publication.
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::{LixError, changelog::CommitId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) const PARTIAL_AUTHORITY_MERGE_RECEIPT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_001d),
    "sync.partial_authority_merge_receipt.v1",
    ValueSemantics::Immutable,
);
const MAX_BYTES: usize = 4096;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredReceipt {
    version: u32,
    repository_id: String,
    account_id: String,
    receipt: PartialMergeReceipt,
}
fn invalid(message: &str) -> LixError {
    LixError::new("LIX_PARTIAL_MERGE_RECEIPT_INVALID", message)
}
fn uuid(value: &str) -> Result<[u8; 16], LixError> {
    crate::storage_codec::id_string::uuid_bytes_from_canonical(value)
        .ok_or_else(|| invalid("merge receipt coordinates must be canonical UUIDs"))
}
// Identity binds every authority/account/branch attempt independently. Reusing
// this key with changed coordinates is a conflict, not a second execution.
fn key(
    repository: &str,
    account: &str,
    request: &PartialMergeRequest,
) -> Result<StorageKey, LixError> {
    request.validate()?;
    let mut value = Vec::with_capacity(64);
    for part in [repository, account, &request.branch_id, &request.attempt_id] {
        value.extend_from_slice(&uuid(part)?);
    }
    Ok(StorageKey(Bytes::from(value)))
}
pub(crate) async fn load_authority_merge_receipt(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &PartialMergeRequest,
) -> Result<Option<PartialMergeReceipt>, LixError> {
    let address = key(repository, account, request)?;
    let values = PointReadPlan::new(PARTIAL_AUTHORITY_MERGE_RECEIPT_SPACE, &[address])
        .materialize(read, Default::default())
        .await?;
    let Some(value) = values.value.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(invalid("receipt read returned an incomplete value"));
    };
    if bytes.len() > MAX_BYTES {
        return Err(invalid("merge receipt exceeds fixed bound"));
    }
    let record: StoredReceipt =
        serde_json::from_slice(&bytes).map_err(|_| invalid("malformed merge receipt"))?;
    record.receipt.request.validate()?;
    uuid(&record.receipt.merge_commit_id)?;
    if record.version != 1
        || record.repository_id != repository
        || record.account_id != account
        || record.receipt.request.branch_id != request.branch_id
        || record.receipt.request.attempt_id != request.attempt_id
    {
        return Err(invalid(
            "merge receipt identity disagrees with its storage key",
        ));
    }
    if &record.receipt.request != request {
        return Err(LixError::new(
            "LIX_PARTIAL_MERGE_ATTEMPT_REUSED",
            "merge attempt ID was previously committed with different coordinates",
        ));
    }
    Ok(Some(record.receipt))
}

/// Capability constructed only after an authority-owned plan has staged its
/// native merge commit. This type does not itself certify a plan or lease.
pub(crate) struct PreparedAuthorityMergeReceipt {
    repository_id: String,
    account_id: String,
    receipt: PartialMergeReceipt,
    writes: StorageWriteSet,
    guards: Vec<StoragePrecondition>,
}
impl PreparedAuthorityMergeReceipt {
    pub(super) fn new(
        repository: &str,
        account: &str,
        request: PartialMergeRequest,
        merge: CommitId,
        source_control_guards: Vec<StoragePrecondition>,
    ) -> Result<Self, LixError> {
        let address = key(repository, account, &request)?;
        let receipt = PartialMergeReceipt {
            request,
            merge_commit_id: merge.to_string(),
        };
        let record = StoredReceipt {
            version: 1,
            repository_id: repository.into(),
            account_id: account.into(),
            receipt: receipt.clone(),
        };
        let bytes =
            serde_json::to_vec(&record).map_err(|_| invalid("merge receipt encoding failed"))?;
        if bytes.len() > MAX_BYTES {
            return Err(invalid("merge receipt exceeds fixed bound"));
        }
        let mut writes = StorageWriteSet::new();
        writes.put(
            PARTIAL_AUTHORITY_MERGE_RECEIPT_SPACE,
            address.clone(),
            StorageValue {
                bytes: bytes.into(),
            },
        );
        let mut guards = source_control_guards;
        guards.push(StoragePrecondition::KeyAbsent {
            space: PARTIAL_AUTHORITY_MERGE_RECEIPT_SPACE,
            key: address,
        });
        Ok(Self {
            repository_id: repository.into(),
            account_id: account.into(),
            receipt,
            writes,
            guards,
        })
    }
    pub(crate) fn repository_id(&self) -> &str {
        &self.repository_id
    }
    pub(crate) fn receipt(&self) -> &PartialMergeReceipt {
        &self.receipt
    }
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }
    pub(crate) fn branch_id(&self) -> &str {
        &self.receipt.request.branch_id
    }
    pub(crate) fn merge_commit_id(&self) -> Result<CommitId, LixError> {
        CommitId::parse_lix(&self.receipt.merge_commit_id, "prepared authority merge")
    }
    pub(crate) async fn with_native_retention(
        mut self,
        read: &(impl StorageAdapterRead + ?Sized),
        now_ms: u64,
    ) -> Result<Self, LixError> {
        let identity = crate::gc::NativeUploadAttemptIdentity {
            repository_id: self.repository_id.clone(),
            account_id: self.account_id.clone(),
            branch_id: self.receipt.request.branch_id.clone(),
            attempt_id: self.receipt.request.attempt_id.clone(),
        };
        let mut writes = StorageWriteSet::new();
        let guards = crate::gc::stage_finalize_native_upload_attempt(
            read,
            &mut writes,
            &identity,
            &self,
            now_ms,
        )
        .await?;
        self.writes.extend(writes);
        self.guards.extend(guards);
        Ok(self)
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        StorageWriteSet,
        Vec<StoragePrecondition>,
        PartialMergeReceipt,
    ) {
        (self.writes, self.guards, self.receipt)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
    fn id(n: u128) -> String {
        uuid::Uuid::from_u128(n).to_string()
    }
    fn request() -> PartialMergeRequest {
        PartialMergeRequest {
            attempt_id: id(1),
            branch_id: id(2),
            base_commit_id: id(3),
            expected_authority_head_commit_id: id(4),
            captured_local_head_commit_id: id(5),
            expected_authority_checkpoint_commit_id: id(6),
            captured_local_checkpoint_commit_id: id(6),
            checkpoint_commit_id: id(6),
            global_head_commit_id: id(7),
            global_checkpoint_commit_id: id(8),
        }
    }
    #[tokio::test]
    async fn exact_receipt_retry_is_idempotent_and_attempt_reuse_rejected() {
        let storage = StorageAdapter::new(crate::Memory::new());
        let request = request();
        let prepared = PreparedAuthorityMergeReceipt::new(
            &id(9),
            &id(10),
            request.clone(),
            CommitId::parse_lix(&id(11), "test").unwrap(),
            vec![],
        )
        .unwrap();
        let (writes, preconditions, receipt) = prepared.into_parts();
        storage
            .commit_write_set(
                writes,
                StorageWriteOptions {
                    preconditions,
                    await_durable: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let read = storage.begin_read(Default::default()).await.unwrap();
        assert_eq!(
            load_authority_merge_receipt(&read, &id(9), &id(10), &request)
                .await
                .unwrap(),
            Some(receipt)
        );
        assert!(
            load_authority_merge_receipt(&read, &id(9), &id(12), &request)
                .await
                .unwrap()
                .is_none()
        );
        let mut altered = request;
        altered.captured_local_head_commit_id = id(13);
        assert_eq!(
            load_authority_merge_receipt(&read, &id(9), &id(10), &altered)
                .await
                .unwrap_err()
                .code,
            "LIX_PARTIAL_MERGE_ATTEMPT_REUSED"
        );
    }
}

pub(super) fn absent_receipt_guard(
    repository: &str,
    account: &str,
    request: &PartialMergeRequest,
) -> Result<StoragePrecondition, LixError> {
    Ok(StoragePrecondition::KeyAbsent {
        space: PARTIAL_AUTHORITY_MERGE_RECEIPT_SPACE,
        key: key(repository, account, request)?,
    })
}
