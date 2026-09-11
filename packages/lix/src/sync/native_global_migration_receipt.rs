use super::native_global_migration_protocol::{
    NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest,
};
// Immutable authority acknowledgment of one exact reconciliation attempt.
// Staged with M and its expected-R branch CAS, never after publication.
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageSpace, StorageSpaceId, StorageValue, StorageWriteSet, ValueSemantics,
};
use crate::{LixError, changelog::CommitId};
use bytes::Bytes;
use serde::{Deserialize, Serialize};

pub(crate) const NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE: StorageSpace = StorageSpace::declare(
    StorageSpaceId(0x0007_0020),
    "sync.native_global_migration_receipt.v1",
    ValueSemantics::Immutable,
);
const MAX_BYTES: usize = 256 * 1024;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredReceipt {
    version: u32,
    repository_id: String,
    account_id: String,
    receipt: NativeGlobalMigrationReceipt,
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
    request: &NativeGlobalMigrationRequest,
) -> Result<StorageKey, LixError> {
    request.validate()?;
    let mut value = Vec::with_capacity(64);
    for part in [repository, account, &request.attempt_id] {
        value.extend_from_slice(&uuid(part)?);
    }
    Ok(StorageKey(Bytes::from(value)))
}
pub(crate) async fn load_native_global_migration_receipt(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &NativeGlobalMigrationRequest,
) -> Result<Option<NativeGlobalMigrationReceipt>, LixError> {
    let address = key(repository, account, request)?;
    let values = PointReadPlan::new(NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE, &[address])
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
    record.receipt.validate()?;
    if record.version != 1
        || record.repository_id != repository
        || record.account_id != account
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
pub(crate) struct PreparedNativeGlobalMigrationReceipt {
    repository_id: String,
    account_id: String,
    receipt: NativeGlobalMigrationReceipt,
    writes: StorageWriteSet,
    guards: Vec<StoragePrecondition>,
}
impl PreparedNativeGlobalMigrationReceipt {
    pub(super) fn new(
        repository: &str,
        account: &str,
        request: NativeGlobalMigrationRequest,
        merge: CommitId,
        source_control_guards: Vec<StoragePrecondition>,
    ) -> Result<Self, LixError> {
        let address = key(repository, account, &request)?;
        let receipt = NativeGlobalMigrationReceipt {
            request,
            merge_commit_id: merge.to_string(),
        };
        receipt.validate()?;
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
            NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE,
            address.clone(),
            StorageValue {
                bytes: bytes.into(),
            },
        );
        let mut guards = source_control_guards;
        guards.push(StoragePrecondition::KeyAbsent {
            space: NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE,
            key: super::native_global_migration_restart::abort_key(
                repository,
                account,
                &receipt.request.attempt_id,
            )?,
        });
        guards.push(StoragePrecondition::KeyAbsent {
            space: NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE,
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
    pub(crate) fn receipt(&self) -> &NativeGlobalMigrationReceipt {
        &self.receipt
    }
    pub(crate) fn account_id(&self) -> &str {
        &self.account_id
    }
    pub(crate) fn merge_commit_id(&self) -> Result<CommitId, LixError> {
        CommitId::parse_lix(&self.receipt.merge_commit_id, "prepared authority merge")
    }
    pub(crate) fn into_parts(
        self,
    ) -> (
        StorageWriteSet,
        Vec<StoragePrecondition>,
        NativeGlobalMigrationReceipt,
    ) {
        (self.writes, self.guards, self.receipt)
    }
}

pub(crate) async fn uncommitted_global_migration_guard(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &NativeGlobalMigrationRequest,
) -> Result<StoragePrecondition, LixError> {
    if load_native_global_migration_receipt(read, repository, account, request)
        .await?
        .is_some()
    {
        return Err(LixError::new(
            "LIX_MIGRATION_GLOBAL_ALREADY_COMMITTED",
            "recover the exact committed global migration outcome",
        ));
    }
    Ok(StoragePrecondition::KeyAbsent {
        space: NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE,
        key: key(repository, account, request)?,
    })
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{StorageAdapter, StorageWriteOptions};
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
            new_branches: vec![super::super::NativeNewBranchCoordinate {
                branch_id: id(6),
                head_commit_id: id(7),
                checkpoint_commit_id: id(8),
            }],
        }
    }
    #[tokio::test]
    async fn lost_global_outcome_response_reloads_exact_new_ref_coordinates() {
        let storage = StorageAdapter::new(crate::Memory::new());
        let request = request();
        let prepared = PreparedNativeGlobalMigrationReceipt::new(
            &id(9),
            &id(10),
            request.clone(),
            CommitId::parse_lix(&id(11), "test").unwrap(),
            vec![],
        )
        .unwrap();
        let (writes, preconditions, receipt) = prepared.into_parts();
        let read = storage.begin_read(Default::default()).await.unwrap();
        let late_body_guard = uncommitted_global_migration_guard(&read, &id(9), &id(10), &request)
            .await
            .unwrap();
        drop(read);
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
            load_native_global_migration_receipt(&read, &id(9), &id(10), &request)
                .await
                .unwrap(),
            Some(receipt)
        );
        assert!(
            load_native_global_migration_receipt(&read, &id(9), &id(12), &request)
                .await
                .unwrap()
                .is_none()
        );
        let mut changed = request.clone();
        changed.new_branches[0].checkpoint_commit_id = id(13);
        assert_eq!(
            load_native_global_migration_receipt(&read, &id(9), &id(10), &changed)
                .await
                .unwrap_err()
                .code,
            "LIX_PARTIAL_MERGE_ATTEMPT_REUSED"
        );
        drop(read);
        assert!(
            storage
                .commit_write_set(
                    StorageWriteSet::new(),
                    StorageWriteOptions {
                        preconditions: vec![late_body_guard],
                        ..Default::default()
                    }
                )
                .await
                .is_err(),
            "in-flight body pin cannot recreate retention after exact global outcome"
        );
    }
}
