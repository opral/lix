//! Terminal source-owner restart. M and abort are mutually exclusive immutable
//! outcomes; no failed RPC is itself permission to release a native root pin.
use super::NATIVE_GLOBAL_MIGRATION_RECEIPT_SPACE as SPACE;
use super::{NativeGlobalMigrationReceipt, NativeGlobalMigrationRequest};
use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, StorageAdapterRead, StorageKey, StoragePrecondition, StorageProjectedValue,
    StorageValue, StorageWriteSet,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct NativeGlobalRestartRequest {
    pub request: NativeGlobalMigrationRequest,
    pub next_attempt_id: String,
}
impl NativeGlobalRestartRequest {
    pub(crate) fn validate(&self) -> Result<(), LixError> {
        self.request.validate()?;
        uuid(&self.next_attempt_id)?;
        if self.next_attempt_id == self.request.attempt_id {
            return Err(invalid());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "camelCase", deny_unknown_fields)]
pub(crate) enum NativeGlobalRestartReceipt {
    Committed {
        receipt: NativeGlobalMigrationReceipt,
    },
    Restarted {
        intent: NativeGlobalRestartRequest,
    },
}
impl NativeGlobalRestartReceipt {
    pub(crate) fn validate_for(&self, intent: &NativeGlobalRestartRequest) -> Result<(), LixError> {
        intent.validate()?;
        match self {
            Self::Committed { receipt } => {
                receipt.validate()?;
                if receipt.request != intent.request {
                    return Err(invalid());
                }
            }
            Self::Restarted { intent: echo } => {
                if echo != intent {
                    return Err(invalid());
                }
            }
        }
        Ok(())
    }
}
#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct StoredAbort {
    version: u32,
    repository: String,
    account: String,
    intent: NativeGlobalRestartRequest,
}
fn invalid() -> LixError {
    LixError::new(
        "LIX_MIGRATION_GLOBAL_RESTART_INVALID",
        "global restart does not match its exact authenticated immutable intent",
    )
}
fn uuid(s: &str) -> Result<[u8; 16], LixError> {
    crate::storage_codec::id_string::uuid_bytes_from_canonical(s).ok_or_else(invalid)
}
pub(super) fn abort_key(
    repository: &str,
    account: &str,
    attempt: &str,
) -> Result<StorageKey, LixError> {
    let mut bytes = b"global-abort-v1\0".to_vec();
    for s in [repository, account, attempt] {
        bytes.extend_from_slice(&uuid(s)?);
    }
    Ok(StorageKey(Bytes::from(bytes)))
}
async fn load_abort(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &NativeGlobalMigrationRequest,
) -> Result<Option<NativeGlobalRestartRequest>, LixError> {
    let values = PointReadPlan::new(
        SPACE,
        &[abort_key(repository, account, &request.attempt_id)?],
    )
    .materialize(read, Default::default())
    .await?
    .value;
    let Some(value) = values.into_iter().next().flatten() else {
        return Ok(None);
    };
    let StorageProjectedValue::FullValue(bytes) = value else {
        return Err(invalid());
    };
    if bytes.len() > 256 * 1024 {
        return Err(invalid());
    }
    let state: StoredAbort = serde_json::from_slice(&bytes).map_err(|_| invalid())?;
    state.intent.validate()?;
    if state.version != 1
        || state.repository != repository
        || state.account != account
        || state.intent.request != *request
    {
        return Err(invalid());
    }
    Ok(Some(state.intent))
}
pub(crate) async fn require_unaborted_global_migration(
    read: &(impl StorageAdapterRead + ?Sized),
    repository: &str,
    account: &str,
    request: &NativeGlobalMigrationRequest,
) -> Result<StoragePrecondition, LixError> {
    if load_abort(read, repository, account, request)
        .await?
        .is_some()
    {
        return Err(LixError::new(
            "LIX_MIGRATION_GLOBAL_ATTEMPT_RESTARTED",
            "recover the durable restart outcome and retry its exact successor attempt",
        ));
    }
    Ok(StoragePrecondition::KeyAbsent {
        space: SPACE,
        key: abort_key(repository, account, &request.attempt_id)?,
    })
}
pub(crate) struct AuthorizedGlobalRestart {
    repository: String,
    account: String,
    request: NativeGlobalMigrationRequest,
}
impl AuthorizedGlobalRestart {
    pub(crate) fn repository(&self) -> &str {
        &self.repository
    }
    pub(crate) fn account(&self) -> &str {
        &self.account
    }
    pub(crate) fn request(&self) -> &NativeGlobalMigrationRequest {
        &self.request
    }
}
pub(crate) async fn stage_restart_native_global_migration(
    read: &(impl StorageAdapterRead + ?Sized),
    writes: &mut StorageWriteSet,
    repository: &str,
    account: &str,
    intent: &NativeGlobalRestartRequest,
) -> Result<(NativeGlobalRestartReceipt, Vec<StoragePrecondition>), LixError> {
    intent.validate()?;
    if let Some(receipt) =
        super::native_global_migration_receipt::load_native_global_migration_receipt(
            read,
            repository,
            account,
            &intent.request,
        )
        .await?
    {
        return Ok((NativeGlobalRestartReceipt::Committed { receipt }, vec![]));
    }
    if let Some(existing) = load_abort(read, repository, account, &intent.request).await? {
        if &existing != intent {
            return Err(invalid());
        }
        return Ok((
            NativeGlobalRestartReceipt::Restarted { intent: existing },
            vec![],
        ));
    }
    let authorization = AuthorizedGlobalRestart {
        repository: repository.into(),
        account: account.into(),
        request: intent.request.clone(),
    };
    let mut guards =
        crate::gc::stage_abandon_global_migration_pin(read, writes, &authorization).await?;
    guards.push(
        super::native_global_migration_receipt::uncommitted_global_migration_guard(
            read,
            repository,
            account,
            &intent.request,
        )
        .await?,
    );
    let address = abort_key(repository, account, &intent.request.attempt_id)?;
    guards.push(StoragePrecondition::KeyAbsent {
        space: SPACE,
        key: address.clone(),
    });
    let record = StoredAbort {
        version: 1,
        repository: repository.into(),
        account: account.into(),
        intent: intent.clone(),
    };
    let bytes = serde_json::to_vec(&record).map_err(|_| invalid())?;
    if bytes.len() > 256 * 1024 {
        return Err(invalid());
    }
    writes.put(
        SPACE,
        address,
        StorageValue {
            bytes: bytes.into(),
        },
    );
    Ok((
        NativeGlobalRestartReceipt::Restarted {
            intent: intent.clone(),
        },
        guards,
    ))
}
