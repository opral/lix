use bytes::Bytes;

use crate::LixError;
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageAdapterRead, StorageGetOptions, StorageKey,
    StorageProjectedValue, StorageSpace, StorageWriteSet, ValueSemantics,
};

/// Repository bootstrap is owned by the authenticated ForkTree object and
/// selector spaces. The protocol marker remains a lifecycle guard only.
pub(crate) const REPOSITORY_PROTOCOL_SPACE: StorageSpace = StorageSpace::engine_declared(
    0x0004_0011,
    "repository.protocol.v1",
    ValueSemantics::Mutable,
);
pub(crate) const REPOSITORY_PROTOCOL_KEY: &[u8] = b"current";
const REPOSITORY_PROTOCOL_VALUE: &[u8] = b"immutable-physical-commit-state.v62";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RepositoryProtocolStatus {
    Current,
    Missing,
    Unsupported,
}

pub(crate) fn stage_repository_protocol(writes: &mut StorageWriteSet) {
    writes.put(
        REPOSITORY_PROTOCOL_SPACE,
        REPOSITORY_PROTOCOL_KEY,
        REPOSITORY_PROTOCOL_VALUE,
    );
}

pub(crate) async fn repository_protocol_status(
    read: &(impl StorageAdapterRead + ?Sized),
) -> Result<RepositoryProtocolStatus, LixError> {
    let values = PointReadPlan::new(
        REPOSITORY_PROTOCOL_SPACE,
        &[StorageKey(Bytes::from_static(REPOSITORY_PROTOCOL_KEY))],
    )
    .materialize(read, StorageGetOptions::default())
    .await?;
    Ok(match values.value.into_iter().next().flatten() {
        Some(StorageProjectedValue::FullValue(value))
            if value.as_ref() == REPOSITORY_PROTOCOL_VALUE =>
        {
            RepositoryProtocolStatus::Current
        }
        Some(_) => RepositoryProtocolStatus::Unsupported,
        None => RepositoryProtocolStatus::Missing,
    })
}

pub(crate) fn unsupported_repository_protocol_error() -> LixError {
    LixError::new(
        "LIX_ERROR_UNSUPPORTED_STORAGE_FORMAT",
        "repository uses an unsupported storage protocol; recreate the repository",
    )
}

pub(crate) fn already_initialized_error() -> LixError {
    LixError::new(
        "LIX_ERROR_ALREADY_INITIALIZED",
        "engine storage is already initialized",
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InitReceipt {
    pub lix_id: String,
    pub global_branch_id: String,
    pub main_branch_id: String,
    pub initial_commit_id: String,
}

pub(crate) async fn initialize<StorageImpl>(
    storage: StorageAdapter<StorageImpl>,
) -> Result<InitReceipt, LixError>
where
    StorageImpl: Storage + Clone + Send + Sync + 'static,
{
    let read = storage.begin_read(Default::default()).await?;
    match repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::Current => return Err(already_initialized_error()),
        RepositoryProtocolStatus::Unsupported => {
            return Err(unsupported_repository_protocol_error());
        }
        RepositoryProtocolStatus::Missing => {}
    }
    drop(read);
    crate::forktree::initialize_empty_repository(storage).await
}
