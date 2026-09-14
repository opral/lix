use crate::LixError;
use crate::init::{
    CURRENT_FORMAT_VERSION, REPOSITORY_PROTOCOL_KEY, REPOSITORY_PROTOCOL_SPACE,
    RepositoryProtocolStatus, parse_repository_protocol,
};
use crate::storage_adapter::{
    Storage, StorageCoreProjection as CoreProjection, StorageError,
    StorageGetManyRequest as GetManyRequest, StorageGetOptions as GetOptions, StorageKey as Key,
    StorageProjectedValue as ProjectedValue, StorageReadOptions as ReadOptions,
};
use bytes::Bytes;
fn storage_error(error: StorageError) -> LixError {
    error.into()
}
/// Repository-format state observed without opening the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum MigrationStatus {
    Current {
        version: u32,
    },
    Required {
        from_version: u32,
        to_version: u32,
    },
    TooNew {
        found_version: u32,
        supported_version: u32,
    },
    Missing,
    Malformed,
}

/// Inspect a repository before constructing a Lix engine.
///
/// This is read-only and intentionally understands only the format marker;
/// migration preflight performs the deeper physical validation.
pub(crate) async fn inspect_lix<S>(storage: &S) -> Result<MigrationStatus, LixError>
where
    S: Storage + ?Sized,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
    inspect_lix_read(&crate::storage_adapter::StorageAdapterReadScope::new(read)).await
}

pub(crate) async fn inspect_lix_with_adapter<S>(
    storage: &crate::storage_adapter::StorageAdapter<S>,
) -> Result<MigrationStatus, LixError>
where
    S: Storage,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
    inspect_lix_read(&read).await
}

pub(crate) async fn inspect_lix_read(
    read: &impl crate::storage_adapter::StorageAdapterRead,
) -> Result<MigrationStatus, LixError> {
    let keys = [Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY))];
    let request = [GetManyRequest {
        space: REPOSITORY_PROTOCOL_SPACE,
        keys: &keys,
        opts: GetOptions {
            projection: CoreProjection::FullValue,
        },
    }];
    let value = read
        .get_many(&request)
        .await
        .map_err(storage_error)?
        .values
        .into_iter()
        .next()
        .flatten();
    Ok(match value {
        None => MigrationStatus::Missing,
        Some(ProjectedValue::FullValue(value)) => match parse_repository_protocol(&value) {
            RepositoryProtocolStatus::Current => MigrationStatus::Current {
                version: CURRENT_FORMAT_VERSION,
            },
            RepositoryProtocolStatus::MigrationRequired { found_version } => {
                MigrationStatus::Required {
                    from_version: found_version,
                    to_version: CURRENT_FORMAT_VERSION,
                }
            }
            RepositoryProtocolStatus::TooNew { found_version } => MigrationStatus::TooNew {
                found_version,
                supported_version: CURRENT_FORMAT_VERSION,
            },
            RepositoryProtocolStatus::Missing => MigrationStatus::Missing,
            RepositoryProtocolStatus::Malformed => MigrationStatus::Malformed,
        },
        Some(ProjectedValue::KeyOnly) => MigrationStatus::Malformed,
    })
}
