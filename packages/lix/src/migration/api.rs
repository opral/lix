use bytes::Bytes;

use crate::init::{
    CURRENT_FORMAT_VERSION, REPOSITORY_PROTOCOL_KEY, REPOSITORY_PROTOCOL_SPACE,
    RepositoryProtocolStatus, parse_repository_protocol,
};
use crate::storage::{
    CoreProjection, GetManyRequest, GetOptions, Key, ProjectedValue, ReadOptions, Storage,
    StorageRead,
};
use crate::LixError;

/// Bounds for the intentionally small-repository v68→v69 migration.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrationOptions {
    pub max_changes: usize,
    pub max_preflight_bytes: usize,
}

impl Default for MigrationOptions {
    fn default() -> Self {
        Self {
            max_changes: 250_000,
            max_preflight_bytes: 512 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MigrationReport {
    pub from_version: u32,
    pub to_version: u32,
    pub changes_rewritten: u64,
    pub commit_members_rewritten: u64,
    pub hot_rows_rewritten: u64,
}

/// Repository-format state observed without opening the engine.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MigrationStatus {
    Current { version: u32 },
    Required { from_version: u32, to_version: u32 },
    TooNew { found_version: u32, supported_version: u32 },
    Missing,
    Malformed,
}

/// Inspect a repository before constructing a Lix engine.
///
/// This is read-only and intentionally understands only the format marker;
/// migration preflight performs the deeper physical validation.
pub async fn inspect_repository<S>(storage: &S) -> Result<MigrationStatus, LixError>
where
    S: Storage + ?Sized,
{
    let read = storage
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
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

/// Offline, bounded v68→v69 repository migration.
///
/// Callers must place the repository in maintenance mode and take their
/// backend-level backup first. All physical validation and conversion happen
/// against one coherent read before a single durable atomic publication.
pub async fn migrate_repository<S>(
    storage: S,
    options: MigrationOptions,
) -> Result<MigrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
    match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired { found_version: 68 } => {}
        RepositoryProtocolStatus::Current => {
            return Ok(MigrationReport {
                from_version: CURRENT_FORMAT_VERSION,
                to_version: CURRENT_FORMAT_VERSION,
                changes_rewritten: 0,
                commit_members_rewritten: 0,
                hot_rows_rewritten: 0,
            });
        }
        RepositoryProtocolStatus::MigrationRequired { found_version } => {
            return Err(migration_error(format!(
                "no registered migration from repository v{found_version}"
            )));
        }
        RepositoryProtocolStatus::TooNew { found_version } => {
            return Err(migration_error(format!(
                "repository v{found_version} is newer than this engine"
            )));
        }
        RepositoryProtocolStatus::Malformed | RepositoryProtocolStatus::Missing => {
            return Err(migration_error(
                "repository has no valid versioned protocol marker",
            ));
        }
    }
    let expected_revision =
        crate::storage_adapter::StorageAdapter::<S>::load_mutation_revision_from_read(&read)
            .await
            .map_err(storage_error)?;
    crate::migration::publish::preflight_backend(&storage).await?;
    let v68_changes = crate::migration::v68::preflight_standalone_changelog(
        &read,
        options.max_changes,
        options.max_preflight_bytes,
    )
    .await?;
    let v68_changes_len = v68_changes.len();
    let changes_rewritten = v68_changes_len as u64;
    let mut publication = crate::migration::publish::PublicationPlan::bounded(
        usize::MAX,
        options.max_preflight_bytes,
    );
    for &space in crate::migration::retired_spaces::ALL {
        publication.clear_space(space);
    }
    let standalone = crate::migration::standalone_plan::plan_standalone_changes(
        v68_changes,
        &mut publication,
    )?;
    let crate::migration::standalone_plan::StandalonePlan {
        mut rewritten,
        catalog,
    } = standalone;
    let commit_plan = crate::migration::commit_plan::plan_commit_authorities(
        &read,
        &rewritten,
        &catalog,
        options.max_changes.saturating_sub(v68_changes_len),
        &mut publication,
    )
    .await?;
    rewritten.extend(commit_plan.recovered_changes);
    let hot_rows_rewritten = crate::migration::hot_plan::plan_hot_rows(
        &read,
        &rewritten,
        &catalog,
        options
            .max_changes
            .saturating_sub(v68_changes_len)
            .saturating_sub(commit_plan.member_count as usize),
        &mut publication,
    )
    .await?;
    drop(read);
    crate::migration::publish::publish(&storage, expected_revision, publication).await?;
    Ok(MigrationReport {
        from_version: 68,
        to_version: CURRENT_FORMAT_VERSION,
        changes_rewritten,
        commit_members_rewritten: commit_plan.member_count,
        hot_rows_rewritten,
    })
}

fn storage_error(error: crate::storage::StorageError) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("repository migration storage error: {error}"),
    )
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}
