use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;

use bytes::Bytes;

use crate::LixError;
use crate::branch::{
    BranchHeadControlContext, branch_head_control_precondition, stage_branch_head_control,
};
use crate::changelog::{ChangelogContext, ChangelogReader, CommitLoadRequest};
use crate::hot_state::{
    CompleteWorkingDiffMode, HotTrackedSnapshot, TrackedHeadContext, TrackedWorkingDiffEpoch,
    WorkingDiffIndexCoverage, stage_tracked_working_diff_epoch,
};
use crate::init::{
    CURRENT_FORMAT_VERSION, REPOSITORY_PROTOCOL_KEY, REPOSITORY_PROTOCOL_SPACE,
    RepositoryProtocolStatus, parse_repository_protocol,
};
use crate::storage_adapter::{
    SharedStorageAdapterRead, Storage, StorageCoreProjection as CoreProjection, StorageError,
    StorageGetManyRequest as GetManyRequest, StorageGetOptions as GetOptions, StorageKey as Key,
    StoragePrecondition as Precondition, StorageProjectedValue as ProjectedValue, StorageRead,
    StorageAdapterRead as _, StorageBeginScanOptions, StorageKeyRange,
    StorageReadOptions as ReadOptions, StorageWrite,
    StorageWriteOptions as WriteOptions,
};
use crate::storage_adapter::StorageWriteOptions as AdapterWriteOptions;
use crate::tracked_state::{
    CommitStateReplayDebt, TrackedStateContext, TrackedStateFilter, TrackedStateKeyRef,
    TrackedStateReadColumns, TrackedStateScanRequest,
    backfill_row_pk_index_for_commit, encode_commit_state_manifest_replacement_for_migration,
    load_rebuild_plans_to_nearest_available_root_bounded, stage_rebuild_plan_with_writer,
};

const REPOSITORY_PROTOCOL_V68: &[u8] = b"tracked-default-branch.v68";
const REPOSITORY_PROTOCOL_V69: &[u8] = b"tracked-default-branch.v69";
const REPOSITORY_PROTOCOL_V70: &[u8] = b"tracked-default-branch.v70";
const REPOSITORY_PROTOCOL_V71: &[u8] = b"tracked-default-branch.v71";
const REPOSITORY_PROTOCOL_V72: &[u8] = b"tracked-default-branch.v72";
const REPOSITORY_PROTOCOL_V73: &[u8] = b"tracked-default-branch.v73";
const REPOSITORY_PROTOCOL_V74: &[u8] = b"tracked-default-branch.v74";
const ACCOUNT_SCHEMA_KEY: &str = "lix_account";

/// Bounds for explicit offline repository migrations.
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
pub async fn inspect_lix<S>(storage: &S) -> Result<MigrationStatus, LixError>
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

/// Offline, bounded repository migration to the current format.
///
/// Callers must place the repository in maintenance mode and take their
/// backend-level backup first. Physical authority and marker replacements
/// publish atomically. A v68 migration may stop in a valid, retryable v69
/// state after its typed rewrite; chronology-root chunks may likewise be
/// persisted unreferenced before each edge's final atomic manifest replacement
/// and marker publication. The v72 logical schema amendment publishes one
/// ordinary durable commit per branch and remains on v72 until every branch is
/// complete, so interruption is also safely retryable.
pub async fn migrate_lix<S>(
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
    let protocol_status = crate::init::repository_protocol_status(&read).await?;
    // The v75 chain traverses commit records with the v5-arity decoder before
    // rewriting them to v6. Repositories below v72 still carry older record
    // arities the chain cannot traverse in place.
    if let RepositoryProtocolStatus::MigrationRequired { found_version } = protocol_status
        && (found_version < 72
            || !crate::migration::registry::has_complete_migration_path(
                found_version,
                CURRENT_FORMAT_VERSION,
            ))
    {
        return Err(migration_error(format!(
            "repository v{found_version} predates the v{CURRENT_FORMAT_VERSION} complete-snapshot commit format; no in-place migration is available"
        )));
    }
    let from_version = match protocol_status {
        RepositoryProtocolStatus::MigrationRequired {
            found_version: found_version @ (72 | 73 | 74),
        } => found_version,
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
    };
    if from_version >= 69 {
        drop(read);
        if from_version == 69 {
            promote_chronology_roots(
                &adapter,
                &storage,
                options,
                69,
                REPOSITORY_PROTOCOL_V69,
                REPOSITORY_PROTOCOL_V70,
            )
            .await?;
        }
        if from_version <= 70 {
            promote_chronology_roots(
                &adapter,
                &storage,
                options,
                70,
                REPOSITORY_PROTOCOL_V70,
                REPOSITORY_PROTOCOL_V71,
            )
            .await?;
        }
        let hot_rows_rewritten = if from_version <= 71 {
            migrate_v71_working_diff_epochs(&adapter, &storage, options).await?
        } else {
            0
        };
        // Every step from here on loads commit records through the current
        // v6 decoder, so the v5 records are rewritten first, under whichever
        // marker the repository currently carries.
        let commit_records_rewritten =
            rewrite_commit_records_to_v6(&adapter, &storage, options, from_version).await?;
        if from_version <= 72 {
            migrate_v72_account_profile_uri(&adapter, &storage, options).await?;
        }
        if from_version <= 73 {
            migrate_v73_row_pk_indexes(&adapter, &storage, options).await?;
        }
        let commit_members_rewritten =
            migrate_v74_complete_snapshot_commits(&adapter, &storage, options).await?;
        return Ok(MigrationReport {
            from_version,
            to_version: CURRENT_FORMAT_VERSION,
            changes_rewritten: commit_records_rewritten,
            commit_members_rewritten,
            hot_rows_rewritten,
        });
    }
    let expected_revision =
        crate::storage_adapter::StorageAdapter::<S>::load_mutation_revision_from_read(&read)
            .await
            .map_err(storage_error)?;
    let (v68_changes, standalone_retained_bytes) =
        crate::migration::v68::preflight_standalone_changelog(
            &read,
            options.max_changes,
            options.max_preflight_bytes,
        )
        .await?;
    let authority_byte_budget = options
        .max_preflight_bytes
        .saturating_sub(standalone_retained_bytes);
    let v68_changes_len = v68_changes.len();
    let changes_rewritten = v68_changes_len as u64;
    let authority_registrations =
        crate::migration::commit_plan::discover_registered_schema_changes(
            &read,
            options.max_changes.saturating_sub(v68_changes_len),
            authority_byte_budget,
        )
        .await?;
    let mut publication = crate::migration::publish::PublicationPlan::bounded(
        usize::MAX,
        options.max_preflight_bytes,
    );
    for &space in crate::migration::retired_spaces::ALL {
        publication.clear_space(space);
    }
    let standalone = crate::migration::standalone_plan::plan_standalone_changes(
        v68_changes,
        &authority_registrations,
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
    crate::migration::publish::publish(
        &storage,
        expected_revision,
        REPOSITORY_PROTOCOL_V68,
        REPOSITORY_PROTOCOL_V69,
        publication,
    )
    .await?;
    promote_chronology_roots(
        &adapter,
        &storage,
        options,
        69,
        REPOSITORY_PROTOCOL_V69,
        REPOSITORY_PROTOCOL_V70,
    )
    .await?;
    promote_chronology_roots(
        &adapter,
        &storage,
        options,
        70,
        REPOSITORY_PROTOCOL_V70,
        REPOSITORY_PROTOCOL_V71,
    )
    .await?;
    let migrated_hot_rows = migrate_v71_working_diff_epochs(&adapter, &storage, options).await?;
    migrate_v72_account_profile_uri(&adapter, &storage, options).await?;
    migrate_v73_row_pk_indexes(&adapter, &storage, options).await?;
    let commit_records_rewritten =
        migrate_v74_complete_snapshot_commits(&adapter, &storage, options).await?;
    Ok(MigrationReport {
        from_version: 68,
        to_version: CURRENT_FORMAT_VERSION,
        changes_rewritten: changes_rewritten.saturating_add(commit_records_rewritten),
        commit_members_rewritten: commit_plan.member_count,
        hot_rows_rewritten: hot_rows_rewritten.saturating_add(migrated_hot_rows),
    })
}

/// Moves the former session-open compatibility repair into the explicit
/// offline repository migration. Once the v72 marker is published, opening a
/// client session only allocates session state and never repairs storage.
async fn migrate_v71_working_diff_epochs<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
) -> Result<u64, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired { found_version: 71 } => {}
        status => {
            return Err(migration_error(format!(
                "v71 working-diff migration observed unexpected repository status {status:?}"
            )));
        }
    }
    let branch_ids = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?
        .into_iter()
        .map(|(branch_id, _)| branch_id)
        .collect::<Vec<_>>();
    if branch_ids.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v71 working-diff migration exceeds configured branch bound: {} branches",
                branch_ids.len()
            ),
        ));
    }
    read.finish().map_err(storage_error)?;

    let mut hot_rows_rewritten = 0_u64;
    for branch_id in branch_ids {
        hot_rows_rewritten = hot_rows_rewritten
            .saturating_add(migrate_working_diff_epoch(adapter, &branch_id).await?);
    }

    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    read.finish().map_err(storage_error)?;
    crate::migration::publish::publish(
        storage,
        expected_revision,
        REPOSITORY_PROTOCOL_V71,
        REPOSITORY_PROTOCOL_V72,
        crate::migration::publish::PublicationPlan::bounded(0, 0),
    )
    .await?;
    Ok(hot_rows_rewritten)
}

/// Persists the additive `lix_account.profile_uri` amendment in every live
/// branch catalog before publishing v73.
///
/// Schema definitions are tracked repository rows. The migration therefore
/// uses ordinary schema-amendment commits instead of silently substituting the
/// bundled initialization JSON at read time. Each branch commit is durable;
/// if the process stops before the final marker publication, retrying v72 is
/// idempotent and finishes the remaining branches.
async fn migrate_v72_account_profile_uri<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    // The v72 logical amendment below publishes ordinary commits. Current
    // commit publication maintains the v74 secondary index, so bootstrap the
    // inherited authorities while the repository is still fenced by its v72
    // marker. Keeping the marker unchanged makes interruption retryable and
    // lets the amendment commits inherit the index normally.
    let marker = load_repository_protocol_marker(adapter).await?;
    if marker.as_deref() == Some(REPOSITORY_PROTOCOL_V72) {
        backfill_missing_row_pk_indexes(
            adapter,
            storage,
            options,
            72,
            REPOSITORY_PROTOCOL_V72,
            crate::init::REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP,
            "v72 row-PK-index bootstrap",
            false,
        )
        .await?;
    } else if marker.as_deref()
        != Some(crate::init::REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP)
    {
        return Err(migration_error(
            "v72 row-PK-index bootstrap observed an unexpected protocol marker",
        ));
    }

    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired { found_version: 72 } => {}
        status => {
            return Err(migration_error(format!(
                "v72 account-schema migration observed unexpected repository status {status:?}"
            )));
        }
    }
    let branch_ids = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?
        .into_iter()
        .map(|(branch_id, _)| branch_id)
        .collect::<Vec<_>>();
    if branch_ids.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v72 account-schema migration exceeds configured branch bound: {} branches",
                branch_ids.len()
            ),
        ));
    }
    read.finish().map_err(storage_error)?;

    let target = crate::schema::seed_schema_definition(ACCOUNT_SCHEMA_KEY)
        .expect("lix_account is a built-in schema")
        .clone();
    let target_schema = lix_schema::from_value(target.clone()).map_err(|error| {
        migration_error(format!("bundled lix_account schema is invalid: {error}"))
    })?;
    let engine = crate::engine::Engine::new_for_migration(storage.clone(), 72).await?;
    for branch_id in branch_ids {
        let session = engine.open_session_at_for_migration(&branch_id);
        let result = session
            .execute(
                "SELECT value FROM lix_registered_schema \
                 WHERE lixcol_row_pk = CAST('[\"lix_account\"]' AS JSONB)",
                &[],
            )
            .await?;
        let rows = result.rows();
        let [row] = rows else {
            return Err(migration_error(format!(
                "branch '{branch_id}' must expose exactly one lix_account schema row, found {}",
                rows.len()
            )));
        };
        let [crate::Value::Jsonb(stored)] = row.values() else {
            return Err(migration_error(format!(
                "branch '{branch_id}' has a non-JSON lix_account schema row"
            )));
        };
        let stored = stored.to_value();
        if stored != target {
            let stored_schema = lix_schema::from_value(stored).map_err(|error| {
                migration_error(format!(
                    "branch '{branch_id}' has an invalid persisted lix_account schema: {error}"
                ))
            })?;
            if let Err(error) = lix_schema::validate_amendment(&stored_schema, &target_schema) {
                // A repository may already contain a compatible amendment
                // beyond the bundled v73 definition. It necessarily includes
                // profile_uri because amendment columns are append-only.
                if lix_schema::validate_amendment(&target_schema, &stored_schema).is_err() {
                    return Err(migration_error(format!(
                        "branch '{branch_id}' has a divergent lix_account schema: {error}"
                    )));
                }
            } else {
                let updated = session
                    .execute(
                        "UPDATE lix_registered_schema SET value = $1 \
                         WHERE lixcol_row_pk = CAST('[\"lix_account\"]' AS JSONB)",
                        &[crate::Value::Jsonb(target.clone().into())],
                    )
                    .await?;
                if updated.rows_affected() != 1 {
                    return Err(migration_error(format!(
                        "branch '{branch_id}' updated {} lix_account schema rows instead of one",
                        updated.rows_affected()
                    )));
                }
            }
        }
        session.close().await?;
    }
    drop(engine);

    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    read.finish().map_err(storage_error)?;
    crate::migration::publish::publish(
        storage,
        expected_revision,
        crate::init::REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP,
        REPOSITORY_PROTOCOL_V73,
        crate::migration::publish::PublicationPlan::bounded(0, 0),
    )
    .await
}

async fn load_repository_protocol_marker<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
) -> Result<Option<Bytes>, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = adapter
        .begin_read(ReadOptions::default())
        .await
        .map_err(storage_error)?;
    let values = crate::storage_adapter::PointReadPlan::new(
        REPOSITORY_PROTOCOL_SPACE,
        &[Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY))],
    )
    .materialize(&read, GetOptions::default())
    .await
    .map_err(storage_error)?;
    Ok(values.value.into_iter().next().flatten().and_then(|value| {
        match value {
            ProjectedValue::FullValue(value) => Some(value),
            ProjectedValue::KeyOnly => None,
        }
    }))
}

/// Backfills every commit authority's row-PK permutation before publishing
/// v74, including rootless replay commits.
///
/// Tree chunks are content-addressed and may safely be staged before the
/// authority flip. Manifest replacements and the protocol marker remain one
/// atomic publication, so interruption either leaves a retryable v73
/// repository or a complete v74 repository.
async fn migrate_v73_row_pk_indexes<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    backfill_missing_row_pk_indexes(
        adapter,
        storage,
        options,
        73,
        REPOSITORY_PROTOCOL_V73,
        REPOSITORY_PROTOCOL_V74,
        "v73 row-PK-index migration",
        true,
    )
    .await
}

/// Bumps a fully rewritten v74 repository to the v75 marker.
///
/// The commit-record rewrite itself runs earlier in the chain (see
/// [`rewrite_commit_records_to_v6`]) so that every later step can load
/// records through the current decoder. This step re-runs the rewrite
/// idempotently under the v74 marker — resuming an interrupted migration —
/// and only then publishes v75.
async fn migrate_v74_complete_snapshot_commits<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
) -> Result<u64, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let rewritten = rewrite_commit_records_to_v6(adapter, storage, options, 74).await?;
    let (members_injected, repaired_manifests) =
        repair_filesystem_closure(adapter, storage, options).await?;
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    read.finish().map_err(storage_error)?;
    let mut publication = crate::migration::publish::PublicationPlan::bounded(
        repaired_manifests.len().saturating_mul(2),
        options.max_preflight_bytes,
    );
    for manifest in repaired_manifests {
        for (space, key, value) in
            encode_commit_state_manifest_replacement_for_migration(&manifest)?
        {
            publication.replace_immutable(space, vec![(key, value)])?;
        }
    }
    crate::migration::publish::publish(
        storage,
        expected_revision,
        REPOSITORY_PROTOCOL_V74,
        crate::init::REPOSITORY_PROTOCOL_VALUE,
        publication,
    )
    .await?;
    Ok(rewritten.saturating_add(members_injected))
}

/// Repairs filesystem closure inside every commit tree before v75 publishes.
///
/// v72-era partial checkpoints on migrated repositories could commit a file
/// descriptor without its swept ancestor directories, leaving trees whose
/// files reference directories absent from the same tree. Every strict v75
/// read of such a tree fails. The repair injects the missing directory rows
/// as tree index entries referencing the directory's existing authoritative
/// change record, resolved from the branch heads' trees — the recoverable
/// descriptor. No new change records are created and commit deltas stay
/// untouched, so touched-scope digests remain valid; the referenced owning
/// commits are already retained by the branches that reach them.
async fn repair_filesystem_closure<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
) -> Result<(u64, Vec<crate::tracked_state::CommitStateManifest>), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    const FILE_DESCRIPTOR_SCHEMA_KEY: &str = "lix_file_descriptor";
    const DIRECTORY_DESCRIPTOR_SCHEMA_KEY: &str = "lix_directory_descriptor";

    fn snapshot_text(
        row: &crate::tracked_state::MaterializedTrackedStateRow,
        field: &str,
    ) -> Option<String> {
        let snapshot = row.decoded_snapshot.as_ref()?;
        match snapshot.row.get(field) {
            Some(lix_schema::Value::Text(value)) => Some(value.clone()),
            Some(lix_schema::Value::Uuid(value)) => Some(value.to_string()),
            _ => None,
        }
    }

    fn index_value_ref(
        row: &crate::tracked_state::MaterializedTrackedStateRow,
    ) -> Result<crate::tracked_state::TrackedStateIndexValueRef, LixError> {
        Ok(crate::tracked_state::TrackedStateIndexValueRef {
            change_id: row.change_id,
            commit_id: row.commit_id,
            deleted: false,
            created_at: crate::common::LixTimestamp::parse(&row.created_at)
                .map_err(|error| migration_error(format!("repair created_at: {error}")))?,
            updated_at: crate::common::LixTimestamp::parse(&row.updated_at)
                .map_err(|error| migration_error(format!("repair updated_at: {error}")))?,
        })
    }

    let operation = "v74 filesystem-closure repair";
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    let commit_ids = crate::tracked_state::scan_commit_state_manifest_commit_ids(&read).await?;

    // The recoverable descriptor source: directory rows visible at any
    // branch head's tree.
    let heads = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?
        .into_iter()
        .map(|(_, control)| control.head_commit_id)
        .collect::<BTreeSet<_>>();
    let mut source = BTreeMap::<String, crate::tracked_state::MaterializedTrackedStateRow>::new();
    let mut reader = TrackedStateContext::new().reader(read.clone());
    for head in &heads {
        let rows = reader
            .scan_batch_at_commit(
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_owned()],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns::default(),
                    limit: Some(options.max_changes),
                },
            )
            .await?
            .into_rows();
        for row in rows {
            let Some(id) = snapshot_text(&row, "id") else {
                continue;
            };
            source.entry(id).or_insert(row);
        }
    }

    let mut chunk_writes = adapter.new_write_set();
    let mut replacements = Vec::new();
    let mut injected_total = 0_u64;
    let mut visited_rows = 0_usize;
    for commit_id in commit_ids {
        let commit_key = commit_id.to_string();
        let rows = reader
            .scan_batch_at_commit(
                &commit_key,
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec![
                            FILE_DESCRIPTOR_SCHEMA_KEY.to_owned(),
                            DIRECTORY_DESCRIPTOR_SCHEMA_KEY.to_owned(),
                        ],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns::default(),
                    limit: Some(options.max_changes.saturating_sub(visited_rows)),
                },
            )
            .await?
            .into_rows();
        visited_rows = visited_rows.saturating_add(rows.len());
        if visited_rows > options.max_changes {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                format!("{operation} exceeds configured row bound"),
            ));
        }

        let mut present = BTreeSet::new();
        let mut referenced = BTreeSet::new();
        for row in &rows {
            match row.schema_key.as_str() {
                DIRECTORY_DESCRIPTOR_SCHEMA_KEY => {
                    if let Some(id) = snapshot_text(row, "id") {
                        present.insert(id);
                    }
                    if let Some(parent) = snapshot_text(row, "parent_id") {
                        referenced.insert(parent);
                    }
                }
                FILE_DESCRIPTOR_SCHEMA_KEY => {
                    if let Some(directory) = snapshot_text(row, "directory_id") {
                        referenced.insert(directory);
                    }
                }
                _ => {}
            }
        }
        let mut missing = Vec::new();
        let mut worklist = referenced
            .difference(&present)
            .cloned()
            .collect::<Vec<_>>();
        let mut seen = BTreeSet::new();
        while let Some(id) = worklist.pop() {
            if !seen.insert(id.clone()) {
                continue;
            }
            let Some(row) = source.get(&id) else {
                return Err(migration_error(format!(
                    "{operation} cannot resolve directory '{id}' referenced by commit \
                     '{commit_id}' from any branch head"
                )));
            };
            if let Some(parent) = snapshot_text(row, "parent_id")
                && !present.contains(&parent)
            {
                worklist.push(parent);
            }
            missing.push(id);
        }
        if missing.is_empty() {
            continue;
        }
        missing.sort();

        let mut manifest = crate::tracked_state::load_commit_state_manifest(&read, commit_id)
            .await?
            .ok_or_else(|| {
                migration_error(format!(
                    "{operation} commit '{commit_id}' has no commit-state manifest"
                ))
            })?;
        let mut injected =
            crate::tracked_state::TrackedStateMutationBatchBuilder::with_row_capacity(
                missing.len(),
            );
        for id in &missing {
            let row = source.get(id).expect("resolved above");
            injected.push(
                TrackedStateKeyRef {
                    schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                    file_id: row.file_id.as_deref(),
                    row_pk: &row.row_pk,
                },
                index_value_ref(row)?,
            );
        }
        let (primary, secondary) =
            crate::tracked_state::with_row_pk_index_mutations(injected.finish())?;

        let mut overlay = crate::tracked_state::TrackedStateChunkOverlay::new();
        let tree = crate::tracked_state::TrackedStateTree::new();
        if let Some(existing_root) = manifest.snapshot_root.as_deref() {
            let result = tree
                .apply_mutations_with_overlay(
                    &read,
                    &mut chunk_writes,
                    &mut overlay,
                    Some(&existing_root.root_id),
                    primary,
                    Some(&commit_key),
                )
                .await?;
            let mut repaired_root = existing_root.clone();
            repaired_root.root_id = result.root_id;
            repaired_root.changed_key_count = repaired_root
                .changed_key_count
                .saturating_add(missing.len() as u64);
            repaired_root.row_count_estimate = repaired_root
                .row_count_estimate
                .saturating_add(missing.len() as u64);
            repaired_root.tree_height = result.tree_height as u32;
            manifest.snapshot_root = Some(Box::new(repaired_root));
        } else {
            // Rootless commits serve reads by replaying deltas, where the
            // injected entries would never appear. Materialize a complete
            // fenced root from the commit's full resolved state plus the
            // injected directories.
            let full_rows = reader
                .scan_batch_at_commit(
                    &commit_key,
                    &TrackedStateScanRequest {
                        limit: Some(options.max_changes),
                        ..TrackedStateScanRequest::default()
                    },
                )
                .await?
                .into_rows();
            let mut full = crate::tracked_state::TrackedStateMutationBatchBuilder::with_row_capacity(
                full_rows.len() + missing.len(),
            );
            for row in &full_rows {
                full.push(
                    TrackedStateKeyRef {
                        schema_key: &row.schema_key,
                        file_id: row.file_id.as_deref(),
                        row_pk: &row.row_pk,
                    },
                    index_value_ref(row)?,
                );
            }
            for id in &missing {
                let row = source.get(id).expect("resolved above");
                full.push(
                    TrackedStateKeyRef {
                        schema_key: DIRECTORY_DESCRIPTOR_SCHEMA_KEY,
                        file_id: row.file_id.as_deref(),
                        row_pk: &row.row_pk,
                    },
                    index_value_ref(row)?,
                );
            }
            let result = tree
                .apply_mutations_with_overlay(
                    &read,
                    &mut chunk_writes,
                    &mut overlay,
                    None,
                    full.finish(),
                    Some(&commit_key),
                )
                .await?;
            manifest.snapshot_root =
                Some(Box::new(crate::tracked_state::TrackedStateCommitRoot {
                    commit_id,
                    root_id: result.root_id,
                    parent_roots: Vec::new(),
                    changed_key_count: result.row_count as u64,
                    row_count_estimate: result.row_count as u64,
                    tree_height: result.tree_height as u32,
                    complete_state_fence: true,
                }));
            manifest.replay_debt = CommitStateReplayDebt::default();
        }

        let row_pk_base = manifest.row_pk_index_root_id.clone();
        let result = tree
            .apply_mutations_with_overlay(
                &read,
                &mut chunk_writes,
                &mut overlay,
                row_pk_base.as_ref(),
                secondary,
                Some(&commit_key),
            )
            .await?;
        manifest.row_pk_index_root_id = Some(result.root_id);

        injected_total = injected_total.saturating_add(missing.len() as u64);
        replacements.push(manifest);
    }
    drop(reader);
    read.finish().map_err(storage_error)?;

    let chunk_stats = chunk_writes.stats();
    if chunk_stats.written_bytes > options.max_preflight_bytes as u64 {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "{operation} exceeds configured byte bound: {} bytes",
                chunk_stats.written_bytes
            ),
        ));
    }
    if chunk_stats.staged_puts != 0 || chunk_stats.staged_deletes != 0 {
        let mut write = storage
            .begin_write(WriteOptions {
                await_durable: true,
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REPOSITORY_PROTOCOL_SPACE,
                        key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                        expected: Bytes::from_static(REPOSITORY_PROTOCOL_V74),
                    },
                    crate::storage_adapter::repository_mutation_revision_precondition(
                        expected_revision,
                    ),
                ],
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        if let Err(error) = chunk_writes.lower_into(&mut write).await {
            let _ = write.rollback().await;
            return Err(error.into());
        }
        write.commit().await.map_err(storage_error)?;
    }
    Ok((injected_total, replacements))
}

/// Rewrites every v5 commit record to the v6 complete-snapshot arity.
///
/// Repositories before v75 never recorded which global commit a local commit
/// observed. The recoverable assignment is chronological: each local commit
/// pins the newest global-lineage commit whose `created_at` does not exceed
/// its own, so no migrated commit claims to have observed global state that
/// did not exist when it was authored. Global-lineage commits — the
/// first-parent chain of the global branch head, the recoverable authored
/// lineage — pin no base. Rewrites publish under the current marker
/// precondition without moving the marker; already-v6 records are skipped, so
/// interrupted runs resume cleanly.
async fn rewrite_commit_records_to_v6<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
    expected_version: u32,
) -> Result<u64, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    /// Byte-for-byte the v5 `CommitRecord` arity, before `base_commit_id`.
    #[derive(Debug, musli::Decode)]
    #[musli(packed)]
    struct CommitRecordV5 {
        format_version: u32,
        commit_id: crate::changelog::CommitId,
        generation: u64,
        parent_commit_ids: Vec<crate::changelog::CommitId>,
        first_parent_jump_commit_id: crate::changelog::CommitId,
        first_parent_jump_span: u64,
        account_id: String,
        created_at: crate::common::LixTimestamp,
        touched_scope_digest: crate::changelog::CommitTouchedScopeDigest,
    }

    struct CommitChronology {
        created_at: crate::common::LixTimestamp,
        generation: u64,
        first_parent: Option<crate::changelog::CommitId>,
    }

    let operation = "commit-record v6 rewrite";
    // The rewrite runs under whichever marker the repository currently
    // carries — including the transitional v72 row-PK bootstrap marker — as
    // long as that marker resolves to the expected source version. The
    // marker's exact bytes fence the publication.
    let marker = load_repository_protocol_marker(adapter)
        .await?
        .ok_or_else(|| migration_error(format!("{operation} found no protocol marker")))?;
    match parse_repository_protocol(&marker) {
        RepositoryProtocolStatus::MigrationRequired { found_version }
            if found_version == expected_version => {}
        status => {
            return Err(migration_error(format!(
                "{operation} observed unexpected repository status {status:?}"
            )));
        }
    }
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;

    let mut cursor = read
        .begin_scan(
            crate::changelog::COMMIT_SPACE,
            StorageKeyRange {
                lower: Bound::Unbounded,
                upper: Bound::Unbounded,
            },
            StorageBeginScanOptions {
                projection: CoreProjection::FullValue,
                ..StorageBeginScanOptions::default()
            },
        )
        .await
        .map_err(storage_error)?;
    let mut pending = Vec::new();
    let mut chronology = BTreeMap::new();
    while let Some(entries) = cursor.next_chunk().await.map_err(storage_error)? {
        for entry in entries {
            if chronology.len() > options.max_changes {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                    format!(
                        "{operation} exceeds configured commit bound: {} commits",
                        chronology.len()
                    ),
                ));
            }
            let ProjectedValue::FullValue(value) = entry.value else {
                return Err(migration_error(format!(
                    "{operation} commit scan omitted a value"
                )));
            };
            // Idempotent resume: a record already carrying the v6 arity was
            // rewritten by an interrupted earlier run and needs no work. The
            // packed codec makes the arities mutually undecodable, so a
            // successful current-decode is authoritative.
            if let Ok(record) = crate::storage_codec::decode::<crate::changelog::CommitRecord>(
                "commit record",
                &value,
            ) {
                chronology.insert(
                    record.commit_id,
                    CommitChronology {
                        created_at: record.created_at,
                        generation: record.generation,
                        first_parent: record.parent_commit_ids.first().copied(),
                    },
                );
                continue;
            }
            let record =
                crate::storage_codec::decode::<CommitRecordV5>("v5 commit record", &value)
                    .map_err(|error| {
                        migration_error(format!(
                            "{operation} could not decode a commit record as v5: {error}"
                        ))
                    })?;
            if record.format_version != 5 {
                return Err(migration_error(format!(
                    "{operation} commit '{}' has unsupported record format v{}",
                    record.commit_id, record.format_version
                )));
            }
            chronology.insert(
                record.commit_id,
                CommitChronology {
                    created_at: record.created_at,
                    generation: record.generation,
                    first_parent: record.parent_commit_ids.first().copied(),
                },
            );
            pending.push((entry.key, record));
        }
    }
    drop(cursor);
    if pending.is_empty() {
        read.finish().map_err(storage_error)?;
        return Ok(0);
    }

    // Branch-family provenance was not persisted before v74. The first-parent
    // chain of the global head is the recoverable authored lineage: merge
    // secondary parents may come from another family and must not become
    // global merely because the current global head reaches them.
    let global_head = BranchHeadControlContext::new()
        .reader(read.clone())
        .load(crate::GLOBAL_BRANCH_ID)
        .await?
        .ok_or_else(|| migration_error(format!("{operation} found no global branch")))?
        .head_commit_id;
    let mut global_chronology = Vec::new();
    let mut global_commits = BTreeSet::new();
    let mut next_global = Some(global_head);
    while let Some(commit_id) = next_global {
        if !global_commits.insert(commit_id) {
            return Err(migration_error(format!(
                "{operation} global lineage revisits commit '{commit_id}'"
            )));
        }
        let node = chronology.get(&commit_id).ok_or_else(|| {
            migration_error(format!(
                "{operation} global lineage commit '{commit_id}' is missing"
            ))
        })?;
        global_chronology.push((node.created_at, node.generation, commit_id));
        next_global = node.first_parent;
    }
    global_chronology.sort();
    read.finish().map_err(storage_error)?;

    let mut writes = adapter.new_write_set();
    let mut rewritten = 0_u64;
    for (key, record) in pending {
        let base_commit_id = if global_commits.contains(&record.commit_id) {
            None
        } else {
            let newest_not_after = global_chronology
                .partition_point(|(created_at, _, _)| *created_at <= record.created_at);
            let Some((_, _, base)) = newest_not_after
                .checked_sub(1)
                .and_then(|index| global_chronology.get(index))
            else {
                return Err(migration_error(format!(
                    "{operation} local commit '{}' predates every global-lineage commit",
                    record.commit_id
                )));
            };
            Some(*base)
        };
        let upgraded = crate::changelog::CommitRecord {
            format_version: crate::changelog::COMMIT_RECORD_FORMAT_VERSION,
            commit_id: record.commit_id,
            generation: record.generation,
            parent_commit_ids: record.parent_commit_ids,
            base_commit_id,
            first_parent_jump_commit_id: record.first_parent_jump_commit_id,
            first_parent_jump_span: record.first_parent_jump_span,
            account_id: record.account_id,
            created_at: record.created_at,
            touched_scope_digest: record.touched_scope_digest,
        };
        let encoded = crate::storage_codec::encode("commit record", &upgraded)?;
        writes.put(crate::changelog::COMMIT_SPACE, key.0.to_vec(), encoded);
        rewritten += 1;
    }

    let mut write = storage
        .begin_write(WriteOptions {
            await_durable: true,
            preconditions: vec![
                Precondition::KeyValueEquals {
                    space: REPOSITORY_PROTOCOL_SPACE,
                    key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                    expected: marker,
                },
                crate::storage_adapter::repository_mutation_revision_precondition(
                    expected_revision,
                ),
            ],
            ..WriteOptions::default()
        })
        .await
        .map_err(storage_error)?;
    if let Err(error) = writes.lower_into(&mut write).await {
        let _ = write.rollback().await;
        return Err(error.into());
    }
    write.commit().await.map_err(storage_error)?;
    Ok(rewritten)
}

#[allow(clippy::too_many_arguments)]
async fn backfill_missing_row_pk_indexes<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
    expected_version: u32,
    expected_protocol: &'static [u8],
    target_protocol: &'static [u8],
    operation: &'static str,
    preflight_reserved_columns: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired { found_version }
            if found_version == expected_version => {}
        status => {
            return Err(migration_error(format!(
                "{operation} observed unexpected repository status {status:?}"
            )));
        }
    }
    if preflight_reserved_columns {
        preflight_v74_registered_schemas(&read, options).await?;
    }
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    let commit_ids = crate::tracked_state::scan_commit_state_manifest_commit_ids(&read).await?;
    if commit_ids.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "{operation} exceeds configured commit bound: {} commits",
                commit_ids.len()
            ),
        ));
    }

    let global_head = BranchHeadControlContext::new()
        .reader(read.clone())
        .load(crate::GLOBAL_BRANCH_ID)
        .await?
        .ok_or_else(|| migration_error("row-PK-index migration found no global branch"))?
        .head_commit_id;
    // Branch-family provenance was not persisted before v74. The first-parent
    // chain is the recoverable authored lineage: merge secondary parents may
    // come from another family and must not become global merely because the
    // current global head reaches them.
    let mut global_commits = BTreeSet::new();
    let mut next_global = Some(global_head);
    while let Some(commit_id) = next_global {
        if !global_commits.insert(commit_id) {
            return Err(migration_error(format!(
                "{operation} found a cycle in the global first-parent lineage at '{commit_id}'"
            )));
        }
        if global_commits.len() > options.max_changes {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                format!("{operation} exceeds configured global-lineage commit bound"),
            ));
        }
        let ids = [commit_id];
        let record = ChangelogContext::new()
            .reader(read.clone())
            .load_commits(CommitLoadRequest { commit_ids: &ids })
            .await?
            .into_iter()
            .next()
            .and_then(|(_, record)| record)
            .ok_or_else(|| {
                migration_error(format!(
                    "{operation} global lineage commit '{commit_id}' is missing"
                ))
            })?;
        next_global = record.parent_commit_ids.first().copied();
    }

    let mut chunk_writes = adapter.new_write_set();
    let mut replacements = Vec::new();
    let mut visited_rows = 0usize;
    for commit_id in commit_ids {
        let mut manifest = crate::tracked_state::load_commit_state_manifest(&read, commit_id)
            .await?
            .ok_or_else(|| {
                migration_error(format!(
                    "{operation} commit '{commit_id}' has no commit-state manifest"
                ))
            })?;
        if manifest.row_pk_index_root_id.is_some() {
            continue;
        }
        let (root, row_count) = backfill_row_pk_index_for_commit(
            &read,
            &mut chunk_writes,
            &manifest,
            options.max_changes.saturating_sub(visited_rows),
        )
        .await?;
        visited_rows = visited_rows.checked_add(row_count).ok_or_else(|| {
            migration_error(format!("{operation} row count exceeds usize"))
        })?;
        manifest.global_scope = global_commits.contains(&commit_id);
        manifest.row_pk_index_root_id = root;
        replacements.push(manifest);
    }

    let chunk_stats = chunk_writes.stats();
    if chunk_stats.written_bytes > options.max_preflight_bytes as u64 {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "{operation} exceeds configured byte bound: {} bytes",
                chunk_stats.written_bytes
            ),
        ));
    }
    if chunk_stats.staged_puts != 0 || chunk_stats.staged_deletes != 0 {
        let mut write = storage
            .begin_write(WriteOptions {
                await_durable: true,
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REPOSITORY_PROTOCOL_SPACE,
                        key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                        expected: Bytes::from_static(expected_protocol),
                    },
                    crate::storage_adapter::repository_mutation_revision_precondition(
                        expected_revision.clone(),
                    ),
                ],
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        if let Err(error) = chunk_writes.lower_into(&mut write).await {
            let _ = write.rollback().await;
            return Err(error.into());
        }
        write.commit().await.map_err(storage_error)?;
    }
    read.finish().map_err(storage_error)?;

    let mut publication = crate::migration::publish::PublicationPlan::bounded(
        replacements.len().saturating_mul(2),
        options.max_preflight_bytes,
    );
    for manifest in replacements {
        for (space, key, value) in
            encode_commit_state_manifest_replacement_for_migration(&manifest)?
        {
            publication.replace_immutable(space, vec![(key, value)])?;
        }
    }
    crate::migration::publish::publish(
        storage,
        expected_revision,
        expected_protocol,
        target_protocol,
        publication,
    )
    .await
}

async fn preflight_v74_registered_schemas(
    read: &(impl crate::storage_adapter::StorageAdapterRead + Clone),
    options: MigrationOptions,
) -> Result<(), LixError> {
    let heads = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?
        .into_iter()
        .map(|(_, control)| control.head_commit_id)
        .collect::<BTreeSet<_>>();
    if heads.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v74 schema preflight exceeds configured branch-head bound: {} heads",
                heads.len()
            ),
        ));
    }

    let mut inspected = 0usize;
    let mut inspected_bytes = 0usize;
    for head in heads {
        let remaining = options.max_changes.saturating_sub(inspected);
        let rows = TrackedStateContext::new()
            .reader(read.clone())
            .scan_batch_at_commit(
                &head.to_string(),
                &TrackedStateScanRequest {
                    filter: TrackedStateFilter {
                        schema_keys: vec!["lix_registered_schema".to_owned()],
                        file_ids: vec![crate::NullableKeyFilter::Null],
                        ..TrackedStateFilter::default()
                    },
                    read_columns: TrackedStateReadColumns {
                        columns: vec!["snapshot".to_owned()],
                    },
                    limit: Some(remaining.saturating_add(1)),
                },
            )
            .await?;
        inspected = inspected.checked_add(rows.len()).ok_or_else(|| {
            migration_error("v74 schema preflight row count exceeds usize")
        })?;
        if inspected > options.max_changes {
            return Err(LixError::new(
                "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                format!(
                    "v74 schema preflight exceeds configured row bound: {inspected} rows"
                ),
            ));
        }
        for row in rows.iter() {
            let schema_key = row.row_pk().as_single_string_owned().map_err(|error| {
                migration_error(format!(
                    "v74 schema preflight found an invalid registered-schema identity: {error}"
                ))
            })?;
            let schema = match row
                .decoded_snapshot()
                .and_then(|typed| typed.row.get("value"))
            {
                Some(lix_schema::Value::Jsonb(value)) => value.clone().into_value(),
                _ => {
                    return Err(migration_error(format!(
                        "v74 schema preflight could not decode registered schema '{schema_key}' at commit '{head}'"
                    )));
                }
            };
            inspected_bytes = inspected_bytes
                .checked_add(serde_json::to_vec(&schema).map_err(|error| {
                    migration_error(format!(
                        "v74 schema preflight could not size registered schema '{schema_key}': {error}"
                    ))
                })?.len())
                .ok_or_else(|| migration_error("v74 schema preflight byte count exceeds usize"))?;
            if inspected_bytes > options.max_preflight_bytes {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                    format!(
                        "v74 schema preflight exceeds configured byte bound: {inspected_bytes} bytes"
                    ),
                ));
            }
            validate_v74_registered_schema(&schema_key, &schema)?;
        }
    }
    Ok(())
}

fn validate_v74_registered_schema(
    schema_key: &str,
    schema: &serde_json::Value,
) -> Result<(), LixError> {
    crate::schema::parse_lix_schema(schema).map(|_| ()).map_err(|error| {
        LixError::new(
            "LIX_ERROR_MIGRATION_SCHEMA_INCOMPATIBLE",
            format!(
                "repository cannot migrate to v74 while registered schema '{schema_key}' uses a reserved column: {}. With a v73-capable engine, register a replacement schema under a new key, migrate its rows, remove the old schema, and then retry",
                error.message
            ),
        )
    })
}

async fn migrate_working_diff_epoch<S>(
    storage: &crate::storage_adapter::StorageAdapter<S>,
    branch_id: &str,
) -> Result<u64, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = SharedStorageAdapterRead::new(
        storage
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    let mut observations = BranchHeadControlContext::new()
        .reader(read.clone())
        .load_observed(&[branch_id.to_owned()])
        .await?;
    let observation = observations
        .pop()
        .expect("one requested branch produces one observation");
    let Some(mut control) = observation.control else {
        return Ok(0);
    };
    let Some(checkpoint_commit_id) = control.working_diff_checkpoint_commit_id else {
        return Ok(0);
    };
    let epoch = TrackedHeadContext::new()
        .reader(read.clone())
        .working_diff_epoch(branch_id)
        .await?;
    if epoch.as_ref().is_some_and(|epoch| {
        epoch.checkpoint_commit_id == checkpoint_commit_id
            && epoch.generation == control.tracked_generation
    }) {
        return Ok(0);
    }

    let current = load_migration_hot_snapshot(&read, branch_id, control.head_commit_id).await?;
    let hot_rows_rewritten = current.len() as u64;
    let checkpoint =
        load_migration_hot_snapshot(&read, branch_id, checkpoint_commit_id).await?;
    let generation = crate::changelog::CommitId::with_change_address_space(uuid::Uuid::now_v7());
    let mut coverage = WorkingDiffIndexCoverage::default();
    let mut writes = storage.new_write_set();
    let (_, schema_keys) = TrackedHeadContext::new()
        .writer(&read, &mut writes)
        .stage_complete_current_state_with_working_diff(
            branch_id,
            generation,
            current,
            Some(control.tracked_generation),
            &[],
            &[],
            &BTreeSet::new(),
            if control.head_commit_id == checkpoint_commit_id {
                CompleteWorkingDiffMode::ResetClean
            } else {
                CompleteWorkingDiffMode::Rebase {
                    checkpoint_commit_id,
                    checkpoint,
                }
            },
            &mut coverage,
        )
        .await?;
    stage_tracked_working_diff_epoch(
        &mut writes,
        branch_id,
        TrackedWorkingDiffEpoch {
            checkpoint_commit_id,
            generation,
            coverage,
        },
    )?;
    control.tracked_generation = generation;
    control.current_state_revision = control
        .current_state_revision
        .checked_add(1)
        .ok_or_else(|| migration_error("branch current-state revision overflowed"))?;
    control.reset_schema_presence();
    control.note_schemas(schema_keys.iter().map(String::as_str));
    stage_branch_head_control(&mut writes, branch_id, control)?;
    let preconditions = vec![branch_head_control_precondition(
        branch_id,
        observation.raw_token,
    )?];
    read.finish().map_err(storage_error)?;
    storage
        .commit_write_set(
            writes,
            AdapterWriteOptions {
                preconditions,
                ..AdapterWriteOptions::default()
            },
        )
        .await
        .map(|_| hot_rows_rewritten)
        .map_err(LixError::from)
}

async fn load_migration_hot_snapshot(
    read: &(impl crate::storage_adapter::StorageAdapterRead + ?Sized),
    branch_id: &str,
    commit_id: crate::changelog::CommitId,
) -> Result<HotTrackedSnapshot, LixError> {
    let rows = TrackedStateContext::new()
        .reader(read)
        .scan_batch_at_commit(
            &commit_id.to_string(),
            &TrackedStateScanRequest {
                filter: TrackedStateFilter {
                    include_tombstones: true,
                    ..TrackedStateFilter::default()
                },
                read_columns: TrackedStateReadColumns::default(),
                limit: None,
            },
        )
        .await?
        .into_rows()
        .into_iter()
        .filter(|row| {
            branch_id == crate::GLOBAL_BRANCH_ID
                || row.schema_key != crate::checkpoint::CHECKPOINT_SCHEMA_KEY
        })
        .collect();
    HotTrackedSnapshot::from_materialized_rows(rows)
}

/// Promotes every distinct live branch chronology root to a durable root.
///
/// A branch's chronology roots are its head and its optional working-diff
/// checkpoint cursor. The cursor may be distinct from an already-rooted head;
/// omitting it leaves checkpoint aliases and chronology GC with a rootless live
/// authority after the protocol marker claims the invariant.
///
/// Chunk publication is deliberately separate from authority publication.
/// Content-addressed chunks are safe to leave unreferenced after a crash; the
/// immutable manifest replacements and protocol marker are not, so those land
/// together in the final atomic publication.
async fn promote_chronology_roots<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
    from_version: u32,
    from_protocol: &'static [u8],
    to_protocol: &'static [u8],
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = SharedStorageAdapterRead::new(
        adapter
            .begin_read(ReadOptions::default())
            .await
            .map_err(storage_error)?,
    );
    match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired { found_version }
            if found_version == from_version => {}
        status => {
            return Err(migration_error(format!(
                "v{from_version} chronology-root promotion observed unexpected repository status {status:?}"
            )));
        }
    }
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    let chronology_root_commit_ids = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?
        .into_iter()
        .flat_map(|(_, control)| {
            [
                Some(control.head_commit_id),
                control.working_diff_checkpoint_commit_id,
            ]
        })
        .flatten()
        .collect::<BTreeSet<_>>();
    if chronology_root_commit_ids.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v{from_version} chronology-root promotion exceeds configured root bound: {} roots",
                chronology_root_commit_ids.len()
            ),
        ));
    }

    let tracked_state = TrackedStateContext::new();
    let mut chunk_writes = adapter.new_write_set();
    let mut promotions = BTreeMap::new();
    let mut staged_commit_ids = BTreeSet::new();
    let mut remaining_members = options.max_changes;
    {
        let mut writer = tracked_state.writer(&read, &mut chunk_writes);
        for chronology_root_commit_id in chronology_root_commit_ids {
            let manifest = crate::tracked_state::load_commit_state_manifest(
                &read,
                chronology_root_commit_id,
            )
                .await?
                .ok_or_else(|| {
                    migration_error(format!(
                        "v{from_version} chronology root '{chronology_root_commit_id}' has no commit-state manifest"
                    ))
                })?;
            if manifest.snapshot_root.is_some() {
                continue;
            }
            let plans = load_rebuild_plans_to_nearest_available_root_bounded(
                &read,
                &chronology_root_commit_id.to_string(),
                true,
                remaining_members,
                &staged_commit_ids,
            )
            .await?;
            remaining_members = remaining_members.saturating_sub(
                plans
                    .iter()
                    .filter(|plan| !staged_commit_ids.contains(&plan.commit_id))
                    .map(|plan| plan.deltas.len())
                    .sum::<usize>(),
            );
            for plan in plans.iter().rev() {
                if staged_commit_ids.insert(plan.commit_id) {
                    if staged_commit_ids.len() > options.max_changes {
                        return Err(LixError::new(
                            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                            format!(
                                "v{from_version} chronology-root promotion exceeds configured replay-commit bound: {} commits",
                                staged_commit_ids.len()
                            ),
                        ));
                    }
                    stage_rebuild_plan_with_writer(&mut writer, plan).await?;
                    let mut plan_manifest =
                        crate::tracked_state::load_commit_state_manifest(&read, plan.commit_id)
                            .await?
                            .ok_or_else(|| {
                                migration_error(format!(
                                    "v{from_version} replay commit '{}' has no commit-state manifest",
                                    plan.commit_id
                                ))
                            })?;
                    if plan_manifest.snapshot_root.is_none() {
                        let snapshot_root = writer
                            .staged_commit_roots()
                            .find(|root| root.commit_id == plan.commit_id)
                            .cloned()
                            .ok_or_else(|| {
                                migration_error(format!(
                                    "v{from_version} chronology-root promotion did not stage replay commit '{}'",
                                    plan.commit_id
                                ))
                            })?;
                        plan_manifest.snapshot_root = Some(Box::new(snapshot_root));
                        plan_manifest.replay_debt = CommitStateReplayDebt::default();
                        promotions.insert(plan.commit_id, plan_manifest);
                    }
                }
            }
            if !promotions.contains_key(&chronology_root_commit_id) {
                return Err(migration_error(format!(
                    "v{from_version} chronology-root promotion did not authorize root '{chronology_root_commit_id}'"
                )));
            }
        }
    }

    let chunk_stats = chunk_writes.stats();
    if chunk_stats.written_bytes > options.max_preflight_bytes as u64 {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v{from_version} chronology-root promotion exceeds configured byte bound: {} bytes",
                chunk_stats.written_bytes
            ),
        ));
    }
    if chunk_stats.staged_puts != 0 || chunk_stats.staged_deletes != 0 {
        // Chunk rows are content-addressed and not authority until the final
        // manifest publication. Persist them without rotating the repository
        // mutation token, so that exact original token still fences every
        // concurrent domain write through the final marker flip for this edge.
        let mut write = storage
            .begin_write(WriteOptions {
                await_durable: true,
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REPOSITORY_PROTOCOL_SPACE,
                        key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                        expected: Bytes::from_static(from_protocol),
                    },
                    crate::storage_adapter::repository_mutation_revision_precondition(
                        expected_revision.clone(),
                    ),
                ],
                ..WriteOptions::default()
            })
            .await
            .map_err(storage_error)?;
        if let Err(error) = chunk_writes.lower_into(&mut write).await {
            let _ = write.rollback().await;
            return Err(error.into());
        }
        write.commit().await.map_err(storage_error)?;
    }
    read.finish().map_err(storage_error)?;

    let mut publication = crate::migration::publish::PublicationPlan::bounded(
        options.max_changes.saturating_mul(2),
        options.max_preflight_bytes,
    );
    for manifest in promotions.into_values() {
        for (space, key, value) in
            encode_commit_state_manifest_replacement_for_migration(&manifest)?
        {
            publication.replace_immutable(space, vec![(key, value)])?;
        }
    }
    crate::migration::publish::publish(
        storage,
        expected_revision,
        from_protocol,
        to_protocol,
        publication,
    )
    .await
}

fn storage_error(error: StorageError) -> LixError {
    LixError::new(
        LixError::CODE_INTERNAL_ERROR,
        format!("repository migration storage error: {error}"),
    )
}

fn migration_error(message: impl Into<String>) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changelog::CommitId;
    use crate::hot_state::{
        TrackedWorkingDiffEpoch, WorkingDiffIndexCoverage, stage_tracked_working_diff_epoch,
    };
    use crate::storage::{Memory, StorageWrite, WriteOptions};
    use crate::storage_adapter::{PutBatch, PutEntry, StorageValue};
    use crate::tracked_state::TrackedStateRootId;

    #[test]
    fn v74_preflight_teaches_legacy_reserved_column_rename() {
        let schema = serde_json::json!({
            "$schema": "https://lix.dev/schema-v1.json",
            "key": "legacy_reserved_column",
            "columns": [
                { "name": "id", "type": "text", "nullable": false },
                { "name": "lixcol_user_value", "type": "text", "nullable": true }
            ],
            "primary_key": ["id"]
        });
        let error = validate_v74_registered_schema("legacy_reserved_column", &schema)
            .expect_err("v74 must reject a formerly valid reserved column name");
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_SCHEMA_INCOMPATIBLE");
        assert!(error.message.contains("lixcol_user_value"));
        assert!(error.message.contains("replacement schema under a new key"));
    }

    #[tokio::test]
    async fn v69_marker_is_rejected_by_the_v75_hard_cut() {
        let storage = Memory::new();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.close().await.unwrap();
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let mut seed = adapter.new_write_set();
        seed.put(
            REPOSITORY_PROTOCOL_SPACE,
            REPOSITORY_PROTOCOL_KEY,
            &b"tracked-default-branch.v69"[..],
        );
        adapter
            .commit_write_set(seed, WriteOptions::default())
            .await
            .unwrap();

        let error = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .expect_err("v75 intentionally has no in-place migration from v69");
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Required {
                from_version: 69,
                to_version: CURRENT_FORMAT_VERSION,
            }
        );
    }

    #[tokio::test]
    async fn v73_repository_migrates_through_the_v74_backfill() {
        let storage = Memory::new();
        let (_branch_id, _head_commit_id, rootless_commit_id, _) =
            seed_rooted_head_with_rootless_checkpoint_cursor(
                &storage,
                REPOSITORY_PROTOCOL_V73,
            )
            .await;
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());

        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Required {
                from_version: 73,
                to_version: CURRENT_FORMAT_VERSION,
            }
        );
        let pre_migration = SharedStorageAdapterRead::new(
            adapter.begin_read(ReadOptions::default()).await.unwrap(),
        );
        let pre_migration_commit_ids =
            crate::tracked_state::scan_commit_state_manifest_commit_ids(&pre_migration)
                .await
                .unwrap();
        for commit_id in pre_migration_commit_ids {
            let manifest =
                crate::tracked_state::load_commit_state_manifest(&pre_migration, commit_id)
                    .await
                    .unwrap()
                    .unwrap();
            assert!(
                manifest.row_pk_index_root_id.is_none(),
                "v73 fixture commit '{commit_id}' must genuinely lack the new index"
            );
        }
        assert!(
            crate::tracked_state::load_commit_state_manifest(
                &pre_migration,
                rootless_commit_id,
            )
            .await
            .unwrap()
            .unwrap()
            .snapshot_root
            .is_none()
        );
        pre_migration.finish().unwrap();
        let report = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .expect("a v73 repository migrates through the v74 backfill to v75");
        assert_eq!(report.from_version, 73);
        assert_eq!(report.to_version, CURRENT_FORMAT_VERSION);
        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Current {
                version: CURRENT_FORMAT_VERSION,
            }
        );
    }

    #[tokio::test]
    async fn v72_index_bootstrap_marker_resumes_through_the_chain() {
        let storage = Memory::new();
        seed_rooted_head_with_rootless_checkpoint_cursor(&storage, REPOSITORY_PROTOCOL_V72).await;
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());

        backfill_missing_row_pk_indexes(
            &adapter,
            &storage,
            MigrationOptions::default(),
            72,
            REPOSITORY_PROTOCOL_V72,
            crate::init::REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP,
            "test v72 bootstrap",
            false,
        )
        .await
        .unwrap();

        assert_eq!(
            load_repository_protocol_marker(&adapter)
                .await
                .unwrap()
                .as_deref(),
            Some(crate::init::REPOSITORY_PROTOCOL_V72_ROW_PK_BOOTSTRAP)
        );
        assert!(
            crate::engine::Engine::new(storage.clone()).await.is_err(),
            "normal engine open must reject an interrupted bootstrap"
        );
        let report = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .expect("an interrupted v72 bootstrap resumes through the chain to v75");
        assert_eq!(report.from_version, 72);
        assert_eq!(report.to_version, CURRENT_FORMAT_VERSION);
        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Current {
                version: CURRENT_FORMAT_VERSION,
            }
        );
    }

    async fn seed_rooted_head_with_rootless_checkpoint_cursor(
        storage: &Memory,
        protocol: &'static [u8],
    ) -> (String, CommitId, CommitId, TrackedStateRootId) {
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value) VALUES ('migration-chronology-head', 'rooted')",
            &[],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_file (id, path) VALUES \
             ('01920000-0000-7000-8000-0000000000a1', '/migration-a'), \
             ('01920000-0000-7000-8000-0000000000a2', '/migration-b')",
            &[],
        )
        .await
        .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key, value, lixcol_file_id) VALUES \
             ('migration-shared-pk', 'a', '01920000-0000-7000-8000-0000000000a1'), \
             ('migration-shared-pk', 'b', '01920000-0000-7000-8000-0000000000a2')",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();

        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let read = SharedStorageAdapterRead::new(
            adapter.begin_read(ReadOptions::default()).await.unwrap(),
        );
        let controls = BranchHeadControlContext::new()
            .reader(read.clone())
            .scan()
            .await
            .unwrap();
        let mut candidates = controls.into_iter().filter_map(|(branch_id, control)| {
            control
                .working_diff_checkpoint_commit_id
                .filter(|checkpoint_commit_id| *checkpoint_commit_id != control.head_commit_id)
                .map(|_| (branch_id, control))
        });
        let (branch_id, control) = candidates
            .next()
            .expect("fixture should have one branch with a distinct checkpoint cursor");
        assert!(
            candidates.next().is_none(),
            "fixture should have only one branch with a distinct checkpoint cursor"
        );
        let head_commit_id = control.head_commit_id;
        let checkpoint_commit_id = control
            .working_diff_checkpoint_commit_id
            .expect("fixture branch should retain its initial checkpoint cursor");
        assert_ne!(head_commit_id, checkpoint_commit_id);

        let head_manifest =
            crate::tracked_state::load_commit_state_manifest(&read, head_commit_id)
                .await
                .unwrap()
                .unwrap();
        let head_root_id = head_manifest
            .snapshot_root
            .as_ref()
            .expect("fixture head should already be rooted")
            .root_id
            .clone();
        let commit_ids = crate::tracked_state::scan_commit_state_manifest_commit_ids(&read)
            .await
            .unwrap();
        let mut manifests = Vec::with_capacity(commit_ids.len());
        for commit_id in commit_ids {
            manifests.push(
                crate::tracked_state::load_commit_state_manifest(&read, commit_id)
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        let checkpoint_manifest = manifests
            .iter_mut()
            .find(|manifest| manifest.commit_id == checkpoint_commit_id)
            .expect("fixture checkpoint manifest should be retained");
        assert!(checkpoint_manifest.snapshot_root.is_some());
        checkpoint_manifest.snapshot_root = None;
        checkpoint_manifest.replay_debt = CommitStateReplayDebt {
            depth: 1,
            rows: u64::from(checkpoint_manifest.mutations.member_count),
            bytes: 1,
        };
        let replacements = manifests
            .into_iter()
            .flat_map(|mut manifest| {
                manifest.row_pk_index_root_id = None;
                encode_commit_state_manifest_replacement_for_migration(&manifest).unwrap()
            })
            .collect::<Vec<_>>();
        read.finish().unwrap();

        let mut fixture = storage.begin_write(WriteOptions::default()).await.unwrap();
        for (space, key, value) in replacements {
            fixture
                .replace_many(
                    space,
                    PutBatch {
                        entries: vec![PutEntry {
                            key: Key(Bytes::from(key)),
                            value: StorageValue {
                                bytes: Bytes::from(value),
                            },
                        }],
                    },
                )
                .await
                .unwrap();
        }
        fixture
            .put_many(
                REPOSITORY_PROTOCOL_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                        value: StorageValue {
                            bytes: Bytes::from_static(protocol),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        fixture.commit().await.unwrap();

        (
            branch_id,
            head_commit_id,
            checkpoint_commit_id,
            head_root_id,
        )
    }

    async fn assert_chronology_roots_promoted(
        storage: &Memory,
        head_commit_id: CommitId,
        checkpoint_commit_id: CommitId,
        original_head_root_id: &TrackedStateRootId,
    ) {
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let read = SharedStorageAdapterRead::new(
            adapter.begin_read(ReadOptions::default()).await.unwrap(),
        );
        let head_manifest =
            crate::tracked_state::load_commit_state_manifest(&read, head_commit_id)
                .await
                .unwrap()
                .unwrap();
        assert_eq!(
            &head_manifest
                .snapshot_root
                .expect("rooted head must remain rooted")
                .root_id,
            original_head_root_id,
        );
        let checkpoint_manifest =
            crate::tracked_state::load_commit_state_manifest(&read, checkpoint_commit_id)
                .await
                .unwrap()
                .unwrap();
        assert!(
            checkpoint_manifest.snapshot_root.is_some(),
            "distinct working-diff checkpoint cursor must be promoted"
        );
        assert_eq!(checkpoint_manifest.replay_debt, CommitStateReplayDebt::default());
        read.finish().unwrap();
    }

    #[tokio::test]
    async fn v69_promotion_roots_head_and_distinct_checkpoint_cursor_before_v70() {
        let storage = Memory::new();
        let (_branch_id, head_commit_id, checkpoint_commit_id, head_root_id) =
            seed_rooted_head_with_rootless_checkpoint_cursor(
                &storage,
                REPOSITORY_PROTOCOL_V69,
            )
            .await;
        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        promote_chronology_roots(
            &adapter,
            &storage,
            MigrationOptions::default(),
            69,
            REPOSITORY_PROTOCOL_V69,
            REPOSITORY_PROTOCOL_V70,
        )
        .await
        .unwrap();
        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Required {
                from_version: 70,
                to_version: CURRENT_FORMAT_VERSION,
            }
        );
        assert_chronology_roots_promoted(
            &storage,
            head_commit_id,
            checkpoint_commit_id,
            &head_root_id,
        )
        .await;

        let error = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .expect_err("v75 intentionally cuts after the tested v69-to-v70 edge");
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
    }

    #[tokio::test]
    async fn v70_repair_promotes_distinct_rootless_checkpoint_cursor() {
        let storage = Memory::new();
        let (branch_id, head_commit_id, checkpoint_commit_id, head_root_id) =
            seed_rooted_head_with_rootless_checkpoint_cursor(
                &storage,
                REPOSITORY_PROTOCOL_V70,
            )
            .await;

        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        promote_chronology_roots(
            &adapter,
            &storage,
            MigrationOptions::default(),
            70,
            REPOSITORY_PROTOCOL_V70,
            REPOSITORY_PROTOCOL_V71,
        )
        .await
        .unwrap();
        let mut stale_epoch = adapter.new_write_set();
        stage_tracked_working_diff_epoch(
            &mut stale_epoch,
            &branch_id,
            TrackedWorkingDiffEpoch {
                checkpoint_commit_id,
                generation: CommitId::with_change_address_space(uuid::Uuid::now_v7()),
                coverage: WorkingDiffIndexCoverage::default(),
            },
        )
        .unwrap();
        adapter
            .commit_write_set(stale_epoch, Default::default())
            .await
            .unwrap();

        assert_eq!(
            inspect_lix(&storage).await.unwrap(),
            MigrationStatus::Required {
                from_version: 71,
                to_version: CURRENT_FORMAT_VERSION,
            }
        );
        let error = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .expect_err("v75 intentionally cuts after the tested v70-to-v71 edge");
        assert_eq!(error.code, "LIX_ERROR_MIGRATION_FAILED");
        assert_chronology_roots_promoted(
            &storage,
            head_commit_id,
            checkpoint_commit_id,
            &head_root_id,
        )
        .await;

    }
}
