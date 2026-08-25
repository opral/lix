use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::LixError;
use crate::branch::{
    BranchHeadControlContext, branch_head_control_precondition, stage_branch_head_control,
};
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
    StorageReadOptions as ReadOptions, StorageWrite, StorageWriteOptions as WriteOptions,
};
use crate::storage_adapter::StorageWriteOptions as AdapterWriteOptions;
use crate::tracked_state::{
    CommitStateReplayDebt, TrackedStateContext, TrackedStateFilter, TrackedStateReadColumns,
    TrackedStateScanRequest,
    encode_commit_state_manifest_replacement_for_migration,
    load_rebuild_plans_to_nearest_available_root_bounded, stage_rebuild_plan_with_writer,
};

const REPOSITORY_PROTOCOL_V68: &[u8] = b"tracked-default-branch.v68";
const REPOSITORY_PROTOCOL_V69: &[u8] = b"tracked-default-branch.v69";
const REPOSITORY_PROTOCOL_V70: &[u8] = b"tracked-default-branch.v70";
const REPOSITORY_PROTOCOL_V71: &[u8] = b"tracked-default-branch.v71";

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
/// backend-level backup first. Each format edge publishes atomically. A v68
/// migration may stop in a valid, retryable v69 state after its typed rewrite;
/// chronology-root chunks may likewise be persisted unreferenced before each
/// edge's final atomic manifest replacement and marker publication.
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
    let from_version = match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired {
            found_version: found_version @ (68 | 69 | 70 | 71),
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
        let hot_rows_rewritten =
            migrate_v71_working_diff_epochs(&adapter, &storage, options).await?;
        return Ok(MigrationReport {
            from_version,
            to_version: CURRENT_FORMAT_VERSION,
            changes_rewritten: 0,
            commit_members_rewritten: 0,
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
    Ok(MigrationReport {
        from_version: 68,
        to_version: CURRENT_FORMAT_VERSION,
        changes_rewritten,
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
        crate::init::REPOSITORY_PROTOCOL_VALUE,
        crate::migration::publish::PublicationPlan::bounded(0, 0),
    )
    .await?;
    Ok(hot_rows_rewritten)
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

    #[tokio::test]
    async fn migrates_v69_with_a_marker_only_publication() {
        let storage = Memory::new();
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

        let report = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .unwrap();
        assert_eq!(
            report,
            MigrationReport {
                from_version: 69,
                to_version: CURRENT_FORMAT_VERSION,
                changes_rewritten: 0,
                commit_members_rewritten: 0,
                hot_rows_rewritten: 0,
            }
        );
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
        let mut checkpoint_manifest =
            crate::tracked_state::load_commit_state_manifest(&read, checkpoint_commit_id)
                .await
                .unwrap()
                .unwrap();
        assert!(checkpoint_manifest.snapshot_root.is_some());
        checkpoint_manifest.snapshot_root = None;
        checkpoint_manifest.replay_debt = CommitStateReplayDebt {
            depth: 1,
            rows: u64::from(checkpoint_manifest.mutations.member_count),
            bytes: 1,
        };
        let replacements = encode_commit_state_manifest_replacement_for_migration(
            &checkpoint_manifest,
        )
        .unwrap();
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
                to_version: 72,
            }
        );
        assert_chronology_roots_promoted(
            &storage,
            head_commit_id,
            checkpoint_commit_id,
            &head_root_id,
        )
        .await;

        migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.create_checkpoint().await.unwrap();
        lix.close().await.unwrap();
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
                to_version: 72,
            }
        );
        let report = migrate_lix(storage.clone(), MigrationOptions::default())
            .await
            .unwrap();
        assert_eq!(report.from_version, 71);
        assert_eq!(report.to_version, 72);
        assert!(
            report.hot_rows_rewritten > 0,
            "the v71 edge must repair the stale working-diff epoch before publishing v72"
        );
        assert_chronology_roots_promoted(
            &storage,
            head_commit_id,
            checkpoint_commit_id,
            &head_root_id,
        )
        .await;

        let lix = crate::open_lix().with_storage(storage).await.unwrap();
        lix.create_checkpoint().await.unwrap();
        lix.close().await.unwrap();
    }
}
