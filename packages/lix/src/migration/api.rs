use std::collections::{BTreeMap, BTreeSet};

use bytes::Bytes;

use crate::LixError;
use crate::branch::BranchHeadControlContext;
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
use crate::tracked_state::{
    CommitStateReplayDebt, TrackedStateContext,
    encode_commit_state_manifest_replacement_for_migration,
    load_rebuild_plans_to_nearest_available_root_bounded, stage_rebuild_plan_with_writer,
};

const REPOSITORY_PROTOCOL_V68: &[u8] = b"tracked-default-branch.v68";
const REPOSITORY_PROTOCOL_V69: &[u8] = b"tracked-default-branch.v69";

/// Bounds for the intentionally small-repository v68 migration.
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

/// Offline, bounded repository migration to the current format.
///
/// Callers must place the repository in maintenance mode and take their
/// backend-level backup first. Each format edge publishes atomically. A v68
/// migration may stop in a valid, retryable v69 state after its typed rewrite;
/// v69 root chunks may likewise be persisted unreferenced before the final
/// atomic manifest replacement and v70 marker publication.
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
    let from_version = match crate::init::repository_protocol_status(&read).await? {
        RepositoryProtocolStatus::MigrationRequired {
            found_version: found_version @ (68 | 69),
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
    let expected_revision =
        crate::storage_adapter::StorageAdapter::<S>::load_mutation_revision_from_read(&read)
            .await
            .map_err(storage_error)?;
    if from_version == 69 {
        drop(read);
        promote_v69_branch_head_roots(&adapter, &storage, options).await?;
        return Ok(MigrationReport {
            from_version,
            to_version: CURRENT_FORMAT_VERSION,
            changes_rewritten: 0,
            commit_members_rewritten: 0,
            hot_rows_rewritten: 0,
        });
    }
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
    promote_v69_branch_head_roots(&adapter, &storage, options).await?;
    Ok(MigrationReport {
        from_version: 68,
        to_version: CURRENT_FORMAT_VERSION,
        changes_rewritten,
        commit_members_rewritten: commit_plan.member_count,
        hot_rows_rewritten,
    })
}

/// Promotes every v69 branch head to a durable root fence before v70 makes
/// complete-state checkpoint aliases depend on that invariant.
///
/// Chunk publication is deliberately separate from authority publication.
/// Content-addressed chunks are safe to leave unreferenced after a crash; the
/// immutable manifest replacements and protocol marker are not, so those land
/// together in the final atomic publication.
async fn promote_v69_branch_head_roots<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    storage: &S,
    options: MigrationOptions,
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
        RepositoryProtocolStatus::MigrationRequired { found_version: 69 } => {}
        status => {
            return Err(migration_error(format!(
                "v69 root promotion observed unexpected repository status {status:?}"
            )));
        }
    }
    let expected_revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .map_err(storage_error)?;
    let head_commit_ids = BranchHeadControlContext::new()
        .reader(read.clone())
        .scan()
        .await?
        .into_iter()
        .map(|(_, control)| control.head_commit_id)
        .collect::<BTreeSet<_>>();
    if head_commit_ids.len() > options.max_changes {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v69 root promotion exceeds configured head bound: {} heads",
                head_commit_ids.len()
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
        for head_commit_id in head_commit_ids {
            let manifest = crate::tracked_state::load_commit_state_manifest(&read, head_commit_id)
                .await?
                .ok_or_else(|| {
                    migration_error(format!(
                        "v69 branch head '{head_commit_id}' has no commit-state manifest"
                    ))
                })?;
            if manifest.snapshot_root.is_some() {
                continue;
            }
            let plans = load_rebuild_plans_to_nearest_available_root_bounded(
                &read,
                &head_commit_id.to_string(),
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
                                "v69 root promotion exceeds configured replay-commit bound: {} commits",
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
                                    "v69 replay commit '{}' has no commit-state manifest",
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
                                    "v69 root promotion did not stage replay commit '{}'",
                                    plan.commit_id
                                ))
                            })?;
                        plan_manifest.snapshot_root = Some(Box::new(snapshot_root));
                        plan_manifest.replay_debt = CommitStateReplayDebt::default();
                        promotions.insert(plan.commit_id, plan_manifest);
                    }
                }
            }
            if !promotions.contains_key(&head_commit_id) {
                return Err(migration_error(format!(
                    "v69 root promotion did not authorize branch head '{head_commit_id}'"
                )));
            }
        }
    }

    let chunk_stats = chunk_writes.stats();
    if chunk_stats.written_bytes > options.max_preflight_bytes as u64 {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
            format!(
                "v69 root promotion exceeds configured byte bound: {} bytes",
                chunk_stats.written_bytes
            ),
        ));
    }
    if chunk_stats.staged_puts != 0 || chunk_stats.staged_deletes != 0 {
        // Chunk rows are content-addressed and not authority until the final
        // manifest publication. Persist them without rotating the repository
        // mutation token, so that exact original token still fences every
        // concurrent domain write through the final v70 marker flip.
        let mut write = storage
            .begin_write(WriteOptions {
                await_durable: true,
                preconditions: vec![
                    Precondition::KeyValueEquals {
                        space: REPOSITORY_PROTOCOL_SPACE,
                        key: Key(Bytes::from_static(REPOSITORY_PROTOCOL_KEY)),
                        expected: Bytes::from_static(REPOSITORY_PROTOCOL_V69),
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
        REPOSITORY_PROTOCOL_V69,
        crate::init::REPOSITORY_PROTOCOL_VALUE,
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
    use crate::storage::{Memory, StorageWrite, WriteOptions};
    use crate::storage_adapter::{PutBatch, PutEntry, StorageValue};

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

        let report = migrate_repository(storage.clone(), MigrationOptions::default())
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
            inspect_repository(&storage).await.unwrap(),
            MigrationStatus::Current {
                version: CURRENT_FORMAT_VERSION,
            }
        );
    }

    #[tokio::test]
    async fn migrates_rootless_v69_branch_heads_before_enabling_checkpoint_aliases() {
        let storage = Memory::new();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        drop(lix);

        let adapter = crate::storage_adapter::StorageAdapter::new(storage.clone());
        let read = SharedStorageAdapterRead::new(
            adapter.begin_read(ReadOptions::default()).await.unwrap(),
        );
        let head_commit_ids = BranchHeadControlContext::new()
            .reader(read.clone())
            .scan()
            .await
            .unwrap()
            .into_iter()
            .map(|(_, control)| control.head_commit_id)
            .collect::<BTreeSet<_>>();
        let mut replacements = Vec::new();
        for commit_id in &head_commit_ids {
            let mut manifest = crate::tracked_state::load_commit_state_manifest(&read, *commit_id)
                .await
                .unwrap()
                .unwrap();
            manifest.snapshot_root = None;
            manifest.replay_debt = CommitStateReplayDebt {
                depth: 1,
                rows: u64::from(manifest.mutations.member_count),
                bytes: 1,
            };
            for replacement in
                encode_commit_state_manifest_replacement_for_migration(&manifest).unwrap()
            {
                replacements.push(replacement);
            }
        }
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
                            bytes: Bytes::from_static(REPOSITORY_PROTOCOL_V69),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        fixture.commit().await.unwrap();

        migrate_repository(storage.clone(), MigrationOptions::default())
            .await
            .unwrap();
        let read = SharedStorageAdapterRead::new(
            adapter.begin_read(ReadOptions::default()).await.unwrap(),
        );
        for commit_id in head_commit_ids {
            let manifest = crate::tracked_state::load_commit_state_manifest(&read, commit_id)
                .await
                .unwrap()
                .unwrap();
            assert!(manifest.snapshot_root.is_some());
        }
        read.finish().unwrap();

        let lix = crate::open_lix().with_storage(storage).await.unwrap();
        lix.create_checkpoint().await.unwrap();
    }
}
