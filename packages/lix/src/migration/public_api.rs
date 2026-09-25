//! Read-only inventory and detached operator entry points. Inspection never
//! admits an engine or repairs a repository as a side effect.
use crate::storage_adapter::StorageAdapterRead as _;
use crate::{
    LixError,
    storage_adapter::{PointReadPlan, Storage, StorageKey, StorageProjectedValue, StorageSession},
};
use bytes::Bytes;
use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryRole {
    Authority,
    PartialReplica,
    FullReplica,
    Standalone,
    Empty,
    Invalid,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RepositoryLayout {
    Active,
    Legacy,
    Interrupted,
    Empty,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RepositoryInspection {
    pub format: Option<u32>,
    pub role: RepositoryRole,
    pub layout: RepositoryLayout,
    pub current: bool,
    pub protocol_epoch: u32,
}

/// The caller must coordinate storage ownership with live applications. This
/// function acquires an adapter session but performs only point reads.
pub async fn inspect_repository<S>(storage: S) -> Result<RepositoryInspection, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    inspect_owned(&storage).await
}

async fn inspect_owned<S>(storage: &S) -> Result<RepositoryInspection, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (layout, epoch_format) = super::epoch::inspect_layout(storage).await?;
    let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
    let read = adapter.begin_read(Default::default()).await?;
    let mut values = Vec::new();
    for (space, key) in [
        (
            crate::init::REPOSITORY_PROTOCOL_SPACE,
            StorageKey(Bytes::from_static(crate::init::REPOSITORY_PROTOCOL_KEY)),
        ),
        (
            crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            crate::sync::authority_state_key(),
        ),
        (
            crate::sync::SYNC_REPLICA_STATE_SPACE,
            crate::sync::replica_state_key(),
        ),
        (
            crate::sync::PARTIAL_REPLICA_STATE_SPACE,
            crate::sync::partial_replica_state_key(),
        ),
    ] {
        values.push(
            PointReadPlan::new(space, &[key])
                .materialize(&read, Default::default())
                .await?
                .value
                .pop()
                .flatten(),
        );
    }
    let marker = match &values[0] {
        Some(StorageProjectedValue::FullValue(value)) => Some(value.as_ref()),
        None => None,
        _ => {
            return Err(LixError::new(
                "LIX_INVALID_REPOSITORY",
                "format probe omitted value",
            ));
        }
    };
    let marker_format = marker
        .and_then(|raw| std::str::from_utf8(raw).ok())
        .and_then(|raw| raw.strip_prefix("tracked-default-branch.v"))
        .and_then(|raw| raw.split('-').next())
        .and_then(|raw| raw.parse::<u32>().ok());
    let format = epoch_format.or(marker_format);
    let roles = [
        values[1].is_some(),
        values[2].is_some(),
        values[3].is_some(),
    ];
    let role = match roles {
        [true, false, false] => RepositoryRole::Authority,
        [false, true, false] => RepositoryRole::FullReplica,
        [false, false, true] => RepositoryRole::PartialReplica,
        [false, false, false] if marker.is_some() => RepositoryRole::Standalone,
        [false, false, false] if layout == RepositoryLayout::Empty => RepositoryRole::Empty,
        _ => RepositoryRole::Invalid,
    };
    let expected = if role == RepositoryRole::PartialReplica {
        crate::init::PARTIAL_REPOSITORY_PROTOCOL_VALUE
    } else {
        crate::init::REPOSITORY_PROTOCOL_VALUE
    };
    Ok(RepositoryInspection {
        format,
        role,
        layout,
        current: layout == RepositoryLayout::Active
            && format == Some(crate::CURRENT_STORAGE_FORMAT_VERSION)
            && marker == Some(expected)
            && (role != RepositoryRole::Authority
                || matches!(&values[1], Some(StorageProjectedValue::FullValue(value)) if value.as_ref() == crate::sync::AUTHORITY_STATE_VALUE))
            && !matches!(role, RepositoryRole::Invalid | RepositoryRole::Empty),
        protocol_epoch: crate::SYNC_PROTOCOL_VERSION,
    })
}

#[derive(Debug, Clone, Serialize)]
pub struct RepositoryMigrationReport {
    /// Portable embedded identity, read from repository contents. Hosted URL/catalog
    /// identities can intentionally differ after cloning or restoring a repository.
    pub embedded_repository_id: String,
    pub before: RepositoryInspection,
    pub after: RepositoryInspection,
    /// Candidate validation is not a substitute for source-format-specific
    /// semantic validation by the fleet tooling.
    pub semantic_preservation_verified: bool,
    pub preservation_basis: &'static str,
    pub expected_content_digest: String,
    pub before_content_digest: String,
    pub after_content_digest: String,
}

/// Explicit, resumable copy-and-activate migration. Close all repository handles
/// and hold the physical owner fence before calling. Source banks are retained.
/// Operator tools can invoke this directly; normal repository opening already
/// coordinates supported upgrades through the same Rust migration machinery.
pub async fn migrate_repository<S>(storage: S) -> Result<RepositoryMigrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    migrate_owned(&storage, super::MigrationOptions::default()).await
}

// Construct the historical orchestration future on its own frame. Keeping it
// inline in restore/open callers leaves too little stack for schema SQL writes.
#[inline(never)]
fn migrate_owned<S>(
    storage: &S,
    options: super::MigrationOptions,
) -> std::pin::Pin<Box<impl Future<Output = Result<RepositoryMigrationReport, LixError>> + '_>>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    Box::pin(migrate_owned_with_progress(storage, options, None))
}

pub(crate) async fn migrate_owned_with_progress<S>(
    storage: &S,
    options: super::MigrationOptions,
    progress: Option<&std::sync::Arc<dyn crate::OpenProgressSink>>,
) -> Result<RepositoryMigrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let before = inspect_owned(storage).await?;
    if matches!(before.role, RepositoryRole::Empty | RepositoryRole::Invalid) {
        return Err(LixError::new(
            "LIX_INVALID_REPOSITORY",
            "migration requires an existing recognized repository",
        ));
    }
    let before_content_digest = content_digest(storage).await?;
    let older_witness = if matches!(before.format, Some(74 | 77 | 78)) {
        Some(
            super::older_witness::plan(storage, options, before.role == RepositoryRole::Authority)
                .await?,
        )
    } else {
        None
    };
    let (preservation_basis, expected_content_digest) = if let Some(witness) = &older_witness {
        (
            "source-descriptors-canonical-chain-v1",
            witness.expected_digest.clone(),
        )
    } else if before.format == Some(79) {
        let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
        let read = super::MigrationPlanningRead::new(&adapter).await?;
        let mut plan = super::incorporation::preservation_plan(&read, options).await?;
        if before.role == RepositoryRole::Authority {
            plan.put_mutable(
                crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                vec![(
                    crate::sync::authority_state_key().0.to_vec(),
                    crate::sync::AUTHORITY_STATE_VALUE.to_vec(),
                )],
            )?;
        }
        super::publish::append_partial_metadata_upgrade(&read, &mut plan).await?;
        read.finish()?;
        Box::pin(super::hot_indexes::append_plan(
            &adapter, options, &mut plan,
        ))
        .await?;
        (
            "v79-canonical-plan-v1",
            content_digest_with_plan(storage, Some(plan)).await?,
        )
    } else if before.role == RepositoryRole::Authority && !before.current {
        let mut plan = super::publish::PublicationPlan::bounded(
            options.max_changes,
            options.max_preflight_bytes,
        );
        plan.put_mutable(
            crate::sync::SYNC_AUTHORITY_STATE_SPACE,
            vec![(
                crate::sync::authority_state_key().0.to_vec(),
                crate::sync::AUTHORITY_STATE_VALUE.to_vec(),
            )],
        )?;
        let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
        // A current-format authority capability upgrade does not rebuild
        // storage indexes; project only mutations the migration will execute.
        if before.format != Some(crate::init::CURRENT_FORMAT_VERSION) {
            Box::pin(super::hot_indexes::append_plan(
                &adapter, options, &mut plan,
            ))
            .await?;
        }
        (
            "authority-capability-marker-v1",
            content_digest_with_plan(storage, Some(plan)).await?,
        )
    } else if matches!(before.format, Some(80 | 81)) {
        let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
        let mut plan = super::publish::PublicationPlan::bounded(
            options.max_changes,
            options.max_preflight_bytes,
        );
        let read = super::MigrationPlanningRead::new(&adapter).await?;
        super::publish::append_partial_metadata_upgrade(&read, &mut plan).await?;
        read.finish()?;
        Box::pin(super::hot_indexes::append_plan(
            &adapter, options, &mut plan,
        ))
        .await?;
        (
            "v82-hot-index-plan-v1",
            content_digest_with_plan(storage, Some(plan)).await?,
        )
    } else {
        ("exact-records-v1", before_content_digest.clone())
    };
    super::epoch::admit_repository_with_options(storage, progress, None, options).await?;
    if before.role == RepositoryRole::Authority {
        super::authority_baseline_fence::upgrade_authority_native_baseline_fence(storage).await?;
    }
    let after = inspect_owned(storage).await?;
    if !after.current || before.role != after.role {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "migrated format or repository role did not validate",
        ));
    }
    let embedded_repository_id = validated_repository_id(storage, after.role).await?;
    // Witness the final candidate, including anything identity validation touched.
    let after_content_digest = content_digest(storage).await?;
    if let Some(witness) = &older_witness {
        witness.verify(storage, options).await?;
    }
    Ok(RepositoryMigrationReport {
        embedded_repository_id,
        semantic_preservation_verified: (older_witness.is_some()
            || before.format.is_some_and(|format| format >= 79))
            && expected_content_digest == after_content_digest,
        preservation_basis,
        expected_content_digest,
        before_content_digest,
        after_content_digest,
        before,
        after,
    })
}

async fn validated_repository_id<S>(storage: &S, role: RepositoryRole) -> Result<String, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
    if role == RepositoryRole::PartialReplica {
        let read = adapter.begin_read(Default::default()).await?;
        let state = crate::sync::load_partial_replica_state(&read)
            .await?
            .ok_or_else(|| {
                LixError::new(
                    "LIX_INVALID_REPOSITORY",
                    "partial repository identity missing",
                )
            })?
            .0;
        drop(read);
        let (engine, session) = crate::engine::Engine::new_partial_replica(
            adapter,
            crate::engine::EngineOptions::new(),
            &state,
        )
        .await?;
        let id = engine.lix_id().to_owned();
        drop(session);
        Ok(id)
    } else {
        let engine =
            crate::engine::Engine::new_with_adapter(adapter, crate::engine::EngineOptions::new())
                .await?;
        Ok(engine.lix_id().to_owned())
    }
}

/// Domain-separated digest of every logical persisted record except the format
/// marker and mutation revision. Derived index changes are projected through
/// a source-derived bounded v82 rebuild plan.
/// Includes pending operations, blobs, history, and all deduplication receipts.
pub(super) async fn content_digest<S>(storage: &S) -> Result<String, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    content_digest_with_plan(storage, None).await
}

async fn content_digest_with_plan<S>(
    storage: &S,
    plan: Option<super::publish::PublicationPlan>,
) -> Result<String, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
    content_digest_with_adapter(&adapter, plan).await
}

pub(super) async fn content_digest_with_adapter<S>(
    adapter: &crate::storage_adapter::StorageAdapter<S>,
    plan: Option<super::publish::PublicationPlan>,
) -> Result<String, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (mut overlay, cleared) = plan
        .map(super::publish::PublicationPlan::into_preservation_overlay)
        .unwrap_or_default();
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let mut digest = blake3::Hasher::new();
    digest.update(b"lix.migration.logical-content.v1");
    for space in crate::storage_spaces::SNAPSHOT_STORAGE_SPACES {
        let mut replacements = overlay.remove(&space.id.0).unwrap_or_default();
        let mut cursor = read
            .begin_scan(
                *space,
                crate::storage_adapter::StoragePrefix {
                    bytes: Bytes::new(),
                }
                .to_range()?,
                Default::default(),
            )
            .await?;
        while let Some(entries) = cursor.next_chunk().await? {
            for entry in entries {
                if *space == crate::init::REPOSITORY_PROTOCOL_SPACE
                    && entry.key.0.as_ref() == crate::init::REPOSITORY_PROTOCOL_KEY
                {
                    continue;
                }
                if *space == crate::storage_adapter::REVISION_SPACE && entry.key.0.as_ref() == b"m"
                {
                    continue;
                }
                let StorageProjectedValue::FullValue(value) = entry.value else {
                    return Err(LixError::new(
                        "LIX_ERROR_MIGRATION_FAILED",
                        "content witness omitted a record value",
                    ));
                };
                while replacements
                    .first_key_value()
                    .is_some_and(|(key, _)| key < &entry.key.0)
                {
                    let (key, value) = replacements.pop_first().expect("checked entry");
                    digest_record(&mut digest, space.id.0, &key, &value);
                }
                if let Some(replacement) = replacements.remove(&entry.key.0) {
                    digest_record(&mut digest, space.id.0, &entry.key.0, &replacement);
                } else if !cleared.contains(&space.id.0) {
                    digest_record(&mut digest, space.id.0, &entry.key.0, &value);
                }
            }
        }
        for (key, value) in replacements {
            digest_record(&mut digest, space.id.0, &key, &value);
        }
    }
    if !overlay.is_empty() {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "preservation plan targeted an unaccounted storage space",
        ));
    }
    read.finish()?;
    Ok(digest.finalize().to_hex().to_string())
}

fn digest_record(digest: &mut blake3::Hasher, space: u32, key: &[u8], value: &[u8]) {
    digest.update(&space.to_le_bytes());
    digest.update(&(key.len() as u64).to_le_bytes());
    digest.update(key);
    digest.update(&(value.len() as u64).to_le_bytes());
    digest.update(value);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage_adapter::{PutBatch, PutEntry, StorageValue, StorageWrite};

    async fn expected_v82_digest<S>(storage: &S) -> String
    where
        S: Storage + Clone + Send + Sync + 'static,
    {
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(storage)
            .await
            .unwrap();
        let options = super::super::MigrationOptions::default();
        let mut plan = super::super::publish::PublicationPlan::bounded(
            options.max_changes,
            options.max_preflight_bytes,
        );
        super::super::hot_indexes::append_plan(&adapter, options, &mut plan)
            .await
            .unwrap();
        content_digest_with_plan(storage, Some(plan)).await.unwrap()
    }

    #[tokio::test]
    async fn normal_open_upgrades_v80_preserves_all_records_and_reports_progress() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('architectural-cut','preserved')",
            &[],
        )
        .await
        .unwrap();
        let original_id = lix.lix_id().to_owned();
        lix.close().await.unwrap();
        super::super::epoch::stage_v80_repository_for_test(&storage, false)
            .await
            .unwrap();
        let before = expected_v82_digest(&storage).await;
        let inspection = inspect_repository(storage.clone()).await.unwrap();
        assert_eq!(inspection.format, Some(80));
        assert!(!inspection.current);
        let events = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
        let observed = events.clone();
        let reopened = crate::open_lix()
            .with_storage(storage.clone())
            .on_progress(move |event| observed.lock().unwrap().push(event))
            .await
            .unwrap();
        assert_eq!(reopened.lix_id(), original_id);
        assert_eq!(
            reopened.open_report().migrations,
            vec![crate::OpenMigration {
                scope: crate::OpenScope::Local,
                from_format: 80,
                to_format: crate::CURRENT_STORAGE_FORMAT_VERSION,
            }]
        );
        assert!(
            events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event.phase == crate::OpenPhase::Migrating)
        );
        assert_eq!(content_digest(&storage).await.unwrap(), before);
        assert_eq!(
            reopened
                .execute(
                    "SELECT value FROM lix_key_value WHERE key='architectural-cut'",
                    &[]
                )
                .await
                .unwrap()
                .rows()
                .len(),
            1
        );
        reopened.close().await.unwrap();
        let current = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        assert!(current.open_report().migrations.is_empty());
        current.close().await.unwrap();
        assert!(
            migrate_repository(storage)
                .await
                .unwrap()
                .semantic_preservation_verified
        );
    }

    #[tokio::test]
    async fn normal_open_upgrades_v80_without_public_checkpoint_membership_loss() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('checkpoint-migration', 'preserved')",
            &[],
        )
        .await
        .unwrap();
        let checkpoint = lix
            .execute("SELECT commit_id FROM lix_create_checkpoint(NULL, NULL)", &[])
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        let undo = lix
            .execute(
                "SELECT commit_id FROM lix_undo($1)",
                &[crate::Value::Text(checkpoint.clone())],
            )
            .await
            .unwrap()
            .rows()[0]
            .get::<String>("commit_id")
            .unwrap();
        assert!(
            lix.execute(
                "SELECT value FROM lix_key_value WHERE key = 'checkpoint-migration'",
                &[],
            )
            .await
            .unwrap()
            .rows()
            .is_empty()
        );
        lix.close().await.unwrap();

        let before = expected_v82_digest(&storage).await;
        super::super::epoch::stage_v80_repository_for_test(&storage, false)
            .await
            .unwrap();
        let reopened = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        assert_eq!(
            reopened.open_report().migrations,
            vec![crate::OpenMigration {
                scope: crate::OpenScope::Local,
                from_format: 80,
                to_format: crate::CURRENT_STORAGE_FORMAT_VERSION,
            }]
        );
        assert_eq!(content_digest(&storage).await.unwrap(), before);

        let missing = reopened
            .execute("SELECT is_checkpoint FROM lix_commit", &[])
            .await
            .expect_err("checkpoint membership is no longer a lix_commit column");
        assert_eq!(missing.code, LixError::CODE_COLUMN_NOT_FOUND);
        for id in [&checkpoint, &undo] {
            assert_eq!(
                reopened
                    .execute(
                        "SELECT id FROM lix_commit WHERE id = $1",
                        &[crate::Value::Text(id.clone())],
                    )
                    .await
                    .unwrap()
                    .rows()
                    .len(),
                1,
                "migration must preserve commit ID {id}"
            );
        }
        assert!(
            reopened
                .execute(
                    "SELECT is_checkpoint FROM lix_log($1) WHERE commit_id = $2",
                    &[
                        crate::Value::Text(checkpoint.clone()),
                        crate::Value::Text(checkpoint.clone()),
                    ],
                )
                .await
                .unwrap()
                .rows()[0]
                .get::<bool>("is_checkpoint")
                .unwrap(),
            "immutable checkpoint membership must survive the upgrade"
        );
        assert!(
            !reopened
                .execute(
                    "SELECT is_checkpoint FROM lix_log($1) WHERE commit_id = $2",
                    &[
                        crate::Value::Text(undo.clone()),
                        crate::Value::Text(checkpoint.clone()),
                    ],
                )
                .await
                .unwrap()
                .rows()[0]
                .get::<bool>("is_checkpoint")
                .unwrap(),
            "undo state must continue to retire the checkpoint at its anchor"
        );
        reopened
            .execute(
                "SELECT commit_id FROM lix_redo($1)",
                &[crate::Value::Text(undo)],
            )
            .await
            .unwrap();
        assert_eq!(
            reopened
                .execute(
                    "SELECT value FROM lix_key_value WHERE key = 'checkpoint-migration'",
                    &[],
                )
                .await
                .unwrap()
                .rows()[0]
                .get::<serde_json::Value>("value")
                .unwrap(),
            serde_json::json!("preserved"),
            "content must remain recoverable after migration"
        );
        reopened.close().await.unwrap();
    }

    #[tokio::test]
    async fn partial_v79_to_v81_migration_preserves_admission_and_resident_records_offline() {
        for format in [79, 80, 81] {
            let authority = crate::open_lix().await.unwrap();
            let state = crate::sync::PartialReplicaState::new(
                format!("https://example.test/lix/{}", authority.lix_id()),
                authority.active_account_id().to_owned(),
                "00000000-0000-7000-8000-000000000599".into(),
                authority.partial_replica_descriptor(None).await.unwrap(),
            )
            .unwrap();
            authority.close().await.unwrap();
            let memory = crate::Memory::new();
            let storage = StorageSession::acquire(crate::sync::durable_memory_for_test(memory))
                .await
                .unwrap();
            let installed =
                super::super::epoch::install_fresh_partial_epoch(storage.clone(), &state)
                    .await
                    .unwrap();
            let expected = content_digest(&storage).await.unwrap();
            let mut legacy_receipt = serde_json::to_value(&state).unwrap();
            legacy_receipt["version"] = serde_json::json!(1);
            legacy_receipt
                .as_object_mut()
                .unwrap()
                .remove("archivedBranchIds");
            let mut writes = installed.adapter.new_write_set();
            // A sparse cache cannot certify whole-collection completeness. The
            // upgrade retires all old index records without requesting hydration.
            writes.put(
                crate::hot_state::INDEX_SPACE,
                b"old-untrusted-index".as_slice(),
                b"partial-cache".as_slice(),
            );
            writes.put(
                crate::sync::PARTIAL_REPLICA_STATE_SPACE,
                crate::sync::partial_replica_state_key(),
                serde_json::to_vec(&legacy_receipt).unwrap(),
            );
            // Seed historical metadata through the fixture's migration writer;
            // ordinary partial writer capabilities remain sync-private.
            use crate::storage_adapter::StorageWrite as _;
            let mut write = installed
                .adapter
                .begin_migration_write(Default::default())
                .await
                .unwrap();
            writes.lower_into(&mut write).await.unwrap();
            write.commit().await.unwrap();
            super::super::epoch::stage_repository_format_for_test(&storage, true, format)
                .await
                .unwrap();
            assert!(
                super::super::epoch::admit_partial_epoch(&storage)
                    .await
                    .is_err()
            );
            assert_ne!(content_digest(&storage).await.unwrap(), expected);
            let report = migrate_repository(storage.clone()).await.unwrap();
            assert!(report.semantic_preservation_verified);
            assert_eq!(report.expected_content_digest, report.after_content_digest);
            assert_eq!(report.after_content_digest, expected);
            let lix = crate::open_lix()
                .with_storage(storage.clone())
                .await
                .unwrap();
            assert_eq!(lix.lix_id(), state.repository_id());
            assert_eq!(report.before.format, Some(format));
            lix.close().await.unwrap();
            assert_eq!(content_digest(&storage).await.unwrap(), expected);
            let admitted = super::super::epoch::admit_partial_epoch(&storage)
                .await
                .unwrap();
            assert_eq!(admitted.state, state);
        }
    }

    #[tokio::test]
    async fn v79_canonical_plan_preserves_all_records_and_rejects_unplanned_changes() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('v79-preserved','source')",
            &[],
        )
        .await
        .unwrap();
        let embedded_id = lix.lix_id().to_owned();
        lix.close().await.unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        super::super::downgrade_headers_for_test(&adapter, false).await;
        drop(adapter);
        super::super::epoch::stage_repository_format_for_test(&storage, false, 79)
            .await
            .unwrap();
        let report = migrate_repository(storage.clone()).await.unwrap();
        assert!(report.semantic_preservation_verified);
        assert_eq!(report.preservation_basis, "v79-canonical-plan-v1");
        assert_ne!(report.before_content_digest, report.after_content_digest);
        assert_eq!(report.expected_content_digest, report.after_content_digest);
        assert_eq!(report.embedded_repository_id, embedded_id);
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "UPDATE lix_key_value SET value='unplanned' WHERE key='v79-preserved'",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        assert_ne!(
            content_digest(&storage).await.unwrap(),
            report.expected_content_digest,
            "an unplanned user-record mutation must never pass the canonical witness"
        );
    }

    #[tokio::test]
    async fn v5_authority_upgrade_changes_only_the_capability_marker() {
        verify_v5_authority_upgrade(false).await;
        verify_v5_authority_upgrade(true).await;
    }

    async fn verify_v5_authority_upgrade(from_v79: bool) {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.execute(
            "INSERT INTO lix_key_value (key,value) VALUES ('preserved','yes')",
            &[],
        )
        .await
        .unwrap();
        lix.close().await.unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let mut write = adapter
            .begin_migration_write(Default::default())
            .await
            .unwrap();
        write
            .put_many(
                crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: crate::sync::authority_state_key(),
                        value: StorageValue {
                            bytes: Bytes::from_static(
                                b"certified-authority-v5-native-baseline-leases",
                            ),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        drop(adapter);
        if from_v79 {
            super::super::epoch::stage_repository_format_for_test(&storage, false, 79)
                .await
                .unwrap();
        }
        let report = migrate_repository(storage.clone()).await.unwrap();
        assert_eq!(report.before.role, RepositoryRole::Authority);
        assert!(!report.before.current);
        assert!(report.after.current);
        assert!(report.semantic_preservation_verified);
        assert_eq!(
            report.preservation_basis,
            if from_v79 {
                "v79-canonical-plan-v1"
            } else {
                "authority-capability-marker-v1"
            }
        );
        assert_eq!(
            content_digest(&storage).await.unwrap(),
            report.expected_content_digest
        );
    }

    #[tokio::test]
    async fn authority_migration_preserves_role_and_logical_records() {
        let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
        crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let mut write = adapter
            .begin_migration_write(Default::default())
            .await
            .unwrap();
        write
            .put_many(
                crate::sync::SYNC_AUTHORITY_STATE_SPACE,
                PutBatch {
                    entries: vec![PutEntry {
                        key: crate::sync::authority_state_key(),
                        value: StorageValue {
                            bytes: Bytes::from_static(crate::sync::AUTHORITY_STATE_VALUE),
                        },
                    }],
                },
            )
            .await
            .unwrap();
        write.commit().await.unwrap();
        drop(adapter);
        super::super::epoch::stage_v80_repository_for_test(&storage, false)
            .await
            .unwrap();
        let report = migrate_repository(storage.clone()).await.unwrap();
        assert_eq!(report.before.role, RepositoryRole::Authority);
        assert_eq!(report.after.role, RepositoryRole::Authority);
        assert!(report.semantic_preservation_verified);
    }
}

/// Restore an historical snapshot into fresh storage and migrate explicitly.
/// The destination remains separate from the snapshot source on all failures.
pub async fn restore_and_migrate_repository<S, R>(
    storage: S,
    source: R,
) -> Result<RepositoryMigrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
    R: futures_io::AsyncRead + Unpin + Send + 'static,
{
    let owned = StorageSession::acquire(storage).await?;
    let owned = crate::snapshot::restore_snapshot(owned, source).await?;
    migrate_owned(&owned, super::MigrationOptions::default()).await
}

/// Explicit resource budgets for historical preflight. Exceeding a limit leaves
/// source storage recoverable and reports LIX_ERROR_MIGRATION_LIMIT_EXCEEDED.
pub async fn migrate_repository_with_options<S>(
    storage: S,
    options: super::MigrationOptions,
) -> Result<RepositoryMigrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    if options.max_changes == 0 || options.max_preflight_bytes == 0 {
        return Err(LixError::new(
            "LIX_ERROR_INVALID_PARAM",
            "migration limits must be positive",
        ));
    }
    let owned = StorageSession::acquire(storage).await?;
    migrate_owned(&owned, options).await
}

/// Sealed source witness for explicit standalone-to-authority certification.
/// Keep the physical owner barrier until verification and catalog publication.
#[derive(Debug)]
pub struct AuthorityActivationWitness {
    before_content_digest: String,
    expected_content_digest: String,
}

#[derive(Debug, Serialize)]
pub struct AuthorityActivationReport {
    pub before_content_digest: String,
    pub after_content_digest: String,
    pub semantic_preservation_verified: bool,
}

/// Prepare the only permitted activation mutation: install the current exact
/// authority capability marker. Serving must still perform its full eligibility
/// validation; this helper does not promote storage or admit an engine.
pub async fn prepare_authority_activation<S>(
    storage: S,
) -> Result<AuthorityActivationWitness, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    let state = inspect_owned(&storage).await?;
    if !state.current || state.role != RepositoryRole::Standalone {
        return Err(LixError::new(
            "LIX_AUTHORITY_ACTIVATION_INVALID",
            "activation witness requires a current standalone source",
        ));
    }
    let before_content_digest = content_digest(&storage).await?;
    let mut plan = super::publish::PublicationPlan::bounded(1, 1024);
    plan.put_mutable(
        crate::sync::SYNC_AUTHORITY_STATE_SPACE,
        vec![(
            crate::sync::authority_state_key().0.to_vec(),
            crate::sync::AUTHORITY_STATE_VALUE.to_vec(),
        )],
    )?;
    let expected_content_digest = content_digest_with_plan(&storage, Some(plan)).await?;
    Ok(AuthorityActivationWitness {
        before_content_digest,
        expected_content_digest,
    })
}

/// Verify all logical bytes after ordinary explicit serving certification.
/// A successful engine open or an Authority role alone is not preservation.
pub async fn verify_authority_activation<S>(
    storage: S,
    witness: AuthorityActivationWitness,
) -> Result<AuthorityActivationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    let state = inspect_owned(&storage).await?;
    let after_content_digest = content_digest(&storage).await?;
    if !state.current
        || state.role != RepositoryRole::Authority
        || after_content_digest != witness.expected_content_digest
    {
        return Err(LixError::new(
            "LIX_MIGRATION_PRESERVATION_FAILED",
            "authority activation changed records beyond the exact capability marker",
        ));
    }
    Ok(AuthorityActivationReport {
        before_content_digest: witness.before_content_digest,
        after_content_digest,
        semantic_preservation_verified: true,
    })
}

#[cfg(all(test, feature = "server-protocol"))]
mod activation_tests {
    use super::*;
    use crate::storage_adapter::{PutBatch, PutEntry, StorageValue, StorageWrite};
    #[tokio::test]
    async fn activation_witness_accepts_only_the_capability_marker() {
        for tamper in [false, true] {
            let storage = StorageSession::acquire(crate::Memory::new()).await.unwrap();
            crate::open_lix()
                .with_storage(storage.clone())
                .await
                .unwrap()
                .close()
                .await
                .unwrap();
            let witness = prepare_authority_activation(storage.clone()).await.unwrap();
            let server = crate::open_lix()
                .with_storage(storage.clone())
                .serve()
                .with_lix_id("00000000-0000-7000-8000-000000000811")
                .await
                .unwrap();
            server.close().await.unwrap();
            drop(server);
            if tamper {
                let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
                    .await
                    .unwrap();
                let mut write = adapter
                    .begin_migration_write(Default::default())
                    .await
                    .unwrap();
                write
                    .put_many(
                        crate::init::REPOSITORY_PROTOCOL_SPACE,
                        PutBatch {
                            entries: vec![PutEntry {
                                key: StorageKey(Bytes::from_static(b"unplanned-activation-change")),
                                value: StorageValue {
                                    bytes: Bytes::from_static(b"must-fail"),
                                },
                            }],
                        },
                    )
                    .await
                    .unwrap();
                write.commit().await.unwrap();
            }
            let result = verify_authority_activation(storage, witness).await;
            if tamper {
                assert!(result.is_err());
            } else {
                assert!(result.unwrap().semantic_preservation_verified);
            }
        }
    }
}
