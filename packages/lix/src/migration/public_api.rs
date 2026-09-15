//! Read-only inventory and detached operator entry points. Inspection never
//! admits an engine or repairs a repository as a side effect.
#[cfg(feature = "offline-migration")]
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

#[cfg(feature = "offline-migration")]
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
/// The reference server invokes this before constructing its serving runtime;
/// the low-level current-format engine does not run historical migrations.
#[cfg(feature = "offline-migration")]
pub async fn migrate_repository<S>(storage: S) -> Result<RepositoryMigrationReport, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let storage = StorageSession::acquire(storage).await?;
    migrate_owned(&storage, super::MigrationOptions::default()).await
}

#[cfg(feature = "offline-migration")]
async fn migrate_owned<S>(
    storage: &S,
    options: super::MigrationOptions,
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
        let plan = super::incorporation::preservation_plan(&read, options).await?;
        read.finish()?;
        (
            "v79-canonical-plan-v1",
            content_digest_with_plan(storage, Some(plan)).await?,
        )
    } else {
        ("exact-records-v1", before_content_digest.clone())
    };
    super::epoch::admit_repository_with_options(storage, None, None, options).await?;
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

#[cfg(feature = "offline-migration")]
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
/// marker and mutation revision, the two intended v81 migration publications.
/// Includes pending operations, blobs, history, and all deduplication receipts.
#[cfg(feature = "offline-migration")]
pub(super) async fn content_digest<S>(storage: &S) -> Result<String, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    content_digest_with_plan(storage, None).await
}

#[cfg(feature = "offline-migration")]
async fn content_digest_with_plan<S>(
    storage: &S,
    plan: Option<super::publish::PublicationPlan>,
) -> Result<String, LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (mut overlay, cleared) = plan
        .map(super::publish::PublicationPlan::into_preservation_overlay)
        .unwrap_or_default();
    let adapter = super::epoch::inspect_existing_epoch_adapter(storage).await?;
    let read = super::MigrationPlanningRead::new(&adapter).await?;
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

#[cfg(feature = "offline-migration")]
fn digest_record(digest: &mut blake3::Hasher, space: u32, key: &[u8], value: &[u8]) {
    digest.update(&space.to_le_bytes());
    digest.update(&(key.len() as u64).to_le_bytes());
    digest.update(key);
    digest.update(&(value.len() as u64).to_le_bytes());
    digest.update(value);
}

#[cfg(all(test, feature = "offline-migration"))]
mod tests {
    use super::*;
    use crate::storage_adapter::{PutBatch, PutEntry, StorageValue, StorageWrite};

    #[tokio::test]
    async fn current_open_rejects_v80_without_writes_then_explicit_migration_preserves_all_records()
    {
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
        let before = content_digest(&storage).await.unwrap();
        let inspection = inspect_repository(storage.clone()).await.unwrap();
        assert_eq!(inspection.format, Some(80));
        assert!(!inspection.current);
        let error = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .err()
            .expect("legacy opening must fail");
        assert_eq!(error.code, "LIX_ERROR_REPOSITORY_MIGRATION_REQUIRED");
        assert_eq!(content_digest(&storage).await.unwrap(), before);
        assert_eq!(
            inspect_repository(storage.clone()).await.unwrap(),
            inspection
        );
        let report = migrate_repository(storage.clone()).await.unwrap();
        assert_eq!(report.embedded_repository_id, original_id);
        assert!(report.after.current);
        assert!(report.semantic_preservation_verified);
        assert_eq!(report.before_content_digest, report.after_content_digest);
        let reopened = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
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
        assert!(reopened.open_report().migration.is_none());
        reopened.close().await.unwrap();
        assert!(
            migrate_repository(storage)
                .await
                .unwrap()
                .semantic_preservation_verified
        );
    }

    #[tokio::test]
    async fn partial_v80_migration_preserves_admission_and_resident_records_offline() {
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
        super::super::epoch::install_fresh_partial_epoch(storage.clone(), &state)
            .await
            .unwrap();
        super::super::epoch::stage_v80_repository_for_test(&storage, true)
            .await
            .unwrap();
        assert!(
            super::super::epoch::admit_partial_epoch(&storage)
                .await
                .is_err()
        );
        let report = migrate_repository(storage.clone()).await.unwrap();
        assert!(report.semantic_preservation_verified);
        assert_eq!(report.embedded_repository_id, state.repository_id());
        assert_eq!(report.after.role, RepositoryRole::PartialReplica);
        let admitted = super::super::epoch::admit_partial_epoch(&storage)
            .await
            .unwrap();
        assert_eq!(admitted.state, state);
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
#[cfg(feature = "offline-migration")]
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
#[cfg(feature = "offline-migration")]
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
#[cfg(feature = "offline-migration")]
#[derive(Debug)]
pub struct AuthorityActivationWitness {
    before_content_digest: String,
    expected_content_digest: String,
}

#[cfg(feature = "offline-migration")]
#[derive(Debug, Serialize)]
pub struct AuthorityActivationReport {
    pub before_content_digest: String,
    pub after_content_digest: String,
    pub semantic_preservation_verified: bool,
}

/// Prepare the only permitted activation mutation: install the current exact
/// authority capability marker. Serving must still perform its full eligibility
/// validation; this helper does not promote storage or admit an engine.
#[cfg(feature = "offline-migration")]
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
#[cfg(feature = "offline-migration")]
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

#[cfg(all(test, feature = "offline-migration", feature = "server-protocol"))]
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
