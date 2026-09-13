//! Canonical v79 header upgrade, independent of resident graph or row coverage.
//! Only proof already encoded in the original header may affect its replacement.
use super::api::MigrationOptions;
use super::publish::{PublicationPlan, publish};
use crate::LixError;
use crate::changelog::CommitId;
use crate::storage_adapter::{
    PointReadPlan, Storage, StorageAdapter, StorageAdapterRead, StorageKey, StoragePrefix,
    StorageProjectedValue,
};
#[cfg(test)]
use crate::tracked_state::CommitStateIncorporation;
use crate::tracked_state::{
    load_published_commit_state_topology, rewrite_commit_state_incorporation_for_migration,
};

fn failure(message: &str) -> LixError {
    LixError::new("LIX_ERROR_MIGRATION_FAILED", message)
}

async fn marker(read: &(impl StorageAdapterRead + ?Sized)) -> Result<bytes::Bytes, LixError> {
    let value = PointReadPlan::new(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        &[StorageKey(bytes::Bytes::from_static(
            crate::init::REPOSITORY_PROTOCOL_KEY,
        ))],
    )
    .materialize(read, Default::default())
    .await?
    .value
    .pop()
    .flatten();
    match value {
        Some(StorageProjectedValue::FullValue(bytes)) => Ok(bytes),
        _ => Err(failure("incorporation migration has no protocol marker")),
    }
}

pub(super) async fn is_legacy_partial<S: Storage>(
    adapter: &StorageAdapter<S>,
) -> Result<bool, LixError> {
    let read = adapter.begin_read(Default::default()).await?;
    Ok(marker(&read).await?.as_ref() == crate::init::PARTIAL_REPOSITORY_PROTOCOL_V79)
}

pub(super) async fn migrate<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    partial: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let (expected, target) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V79,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_VALUE,
        )
    } else {
        (
            crate::init::REPOSITORY_PROTOCOL_V79,
            crate::init::REPOSITORY_PROTOCOL_VALUE,
        )
    };
    let actual = marker(&read).await?;
    if actual.as_ref() == target {
        return Ok(());
    }
    if actual.as_ref() != expected {
        return Err(failure(
            "incorporation migration requires completed v79 format",
        ));
    }
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    let space = crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE;
    let mut cursor = read
        .begin_scan(
            space,
            StoragePrefix {
                bytes: bytes::Bytes::new(),
            }
            .to_range()?,
            Default::default(),
        )
        .await?;
    let mut plan = PublicationPlan::bounded(options.max_changes, options.max_preflight_bytes);
    let mut entries = 0usize;
    let mut bytes = 0usize;
    while let Some(page) = cursor.next_chunk().await? {
        for entry in page {
            let StorageProjectedValue::FullValue(raw) = entry.value else {
                return Err(failure("incorporation header scan omitted a value"));
            };
            entries = entries.saturating_add(1);
            bytes = bytes.saturating_add(raw.len());
            if entries > options.max_changes || bytes > options.max_preflight_bytes {
                return Err(LixError::new(
                    "LIX_ERROR_MIGRATION_LIMIT_EXCEEDED",
                    "incorporation header migration exceeds configured bounds",
                ));
            }
            let id = CommitId::new(
                uuid::Uuid::from_slice(&entry.key.0)
                    .map_err(|_| failure("incorporation header key is not a UUID"))?,
            );
            let topology = load_published_commit_state_topology(&read, id)
                .await?
                .ok_or_else(|| failure("incorporation header disappeared"))?;
            let incorporation = topology.incorporation();
            let encoded = rewrite_commit_state_incorporation_for_migration(&raw, incorporation)?;
            plan.replace_immutable(space, vec![(entry.key.0.to_vec(), encoded)])?;
        }
    }
    drop(cursor);
    super::selected_locators::stage_repair(&read, &mut plan, &options, &mut entries, &mut bytes)
        .await?;
    super::omitted_owners::stage_repair(&read, &mut plan, &options, &mut entries, &mut bytes)
        .await?;
    drop(read);
    publish(adapter, revision, expected, target, plan).await
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(crate) async fn migrate_headers_for_test<S: Storage + Clone + Send + Sync + 'static>(
    adapter: &StorageAdapter<S>,
    partial: bool,
) {
    migrate(adapter, MigrationOptions::automatic(), partial)
        .await
        .unwrap();
}

#[cfg(test)]
pub(crate) async fn downgrade_headers_for_test<S: Storage + Clone + Send + Sync + 'static>(
    adapter: &StorageAdapter<S>,
    partial: bool,
) {
    rewrite_headers_for_test(adapter, partial, true).await;
}

#[cfg(test)]
pub(crate) async fn mark_header_incorporation_unknown_for_test<
    S: Storage + Clone + Send + Sync + 'static,
>(
    adapter: &StorageAdapter<S>,
) {
    rewrite_headers_for_test(adapter, false, false).await;
}

#[cfg(test)]
async fn rewrite_headers_for_test<S: Storage + Clone + Send + Sync + 'static>(
    adapter: &StorageAdapter<S>,
    partial: bool,
    legacy_encoding: bool,
) {
    let read = adapter.begin_read(Default::default()).await.unwrap();
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read)
        .await
        .unwrap();
    let mut cursor = read
        .begin_scan(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            StoragePrefix {
                bytes: bytes::Bytes::new(),
            }
            .to_range()
            .unwrap(),
            Default::default(),
        )
        .await
        .unwrap();
    let mut plan = PublicationPlan::bounded(usize::MAX, usize::MAX);
    for entry in cursor.collect_all().await.unwrap() {
        let StorageProjectedValue::FullValue(bytes) = entry.value else {
            panic!("header payload")
        };
        plan.replace_immutable(
            crate::tracked_state::TRACKED_STATE_COMMIT_STATE_MANIFEST_SPACE,
            vec![(
                entry.key.0.to_vec(),
                if legacy_encoding {
                    crate::tracked_state::encode_legacy_commit_state_header_for_migration_test(
                        &bytes,
                    )
                    .unwrap()
                } else {
                    rewrite_commit_state_incorporation_for_migration(
                        &bytes,
                        CommitStateIncorporation::LegacyUnknown,
                    )
                    .unwrap()
                },
            )],
        )
        .unwrap();
    }
    drop(cursor);
    drop(read);
    let (current, legacy) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_VALUE,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V79,
        )
    } else {
        (
            crate::init::REPOSITORY_PROTOCOL_VALUE,
            crate::init::REPOSITORY_PROTOCOL_V79,
        )
    };
    publish(
        adapter,
        revision,
        current,
        if legacy_encoding { legacy } else { current },
        plan,
    )
    .await
    .unwrap();
}
