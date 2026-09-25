//! v82 -> v83 upgrades mutable branch controls and publishes the author-aware
//! storage epoch. Historical immutable leaves retain their v82 encoding and
//! are decoded by the compatibility reader.

use crate::LixError;
use crate::storage_adapter::{Storage, StorageAdapter};

use super::publish::PublicationPlan;

pub(super) async fn migrate<S>(
    adapter: &StorageAdapter<S>,
    options: super::MigrationOptions,
    partial: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    read.finish()?;
    let mut plan = PublicationPlan::bounded(options.max_changes, options.max_preflight_bytes);
    if !partial {
        append_plan(adapter, &mut plan).await?;
    }
    let (source, target) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V82,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_VALUE,
        )
    } else {
        (crate::init::REPOSITORY_PROTOCOL_V82, crate::init::REPOSITORY_PROTOCOL_VALUE)
    };
    super::publish::publish(
        adapter,
        revision,
        source,
        target,
        plan,
    )
    .await?;
    Ok(())
}

pub(super) async fn append_plan<S>(
    adapter: &StorageAdapter<S>,
    plan: &mut PublicationPlan,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let controls = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .scan()
        .await?;
    read.finish()?;
    let entries = controls
        .iter()
        .map(|(branch_id, control)| crate::branch::encode_control_for_migration(branch_id, control))
        .collect::<Result<Vec<_>, _>>()?;
    plan.put_mutable(crate::branch::BRANCH_HEAD_CONTROL_SPACE, entries)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::storage_adapter::{
        Memory, StorageSession, StorageKey, StorageValue,
    };

    #[tokio::test]
    async fn v82_controls_upgrade_and_repository_reopens() {
        let storage = StorageSession::acquire(Memory::new()).await.unwrap();
        let lix = crate::open_lix()
            .with_storage(storage.clone())
            .await
            .unwrap();
        lix.close().await.unwrap();
        super::super::epoch::stage_repository_format_for_test(&storage, false, 82)
            .await
            .unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let controls = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .unwrap();
        drop(read);
        let mut writes = adapter.new_write_set();
        for (branch_id, control) in &controls {
            let (key, value) = crate::branch::encode_v82_control_for_test(branch_id, control)
                .unwrap();
            writes.put(
                crate::branch::BRANCH_HEAD_CONTROL_SPACE,
                StorageKey(Bytes::from(key)),
                StorageValue { bytes: Bytes::from(value) },
            );
        }
        adapter.commit_write_set(writes, Default::default()).await.unwrap();
        let report = crate::migration::migrate_repository(storage.clone())
            .await
            .unwrap();
        assert_eq!(report.before.format, Some(82));
        assert_eq!(report.after.format, Some(83));
        let lix = crate::open_lix().with_storage(storage.clone()).await.unwrap();
        let rows = lix.execute("SELECT id FROM lix_branch", &[]).await.unwrap();
        assert!(!rows.is_empty());
        lix.close().await.unwrap();
        let adapter = super::super::epoch::inspect_existing_epoch_adapter(&storage)
            .await
            .unwrap();
        let read = adapter.begin_read(Default::default()).await.unwrap();
        let upgraded = crate::branch::BranchHeadControlContext::new()
            .reader(&read)
            .scan()
            .await
            .unwrap();
        assert_eq!(upgraded.len(), controls.len());
    }
}
