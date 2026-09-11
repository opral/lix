//! Mandatory native setting-presence proofs for repository format v79.
//! Backfill is migration-owned; ordinary opening never repairs a missing proof.
use super::api::MigrationOptions;
use crate::LixError;
use crate::storage_adapter::{
    Storage, StorageAdapter, StorageKey, StoragePrecondition, StorageWrite, StorageWriteOptions,
};

/// Older logical migrations execute native SQL while their old marker is still
/// fenced in the hidden epoch. Install additive proofs first without changing
/// the old control codec. Finish publishes the v79 marker with the final proof.
pub(super) async fn backfill<S>(
    adapter: &StorageAdapter<S>,
    options: MigrationOptions,
    finish: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = adapter.begin_read(Default::default()).await?;
    let marker = crate::storage_adapter::PointReadPlan::new(
        crate::init::REPOSITORY_PROTOCOL_SPACE,
        &[StorageKey(bytes::Bytes::from_static(
            crate::init::REPOSITORY_PROTOCOL_KEY,
        ))],
    )
    .materialize(&read, Default::default())
    .await?
    .value
    .into_iter()
    .next()
    .flatten();
    let Some(crate::storage_adapter::StorageProjectedValue::FullValue(marker)) = marker else {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "setting witness migration has no protocol marker",
        ));
    };
    if marker.as_ref() == crate::init::REPOSITORY_PROTOCOL_VALUE {
        return Ok(());
    }
    if finish && marker.as_ref() != crate::init::REPOSITORY_PROTOCOL_V78 {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "setting witness publication requires the completed v78 migration",
        ));
    }
    let observed = crate::branch::BranchHeadControlContext::new()
        .reader(&read)
        .load_observed(&[crate::GLOBAL_BRANCH_ID.to_owned()])
        .await?
        .pop()
        .ok_or_else(|| {
            LixError::new(
                "LIX_ERROR_MIGRATION_FAILED",
                "global branch observation is absent",
            )
        })?;
    let control = observed.control.ok_or_else(|| {
        LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "global branch control is absent",
        )
    })?;
    let mut writes = adapter.new_write_set();
    let mut guards = crate::hot_state::stage_deterministic_identity_witness_migration(
        &read,
        &mut writes,
        control.tracked_generation,
        control.current_state_revision,
        options.max_changes,
        options.max_preflight_bytes,
    )
    .await?;
    guards.push(crate::branch::branch_head_control_precondition(
        crate::GLOBAL_BRANCH_ID,
        observed.raw_token,
    )?);
    guards.push(
        crate::storage_adapter::repository_mutation_revision_precondition(
            crate::storage_adapter::load_repository_mutation_revision(&read).await?,
        ),
    );
    guards.push(StoragePrecondition::KeyValueEquals {
        space: crate::init::REPOSITORY_PROTOCOL_SPACE,
        key: StorageKey(bytes::Bytes::copy_from_slice(
            crate::init::REPOSITORY_PROTOCOL_KEY,
        )),
        expected: marker,
    });
    if finish {
        crate::init::stage_repository_protocol(&mut writes);
    }
    drop(read);
    // Migration already owns the hidden epoch and must preserve its copied
    // authority/replica role. Ordinary writer admission would append ownership
    // absence guards and reject a valid owned candidate.
    let mut write = adapter
        .begin_migration_write(StorageWriteOptions {
            preconditions: guards,
            await_durable: true,
            ..Default::default()
        })
        .await?;
    let staged: Result<(), LixError> = async {
        let stats = writes.lower_into(&mut write).await?;
        if stats.staged_puts != 0 || stats.staged_deletes != 0 {
            crate::storage_adapter::stage_mutation_revision(&mut write).await?;
        }
        Ok(())
    }
    .await;
    if let Err(error) = staged {
        let _ = write.rollback().await;
        return Err(error);
    }
    write.commit().await?;
    Ok(())
}

#[cfg(test)]
mod tests;
