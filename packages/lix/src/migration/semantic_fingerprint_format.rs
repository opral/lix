//! Marker-only v83 -> v84 cut for authenticated tracked-state fingerprints.
//!
//! Existing roots and commit deltas remain valid without fingerprints and use
//! the exact payload comparison path. The format gate prevents an older reader
//! from admitting roots written with the new packed value tail; copy-and-activate
//! owns the repository-wide preservation check.
use crate::{
    LixError,
    storage_adapter::{Storage, StorageAdapter},
};

pub(super) async fn migrate<S>(
    adapter: &StorageAdapter<S>,
    partial: bool,
) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let (source, target) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V83,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_VALUE,
        )
    } else {
        (
            crate::init::REPOSITORY_PROTOCOL_V83,
            crate::init::REPOSITORY_PROTOCOL_VALUE,
        )
    };

    let marker = super::api::load_repository_protocol_marker(adapter).await?;
    if marker.as_deref() == Some(target) {
        return Ok(());
    }
    if marker.as_deref() != Some(source) {
        return Err(LixError::new(
            "LIX_ERROR_MIGRATION_FAILED",
            "semantic fingerprint format migration observed an unexpected protocol marker",
        ));
    }

    let read = super::MigrationPlanningRead::new(adapter).await?;
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    read.finish()?;
    super::publish::publish(
        adapter,
        revision,
        source,
        target,
        super::publish::PublicationPlan::bounded(0, 0),
    )
    .await
}
