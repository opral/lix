//! Marker-only v80 -> v81 cut. Copy-and-activate owns the surrounding migration;
//! all content, pending upload identities, and authority receipts remain intact.
use crate::{
    LixError,
    storage_adapter::{Storage, StorageAdapter},
};

pub(super) async fn migrate<S>(adapter: &StorageAdapter<S>, partial: bool) -> Result<(), LixError>
where
    S: Storage + Clone + Send + Sync + 'static,
{
    let read = super::MigrationPlanningRead::new(adapter).await?;
    let revision = crate::storage_adapter::load_repository_mutation_revision(&read).await?;
    read.finish()?;
    let (source, target) = if partial {
        (
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_V80,
            crate::init::PARTIAL_REPOSITORY_PROTOCOL_VALUE,
        )
    } else {
        (
            crate::init::REPOSITORY_PROTOCOL_V80,
            crate::init::REPOSITORY_PROTOCOL_VALUE,
        )
    };
    super::publish::publish(
        adapter,
        revision,
        source,
        target,
        super::publish::PublicationPlan::bounded(0, 0),
    )
    .await?;
    Ok(())
}
