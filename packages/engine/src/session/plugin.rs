use crate::LixError;
use crate::plugin::install_plugin_archive_with_transaction;
use crate::storage::StorageBackend;

use super::SessionContext;

impl<B> SessionContext<B>
where
    B: StorageBackend + Clone + Send + Sync + 'static,
    for<'backend> B::Read<'backend>: Send,
    for<'backend> B::Write<'backend>: Send,
{
    pub async fn install_plugin_archive(&self, archive_bytes: &[u8]) -> Result<(), LixError> {
        let archive_bytes = archive_bytes.to_vec();
        self.with_write_transaction(|transaction| {
            Box::pin(async move {
                install_plugin_archive_with_transaction(&archive_bytes, transaction).await
            })
        })
        .await
    }
}
