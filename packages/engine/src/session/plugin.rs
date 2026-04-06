use crate::write_runtime::{install_plugin_archive_with_write_context, WriteTransaction};
use crate::{ExecuteOptions, LixError, Session};

pub(crate) async fn install_plugin_in_session(
    session: &Session,
    archive_bytes: &[u8],
) -> Result<(), LixError> {
    let transaction = session.runtime().begin_write_unit().await?;
    let mut write_transaction = WriteTransaction::new_buffered_write(transaction);
    let mut context = session.new_execution_context(ExecuteOptions::default());

    let install_result = install_plugin_archive_with_write_context(
        session.engine().as_ref(),
        &mut write_transaction,
        archive_bytes,
        &mut context,
    )
    .await;

    match install_result {
        Ok(()) => {
            write_transaction.mark_public_surface_registry_refresh_pending();
            write_transaction.mark_installed_plugins_cache_invalidation_pending();
            let outcome = write_transaction
                .commit_buffered_write(session.engine().as_ref(), context)
                .await?;
            session.apply_transaction_commit_outcome(outcome).await?;
            Ok(())
        }
        Err(error) => {
            let _ = write_transaction.rollback_buffered_write().await;
            Err(error)
        }
    }
}
