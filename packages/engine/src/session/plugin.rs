use async_trait::async_trait;

use crate::contracts::artifacts::FilesystemProjectionScope;
use crate::paths::filesystem::NormalizedDirectoryPath;
use crate::transaction::lookup_directory_id_by_path_in_transaction;
use crate::write_pipeline::{
    ensure_execution_runtime_state_for_write_scope, prepared_write_runtime_state_for_execution,
};
use crate::write_runtime::{
    install_plugin_archive_with_writer, stage_prepared_write_step, PluginInstallWriteExecutor,
    PreparedWriteExecutionStep, SemanticWriteContext, WriteTransaction,
};
use crate::{ExecuteOptions, LixError, Session};

const GLOBAL_VERSION_ID: &str = "global";

struct SessionPluginInstallWriter<'a, 'tx> {
    transaction: &'a mut WriteTransaction<'tx>,
    semantic_context: SemanticWriteContext,
}

#[async_trait(?Send)]
impl<'a, 'tx> PluginInstallWriteExecutor for SessionPluginInstallWriter<'a, 'tx> {
    fn semantic_write_context(&self) -> SemanticWriteContext {
        self.semantic_context.clone()
    }

    fn stage_prepared_write_step(
        &mut self,
        step: PreparedWriteExecutionStep,
    ) -> Result<(), LixError> {
        stage_prepared_write_step(self.transaction, step)
    }

    async fn resolve_directory_id(
        &mut self,
        path: &NormalizedDirectoryPath,
    ) -> Result<Option<String>, LixError> {
        lookup_directory_id_by_path_in_transaction(
            self.transaction.backend_transaction_mut()?,
            GLOBAL_VERSION_ID,
            path,
            FilesystemProjectionScope::ExplicitVersion,
        )
        .await
    }
}

pub(crate) async fn install_plugin_in_session(
    session: &Session,
    archive_bytes: &[u8],
) -> Result<(), LixError> {
    let transaction = session.collaborators().begin_write_unit().await?;
    let mut write_transaction = WriteTransaction::new_buffered_write(transaction);
    let mut context = session.new_execution_context(ExecuteOptions::default());
    ensure_execution_runtime_state_for_write_scope(
        session.collaborators(),
        write_transaction.backend_transaction_mut()?,
        &mut context,
    )
    .await?;
    let semantic_context = SemanticWriteContext::new(
        prepared_write_runtime_state_for_execution(
            context
                .execution_runtime_state()
                .expect("plugin install should prepare write runtime state"),
        ),
        context.public_surface_registry.clone(),
        context.active_account_ids.clone(),
        context.options.writer_key.clone(),
    );

    let install_result = {
        let mut writer = SessionPluginInstallWriter {
            transaction: &mut write_transaction,
            semantic_context,
        };
        install_plugin_archive_with_writer(archive_bytes, &mut writer).await
    };

    match install_result {
        Ok(()) => {
            let outcome = write_transaction
                .commit_buffered_write(context.buffered_write_execution_input())
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
