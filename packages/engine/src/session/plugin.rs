use async_trait::async_trait;

use crate::catalog::FilesystemProjectionScope;
use crate::common::NormalizedDirectoryPath;
use crate::session::semantic_write::{
    install_plugin_archive_with_writer, PluginInstallWriteExecutor, SemanticWriteContext,
};
use crate::transaction::{
    ensure_function_runtime_state_for_write_scope, lookup_directory_id_by_path_in_transaction,
    prepared_write_runtime_state_for_execution, stage_prepared_write_statement,
    BufferedWriteTransaction, WriteCommand,
};
use crate::{ExecuteOptions, LixError, Session};

const GLOBAL_VERSION_ID: &str = "global";

struct SessionPluginInstallWriter<'a, 'tx> {
    transaction: &'a mut BufferedWriteTransaction<'tx>,
    semantic_context: SemanticWriteContext,
}

#[async_trait(?Send)]
impl<'a, 'tx> PluginInstallWriteExecutor for SessionPluginInstallWriter<'a, 'tx> {
    fn semantic_write_context(&self) -> SemanticWriteContext {
        self.semantic_context.clone()
    }

    fn stage_prepared_write_statement(&mut self, statement: WriteCommand) -> Result<(), LixError> {
        stage_prepared_write_statement(&mut *self.transaction, statement)
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
    let transaction = session.session_runtime().begin_write_unit().await?;
    let mut write_transaction = BufferedWriteTransaction::new(transaction);
    let mut context = session.new_execution_state(ExecuteOptions::default());
    ensure_function_runtime_state_for_write_scope(
        session.session_runtime(),
        write_transaction.backend_transaction_mut()?,
        &mut context,
    )
    .await?;
    let semantic_context = SemanticWriteContext::new(
        prepared_write_runtime_state_for_execution(
            context
                .function_runtime_state()
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
                .commit_buffered_write(
                    session.session_runtime.as_ref(),
                    context.buffered_write_execution_input(),
                )
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
