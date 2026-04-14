use async_trait::async_trait;

use crate::catalog::FilesystemProjectionScope;
use crate::common::NormalizedDirectoryPath;
use crate::plugin::{
    install_plugin_archive_with_writer, PluginInstallWriteContext, PluginInstallWriteExecutor,
};
use crate::transaction::{
    ensure_function_bindings_for_write_scope, lookup_directory_id_by_path_in_transaction,
    prepared_write_function_bindings_for_execution, stage_prepared_write_statement,
    BufferedWriteTransaction, WriteCommand,
};
use crate::version::GLOBAL_VERSION_ID;
use crate::{ExecuteOptions, LixError, Session};

struct SessionPluginInstallWriter<'a, 'tx> {
    transaction: &'a mut BufferedWriteTransaction<'tx>,
    plugin_install_context: PluginInstallWriteContext,
}

#[async_trait(?Send)]
impl<'a, 'tx> PluginInstallWriteExecutor for SessionPluginInstallWriter<'a, 'tx> {
    fn plugin_install_write_context(&self) -> PluginInstallWriteContext {
        self.plugin_install_context.clone()
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
    let transaction = session.session_host().begin_write_unit().await?;
    let mut write_transaction = BufferedWriteTransaction::new(transaction);
    let mut context = session.new_compiler_state(ExecuteOptions::default());
    let execution_context = session.execution_context();
    ensure_function_bindings_for_write_scope(
        &execution_context,
        write_transaction.backend_transaction_mut()?,
        &mut context,
    )
    .await?;
    let plugin_install_context = PluginInstallWriteContext::new(
        prepared_write_function_bindings_for_execution(
            context
                .function_bindings()
                .expect("plugin install should prepare function bindings"),
        ),
        context.public_surface_registry.clone(),
        context.active_account_ids.clone(),
        context.writer_key.clone(),
    );

    let install_result = {
        let mut writer = SessionPluginInstallWriter {
            transaction: &mut write_transaction,
            plugin_install_context,
        };
        install_plugin_archive_with_writer(archive_bytes, &mut writer).await
    };

    match install_result {
        Ok(()) => {
            let outcome = write_transaction
                .commit(&execution_context, context.buffered_write_execution_input())
                .await?;
            session.apply_transaction_commit_outcome(outcome).await?;
            Ok(())
        }
        Err(error) => {
            let _ = write_transaction.rollback().await;
            Err(error)
        }
    }
}
