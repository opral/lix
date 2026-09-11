//! Required operations shared by the host bindings. This is an internal adapter
//! contract, not another engine API: local and connected sync use `Lix`, and
//! remote uses the canonical Rust protocol client.

use lix::storage::Storage;
use lix::{
    CreateBranchOptions, CreateBranchReceipt, ExecuteBatchStatement, ExecuteResult, Lix, LixError,
    MergeBranchOptions, MergeBranchPreview, MergeBranchPreviewOptions, MergeBranchReceipt,
    RedoReceipt, SwitchBranchOptions, SwitchBranchReceipt, UndoReceipt, Value,
};

#[derive(Default, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ExecuteOptions {
    pub(crate) origin_key: Option<String>,
    // Transport retry identity is only used by the remote protocol client.
    pub(crate) idempotency_key: Option<String>,
}

// No default methods: every backend must explicitly route every operation.
// Static dispatch deliberately allows non-Send browser transports/futures.
pub(crate) trait SessionOperations: Sized {
    type Transaction;
    type Observation;
    type Snapshot;

    async fn open_another_session(
        &self,
        branch_id: Option<String>,
        account_id: Option<String>,
    ) -> Result<Self, LixError>;
    async fn begin_transaction(&self) -> Result<Self::Transaction, LixError>;
    async fn observe(&self, sql: &str, params: &[Value]) -> Result<Self::Observation, LixError>;
    async fn export_snapshot(&self) -> Result<Self::Snapshot, LixError>;
    async fn close(&self) -> Result<(), LixError>;
    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError>;
    async fn execute_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError>;
    async fn active_branch_id(&self) -> Result<String, LixError>;
    async fn active_account_id(&self) -> Result<String, LixError>;
    async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError>;
    async fn undo(&self) -> Result<UndoReceipt, LixError>;
    async fn redo(&self) -> Result<RedoReceipt, LixError>;
    async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError>;
    async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError>;
    async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError>;
}

impl<S: Storage + Clone + Send + Sync + 'static> SessionOperations for Lix<S> {
    type Transaction = lix::LixTransaction<S>;
    type Observation = lix::ObserveEvents<S>;
    type Snapshot = lix::snapshot::SnapshotExportBuilder<S>;

    async fn open_another_session(
        &self,
        branch_id: Option<String>,
        account_id: Option<String>,
    ) -> Result<Self, LixError> {
        let mut builder = Lix::open_another_session(self);
        if let Some(branch_id) = branch_id {
            builder = builder.with_branch(branch_id);
        }
        if let Some(account_id) = account_id {
            builder = builder.with_account(account_id);
        }
        builder.await
    }

    async fn begin_transaction(&self) -> Result<Self::Transaction, LixError> {
        Lix::begin_transaction(self).await
    }

    async fn observe(&self, sql: &str, params: &[Value]) -> Result<Self::Observation, LixError> {
        Lix::observe(self, sql, params)
    }

    async fn export_snapshot(&self) -> Result<Self::Snapshot, LixError> {
        Ok(Lix::export_snapshot(self))
    }

    async fn close(&self) -> Result<(), LixError> {
        Lix::close(self).await
    }

    async fn execute(
        &self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let _ = options.idempotency_key;
        let execution = Lix::execute(self, sql, params);
        match options.origin_key {
            Some(origin_key) => execution.with_origin_key(origin_key).await,
            None => execution.await,
        }
    }

    async fn execute_batch(
        &self,
        statements: &[ExecuteBatchStatement],
        options: ExecuteOptions,
    ) -> Result<Vec<ExecuteResult>, LixError> {
        let execution = Lix::execute_batch(self, statements);
        match options.origin_key {
            Some(origin_key) => execution.with_origin_key(origin_key).await,
            None => execution.await,
        }
    }

    async fn active_branch_id(&self) -> Result<String, LixError> {
        Lix::active_branch_id(self).await
    }

    async fn active_account_id(&self) -> Result<String, LixError> {
        Ok(Lix::active_account_id(self).to_owned())
    }

    async fn create_branch(
        &self,
        options: CreateBranchOptions,
    ) -> Result<CreateBranchReceipt, LixError> {
        Lix::create_branch(self, options).await
    }

    async fn undo(&self) -> Result<UndoReceipt, LixError> {
        Lix::undo(self).await
    }
    async fn redo(&self) -> Result<RedoReceipt, LixError> {
        Lix::redo(self).await
    }

    async fn switch_branch(
        &self,
        options: SwitchBranchOptions,
    ) -> Result<SwitchBranchReceipt, LixError> {
        Lix::switch_branch(self, options).await
    }

    async fn merge_branch_preview(
        &self,
        options: MergeBranchPreviewOptions,
    ) -> Result<MergeBranchPreview, LixError> {
        Lix::merge_branch_preview(self, options).await
    }

    async fn merge_branch(
        &self,
        options: MergeBranchOptions,
    ) -> Result<MergeBranchReceipt, LixError> {
        Lix::merge_branch(self, options).await
    }
}

/// Transaction wrappers own their optional handle. Keep each engine's existing
/// finalization semantics: local consumes on an attempt; remote retains a handle
/// when a protocol failure can still be retried.
pub(crate) trait TransactionOperations {
    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError>;
    async fn commit(&mut self) -> Result<(), LixError>;
    async fn rollback(&mut self) -> Result<(), LixError>;
}

fn transaction_closed_error() -> LixError {
    LixError::new("LIX_INVALID_TRANSACTION_STATE", "Lix transaction is closed")
}

impl<S: Storage + Clone + Send + Sync + 'static> TransactionOperations
    for Option<lix::LixTransaction<S>>
{
    async fn execute(
        &mut self,
        sql: &str,
        params: &[Value],
        options: ExecuteOptions,
    ) -> Result<ExecuteResult, LixError> {
        let transaction = self.as_mut().ok_or_else(transaction_closed_error)?;
        let execution = transaction.execute(sql, params);
        match options.origin_key {
            Some(origin_key) => execution.with_origin_key(origin_key).await,
            None => execution.await,
        }
    }

    async fn commit(&mut self) -> Result<(), LixError> {
        self.take()
            .ok_or_else(transaction_closed_error)?
            .commit()
            .await
    }

    async fn rollback(&mut self) -> Result<(), LixError> {
        self.take()
            .ok_or_else(transaction_closed_error)?
            .rollback()
            .await
    }
}

#[cfg(target_family = "wasm")]
mod remote {
    use super::*;
    use lix::server_protocol::client::{
        ClientCore, ProtocolClient, ProtocolExecuteOptions, ProtocolHttp,
    };

    impl From<ExecuteOptions> for ProtocolExecuteOptions {
        fn from(options: ExecuteOptions) -> Self {
            Self {
                origin_key: options.origin_key,
                idempotency_key: options.idempotency_key,
            }
        }
    }

    impl<H: ProtocolHttp> TransactionOperations
        for Option<lix::server_protocol::client::ProtocolTransaction<H>>
    {
        async fn execute(
            &mut self,
            sql: &str,
            params: &[Value],
            options: ExecuteOptions,
        ) -> Result<ExecuteResult, LixError> {
            self.as_ref()
                .ok_or_else(transaction_closed_error)?
                .execute(sql, params, Some(options.into()))
                .await
        }

        async fn commit(&mut self) -> Result<(), LixError> {
            self.as_ref()
                .ok_or_else(transaction_closed_error)?
                .commit()
                .await?;
            *self = None;
            Ok(())
        }

        async fn rollback(&mut self) -> Result<(), LixError> {
            self.as_ref()
                .ok_or_else(transaction_closed_error)?
                .rollback()
                .await?;
            *self = None;
            Ok(())
        }
    }

    impl<H: ProtocolHttp + Clone + 'static> SessionOperations for ProtocolClient<H> {
        type Transaction = lix::server_protocol::client::ProtocolTransaction<H>;
        type Observation = lix::server_protocol::client::ProtocolObserveEvents<ClientCore<H>>;
        type Snapshot = lix::server_protocol::client::ProtocolSnapshotExport;

        async fn open_another_session(
            &self,
            branch_id: Option<String>,
            account_id: Option<String>,
        ) -> Result<Self, LixError> {
            ProtocolClient::open_another_session(self, branch_id, account_id).await
        }

        async fn begin_transaction(&self) -> Result<Self::Transaction, LixError> {
            ClientCore::begin_transaction(self).await
        }

        async fn observe(
            &self,
            sql: &str,
            params: &[Value],
        ) -> Result<Self::Observation, LixError> {
            ProtocolClient::observe(self, sql, params.to_vec()).await
        }

        async fn export_snapshot(&self) -> Result<Self::Snapshot, LixError> {
            ClientCore::export_snapshot(self).await
        }

        async fn close(&self) -> Result<(), LixError> {
            ProtocolClient::close(self).await
        }

        async fn execute(
            &self,
            sql: &str,
            params: &[Value],
            options: ExecuteOptions,
        ) -> Result<ExecuteResult, LixError> {
            ClientCore::execute(self, sql, params, Some(options.into())).await
        }

        async fn execute_batch(
            &self,
            statements: &[ExecuteBatchStatement],
            options: ExecuteOptions,
        ) -> Result<Vec<ExecuteResult>, LixError> {
            ClientCore::execute_batch(self, statements, Some(options.into())).await
        }

        async fn active_branch_id(&self) -> Result<String, LixError> {
            ClientCore::active_branch_id(self).await
        }

        async fn active_account_id(&self) -> Result<String, LixError> {
            ClientCore::active_account_id(self).await
        }

        async fn create_branch(
            &self,
            options: CreateBranchOptions,
        ) -> Result<CreateBranchReceipt, LixError> {
            ClientCore::create_branch(self, options).await
        }

        async fn undo(&self) -> Result<UndoReceipt, LixError> {
            ClientCore::undo(self).await
        }
        async fn redo(&self) -> Result<RedoReceipt, LixError> {
            ClientCore::redo(self).await
        }

        async fn switch_branch(
            &self,
            options: SwitchBranchOptions,
        ) -> Result<SwitchBranchReceipt, LixError> {
            self.switch_branch_and_restart(&options.branch_id).await
        }

        async fn merge_branch_preview(
            &self,
            options: MergeBranchPreviewOptions,
        ) -> Result<MergeBranchPreview, LixError> {
            ClientCore::merge_branch_preview(self, options).await
        }

        async fn merge_branch(
            &self,
            options: MergeBranchOptions,
        ) -> Result<MergeBranchReceipt, LixError> {
            ClientCore::merge_branch(self, options).await
        }
    }
}
