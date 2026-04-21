#![allow(dead_code)]

//! SQL-backed adapter slot for canonical persistence.
//!
//! This module is the intended home for `CanonicalReadStore` and
//! `CanonicalWriteStore` implementations that still rely on raw backend,
//! transaction, executor, or lower `backend/*` helpers during the MVP.

use async_trait::async_trait;

use crate::canonical::store::{
    CanonicalBackendRef, CanonicalExecutorRef, CanonicalReadStore, CanonicalTransactionRef,
    CanonicalWriteStore,
};
use crate::functions::LixFunctionProvider;
use crate::streams::load_durable_state_commit_low_watermark;
use crate::{LixError, QueryResult, Value};

use super::{
    api, checkpoint_labels, init, CanonicalAppendSummary, CanonicalChange, CanonicalChangeWrite,
    CanonicalCommit, CanonicalHistoryRequest, CanonicalHistoryRow,
    CanonicalUntrackedVisibilityWrite, CanonicalVisibleStateRequest, CanonicalVisibleStateRow,
};

pub(crate) struct SqlCanonicalReadStore<'a> {
    backend: CanonicalBackendRef<'a>,
}

impl<'a> SqlCanonicalReadStore<'a> {
    pub(crate) fn new(backend: CanonicalBackendRef<'a>) -> Self {
        Self { backend }
    }
}

pub(crate) struct SqlCanonicalExecutorReadStore<'a> {
    executor: CanonicalExecutorRef<'a>,
}

impl<'a> SqlCanonicalExecutorReadStore<'a> {
    pub(crate) fn new(executor: CanonicalExecutorRef<'a>) -> Self {
        Self { executor }
    }
}

pub(crate) struct SqlCanonicalWriteStore<'a> {
    transaction: CanonicalTransactionRef<'a>,
}

impl<'a> SqlCanonicalWriteStore<'a> {
    pub(crate) fn new(transaction: CanonicalTransactionRef<'a>) -> Self {
        Self { transaction }
    }
}

pub(crate) async fn execute_query_with_backend(
    backend: CanonicalBackendRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    backend.execute(sql, params).await
}

pub(crate) async fn execute_query_with_executor(
    executor: CanonicalExecutorRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    executor.execute(sql, params).await
}

pub(crate) async fn execute_query_with_transaction(
    transaction: CanonicalTransactionRef<'_>,
    sql: &str,
    params: &[Value],
) -> Result<QueryResult, LixError> {
    transaction.execute(sql, params).await
}

pub(crate) async fn execute_batch_with_transaction(
    transaction: CanonicalTransactionRef<'_>,
    batch: &crate::canonical::store::CanonicalPreparedBatch,
) -> Result<(), LixError> {
    transaction.execute_batch(batch).await.map(|_| ())
}

pub(crate) async fn execute_ddl_batch_with_backend(
    backend: CanonicalBackendRef<'_>,
    batch_name: &str,
    statements: &[&str],
) -> Result<(), LixError> {
    crate::backend::execute_ddl_batch(backend, batch_name, statements).await
}

pub(crate) async fn add_column_if_missing_with_backend(
    backend: CanonicalBackendRef<'_>,
    table_name: &str,
    column_name: &str,
    column_sql: &str,
) -> Result<(), LixError> {
    crate::backend::add_column_if_missing(backend, table_name, column_name, column_sql).await
}

pub(crate) fn executor_from_transaction(
    transaction: CanonicalTransactionRef<'_>,
) -> impl crate::QueryExecutor + '_ {
    crate::backend::transaction_backend_view(transaction)
}

pub(crate) async fn load_durable_state_commit_low_watermark_in_transaction(
    transaction: CanonicalTransactionRef<'_>,
) -> Result<Option<crate::streams::DurableStateCommitCursor>, LixError> {
    let backend = crate::backend::transaction_backend_view(transaction);
    load_durable_state_commit_low_watermark(&backend).await
}

pub(crate) async fn init_storage(backend: CanonicalBackendRef<'_>) -> Result<(), LixError> {
    init::init_storage(backend).await
}

pub(crate) async fn resolve_last_checkpoint_commit_id_for_tip(
    executor: CanonicalExecutorRef<'_>,
    head_commit_id: &str,
) -> Result<Option<String>, LixError> {
    checkpoint_labels::resolve_last_checkpoint_commit_id_for_tip_with_executor(executor, head_commit_id).await
}

#[async_trait(?Send)]
impl CanonicalReadStore for SqlCanonicalReadStore<'_> {
    async fn load_commit(&mut self, commit_id: &str) -> Result<Option<CanonicalCommit>, LixError> {
        let _ = commit_id;
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }

    async fn load_change(&mut self, change_id: &str) -> Result<Option<CanonicalChange>, LixError> {
        let _ = change_id;
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }

    async fn load_history(
        &mut self,
        request: &CanonicalHistoryRequest,
    ) -> Result<Vec<CanonicalHistoryRow>, LixError> {
        api::load_history(self.backend, request).await
    }

    async fn load_visible_state(
        &mut self,
        request: &CanonicalVisibleStateRequest,
    ) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
        let _ = request;
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }

    async fn resolve_merge_base(
        &mut self,
        left_head_commit_id: &str,
        right_head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        let _ = (left_head_commit_id, right_head_commit_id);
        Err(invalid_canonical_store_access(
            "executor-backed canonical read store",
        ))
    }
}

#[async_trait(?Send)]
impl CanonicalReadStore for SqlCanonicalExecutorReadStore<'_> {
    async fn load_commit(&mut self, commit_id: &str) -> Result<Option<CanonicalCommit>, LixError> {
        api::load_commit(self.executor, commit_id).await
    }

    async fn load_change(&mut self, change_id: &str) -> Result<Option<CanonicalChange>, LixError> {
        api::load_change(self.executor, change_id).await
    }

    async fn load_history(
        &mut self,
        request: &CanonicalHistoryRequest,
    ) -> Result<Vec<CanonicalHistoryRow>, LixError> {
        let _ = request;
        Err(invalid_canonical_store_access(
            "backend-backed canonical read store",
        ))
    }

    async fn load_visible_state(
        &mut self,
        request: &CanonicalVisibleStateRequest,
    ) -> Result<Vec<CanonicalVisibleStateRow>, LixError> {
        api::load_visible_state(self.executor, request).await
    }

    async fn resolve_merge_base(
        &mut self,
        left_head_commit_id: &str,
        right_head_commit_id: &str,
    ) -> Result<Option<String>, LixError> {
        api::resolve_merge_base(self.executor, left_head_commit_id, right_head_commit_id).await
    }
}

#[async_trait(?Send)]
impl CanonicalWriteStore for SqlCanonicalWriteStore<'_> {
    async fn append_changes(
        &mut self,
        changes: &[CanonicalChangeWrite],
        functions: &mut dyn LixFunctionProvider,
    ) -> Result<CanonicalAppendSummary, LixError> {
        api::append_changes(self.transaction, changes, functions).await
    }

    async fn append_untracked_change_visibility_rows(
        &mut self,
        visibility_rows: &[CanonicalUntrackedVisibilityWrite],
    ) -> Result<(), LixError> {
        api::append_untracked_change_visibility_rows(self.transaction, visibility_rows)
            .await
    }

    async fn replace_snapshot_content(
        &mut self,
        snapshot_id: &str,
        snapshot_content: &str,
    ) -> Result<(), LixError> {
        api::replace_snapshot_content_in_transaction(self.transaction, snapshot_id, snapshot_content)
            .await
    }
}

fn invalid_canonical_store_access(expected: &str) -> LixError {
    LixError::unknown(format!(
        "canonical store access requires a {expected}"
    ))
}
