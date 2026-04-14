use std::collections::BTreeSet;

use crate::backend::PreparedBatch;
use crate::canonical::CanonicalCommitReceipt;
use crate::sql::{PlanEffects, ResultContract};
use crate::streams::StateCommitStreamChange;
use crate::transaction::PreparedDirectWriteArtifact;
use crate::{LixBackendTransaction, LixError, QueryResult};

pub(crate) struct WriteExecutionOutcome {
    pub(crate) public_result: QueryResult,
    pub(crate) direct_write_file_cache_targets: BTreeSet<(String, String)>,
    pub(crate) plugin_changes_committed: bool,
    pub(crate) canonical_commit_receipt: Option<CanonicalCommitReceipt>,
    pub(crate) plan_effects_override: Option<PlanEffects>,
    pub(crate) state_commit_stream_changes: Vec<StateCommitStreamChange>,
    pub(crate) observe_tick_emitted: bool,
}

pub(crate) fn empty_public_write_execution_outcome() -> WriteExecutionOutcome {
    WriteExecutionOutcome {
        public_result: QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        direct_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        canonical_commit_receipt: None,
        plan_effects_override: Some(PlanEffects::default()),
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    }
}

pub(crate) async fn execute_direct_execution_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    direct: &PreparedDirectWriteArtifact,
    result_contract: ResultContract,
    functions: &dyn crate::contracts::LixFunctionProvider,
    writer_key: Option<&str>,
) -> Result<WriteExecutionOutcome, LixError> {
    let _ = (functions, writer_key, direct.should_refresh_file_cache);
    let direct_result =
        execute_prepared_with_transaction(transaction, &direct.prepared_batch).await?;
    let public_result = public_result_from_contract(result_contract, &direct_result);

    Ok(WriteExecutionOutcome {
        public_result,
        direct_write_file_cache_targets: BTreeSet::new(),
        plugin_changes_committed: false,
        canonical_commit_receipt: None,
        plan_effects_override: None,
        state_commit_stream_changes: Vec::new(),
        observe_tick_emitted: false,
    })
}

fn public_result_from_contract(
    contract: ResultContract,
    direct_result: &QueryResult,
) -> QueryResult {
    match contract {
        ResultContract::DmlNoReturning => QueryResult {
            rows: Vec::new(),
            columns: Vec::new(),
        },
        ResultContract::Select | ResultContract::DmlReturning | ResultContract::Nothing => {
            direct_result.clone()
        }
    }
}

async fn execute_prepared_with_transaction(
    transaction: &mut dyn LixBackendTransaction,
    batch: &PreparedBatch,
) -> Result<QueryResult, LixError> {
    let mut last_result = QueryResult {
        rows: Vec::new(),
        columns: Vec::new(),
    };
    for statement in &batch.steps {
        last_result = transaction
            .execute(&statement.sql, &statement.params)
            .await?;
    }
    Ok(last_result)
}
