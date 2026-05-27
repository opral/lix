use crate::sql2::optimize::simple_write::{FastDeletePlan, FastUpdatePlan, FastWritePlan};
use crate::sql2::SqlWriteExecutionContext;
use crate::{LixError, Value};

pub(crate) async fn try_execute_simple_write(
    _ctx: &mut dyn SqlWriteExecutionContext,
    plan: FastWritePlan,
    _params: &[Value],
) -> Result<u64, LixError> {
    match plan {
        FastWritePlan::Update(plan) => execute_fast_update(plan).await,
        FastWritePlan::Delete(plan) => execute_fast_delete(plan).await,
    }
}

async fn execute_fast_update(_plan: FastUpdatePlan) -> Result<u64, LixError> {
    Ok(0)
}

async fn execute_fast_delete(_plan: FastDeletePlan) -> Result<u64, LixError> {
    Ok(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn fast_no_match_update_returns_zero_rows_affected() {
        let rows_affected = try_execute_simple_write(
            &mut PanicWriteContext,
            FastWritePlan::Update(FastUpdatePlan),
            &[],
        )
        .await
        .expect("fast no-match update should execute");

        assert_eq!(rows_affected, 0);
    }

    struct PanicWriteContext;

    #[async_trait::async_trait]
    impl SqlWriteExecutionContext for PanicWriteContext {
        fn active_branch_id(&self) -> &str {
            panic!("fast no-match execution should not inspect context")
        }

        fn functions(&self) -> crate::functions::FunctionProviderHandle {
            panic!("fast no-match execution should not inspect context")
        }

        fn list_visible_schemas(&self) -> Result<Vec<serde_json::Value>, LixError> {
            panic!("fast no-match execution should not inspect context")
        }

        async fn load_bytes_many(
            &mut self,
            _hashes: &[crate::binary_cas::BlobHash],
        ) -> Result<crate::binary_cas::BlobBytesBatch, LixError> {
            panic!("fast no-match execution should not inspect context")
        }

        async fn scan_live_state(
            &mut self,
            _request: &crate::live_state::LiveStateScanRequest,
        ) -> Result<Vec<crate::live_state::MaterializedLiveStateRow>, LixError> {
            panic!("fast no-match execution should not inspect context")
        }

        async fn load_branch_head(&mut self, _branch_id: &str) -> Result<Option<String>, LixError> {
            panic!("fast no-match execution should not inspect context")
        }

        async fn stage_write(
            &mut self,
            _write: crate::transaction::types::TransactionWrite,
        ) -> Result<crate::transaction::types::TransactionWriteOutcome, LixError> {
            panic!("fast no-match execution should not inspect context")
        }
    }
}
