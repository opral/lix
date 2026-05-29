use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use futures_util::TryStreamExt;

pub(crate) async fn collect_dataframe(dataframe: DataFrame) -> Result<Vec<RecordBatch>> {
    let task_ctx = Arc::new(dataframe.task_ctx());
    let plan = dataframe.create_physical_plan().await?;
    collect_input_plan(plan, task_ctx).await
}

pub(crate) async fn collect_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    validate_physical_plan(&plan)?;
    let partition_count = plan.output_partitioning().partition_count();
    let mut batches = Vec::new();
    for partition in 0..partition_count {
        let partition_batches = plan
            .execute(partition, Arc::clone(&task_ctx))?
            .try_collect::<Vec<_>>()
            .await?;
        batches.extend(partition_batches);
    }
    Ok(batches)
}

#[cfg(not(target_family = "wasm"))]
#[expect(clippy::unnecessary_wraps)]
fn validate_physical_plan(_plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    Ok(())
}

#[cfg(target_family = "wasm")]
fn validate_physical_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<()> {
    let operator_name = plan.name();
    if is_wasm_unsafe_operator(operator_name) {
        return Err(datafusion::error::DataFusionError::Plan(format!(
            "SQL physical operator '{operator_name}' is not supported by the WebAssembly runtime yet"
        )));
    }

    for child in plan.children() {
        validate_physical_plan(child)?;
    }

    Ok(())
}

#[cfg(target_family = "wasm")]
fn is_wasm_unsafe_operator(operator_name: &str) -> bool {
    matches!(
        operator_name,
        "CoalescePartitionsExec" | "RepartitionExec" | "SortPreservingMergeExec"
    )
}
