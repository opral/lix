use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, internal_err};
use datafusion::dataframe::DataFrame;
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::execution_plan::CardinalityEffect;
use datafusion::physical_plan::limit::LimitStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use futures_util::{StreamExt, TryStreamExt, stream};

pub(crate) async fn collect_dataframe(dataframe: DataFrame) -> Result<Vec<RecordBatch>> {
    let task_ctx = Arc::new(dataframe.task_ctx());
    let plan = dataframe.create_physical_plan().await?;
    collect_input_plan(plan, task_ctx).await
}

pub(crate) async fn collect_input_plan(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> Result<Vec<RecordBatch>> {
    let plan = adapt_runtime_plan(plan)?;
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

fn adapt_runtime_plan(plan: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let mut children_changed = false;
    let mut children = Vec::new();
    for child in plan.children() {
        let original = Arc::clone(child);
        let adapted = adapt_runtime_plan(Arc::clone(child))?;
        children_changed |= !Arc::ptr_eq(&original, &adapted);
        children.push(adapted);
    }
    let plan = if children_changed {
        plan.with_new_children(children)?
    } else {
        plan
    };

    let Some(coalesce) = plan.as_any().downcast_ref::<CoalescePartitionsExec>() else {
        return Ok(plan);
    };
    Ok(Arc::new(SerialCoalescePartitionsExec::new(
        Arc::clone(coalesce.input()),
        coalesce.fetch(),
    )))
}

/// Runtime-neutral partition coalescing.
///
/// DataFusion's coalescer spawns one task per input partition. Lix deliberately
/// uses a single-partition SQL session and can merge the structural partitions
/// produced by operators such as `UNION ALL` serially instead. Keeping this
/// adapter target-independent gives native and WebAssembly the same SQL plan
/// semantics without relying on runtime-specific task spawning.
#[derive(Debug)]
struct SerialCoalescePartitionsExec {
    input: Arc<dyn ExecutionPlan>,
    fetch: Option<usize>,
    metrics: ExecutionPlanMetricsSet,
    properties: Arc<PlanProperties>,
}

impl SerialCoalescePartitionsExec {
    fn new(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Self {
        let datafusion_plan = CoalescePartitionsExec::new(Arc::clone(&input)).with_fetch(fetch);
        Self {
            input,
            fetch,
            metrics: ExecutionPlanMetricsSet::new(),
            properties: Arc::clone(datafusion_plan.properties()),
        }
    }
}

impl DisplayAs for SerialCoalescePartitionsExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SerialCoalescePartitionsExec")?;
        if let Some(fetch) = self.fetch {
            write!(f, ": fetch={fetch}")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for SerialCoalescePartitionsExec {
    fn name(&self) -> &'static str {
        "SerialCoalescePartitionsExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "SerialCoalescePartitionsExec expects one child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(children.swap_remove(0), self.fetch)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return internal_err!(
                "SerialCoalescePartitionsExec only exposes partition 0, got {partition}"
            );
        }
        let partition_count = self.input.output_partitioning().partition_count();
        if partition_count == 0 {
            return internal_err!(
                "SerialCoalescePartitionsExec requires at least one input partition"
            );
        }

        let streams = (0..partition_count)
            .map(|input_partition| self.input.execute(input_partition, Arc::clone(&context)))
            .collect::<Result<Vec<_>>>()?;
        let schema = self.schema();
        let serial_stream = stream::iter(streams).flatten();
        let adapted: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            serial_stream,
        ));

        if self.fetch.is_none() {
            return Ok(adapted);
        }
        Ok(Box::pin(LimitStream::new(
            adapted,
            0,
            self.fetch,
            BaselineMetrics::new(&self.metrics, partition),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        self.input
            .partition_statistics(None)?
            .with_fetch(self.fetch, 0, 1)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}
