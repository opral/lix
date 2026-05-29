use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::TaskContext;
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType, PlanProperties};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use futures_util::stream;

use super::runtime;

#[async_trait]
pub(crate) trait InsertSink: Debug + DisplayAs + Send + Sync {
    async fn write_batches(
        &self,
        batches: Vec<RecordBatch>,
        context: &Arc<TaskContext>,
    ) -> Result<u64>;
}

pub(crate) struct InsertExec {
    input: Arc<dyn ExecutionPlan>,
    sink: Arc<dyn InsertSink>,
    result_schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl InsertExec {
    pub(crate) fn new(input: Arc<dyn ExecutionPlan>, sink: Arc<dyn InsertSink>) -> Self {
        let result_schema = dml_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&result_schema)),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Final,
            Boundedness::Bounded,
        );
        Self {
            input,
            sink,
            result_schema,
            properties: Arc::new(properties),
        }
    }
}

impl Debug for InsertExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InsertExec").finish()
    }
}

impl DisplayAs for InsertExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "InsertExec: sink=")?;
                self.sink.fmt_as(t, f)
            }
            DisplayFormatType::TreeRender => write!(f, "InsertExec"),
        }
    }
}

impl ExecutionPlan for InsertExec {
    fn name(&self) -> &'static str {
        "InsertExec"
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

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Execution(format!(
                "InsertExec expects one input child, got {}",
                children.len()
            )));
        }
        Ok(Arc::new(Self::new(
            children.swap_remove(0),
            Arc::clone(&self.sink),
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Execution(format!(
                "InsertExec only exposes one partition, got {partition}"
            )));
        }

        let input = Arc::clone(&self.input);
        let sink = Arc::clone(&self.sink);
        let stream_schema = Arc::clone(&self.result_schema);
        let result_schema = Arc::clone(&self.result_schema);
        let stream = stream::once(async move {
            let batches = runtime::collect_input_plan(input, Arc::clone(&context)).await?;
            let count = sink.write_batches(batches, &context).await?;
            dml_count_batch(stream_schema, count)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            result_schema,
            stream,
        )))
    }
}

fn dml_count_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

#[expect(trivial_casts)]
fn dml_count_batch(schema: SchemaRef, count: u64) -> Result<RecordBatch> {
    RecordBatch::try_new(
        schema,
        vec![Arc::new(UInt64Array::from(vec![count])) as ArrayRef],
    )
    .map_err(DataFusionError::from)
}
