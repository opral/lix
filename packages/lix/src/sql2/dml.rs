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

/// Consume source batches incrementally into statement-owned mutation state.
/// Implementations must exhaust the input successfully before publishing writes:
/// later partitions may read the target, or fail after earlier batches succeeded.
#[async_trait]
pub(crate) trait InsertSink: Debug + DisplayAs + Send + Sync {
    async fn write_batches(
        &self,
        batches: SendableRecordBatchStream,
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
            let batches = runtime::stream_input_plan(input, Arc::clone(&context))?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::{StreamExt, TryStreamExt};
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// Each next batch is available only after the sink consumed the previous
    /// one. An eager collector therefore fails instead of merely using more RAM.
    #[derive(Debug)]
    struct PullInput {
        consumed: Arc<AtomicUsize>,
        properties: Arc<PlanProperties>,
    }

    impl DisplayAs for PullInput {
        fn fmt_as(
            &self,
            _: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "PullInput")
        }
    }

    impl ExecutionPlan for PullInput {
        fn name(&self) -> &'static str {
            "PullInput"
        }
        fn as_any(&self) -> &dyn Any {
            self
        }
        fn properties(&self) -> &Arc<PlanProperties> {
            &self.properties
        }
        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![]
        }
        fn with_new_children(
            self: Arc<Self>,
            _: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(self)
        }
        fn execute(
            &self,
            partition: usize,
            _: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let consumed = Arc::clone(&self.consumed);
            let schema = dml_count_schema();
            let batch_schema = Arc::clone(&schema);
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream::iter(0..3).map(move |index| {
                    if consumed.load(Ordering::SeqCst) != partition * 3 + index {
                        return Err(DataFusionError::Execution(
                            "source was drained ahead of sink".into(),
                        ));
                    }
                    dml_count_batch(Arc::clone(&batch_schema), index as u64)
                }),
            )))
        }
    }

    #[derive(Debug)]
    struct PullSink(Arc<AtomicUsize>);
    impl DisplayAs for PullSink {
        fn fmt_as(
            &self,
            _: DisplayFormatType,
            f: &mut std::fmt::Formatter<'_>,
        ) -> std::fmt::Result {
            write!(f, "PullSink")
        }
    }
    #[async_trait]
    impl InsertSink for PullSink {
        async fn write_batches(
            &self,
            mut batches: SendableRecordBatchStream,
            _: &Arc<TaskContext>,
        ) -> Result<u64> {
            let mut count = 0;
            while let Some(batch) = batches.try_next().await? {
                count += batch.num_rows() as u64;
                self.0.fetch_add(1, Ordering::SeqCst);
            }
            Ok(count)
        }
    }

    #[tokio::test]
    async fn insert_sink_pulls_batches_across_partitions() {
        let consumed = Arc::new(AtomicUsize::new(0));
        let input = Arc::new(PullInput {
            consumed: Arc::clone(&consumed),
            properties: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(dml_count_schema()),
                Partitioning::UnknownPartitioning(2),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        });
        let exec = InsertExec::new(input, Arc::new(PullSink(Arc::clone(&consumed))));
        let batches = exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(consumed.load(Ordering::SeqCst), 6);
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(0),
            6
        );
    }
}
