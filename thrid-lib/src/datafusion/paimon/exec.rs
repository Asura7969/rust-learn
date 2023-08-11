use std::{any::Any, sync::Arc};

use arrow_array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_common::Statistics;
// use futures::StreamExt;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MergeExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
}

impl MergeExec {
    /// Create a new MergeExec
    #[allow(dead_code)]
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        MergeExec { input }
    }
}

impl DisplayAs for MergeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "MergeExec")
            }
        }
    }
}
impl ExecutionPlan for MergeExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.input.schema())
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        datafusion::physical_plan::Partitioning::UnknownPartitioning(1)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    /// see AnalyzeExec
    fn execute(
        &self,
        _partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // let mut builder = RecordBatchReceiverStream::builder(self.schema(), 1);
        // let mut input_stream = builder.build();
        let captured_input = self.input.clone();
        let captured_schema = self.input.schema();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.input.schema(),
            futures::stream::once(merge_batch(captured_input, captured_schema, context)),
        )))
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}

async fn merge_batch(
    input: Arc<dyn ExecutionPlan>,
    _schema: SchemaRef,
    context: Arc<TaskContext>,
) -> Result<RecordBatch> {
    let _read: Vec<datafusion::arrow::record_batch::RecordBatch> = collect(input, context).await?;
    todo!()
}
