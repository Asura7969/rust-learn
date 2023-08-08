use std::{any::Any, sync::Arc};

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::error::Result;
use datafusion::{
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_common::Statistics;

use super::snapshot::Snapshot;

#[allow(dead_code)]
#[derive(Debug)]
pub struct StreamExec {
    snapshot: Snapshot,
    schema: SchemaRef,
}

impl StreamExec {
    #[allow(dead_code)]
    pub fn new(snapshot: Snapshot, schema: SchemaRef) -> Self {
        Self { snapshot, schema }
    }
}

impl DisplayAs for StreamExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "StreamExec",)
            }
        }
    }
}

impl ExecutionPlan for StreamExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
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

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        todo!()
    }

    fn statistics(&self) -> Statistics {
        unimplemented!()
    }
}
