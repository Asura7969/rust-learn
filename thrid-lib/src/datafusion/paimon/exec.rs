use std::{any::Any, sync::Arc};

use ahash::AHashMap;
use arrow::compute::{filter_record_batch, gt_scalar};
use arrow_array::cast::downcast_array;
use arrow_array::{BooleanArray, Int8Array, RecordBatch, StringArray, UInt64Array};
use arrow_select::concat::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::{
    execution::TaskContext,
    physical_expr::PhysicalSortExpr,
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
};
use datafusion_common::Result;
// use datafusion::error::Result;
use datafusion_common::Statistics;
// use itertools::Itertools;

// use futures::StreamExt;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct MergeExec {
    pub(crate) input: Arc<dyn ExecutionPlan>,
    // merge_func: Fn<>
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
        Result::Ok(self)
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

        Result::Ok(Box::pin(RecordBatchStreamAdapter::new(
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
    schema: SchemaRef,
    context: Arc<TaskContext>,
) -> Result<RecordBatch> {
    let records: Vec<RecordBatch> = collect(input, context).await?;
    let batch = concat_batches(&schema, records.iter())?;
    // column_by_name
    // use arrow_select::take::take;
    let arr: Int8Array = downcast_array(batch.column(2).as_ref());
    let filter = gt_scalar(&arr, 0i8).unwrap();
    let delete_or_update = filter_record_batch(&batch, &filter).unwrap();
    if delete_or_update.num_rows() == 0 {
        return std::result::Result::Ok(batch);
    }

    let inner_field = batch.project(&[0, 1, 2])?;
    let count = inner_field.num_rows();
    let pk: StringArray = downcast_array(inner_field.column(0));
    let seq: UInt64Array = downcast_array(inner_field.column(1));
    let kind: Int8Array = downcast_array(inner_field.column(2));
    let mut map: AHashMap<&str, (&str, u64, i8, usize)> = AHashMap::new();

    for idx in 0..count {
        let pk_v = pk.value(idx);
        let seq_v = seq.value(idx);
        let kind_v = kind.value(idx);

        match kind_v {
            0 | 2 => {
                map.insert(pk_v, (pk_v, seq_v, kind_v, idx));
            }
            1 => {}
            3 => {
                map.remove(pk_v);
            }
            _ => panic!("unknown rowkind, maybe new kind"),
        };
    }
    println!("map size: {}, row count: {}", map.len(), count);
    let idx = map
        .into_values()
        .map(|(_, _, _, idx)| idx)
        .collect::<Vec<usize>>();
    let idx: Vec<_> = (0..count).map(|x| idx.contains(&x)).collect();
    let merge_filter: BooleanArray = BooleanArray::from(idx);
    let batch = filter_record_batch(&batch, &merge_filter).unwrap();
    std::result::Result::Ok(batch)
}

// struct RowBatch<T> {
//     pk: T,
//     seq: u64,
//     kind: i8,
//     idx: usize,
// }
