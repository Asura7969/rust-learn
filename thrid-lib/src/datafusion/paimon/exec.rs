use std::{any::Any, sync::Arc};

use ahash::{AHashMap, RandomState};
use arrow::compute::{filter_record_batch, gt_scalar};
use arrow_array::cast::downcast_array;
use arrow_array::{BooleanArray, Int8Array, RecordBatch, StringArray, UInt64Array};
use arrow_select::concat::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::hash_utils::create_hashes;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::PhysicalExpr;
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

use crate::datafusion::paimon::PrimaryKeys;
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
    let records: Vec<RecordBatch> = collect(input, context.clone()).await?;
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
    let idx = map
        .into_values()
        .map(|(_, _, _, idx)| idx)
        .collect::<Vec<usize>>();
    let idx: Vec<_> = (0..count).map(|x| idx.contains(&x)).collect();
    let merge_filter: BooleanArray = BooleanArray::from(idx);
    let batch = filter_record_batch(&batch, &merge_filter).unwrap();
    std::result::Result::Ok(batch)
}

#[allow(dead_code)]
fn extract_merge_key(batch: &RecordBatch, ctx: Arc<TaskContext>) {
    let pks = &ctx
        .session_config()
        .get_extension::<PrimaryKeys>()
        .unwrap()
        .0;

    let pk_len = pks.len();

    let key = pks
        .iter()
        .enumerate()
        .map(|(idx, pk_name)| Column::new(pk_name.as_str(), idx))
        .collect::<Vec<_>>();
    let keys_values = key
        .iter()
        .map(|c| Ok(c.evaluate(batch)?.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let random_state = RandomState::with_seeds(0, 0, 0, 0);
    let mut hashes_buffer = vec![0; batch.num_rows()];

    let _hash_values = create_hashes(&keys_values, &random_state, &mut hashes_buffer).unwrap();

    let _seq: UInt64Array = downcast_array(batch.column(pk_len));
    let _kind: Int8Array = downcast_array(batch.column(pk_len + 1));

    let _row_num = batch.num_rows();

    todo!()
}

// fn print_primitive<T: ByteArrayType>(array: &dyn Array) -> &GenericByteArray<T> {
//     downcast_primitive_array!(
//         array => array,
//         DataType::Utf8 => as_string_array(array),
//         t => panic!("Unsupported datatype {}", t)
//     )
// }

// struct RowBatch<T> {
//     pk: T,
//     seq: u64,
//     kind: i8,
//     idx: usize,
// }
