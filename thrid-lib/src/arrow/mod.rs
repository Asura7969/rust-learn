pub mod ipc;

#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};
    use arrow::array::*;
    use arrow_array::RecordBatch;
    use arrow_cast::pretty::print_batches;
    use arrow_csv::reader::Format;
    use arrow_csv::ReaderBuilder;
    use arrow_schema::*;
    use futures::TryStreamExt;
    use parquet::arrow::arrow_reader::{
        ArrowPredicateFn, ParquetRecordBatchReaderBuilder, RowFilter,
    };
    use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder, ProjectionMask};
    use std::io::Seek;
    use std::time::SystemTime;
    use std::{fs::File, sync::Arc};
    use tempfile::NamedTempFile;

    #[test]
    fn test_write_and_read_parquet_file() {
        // write file
        let ids = Int32Array::from(vec![1, 2, 3, 4]);
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let file = NamedTempFile::new().unwrap();

        let read_file = file.reopen().unwrap();

        println!("{:?}", read_file);

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids)]).unwrap();

        let batches = vec![batch];
        let mut writer = ArrowWriter::try_new(file, Arc::clone(&schema), None).unwrap();

        for batch in batches {
            writer.write(&batch).expect("Writing batch");
        }

        writer.close().unwrap();

        // read file
        let builder = ParquetRecordBatchReaderBuilder::try_new(read_file).unwrap();
        println!("Converted arrow schema is: {}", builder.schema());
        let mut reader = builder.build().unwrap();
        let record_batch = reader.next().unwrap().unwrap();

        print_batches(&[record_batch]).unwrap();
    }

    #[tokio::test]
    async fn test_async_read_parquet_file() -> Result<()> {
        let file = tokio::fs::File::open("src/test/data/alltypes_plain.parquet").await?;

        let mut builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .with_batch_size(8192);

        let file_metadata = builder.metadata().file_metadata().clone();
        let mask = ProjectionMask::roots(file_metadata.schema_descr(), [0, 1, 2]);

        // 设置只读取 0, 1, 2 列
        builder = builder.with_projection(mask);

        let filter = ArrowPredicateFn::new(
            ProjectionMask::roots(file_metadata.schema_descr(), [0]),
            // 定义 where 条件, 0列(id)值 > 1
            |record_batch| arrow::compute::gt_dyn_scalar(record_batch.column(0), 1),
        );
        let row_filter = RowFilter::new(vec![Box::new(filter)]);
        builder = builder.with_row_filter(row_filter);

        // Build a async parquet reader.
        let stream = builder.build().unwrap();

        let start = SystemTime::now();

        let result = stream.try_collect::<Vec<_>>().await?;

        println!("took: {} ms", start.elapsed().unwrap().as_millis());

        print_batches(&result).unwrap();

        Ok(())
    }

    #[test]
    fn test_read_csv_file() {
        let mut file = File::open("src/test/data/uk_cities_with_headers.csv").unwrap();

        let (schema, _) = Format::default()
            .with_header(true)
            .infer_schema(&mut file, None)
            .unwrap();

        file.rewind().unwrap();
        let builder = ReaderBuilder::new(Arc::new(schema)).has_header(true);

        let mut csv = builder.build(file).unwrap();
        let expected_schema = Schema::new(vec![
            Field::new("city", DataType::Utf8, true),
            Field::new("lat", DataType::Float64, true),
            Field::new("lng", DataType::Float64, true),
        ]);
        assert_eq!(Arc::new(expected_schema), csv.schema());
        let batch = csv.next().unwrap().unwrap();
        assert_eq!(37, batch.num_rows());
        assert_eq!(3, batch.num_columns());

        // access data from a primitive array
        let lat = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        assert_eq!(57.653484, lat.value(0));

        // access data from a string array (ListArray<u8>)
        let city = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        assert_eq!("Aberdeen, Aberdeen City, UK", city.value(13));
    }
}
