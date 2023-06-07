#[cfg(test)]
mod tests {
    use anyhow::{Ok, Result};
    use arrow::array::*;
    use arrow_cast::pretty::print_batches;
    use arrow_csv::reader::Format;
    use arrow_csv::ReaderBuilder;
    use arrow_schema::*;
    use futures::TryStreamExt;
    use parquet::arrow::arrow_reader::{ArrowPredicateFn, RowFilter};
    use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
    use std::io::Seek;
    use std::time::SystemTime;
    use std::{fs::File, sync::Arc};

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
