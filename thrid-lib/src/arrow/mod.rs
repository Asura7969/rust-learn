#[warn(dead_code)]
#[warn(unused_imports)]
#[cfg(test)]
mod tests {
    use arrow::array::*;
    use arrow_csv::reader::Format;
    use arrow_csv::ReaderBuilder;
    use arrow_schema::*;
    use std::io::Seek;
    use std::{fs::File, sync::Arc};

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
