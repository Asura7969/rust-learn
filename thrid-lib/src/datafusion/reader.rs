use std::fs::File;

use datafusion::{
    arrow::datatypes::SchemaRef,
    datasource::avro_to_arrow::{Reader, ReaderBuilder},
};

pub(crate) enum FileFormat {
    #[allow(dead_code)]
    Parquet,
    #[allow(dead_code)]
    Avro,
    #[allow(dead_code)]
    Orc,
}

impl From<&String> for FileFormat {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "parquet" => FileFormat::Parquet,
            "orc" => FileFormat::Orc,
            _ => FileFormat::Avro,
        }
    }
}

#[allow(dead_code)]
fn build_reader(path: &str, _schema: SchemaRef) -> Reader<File> {
    let builder = ReaderBuilder::new().read_schema().with_batch_size(64);
    builder.build(File::open(path).unwrap()).unwrap()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use anyhow::Result;
    use datafusion::{
        arrow::datatypes::{DataType, Field, Fields, Schema},
        prelude::{AvroReadOptions, SessionContext},
    };

    #[tokio::test]
    async fn read_avro() -> Result<()> {
        let fields = vec![
            Field::new("_MIN_VALUES", DataType::Binary, false),
            Field::new("_MAX_VALUES", DataType::Binary, false),
            Field::new("_NULL_COUNTS", DataType::LargeBinary, false),
        ];

        let _schema = Arc::new(Schema::new(vec![
            Field::new("_VERSION", DataType::Int32, false),
            Field::new("_FILE_NAME", DataType::Utf8, false),
            Field::new("_FILE_SIZE", DataType::Int64, false),
            Field::new("_NUM_ADDED_FILES", DataType::Int64, false),
            Field::new("_NUM_DELETED_FILES", DataType::Int64, false),
            Field::new(
                "_PARTITION_STATS",
                DataType::Struct(Fields::from(fields)),
                true,
            ),
            Field::new("_SCHEMA_ID", DataType::Int64, false),
        ]));

        // let file_path = "src/test/paimon/default.db/ods_mysql_paimon_points_5/manifest/manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-3";
        // let mut reader = build_reader(file_path, schema);
        // let batch = reader.next().unwrap().unwrap();
        // println!("batch: {:?}", batch);
        // let schema = reader.schema();
        // println!("schema: {schema}");

        let ctx = SessionContext::new();
        let op = AvroReadOptions {
            schema: None,
            file_extension: " ",
            table_partition_cols: vec![],
            infinite: false,
        };

        ctx.register_avro("manifestlist", "src/test/paimon/default.db/ods_mysql_paimon_points_5/manifest/manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-13", op)
            .await?;
        let df = ctx.sql("SELECT * FROM manifestlist").await?;
        df.show_limit(10).await?;
        Ok(())
    }
}
