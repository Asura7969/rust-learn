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
    use std::fs;

    use anyhow::Result;
    use apache_avro::{from_value, Reader as AvroReader};

    use crate::datafusion::ManifestFileMeta;

    #[tokio::test]
    async fn read_avro() -> Result<()> {
        let path = "src/test/paimon/default.db/ods_mysql_paimon_points_5/manifest/manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-12";
        let r = fs::File::open(path)?;
        let reader = AvroReader::new(r)?;
        let writer_schema = reader.writer_schema().clone();
        println!("schema: {:?}", writer_schema);

        println!("");
        println!("");
        println!("");

        let mut manifestlist = Vec::new();

        for value in reader {
            let record = value.unwrap();
            let meta: ManifestFileMeta = from_value(&record)?;
            manifestlist.push(meta);
        }

        let serialized = serde_json::to_string_pretty(&manifestlist).unwrap();
        println!("{}", serialized);

        Ok(())
    }
}
