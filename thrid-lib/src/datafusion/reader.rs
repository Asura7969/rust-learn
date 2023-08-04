use std::fs;

use anyhow::Result;
use apache_avro::{from_value, Reader as AvroReader};

use crate::datafusion::ManifestFileMeta;
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

pub(crate) fn manifest_list(path: &str, format: &FileFormat) -> Result<Vec<ManifestFileMeta>> {
    match format {
        FileFormat::Avro => read_avro(path),
        FileFormat::Parquet => unimplemented!(),
        FileFormat::Orc => unimplemented!(),
    }
}

fn read_avro(path: &str) -> Result<Vec<ManifestFileMeta>> {
    // TODO: remote read, such as OSS, HDFS, ect.
    let r = fs::File::open(path)?;
    let reader = AvroReader::new(r)?;
    // let writer_schema = reader.writer_schema().clone();
    // println!("schema: {:?}", writer_schema);

    let mut manifestlist: Vec<ManifestFileMeta> = Vec::new();

    for value in reader {
        let record = value.unwrap();
        let meta: ManifestFileMeta = from_value(&record)?;
        manifestlist.push(meta);
    }

    Ok(manifestlist)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn read_avro_test() -> Result<()> {
        let path = "src/test/paimon/default.db/ods_mysql_paimon_points_5/manifest/manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-12";

        let manifestlist = read_avro(path)?;

        let serialized = serde_json::to_string_pretty(&manifestlist).unwrap();
        println!("{}", serialized);

        Ok(())
    }
}
