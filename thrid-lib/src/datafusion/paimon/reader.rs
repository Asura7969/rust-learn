use std::fs;

use anyhow::{Ok, Result};
use apache_avro::{from_value, Reader as AvroReader};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::{FileScanConfig, ParquetExec};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion_common::Statistics;
use serde::{Deserialize, Serialize};

use crate::datafusion::paimon::{to_schema_ref, ManifestFileMeta};

use super::{manifest::ManifestEntry, PaimonSchema};
use object_store::ObjectMeta;
pub enum FileFormat {
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

pub fn manifest_list(path: &str, format: &FileFormat) -> Result<Vec<ManifestFileMeta>> {
    match format {
        FileFormat::Avro => read_avro::<ManifestFileMeta>(path),
        FileFormat::Parquet => unimplemented!(),
        FileFormat::Orc => unimplemented!(),
    }
}

#[allow(dead_code)]
pub fn manifest(path: &str, format: &FileFormat) -> Result<Vec<ManifestEntry>> {
    match format {
        FileFormat::Avro => read_avro::<ManifestEntry>(path),
        FileFormat::Parquet => unimplemented!(),
        FileFormat::Orc => unimplemented!(),
    }
}

fn read_avro<T: Serialize + for<'a> Deserialize<'a>>(path: &str) -> Result<Vec<T>> {
    // TODO: remote read, such as OSS, HDFS, etc.
    let r = fs::File::open(path)?;
    let reader = AvroReader::new(r)?;
    // let writer_schema = reader.writer_schema().clone();
    // println!("schema: {:?}", writer_schema);

    let mut manifestlist: Vec<T> = Vec::new();

    for value in reader {
        let record = value.unwrap();
        // println!("{:?}", record);
        let meta: T = from_value::<T>(&record)?;
        manifestlist.push(meta);
    }

    Ok(manifestlist)
}

#[allow(dead_code)]
pub fn read_parquet(
    table_path: &str,
    entries: &[ManifestEntry],
    schema: &mut PaimonSchema,
) -> Result<ParquetExec> {
    let file_groups = entries
        .iter()
        .map(|e| {
            let p: Option<ObjectMeta> = e.to_object_meta(table_path);
            p
        })
        .filter(|o| o.is_some())
        .map(|o| Into::into(o.unwrap()))
        .collect::<Vec<PartitionedFile>>();

    let file_schema = to_schema_ref(schema);
    // Create a async parquet reader builder with batch_size.
    // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024

    // schema_coercion.rs
    let parquet_exec: ParquetExec = ParquetExec::new(
        FileScanConfig {
            object_store_url: ObjectStoreUrl::local_filesystem(),
            file_groups: vec![file_groups],
            file_schema,
            statistics: Statistics::default(),
            projection: None,
            limit: None,
            table_partition_cols: vec![],
            output_ordering: vec![],
            infinite_source: false,
        },
        None,
        None,
    );

    Ok(parquet_exec)
}

#[allow(unused_imports)]
#[cfg(test)]
mod tests {

    use crate::datafusion::paimon::{manifest::ManifestEntry, snapshot::SnapshotManager};
    use anyhow::Ok;
    use arrow::util::pretty::print_batches as arrow_print_batches;
    use futures::TryStreamExt;
    use parquet::arrow::{
        arrow_reader::{ArrowPredicateFn, RowFilter},
        ParquetRecordBatchStreamBuilder, ProjectionMask,
    };
    use std::time::SystemTime;
    use tokio::fs::File;

    use super::*;

    #[tokio::test]
    async fn read_manifest_list_test() -> Result<()> {
        let path = "src/test/paimon/default.db/ods_mysql_paimon_points_5/manifest/manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-12";

        let manifestlist = read_avro::<ManifestFileMeta>(path)?;

        let serialized = serde_json::to_string_pretty(&manifestlist).unwrap();
        println!("{}", serialized);

        Ok(())
    }

    #[tokio::test]
    async fn read_manifest_test() -> Result<()> {
        let path = "src/test/paimon/default.db/ods_mysql_paimon_points_5/manifest/manifest-5246a8f1-fdf4-4524-a2a2-fcd99dc08a1b-0";

        let manifest = read_avro::<ManifestEntry>(path)?;

        let serialized = serde_json::to_string(&manifest).unwrap();
        println!("{}", serialized);

        Ok(())
    }

    #[tokio::test]
    async fn merge_stream() -> Result<()> {
        let table_path = "src/test/paimon/default.db/ods_mysql_paimon_points_5";
        let _manager = SnapshotManager::new(table_path);

        // let _snapshot = manager.latest_snapshot().map(|s| {});

        // let mut streams = vec![];

        Ok(())
    }

    #[tokio::test]
    async fn async_read_parquet_files_test() -> Result<()> {
        let path = "src/test/paimon/default.db/ods_mysql_paimon_points_5/bucket-0/data-d8b88949-3406-4894-b282-88f19d1e6fcd-1.parquet";
        let file = File::open(path).await.unwrap();

        // Create a async parquet reader builder with batch_size.
        // batch_size is the number of rows to read up to buffer once from pages, defaults to 1024
        let mut builder = ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap()
            .with_batch_size(8192);

        let file_metadata = builder.metadata().file_metadata().clone();
        let mask = ProjectionMask::roots(
            file_metadata.schema_descr(),
            [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13],
        );
        // Set projection mask to read only root columns 1 and 2.
        builder = builder.with_projection(mask);

        // Highlight: set `RowFilter`, it'll push down filter predicates to skip IO and decode.
        // For more specific usage: please refer to https://github.com/apache/arrow-datafusion/blob/master/datafusion/core/src/physical_plan/file_format/parquet/row_filter.rs.

        // let filter = ArrowPredicateFn::new(
        //     ProjectionMask::roots(file_metadata.schema_descr(), [0]),
        //     |record_batch| arrow::compute::eq_dyn_scalar(record_batch.column(0), 1),
        // );
        // let row_filter = RowFilter::new(vec![Box::new(filter)]);
        // builder = builder.with_row_filter(row_filter);

        // Build a async parquet reader.
        let stream = builder.build().unwrap();

        let start = SystemTime::now();

        let result = stream.try_collect::<Vec<_>>().await?;

        println!("took: {} ms", start.elapsed().unwrap().as_millis());

        arrow_print_batches(&result).unwrap();

        Ok(())
    }
}
