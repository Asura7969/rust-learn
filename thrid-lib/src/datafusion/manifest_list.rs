use serde::{Deserialize, Serialize};

use super::PartitionStat;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(crate) struct ManifestFileMeta {
    #[serde(rename = "_VERSION")]
    version: i32,
    #[serde(rename = "_FILE_NAME")]
    file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    file_size: i64,
    #[serde(rename = "_NUM_ADDED_FILES")]
    num_added_files: i64,
    #[serde(rename = "_NUM_DELETED_FILES")]
    num_deleted_files: i64,
    #[serde(rename = "_PARTITION_STATS")]
    partition_stats: Option<PartitionStat>,
    #[serde(rename = "_SCHEMA_ID")]
    schema_id: i64,
}
