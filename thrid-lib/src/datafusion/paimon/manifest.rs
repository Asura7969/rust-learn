use serde::{Deserialize, Serialize};

use super::PartitionStat;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ManifestEntry {
    #[serde(rename = "_KIND")]
    kind: i32,
    #[serde(rename = "_PARTITION", with = "serde_bytes")]
    partition: Vec<u8>,
    #[serde(rename = "_BUCKET")]
    bucket: i32,
    #[serde(rename = "_TOTAL_BUCKETS")]
    total_bucket: i32,
    #[serde(rename = "_FILE")]
    file: Option<DataFileMeta>,
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct DataFileMeta {
    #[serde(rename = "_FILE_NAME")]
    file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    file_size: i64,
    #[serde(rename = "_ROW_COUNT")]
    row_count: i64,
    #[serde(rename = "_MIN_KEY", with = "serde_bytes")]
    min_key: Vec<u8>,
    #[serde(rename = "_MAX_KEY", with = "serde_bytes")]
    max_key: Vec<u8>,
    #[serde(rename = "_KEY_STATS")]
    key_stats: Option<PartitionStat>,
    #[serde(rename = "_VALUE_STATS")]
    value_stats: Option<PartitionStat>,
    #[serde(rename = "_MIN_SEQUENCE_NUMBER")]
    min_sequence_number: i64,
    #[serde(rename = "_MAX_SEQUENCE_NUMBER")]
    max_sequence_number: i64,
    #[serde(rename = "_SCHEMA_ID")]
    schema_id: i64,
    #[serde(rename = "_LEVEL")]
    level: i32,
    #[serde(rename = "_EXTRA_FILES")]
    extra_files: Vec<String>,
    #[serde(rename = "_CREATION_TIME")]
    creation_time: i64,
}
