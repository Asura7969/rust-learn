use serde::{Deserialize, Serialize};

use super::{
    error::PaimonError, manifest::ManifestEntry, reader::manifest, PaimonSchema, PartitionStat,
};
use anyhow::Result;

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct ManifestFileMeta {
    #[serde(rename = "_VERSION")]
    pub version: i32,
    #[serde(rename = "_FILE_NAME")]
    pub file_name: String,
    #[serde(rename = "_FILE_SIZE")]
    pub file_size: i64,
    #[serde(rename = "_NUM_ADDED_FILES")]
    pub num_added_files: i64,
    #[serde(rename = "_NUM_DELETED_FILES")]
    pub num_deleted_files: i64,
    #[serde(rename = "_PARTITION_STATS")]
    pub partition_stats: Option<PartitionStat>,
    #[serde(rename = "_SCHEMA_ID")]
    pub schema_id: i64,
}

impl ManifestFileMeta {
    pub fn manifest(
        &self,
        table_path: &str,
        schema: &PaimonSchema,
    ) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("{}/manifest/{}", table_path, self.file_name);
        manifest(path.as_str(), &schema.get_manifest_format())
    }
}
