use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use super::{
    error::PaimonError, manifest_list::ManifestFileMeta, reader::manifest_list, CommitKind,
    PaimonSchema,
};
use crate::datafusion::paimon::{manifest::ManifestEntry, utils::read_to_string};

#[allow(dead_code)]
const SNAPSHOT_PREFIX: &str = "snapshot-";
#[allow(dead_code)]
const EARLIEST: &str = "EARLIEST";
const LATEST: &str = "LATEST";

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Snapshot {
    version: Option<i32>,
    id: i64,
    #[serde(rename = "schemaId")]
    schema_id: i64,
    #[serde(rename = "baseManifestList")]
    base_manifest_list: String,
    #[serde(rename = "deltaManifestList")]
    delta_manifest_list: String,
    #[serde(rename = "changelogManifestList")]
    changelog_manifest_list: Option<String>,
    #[serde(rename = "indexManifest")]
    index_manifest: Option<String>,
    #[serde(rename = "commitUser")]
    commit_user: String,
    #[serde(rename = "commitIdentifier")]
    commit_identifier: i64,
    #[serde(rename = "commitKind")]
    commit_kind: CommitKind,
    #[serde(rename = "timeMillis")]
    time_millis: i64,
    #[serde(rename = "logOffsets")]
    log_offsets: HashMap<i32, i64>,
    #[serde(rename = "totalRecordCount")]
    total_record_count: Option<i64>,
    #[serde(rename = "deltaRecordCount")]
    delta_record_count: Option<i64>,
    #[serde(rename = "changelogRecordCount")]
    changelog_record_count: Option<i64>,
    watermark: Option<i64>,
}

impl Snapshot {
    pub fn get_schema(&self, table_path: &str) -> Result<PaimonSchema, PaimonError> {
        let latest_schema_path = format!("{}/schema/schema-{}", table_path, self.schema_id);
        let schema_str = read_to_string(latest_schema_path.as_str())?;
        let schema: PaimonSchema = serde_json::from_str(schema_str.as_str())?;
        Ok(schema)
    }

    #[allow(dead_code)]
    pub fn all(&self, table_path: &str) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("{}/manifest/{}", table_path, self.delta_manifest_list);
        let schema = self.get_schema(table_path)?;
        let format = &schema.get_manifest_format();
        let mut delta_file_meta = manifest_list(path.as_str(), format)?;

        let path = format!("{}/manifest/{}", table_path, self.base_manifest_list);
        let mut base_file_meta = manifest_list(path.as_str(), format)?;
        delta_file_meta.append(&mut base_file_meta);
        let entry = self.get_manifest_entry(delta_file_meta, table_path, &schema);
        Ok(entry)
    }

    #[allow(dead_code)]
    pub fn base(&self, table_path: &str) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("{}/manifest/{}", table_path, self.base_manifest_list);
        let schema = self.get_schema(table_path)?;
        let file_meta = manifest_list(path.as_str(), &schema.get_manifest_format())?;

        let entry = self.get_manifest_entry(file_meta, table_path, &schema);
        Ok(entry)
    }

    #[allow(dead_code)]
    pub fn delta(&self, table_path: &str) -> Result<Vec<ManifestEntry>, PaimonError> {
        let path = format!("{}/manifest/{}", table_path, self.delta_manifest_list);
        let schema = self.get_schema(table_path)?;
        let file_meta = manifest_list(path.as_str(), &schema.get_manifest_format())?;

        let entry = self.get_manifest_entry(file_meta, table_path, &schema);
        Ok(entry)
    }

    fn get_manifest_entry(
        &self,
        file_meta: Vec<ManifestFileMeta>,
        table_path: &str,
        schema: &PaimonSchema,
    ) -> Vec<ManifestEntry> {
        let r = file_meta
            .iter()
            .flat_map(|e| {
                let _file_name = &e.file_name;
                // let err_msg = format!("read {}", file_name.as_str());
                // TODO: Custom error
                e.manifest(table_path, schema).unwrap()
            })
            .collect::<Vec<ManifestEntry>>();
        // let serialized = serde_json::to_string(&r).unwrap();
        // println!("{}", serialized);
        r
    }
}

pub(crate) struct SnapshotManager {
    table_path: String,
}

#[allow(dead_code)]
impl SnapshotManager {
    pub(crate) fn new(table_path: &str) -> SnapshotManager {
        Self {
            table_path: table_path.to_string(),
        }
    }

    fn snapshot_path(&self, snapshot_id: i64) -> String {
        format!(
            "{}/snapshot/{}{}",
            self.table_path, SNAPSHOT_PREFIX, snapshot_id
        )
    }

    pub(crate) fn snapshot(&self, snapshot_id: i64) -> Result<Snapshot, PaimonError> {
        let path = self.snapshot_path(snapshot_id);
        let content = read_to_string(path.as_str())?;
        let s: Snapshot = serde_json::from_str(content.as_str())?;
        Ok(s)
    }

    fn latest_snapshot_id(&self) -> Option<i64> {
        let latest_path = format!("{}/snapshot/{}", self.table_path, LATEST);
        let id_string = read_to_string(latest_path.as_str()).unwrap();
        match id_string.parse::<i64>() {
            core::result::Result::Ok(id) => Some(id),
            Err(_) => None,
        }
    }

    pub(crate) fn latest_snapshot(&self) -> Option<Snapshot> {
        if let Some(id) = self.latest_snapshot_id() {
            match self.snapshot(id) {
                core::result::Result::Ok(s) => Some(s),
                Err(_) => None,
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    pub(crate) fn get_latest_metedata_file(table_path: &str) -> Result<Snapshot, PaimonError> {
        let latest_path = format!("{}/snapshot/LATEST", table_path);
        let latest_num = read_to_string(latest_path.as_str())?;

        let latest_path = format!("{}/snapshot/snapshot-{}", table_path, latest_num);

        let content = read_to_string(latest_path.as_str())?;
        let snapshot = serde_json::from_str(content.as_str())?;
        Ok(snapshot)
    }
    #[test]
    fn read_snapshot() -> Result<(), PaimonError> {
        let table_path = "src/test/paimon/default.db/ods_mysql_paimon_points_5";
        let json = r#"
            {
                "version" : 3,
                "id" : 5,
                "schemaId" : 0,
                "baseManifestList" : "manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-12",
                "deltaManifestList" : "manifest-list-a2f5adb6-adf1-4026-be6a-a01b5fb2cebd-13",
                "changelogManifestList" : null,
                "commitUser" : "e75f405b-210d-4d84-b350-ec445fed9530",
                "commitIdentifier" : 6,
                "commitKind" : "APPEND",
                "timeMillis" : 1691031342569,
                "logOffsets" : { },
                "totalRecordCount" : 9,
                "deltaRecordCount" : 0,
                "changelogRecordCount" : 0,
                "watermark" : -9223372036854775808
            }
            "#;
        let expected: Snapshot = serde_json::from_str(json).unwrap();
        let actual = get_latest_metedata_file(table_path)?;

        assert_eq!(expected, actual);
        assert_eq!(expected.version, actual.version);
        assert_eq!(expected.id, actual.id);
        assert_eq!(expected.schema_id, actual.schema_id);
        assert_eq!(expected.base_manifest_list, actual.base_manifest_list);
        assert_eq!(expected.delta_manifest_list, actual.delta_manifest_list);
        assert_eq!(
            expected.changelog_manifest_list,
            actual.changelog_manifest_list
        );
        assert_eq!(expected.commit_user, actual.commit_user);
        assert_eq!(expected.commit_identifier, actual.commit_identifier);
        assert_eq!(actual.commit_kind, actual.commit_kind);
        assert_eq!(expected.time_millis, actual.time_millis);
        assert_eq!(expected.log_offsets, actual.log_offsets);
        assert_eq!(expected.total_record_count, actual.total_record_count);
        assert_eq!(expected.delta_record_count, actual.delta_record_count);
        assert_eq!(
            expected.changelog_record_count,
            actual.changelog_record_count
        );
        assert_eq!(expected.watermark, actual.watermark);

        let actual = actual.get_schema(table_path)?;
        let schema_str = r#"
            {
                "id" : 0,
                "fields" : [ {
                "id" : 0,
                "name" : "point_id",
                "type" : "STRING NOT NULL"
                }, {
                "id" : 1,
                "name" : "version",
                "type" : "INT NOT NULL"
                }, {
                "id" : 2,
                "name" : "version_info",
                "type" : "STRING"
                }, {
                "id" : 3,
                "name" : "address",
                "type" : "STRING"
                }, {
                "id" : 4,
                "name" : "lon",
                "type" : "STRING"
                }, {
                "id" : 5,
                "name" : "lat",
                "type" : "STRING"
                }, {
                "id" : 6,
                "name" : "operator",
                "type" : "STRING"
                }, {
                "id" : 7,
                "name" : "orders",
                "type" : "INT"
                }, {
                "id" : 8,
                "name" : "battery",
                "type" : "STRING"
                }, {
                "id" : 9,
                "name" : "ac_guns",
                "type" : "INT"
                }, {
                "id" : 10,
                "name" : "pre_gun_charge",
                "type" : "STRING"
                } ],
                "highestFieldId" : 10,
                "partitionKeys" : [ ],
                "primaryKeys" : [ "point_id" ],
                "options" : {
                "bucket" : "2",
                "auto-create" : "true",
                "path" : "oss://strategy-map/paimon/default.db/ods_mysql_paimon_points_5",
                "changelog-producer" : "input",
                "manifest.format" : "avro",
                "file.format" : "parquet",
                "type" : "paimon"
                }
            }
        "#;
        let expected: PaimonSchema = serde_json::from_str(schema_str).unwrap();
        assert_eq!(expected, actual);

        Ok(())
    }
}
