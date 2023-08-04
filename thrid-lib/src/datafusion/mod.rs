use anyhow::{Ok, Result};
use chrono::Local;
use datafusion::arrow::datatypes::{DataType, Field as AField, Schema, SchemaRef, TimeUnit};
use nom::{bytes::complete::take_until, error::ErrorKind, IResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
};

use crate::datafusion::utils::read_to_string;

use self::reader::{manifest_list, FileFormat};

mod paimon;
mod reader;
mod utils;

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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum CommitKind {
    APPEND,
    OVERWRITE,
    COMPACT,
}

#[allow(dead_code)]
const SNAPSHOT_PREFIX: &str = "snapshot-";
#[allow(dead_code)]
const EARLIEST: &str = "EARLIEST";
const LATEST: &str = "LATEST";

pub(crate) struct SnapshotManager {
    table_path: String,
}

#[allow(dead_code)]
impl SnapshotManager {
    fn new(table_path: &str) -> SnapshotManager {
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

    fn snapshot(&self, snapshot_id: i64) -> Result<Snapshot> {
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

    fn latest_snapshot(&self) -> Option<Snapshot> {
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

impl Snapshot {
    fn get_schema(&self, table_path: &str) -> Result<PaimonSchema> {
        let latest_schema_path = format!("{}/schema/schema-{}", table_path, self.schema_id);
        let schema_str = read_to_string(latest_schema_path.as_str())?;
        let schema: PaimonSchema = serde_json::from_str(schema_str.as_str())?;
        Ok(schema)
    }
}

#[allow(dead_code)]
fn get_manifest_list(
    table_path: &str,
    file_name: &str,
    format: &FileFormat,
) -> Result<Vec<ManifestFileMeta>> {
    let path = format!("{}/manifest/{}", table_path, file_name);
    manifest_list(path.as_str(), format)
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct PaimonSchema {
    id: u64,
    fields: Vec<Field>,
    #[serde(rename = "highestFieldId")]
    highest_field_id: u32,
    #[serde(rename = "partitionKeys")]
    partition_keys: Vec<String>,
    #[serde(rename = "primaryKeys")]
    primary_keys: Vec<String>,
    options: HashMap<String, String>,
}

impl PaimonSchema {
    #[allow(dead_code)]
    fn get_manifest_format(&self) -> FileFormat {
        match self.options.get("manifest.format") {
            Some(format) => FileFormat::from(format),
            None => FileFormat::Avro,
        }
    }

    #[allow(dead_code)]
    fn get_file_format(&self) -> FileFormat {
        match self.options.get("file.format") {
            Some(format) => FileFormat::from(format),
            None => FileFormat::Orc,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub struct Field {
    id: u64,
    name: String,
    #[serde(rename = "type")]
    field_type: String,
}

impl Field {
    fn to_arrow_field(&self) -> AField {
        let (datatype, nullable) = from(self.field_type.as_str());
        AField::new(self.name.clone(), datatype, nullable)
    }
}

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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub(crate) struct PartitionStat {
    #[serde(rename = "_MIN_VALUES", with = "serde_bytes")]
    min_values: Vec<u8>,
    #[serde(rename = "_MAX_VALUES", with = "serde_bytes")]
    max_values: Vec<u8>,
    #[serde(rename = "_NULL_COUNTS")]
    null_counts: Option<Vec<i64>>,
}

#[allow(dead_code)]
pub(crate) fn read_schema(path: &str) -> Result<BTreeMap<String, PaimonSchema>> {
    let mut schema_tree = BTreeMap::new();

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();
        let file_name = entry.file_name().into_string().unwrap();
        let content = fs::read_to_string(file_path)?;
        let content = content.as_str();
        let schema: PaimonSchema = serde_json::from_str(content)?;
        schema_tree.insert(file_name, schema);
    }
    Ok(schema_tree)
}

pub(crate) fn to_schema_ref(schema: &mut PaimonSchema) -> SchemaRef {
    schema.fields.sort_by(|a, b| a.id.cmp(&b.id));
    let fields = schema
        .fields
        .iter()
        .map(|field| field.to_arrow_field())
        .collect::<Vec<AField>>();

    SchemaRef::new(Schema::new(fields))
}

fn from(value: &str) -> (DataType, bool) {
    let nullable = value.ends_with("NOT NULL");
    let (datatype_str, tuple2) = match extract_num(value) {
        core::result::Result::Ok((input, num)) => (input, Some(num)),
        core::result::Result::Err(err) => {
            if let nom::Err::Error(v) = err {
                (v.input, None)
            } else {
                panic!("parse filed error")
            }
        }
    };
    let tuple2 = tuple2.map_or((i32::MAX, None), |v| v);
    let data_type = match datatype_str {
        "STRING" | "VARCHAR" | "CHAR" | "TEXT" => DataType::Utf8,
        "TINYINT" => DataType::UInt8,
        "SMALLINT" => DataType::UInt16,
        "INT" | "INTEGER" => DataType::UInt32,
        "BIGINT" => DataType::UInt64,
        "FLOAT" | "REAL" => DataType::Float32,
        "DECIMAL" => DataType::Decimal256(tuple2.0 as u8, tuple2.1.map_or(i8::MAX, |v| v as i8)),
        "BOOLEAN" => DataType::Boolean,
        "DOUBLE" => DataType::Float64,
        "VARBINARY" | "BYTES" | "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIME" => DataType::Time64(get_time_unit(tuple2.0)),
        "TIMESTAMP" => {
            let unit = get_time_unit(tuple2.0);
            if value.contains("WITH LOCAL TIME ZONE") {
                DataType::Timestamp(unit, Some(time_zone().into()))
            } else {
                DataType::Timestamp(unit, None)
            }
        }
        data => panic!("Not support datatype: {}", data),
    };
    (data_type, nullable)
}

fn get_time_unit(v: i32) -> TimeUnit {
    match v {
        0 => TimeUnit::Second,
        1..=3 => TimeUnit::Millisecond,
        4..=6 => TimeUnit::Microsecond,
        7..=9 => TimeUnit::Nanosecond,
        _ => panic!(""),
    }
}

fn time_zone() -> String {
    Local::now().format("%:z").to_string()
}

/// input-1: STRING(10) NOT NULL -> (STRING, (10, None))
///
/// input-2: STRING(10) -> (STRING, (10, None))
///
/// input-3: STRING -> Err(STRING)
///
/// input-4: DECIMAL(1, 38) -> (DECIMAL, (1, Some(38)))
fn extract_num(input: &str) -> IResult<&str, (i32, Option<i32>)> {
    let input = match input.find(" NOT NULL") {
        Some(idx) => input.split_at(idx).0,
        _ => input,
    };

    if input.contains('(') {
        let split_index = input.find('(').expect("");
        let (datatype_str, fix_num) = input.split_at(split_index + 1);
        let (_, fix_num) = take_until(")")(fix_num)?;
        let sp = fix_num
            .split(',')
            .map(|s| {
                let s = s.trim().to_string();
                s.parse::<i32>()
                    .unwrap_or_else(|_| panic!("transform number error: {}", fix_num))
            })
            .collect::<Vec<i32>>();
        let tuple = match sp[..] {
            [a, b] => (a, Some(b)),
            [a] => (a, None),
            _ => panic!("paimon datatype has multiple qualifications"),
        };

        let datatype_str = datatype_str.as_bytes();
        let datatype_str = &datatype_str[0..datatype_str.len() - 1];
        core::result::Result::Ok((std::str::from_utf8(datatype_str).unwrap(), tuple))
    } else {
        Err(nom::Err::Error(nom::error::Error::new(
            input,
            ErrorKind::Fail,
        )))
    }
}

pub(crate) fn get_latest_metedata_file(table_path: &str) -> Result<Snapshot> {
    let latest_path = format!("{}/snapshot/LATEST", table_path);
    let latest_num = read_to_string(latest_path.as_str())?;

    let latest_path = format!("{}/snapshot/snapshot-{}", table_path, latest_num);

    let content = read_to_string(latest_path.as_str())?;
    let snapshot = serde_json::from_str(content.as_str())?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extract_num_test() {
        let input = "STRING(10) NOT NULL";
        assert_eq!(
            extract_num(input),
            core::result::Result::Ok(("STRING", (10, None)))
        );

        let input = "STRING(20)";
        assert_eq!(
            extract_num(input),
            core::result::Result::Ok(("STRING", (20, None)))
        );

        let input = "STRING";
        assert_eq!(
            extract_num(input),
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                ErrorKind::Fail,
            )))
        );
        let input = "DECIMAL(1, 38)";
        assert_eq!(
            extract_num(input),
            core::result::Result::Ok(("DECIMAL", (1, Some(38))))
        );
    }

    #[test]
    fn read_snapshot() -> Result<()> {
        let table_path = "src/test/paimon/default.db/ods_mysql_paimon_points_4";
        let json = r#"
            {
                "version" : 3,
                "id" : 11,
                "schemaId" : 0,
                "baseManifestList" : "manifest-list-21e785dc-5ca9-43ef-bd79-04b2fd410370-25",
                "deltaManifestList" : "manifest-list-21e785dc-5ca9-43ef-bd79-04b2fd410370-26",
                "changelogManifestList" : null,
                "commitUser" : "5adce4a8-a6ac-4659-8660-cb26a4d32157",
                "commitIdentifier" : 13,
                "commitKind" : "APPEND",
                "timeMillis" : 1690966977203,
                "logOffsets" : { },
                "totalRecordCount" : 18,
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
                "path" : "oss://strategy-map/paimon/default.db/ods_mysql_paimon_points_4",
                "changelog-producer" : "input",
                "manifest.format" : "orc",
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
