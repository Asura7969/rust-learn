use anyhow::{Ok, Result};
use datafusion::arrow::datatypes::{DataType, Field as AField, Schema, SchemaRef, TimeUnit};
use nom::{bytes::complete::take_until, error::ErrorKind, IResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
};

use crate::datafusion::utils::read_to_string;

mod paimon;
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

enum FileType {
    #[allow(dead_code)]
    Parquet,
    #[allow(dead_code)]
    Avro,
    #[allow(dead_code)]
    Orc,
}

impl From<&String> for FileType {
    fn from(value: &String) -> Self {
        match value.as_str() {
            "parquet" => FileType::Parquet,
            "orc" => FileType::Orc,
            _ => FileType::Avro,
        }
    }
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
    fn get_manifest_format(&self) -> FileType {
        match self.options.get("manifest.format") {
            Some(format) => FileType::from(format),
            None => FileType::Avro,
        }
    }

    #[allow(dead_code)]
    fn get_file_format(&self) -> FileType {
        match self.options.get("file.format") {
            Some(format) => FileType::from(format),
            None => FileType::Orc,
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

#[derive(Serialize, PartialEq, Eq, Debug)]
struct ManifestFileMeta<'de> {
    file_name: String,
    file_size: i64,
    num_added_files: i64,
    num_deleted_files: i64,
    partition_stats: Vec<PartitionStat<'de>>,
    schema_id: i64,
}

impl<'a, 'de> Deserialize<'de> for ManifestFileMeta<'a> {
    fn deserialize<D>(_deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        todo!()
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
struct PartitionStat<'a> {
    min_values: &'a [u8],
    max_values: &'a [u8],
    null_counts: Option<Vec<i64>>,
}

// https://www.rectcircle.cn/posts/rust-serde/
// use serde::ser::{Serialize as SSerialize, SerializeStruct, Serializer};

// impl<'a> Serialize for PartitionStat<'a> {
//     fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         let mut state = serializer.serialize_struct("PartitionStat", 3)?;
//         state.serialize_field("min_values", {
//             // 对 bytes 添加了一层包装，用来代理调用 `serde_bytes::serialize` 方法
//             // 这样就能 使用高效的 字节数组序列化
//             struct SerializeWith<'__a, 'a: '__a> {
//                 values: (&'__a &'a [u8],),
//                 phantom: serde::export::PhantomData<PartitionStat<'a>>,
//             }
//             impl<'__a, 'a: '__a> serde::Serialize for SerializeWith<'__a, 'a> {
//                 fn serialize<__S>(&self, __s: __S) -> serde::export::Result<__S::Ok, __S::Error>
//                 where
//                     __S: serde::Serializer,
//                 {
//                     serde_bytes::serialize(self.values.0, __s)
//                 }
//             }
//             &SerializeWith {
//                 values: (&self.min_values,),
//                 phantom: serde::export::PhantomData::<PartitionStat<'a>>,
//             }
//         })?;

//         todo!()
//     }
// }

// impl<'de> Deserialize<'de> for PartitionStat<'de> {
//     fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         todo!()
//     }
// }

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

// pub enum MyDataType {
//     BINARY(i32), // BINARY(%d)
//     BIGINT,
//     BOOLEAN,
//     Char, // CHAR(%d)
//     DATE,
//     DECIMAL(f64, f64), // DECIMAL(%d, %d)
//     DOUBLE,
//     FLOAT,
//     INT,
//     TimestampWithLocalTimeZone(i32), // TIMESTAMP(%d) WITH LOCAL TIME ZONE
//     MAP,
//     SMALLINT,
//     TIME(i32),      // TIME(%d)
//     TIMESTAMP(i32), // TIMESTAMP(%d)
//     TINYINT,
//     VARBINARY(i32), // VARBINARY(%d)
//     VARCHAR(i32),   // VARCHAR(%d)
//     ARRAY(String),  // ARRAY(%s)
// }

fn from(value: &str) -> (DataType, bool) {
    let nullable = value.ends_with("NOT NULL");
    let (datatype_str, num) = match extract_num(value) {
        core::result::Result::Ok((input, num)) => (input, Some(num)),
        core::result::Result::Err(err) => {
            if let nom::Err::Error(v) = err {
                (v.input, None)
            } else {
                panic!("parse filed error")
            }
        }
    };
    let num = num.map_or(i32::MAX, |v| v);
    // TODO: 完善更多类型数据
    let data_type = match datatype_str {
        "STRING" => DataType::Utf8,
        "BYTES" => DataType::FixedSizeBinary(num),
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "INT" => DataType::Int32,
        "TIME" => DataType::Time64(TimeUnit::Millisecond),
        data => panic!("Not support datatype: {}", data),
    };
    (data_type, nullable)
}

/// input-1: STRING(10) NOT NULL -> (STRING, 10)
///
/// input-2: STRING(10) -> (STRING, 10)
///
/// input-3: STRING -> Err(STRING)
fn extract_num(input: &str) -> IResult<&str, i32> {
    let input = match input.find(" NOT NULL") {
        Some(idx) => input.split_at(idx).0,
        _ => input,
    };

    if input.contains('(') {
        let split_index = input.find('(').expect("");
        let (datatype_str, fix_num) = input.split_at(split_index + 1);
        let (_, fix_num) = take_until(")")(fix_num)?;
        let fix_num = fix_num.to_string();
        let fix_num = fix_num
            .parse::<i32>()
            .unwrap_or_else(|_| panic!("transform number error: {}", fix_num));
        let datatype_str = datatype_str.as_bytes();
        let datatype_str = &datatype_str[0..datatype_str.len() - 1];
        core::result::Result::Ok((std::str::from_utf8(datatype_str).unwrap(), fix_num))
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
        assert_eq!(extract_num(input), core::result::Result::Ok(("STRING", 10)));

        let input = "STRING(20)";
        assert_eq!(extract_num(input), core::result::Result::Ok(("STRING", 20)));

        let input = "STRING";
        assert_eq!(
            extract_num(input),
            Err(nom::Err::Error(nom::error::Error::new(
                input,
                ErrorKind::Fail,
            )))
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
