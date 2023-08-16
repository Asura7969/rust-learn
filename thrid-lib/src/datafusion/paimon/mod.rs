use arrow_schema::DataType;
use datafusion::arrow::datatypes::{Field as AField, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
};

use crate::datafusion::paimon::utils::read_to_string;

use self::{
    error::PaimonError,
    manifest_list::ManifestFileMeta,
    reader::{manifest_list, FileFormat},
    snapshot::Snapshot,
    utils::from,
};

mod error;
mod example;
mod exec;
mod manifest;
mod manifest_list;
mod reader;
mod snapshot;
mod table;
mod utils;

#[allow(dead_code)]
pub struct PrimaryKeys(Vec<String>);
#[allow(dead_code)]
pub struct PartitionKeys(Vec<String>);

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum CommitKind {
    #[serde(rename = "APPEND")]
    Append,
    #[serde(rename = "OVERWRITE")]
    Overwrite,
    #[serde(rename = "COMPACT")]
    Compact,
}

#[allow(dead_code)]
fn get_manifest_list(
    table_path: &str,
    file_name: &str,
    format: &FileFormat,
) -> Result<Vec<ManifestFileMeta>, PaimonError> {
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
    pub fn get_manifest_format(&self) -> FileFormat {
        match self.options.get("manifest.format") {
            Some(format) => FileFormat::from(format),
            None => FileFormat::Avro,
        }
    }

    #[allow(dead_code)]
    pub fn get_file_format(&self) -> FileFormat {
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
pub struct PartitionStat {
    #[serde(rename = "_MIN_VALUES", with = "serde_bytes")]
    min_values: Vec<u8>,
    #[serde(rename = "_MAX_VALUES", with = "serde_bytes")]
    max_values: Vec<u8>,
    #[serde(rename = "_NULL_COUNTS")]
    null_counts: Option<Vec<i64>>,
}

#[allow(dead_code)]
pub fn read_schema(path: &str) -> Result<BTreeMap<String, PaimonSchema>, PaimonError> {
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
    let mut fields = schema
        .fields
        .iter()
        .map(|field| field.to_arrow_field())
        .collect::<Vec<AField>>();
    let mut system_fields = vec![
        AField::new("_KEY_point_id", DataType::Utf8, false),
        AField::new("_SEQUENCE_NUMBER", DataType::UInt64, false),
        AField::new("_VALUE_KIND", DataType::Int8, false),
    ];
    system_fields.append(&mut fields);
    SchemaRef::new(Schema::new(system_fields))
}

pub(crate) fn get_latest_metedata_file(table_path: &str) -> Result<Snapshot, PaimonError> {
    let latest_path = format!("{}/snapshot/LATEST", table_path);
    let latest_num = read_to_string(latest_path.as_str())?;

    let latest_path = format!("{}/snapshot/snapshot-{}", table_path, latest_num);

    let content = read_to_string(latest_path.as_str())?;
    let snapshot = serde_json::from_str(content.as_str())?;
    Ok(snapshot)
}
