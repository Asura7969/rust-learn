use arrow_schema::DataType;
use datafusion::arrow::datatypes::{Field as AField, Schema, SchemaRef};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
    path::PathBuf,
};

use self::{
    error::PaimonError,
    manifest_list::ManifestFileMeta,
    reader::{manifest_list, FileFormat},
    utils::from,
};

pub mod error;
mod example;
mod exec;
mod manifest;
mod manifest_list;
mod reader;
pub mod snapshot;
pub mod table;
mod utils;

#[allow(dead_code)]
pub struct PrimaryKeys(pub Vec<String>);
#[allow(dead_code)]
pub struct PartitionKeys(pub Vec<String>);

#[allow(dead_code)]
pub enum WriteMode {
    Appendonly,
    Changelog,
}

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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct PaimonSchema {
    pub id: u64,
    pub fields: Vec<Field>,
    #[serde(rename = "highestFieldId")]
    pub highest_field_id: u32,
    #[serde(rename = "partitionKeys")]
    pub partition_keys: Vec<String>,
    #[serde(rename = "primaryKeys")]
    pub primary_keys: Vec<String>,
    pub options: HashMap<String, String>,
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

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
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

#[allow(dead_code)]
pub(crate) fn test_paimonm_table_path(table_name: &str) -> PathBuf {
    let mut config_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    config_path.push("src");
    config_path.push("test");
    config_path.push("paimon");
    config_path.push("default.db");
    config_path.push(table_name);
    config_path
}
