use anyhow::{Ok, Result};
use datafusion::arrow::datatypes::{DataType, Field as AField, Schema, SchemaRef};
use nom::{bytes::complete::take_until, error::ErrorKind, IResult};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap},
    fs,
};

mod paimon;

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize)]
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

pub enum MyDataType {
    BINARY(i32), // BINARY(%d)
    BIGINT,
    BOOLEAN,
    Char, // CHAR(%d)
    DATE,
    DECIMAL(f64, f64), // DECIMAL(%d, %d)
    DOUBLE,
    FLOAT,
    INT,
    TimestampWithLocalTimeZone(i32), // TIMESTAMP(%d) WITH LOCAL TIME ZONE
    MAP,
    SMALLINT,
    TIME(i32),      // TIME(%d)
    TIMESTAMP(i32), // TIMESTAMP(%d)
    TINYINT,
    VARBINARY(i32), // VARBINARY(%d)
    VARCHAR(i32),   // VARCHAR(%d)
    ARRAY(String),  // ARRAY(%s)
}

fn from(value: &str) -> (DataType, bool) {
    let nullable = value.ends_with("NOT NULL");
    let (datatype_str, _num) = match extract_num(value) {
        core::result::Result::Ok((input, num)) => (input, Some(num)),
        core::result::Result::Err(err) => {
            if let nom::Err::Error(v) = err {
                (v.input, None)
            } else {
                panic!("parse filed error")
            }
        }
    };
    let data_type = match datatype_str {
        "STRING" => DataType::Utf8,
        "BYTES" => DataType::FixedSizeBinary(1),

        _ => todo!(),
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
}
