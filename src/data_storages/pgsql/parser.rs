use std::collections::HashMap;

use crate::data_storages::{
    data_storages,
    pgsql::{error::ParseError, utils},
};
use sqlx::postgres::PgRow;
use sqlx::{Column, Row};

pub fn parse_col_to_typed_value(
    type_name: &str,
    column_name: &str,
    row: &PgRow,
) -> Result<data_storages::SchemaTypeWithValue, Box<dyn std::error::Error>> {
    match type_name {
        "VARCHAR" => Ok(data_storages::SchemaTypeWithValue::String(
            row.get(column_name),
        )),
        "INT4" => Ok(data_storages::SchemaTypeWithValue::Int32(
            row.get(column_name),
        )),
        unk => Err(
            ParseError::new("cannot parse type {unk} from pg row, may not supported yet.").into(),
        ),
    }
}

fn parse_pg_type(
    type_name: &str,
) -> Result<(data_storages::SchemaType, HashMap<String, String>), Box<dyn std::error::Error>> {
    match type_name {
        "VARCHAR" => Ok((data_storages::SchemaType::String, HashMap::new())),
        "INT4" => Ok((
            data_storages::SchemaType::Int32,
            HashMap::from([("length".to_string(), "4".to_string())]),
        )),
        unk => {
            Err(ParseError::new("unknown type {unk} from pg row, may not supported yet.").into())
        }
    }
}

pub fn parse_row_schema(row: &PgRow) -> Result<data_storages::Schema, Box<dyn std::error::Error>> {
    Ok(data_storages::Schema(
        row.columns()
            .iter()
            .map(|column| {
                let type_info = column.type_info();
                let type_str = type_info.to_string();
                let column_name = column.name();
                let (type_, extra) = parse_pg_type(&type_str)?;
                Ok(data_storages::SchemaField {
                    name: column_name.to_string(),
                    type_,
                    extra,
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?,
    ))
}

pub struct ColumnSchemaInDB {
    column_name: String,
    udt_name: String,
    is_nullable: Option<String>,
    character_maximum_length: Option<i32>,
}

impl From<PgRow> for ColumnSchemaInDB {
    fn from(value: PgRow) -> Self {
        ColumnSchemaInDB {
            column_name: value.get("column_name"),
            udt_name: value.get("udt_name"),
            is_nullable: value.get("is_nullable"),
            character_maximum_length: value.get("character_maximum_length"),
        }
    }
}

impl ColumnSchemaInDB {
    pub fn to_data_schema(&self) -> Result<data_storages::SchemaField, Box<dyn std::error::Error>> {
        let mut extra: HashMap<String, String> =
            HashMap::from([("pg_type".to_string(), self.udt_name.clone())]);
        if let Some(nullable) = &self.is_nullable {
            extra.insert("nullable".to_string(), utils::bool_str(nullable == "YES"));
        }
        if let Some(length) = self.character_maximum_length {
            extra.insert("length".to_string(), length.to_string());
        }
        match self.udt_name.as_str() {
            "varchar" => Ok(data_storages::SchemaField {
                name: self.column_name.clone(),
                type_: data_storages::SchemaType::String,
                extra,
            }),
            "int4" => Ok(data_storages::SchemaField {
                name: self.column_name.clone(),
                type_: data_storages::SchemaType::Int32,
                extra,
            }),
            unk => Err(ParseError::new("cannot parse type {unk}, may not supported yet.").into()),
        }
    }
}
