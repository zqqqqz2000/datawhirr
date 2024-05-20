use std::collections::HashMap;

use crate::data_storages::{data_storages, pgsql::utils};
use sqlx::postgres::PgRow;
use sqlx::{Column, Row};

pub fn parse_col_to_typed_value(
    type_name: &str,
    column_name: &str,
    row: &PgRow,
) -> data_storages::SchemaTypeWithValue {
    match type_name {
        "VARCHAR" => data_storages::SchemaTypeWithValue::String(row.get(column_name)),
        "INT4" => data_storages::SchemaTypeWithValue::Int32(row.get(column_name)),
        unk => panic!("cannot parse type {unk}, may not supported yet."),
    }
}

fn parse_pg_type(type_name: &str) -> (data_storages::SchemaType, HashMap<String, String>) {
    match type_name {
        "VARCHAR" => (data_storages::SchemaType::String, HashMap::new()),
        "INT4" => (
            data_storages::SchemaType::Int32,
            HashMap::from([("length".to_string(), "4".to_string())]),
        ),
        unk => panic!("unknown type {unk} from postgres, may not supported yet."),
    }
}

pub fn parse_row_schema(row: &PgRow) -> data_storages::Schema {
    data_storages::Schema(
        row.columns()
            .iter()
            .map(|column| {
                let type_info = column.type_info();
                let type_str = type_info.to_string();
                let column_name = column.name();
                let (type_, extra) = parse_pg_type(&type_str);
                data_storages::SchemaField {
                    name: column_name.to_string(),
                    type_,
                    extra,
                }
            })
            .collect::<Vec<_>>(),
    )
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
    pub fn to_data_schema(&self) -> data_storages::SchemaField {
        let mut extra: HashMap<String, String> =
            HashMap::from([("pg_type".to_string(), self.udt_name.clone())]);
        if let Some(nullable) = &self.is_nullable {
            extra.insert("nullable".to_string(), utils::bool_str(nullable == "YES"));
        }
        if let Some(length) = self.character_maximum_length {
            extra.insert("length".to_string(), length.to_string());
        }
        match self.udt_name.as_str() {
            "varchar" => data_storages::SchemaField {
                name: self.column_name.clone(),
                type_: data_storages::SchemaType::String,
                extra,
            },
            "int4" => data_storages::SchemaField {
                name: self.column_name.clone(),
                type_: data_storages::SchemaType::Int32,
                extra,
            },
            unk => panic!("cannot parse type {unk}, may not supported yet."),
        }
    }
}
