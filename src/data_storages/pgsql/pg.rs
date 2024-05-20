use super::super::data_storages;
use std::{
    collections::HashMap,
    fmt::{self, Display, Formatter},
};

use futures::TryStreamExt;
use sqlx::{
    error::Error as SqlXError,
    postgres::{PgConnection, PgRow},
    Column, Connection, Row,
};

#[derive(Debug)]
pub struct ParameterError {
    reason: String,
}
impl std::error::Error for ParameterError {}

impl Display for ParameterError {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "parameter error: {}", self.reason)
    }
}
impl ParameterError {
    fn new(reason: &str) -> ParameterError {
        ParameterError {
            reason: reason.to_string(),
        }
    }
}

pub struct PgSqlStorage {
    connection: PgConnection,
}

fn valid_symbol(table_or_col_name: &str) {
    // TODO: valid symbol, panic if invalid
}

impl PgSqlStorage {
    pub async fn new(uri: &str) -> Result<Self, SqlXError> {
        Ok(PgSqlStorage {
            connection: PgConnection::connect(uri).await?,
        })
    }
}

struct ChunkReadOptions {
    pk: String,
    query: String,
}

fn parse_chunkread_options(options: &std::collections::HashMap<&str, &str>) -> ChunkReadOptions {
    let query = if let Some(table) = options.get("table") {
        format!("select * from {}", table)
    } else {
        options
            .get("query")
            .expect("cannot find any `query` or `table` in options")
            .to_string()
    };
    ChunkReadOptions {
        pk: options
            .get("pk")
            .expect("cannot find required options `pk` on chunk_read")
            .to_string(),
        query: query.to_string(),
    }
}

fn sql_page_condition(limit: u32, pk: &str, cursor: Option<&str>) -> String {
    valid_symbol(pk);
    match cursor {
        Some(ucursor) => format!("where {} > {} limit {}", pk, ucursor, limit),
        _ => format!("limit {}", limit),
    }
}

fn parse_col_to_typed_value(
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

fn parse_row_schema(row: &PgRow) -> data_storages::Schema {
    data_storages::Schema(
        row.columns()
            .into_iter()
            .map(|column| {
                let type_info = column.type_info();
                let type_str = type_info.to_string();
                let column_name = column.name();
                let (type_, extra) = parse_pg_type(&type_str);
                data_storages::SchemaField {
                    name: column_name.to_string(),
                    type_: type_,
                    extra: extra,
                }
            })
            .collect::<Vec<_>>(),
    )
}

fn pgrow_to_row(row: PgRow) -> data_storages::Row {
    data_storages::Row(
        row.columns()
            .into_iter()
            .map(|column| {
                let type_info = column.type_info();
                let type_str = type_info.to_string();
                let column_name = column.name();
                data_storages::Column {
                    name: column_name.to_string(),
                    value: parse_col_to_typed_value(type_str.as_str(), column_name, &row),
                }
            })
            .collect::<Vec<_>>(),
    )
}

struct ColumnSchemaInDB {
    column_name: String,
    udt_name: String,
    is_nullable: Option<String>,
    character_maximum_length: Option<i32>,
}

fn bool_str(b: bool) -> String {
    if b { "true" } else { "false" }.to_string()
}

impl ColumnSchemaInDB {
    fn to_data_schema(&self) -> data_storages::SchemaField {
        let mut extra: HashMap<String, String> =
            HashMap::from([("pg_type".to_string(), self.udt_name.clone())]);
        if let Some(nullable) = &self.is_nullable {
            extra.insert("nullable".to_string(), bool_str(nullable == "YES"));
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

impl data_storages::DataStorage for PgSqlStorage {
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<&str, &str>,
    ) -> Result<data_storages::Schema, Box<dyn std::error::Error>> {
        if let Some(table) = options.get("table") {
            let sql = "
            SELECT *  
            FROM information_schema.columns 
            WHERE table_name = $1";
            let mut rows = sqlx::query(sql).bind(table).fetch(&mut self.connection);
            let mut results: Vec<data_storages::SchemaField> = Vec::new();
            while let Some(row) = rows.try_next().await? {
                results.push(
                    ColumnSchemaInDB {
                        column_name: row.get("column_name"),
                        udt_name: row.get("udt_name"),
                        is_nullable: row.get("is_nullable"),
                        character_maximum_length: row.get("character_maximum_length"),
                    }
                    .to_data_schema(),
                )
            }
            Ok(data_storages::Schema(results))
        } else {
            Err(ParameterError::new("cannot find `table` in options").into())
        }
    }

    async fn chunk_read(
        &mut self,
        cursor: Option<&str>,
        limit: u32,
        options: &std::collections::HashMap<&str, &str>,
    ) -> Result<(Vec<data_storages::Row>, data_storages::Schema), Box<dyn std::error::Error>> {
        let parsed_options = parse_chunkread_options(options);
        let sql = format!(
            "select * from ({}) {}",
            parsed_options.query,
            sql_page_condition(limit, parsed_options.pk.as_str(), cursor)
        );
        let mut rows = sqlx::query(sql.as_str()).fetch(&mut self.connection);
        let mut results: Vec<data_storages::Row> = Vec::new();
        let mut schema: Option<data_storages::Schema> = None;
        while let Some(row) = rows.try_next().await? {
            if results.is_empty() {
                schema = Some(parse_row_schema(&row));
            }
            results.push(pgrow_to_row(row))
        }
        Ok((results, schema.expect("cannot get any data from query")))
    }

    async fn write(
        &mut self,
        options: &std::collections::HashMap<&str, &str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err(ParameterError::new("notimpl").into())
    }

    async fn read(
        &mut self,
        options: &std::collections::HashMap<&str, &str>,
    ) -> Result<(Vec<data_storages::Row>, data_storages::Schema), Box<dyn std::error::Error>> {
        Err(ParameterError::new("notimpl").into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::data_storages::data_storages::DataStorage;

    use super::PgSqlStorage;
    #[tokio::test]
    async fn testtest() {
        let options = HashMap::from([("pk", "a"), ("table", "test")]);
        let mut sql_storage = PgSqlStorage::new("postgres://test:test@localhost:5432/test")
            .await
            .unwrap();
        let (rows, schema) = sql_storage.chunk_read(None, 100, &options).await.unwrap();
        rows.into_iter().for_each(|row| println!("{:?}", row));
        let schema_from_table = sql_storage.read_schema(&options).await.unwrap();
        println!("{:?}", schema_from_table);
        panic!("fuck");
    }
}
