use crate::data_storages::{
    data_storages::{self, ReadResult, SchemaType, SchemaTypeWithValue},
    pgsql::{
        error::ParameterError,
        parser::{parse_col_to_typed_value, parse_row_schema, ColumnSchemaInDB},
    },
};

use futures::TryStreamExt;
use regex::Regex;
use serde_yaml::to_string;
use sqlx::{
    error::Error as SqlXError,
    postgres::{PgConnection, PgRow},
    Column, Connection, Row,
};

pub struct PgSqlStorage {
    connection: PgConnection,
}

fn valid_symbol(table_or_col_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let table_col_re = Regex::new("^[a-zA-Z_][a-zA-Z0-9_]{0,127}$")?;
    if table_col_re.is_match(table_or_col_name) {
        Ok(())
    } else {
        Err(ParameterError::new("invalid table or column name").into())
    }
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
    pk_type: Option<SchemaType>,
    query: String,
    table: Option<String>,
}

fn parse_chunkread_options(
    options: &std::collections::HashMap<&str, &str>,
) -> Result<ChunkReadOptions, Box<dyn std::error::Error>> {
    let query = if let Some(table) = options.get("table") {
        format!("select * from {}", table)
    } else {
        options
            .get("query")
            .ok_or(ParameterError::new(
                "cannot find any `query` or `table` in options",
            ))?
            .to_string()
    };
    Ok(ChunkReadOptions {
        pk: options
            .get("pk")
            .ok_or(ParameterError::new(
                "cannot find required options `pk` on chunk_read",
            ))?
            .to_string(),
        pk_type: options
            .get("pk_type")
            .map(|str_type| option_str_to_type(str_type))
            .transpose()?,
        query: query.to_string(),
        table: options.get("table").map(|s| s.to_string()),
    })
}

fn option_str_to_type(type_str: &str) -> Result<SchemaType, Box<dyn std::error::Error>> {
    match type_str {
        "varchar" => Ok(SchemaType::String),
        unk => Err(ParameterError::new(format!("unknow pk type {unk}").as_str()).into()),
    }
}

fn sql_page_condition(
    limit: u32,
    pk: &str,
    cursor_exist: bool,
) -> Result<String, Box<dyn std::error::Error>> {
    valid_symbol(pk)?;
    Ok(match cursor_exist {
        true => format!("where '{}' > {{}} order by {} asc limit {}", pk, pk, limit),
        false => format!("limit {}", limit),
    })
}

fn pgrow_to_row(row: PgRow) -> Result<data_storages::Row, Box<dyn std::error::Error>> {
    Ok(data_storages::Row(
        row.columns()
            .iter()
            .map(|column| {
                let type_info = column.type_info();
                let type_str = type_info.to_string();
                let column_name = column.name();
                Ok(data_storages::Column {
                    name: column_name.to_string(),
                    value: parse_col_to_typed_value(type_str.as_str(), column_name, &row)?,
                })
            })
            .collect::<Result<Vec<_>, Box<dyn std::error::Error>>>()?,
    ))
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
                results.push(ColumnSchemaInDB::from(row).to_data_schema()?)
            }
            Ok(data_storages::Schema(results))
        } else {
            Err(ParameterError::new("cannot find `table` in options").into())
        }
    }

    async fn chunk_read(
        &mut self,
        cursor: Option<SchemaTypeWithValue>,
        limit: u32,
        options: &std::collections::HashMap<&str, &str>,
    ) -> Result<ReadResult, Box<dyn std::error::Error>> {
        let parsed_options = parse_chunkread_options(options)?;
        let sql = format!(
            "select * from ({}) {}",
            parsed_options.query,
            sql_page_condition(limit, parsed_options.pk.as_str(), cursor.is_some())?
        );
        let mut rows = sqlx::query(sql.as_str()).fetch(&mut self.connection);
        let mut results: Vec<data_storages::Row> = Vec::new();
        let mut schema: Option<data_storages::Schema> = None;
        while let Some(row) = rows.try_next().await? {
            if results.is_empty() {
                schema = Some(parse_row_schema(&row)?);
            }
            results.push(pgrow_to_row(row)?)
        }
        if let Some(schema_value) = schema {
            Ok(ReadResult {
                data: results,
                schema: schema_value,
                cursor: None,
            })
        } else {
            Err(ParameterError::new("cannot get any data from query").into())
        }
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
    ) -> Result<ReadResult, Box<dyn std::error::Error>> {
        Err(ParameterError::new("notimpl").into())
    }
}

#[cfg(test)]
mod tests {
    use super::{valid_symbol, PgSqlStorage};
    use crate::data_storages::data_storages::DataStorage;
    use std::collections::HashMap;

    #[tokio::test]
    async fn testtest() {
        let options = HashMap::from([("pk", "a"), ("table", "test")]);
        let mut sql_storage = PgSqlStorage::new("postgres://test:test@localhost:5432/test")
            .await
            .unwrap();
        let res = sql_storage.chunk_read(None, 100, &options).await.unwrap();
        res.data.into_iter().for_each(|row| println!("{:?}", row));
        let schema_from_table = sql_storage.read_schema(&options).await.unwrap();
        println!("{:?}", schema_from_table);
        panic!("fuck");
    }

    #[test]
    fn test_valid_symbol() {
        assert!(valid_symbol("valid").is_ok());
        assert!(valid_symbol("_valid").is_ok());
        assert!(valid_symbol("_124v_456alid").is_ok());
        assert!(valid_symbol("0_not_valid").is_err());
    }
}
