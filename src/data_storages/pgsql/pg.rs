use crate::data_storages::{
    data_storages,
    pgsql::{
        error::ParameterError,
        parser::{parse_col_to_typed_value, parse_row_schema, ColumnSchemaInDB},
    },
};

use futures::TryStreamExt;
use sqlx::{
    error::Error as SqlXError,
    postgres::{PgConnection, PgRow},
    Column, Connection, Row,
};

pub struct PgSqlStorage {
    connection: PgConnection,
}

fn valid_symbol(table_or_col_name: &str) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
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

fn sql_page_condition(
    limit: u32,
    pk: &str,
    cursor: Option<&str>,
) -> Result<String, Box<dyn std::error::Error>> {
    valid_symbol(pk)?;
    Ok(match cursor {
        Some(ucursor) => format!("where {} > {} limit {}", pk, ucursor, limit),
        _ => format!("limit {}", limit),
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
        cursor: Option<&str>,
        limit: u32,
        options: &std::collections::HashMap<&str, &str>,
    ) -> Result<(Vec<data_storages::Row>, data_storages::Schema), Box<dyn std::error::Error>> {
        let parsed_options = parse_chunkread_options(options);
        let sql = format!(
            "select * from ({}) {}",
            parsed_options.query,
            sql_page_condition(limit, parsed_options.pk.as_str(), cursor)?
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