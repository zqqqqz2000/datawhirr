use std::collections::HashMap;

use super::{
    data_storages::{self, SchemaField},
    none::NoneErr,
};
use futures::TryStreamExt;
use sqlx::{
    error::Error as SqlXError,
    postgres::{PgConnection, PgRow},
    Column, Connection, Row,
};

struct PgSqlStorage {
    connection: PgConnection,
}

impl PgSqlStorage {
    async fn new(uri: &str) -> Result<Self, SqlXError> {
        Ok(PgSqlStorage {
            connection: PgConnection::connect(uri).await?,
        })
    }
}

struct Options {
    pk: String,
    query: String,
}

fn parse_options(options: &std::collections::HashMap<String, String>) -> Options {
    let query = options
        .get("table")
        .or_else(|| options.get("query"))
        .expect("cannot find any `query` or `table` in options")
        .clone();
    Options {
        pk: options
            .get("pk")
            .expect("cannot find required options `pk` on chunk_read")
            .clone(),
        query: query,
    }
}

fn sql_page_condition(limit: u32, pk: &str, cursor: Option<&str>) -> String {
    match cursor {
        // TODO: may not safe here, check pk
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
        "STRING" => data_storages::SchemaTypeWithValue::String(row.get(column_name)),
        unk => panic!("cannot parse type {unk}, may not supported yet."),
    }
}

fn parse_pg_type(type_name: &str) -> data_storages::SchemaType {
    match type_name {
        "STRING" => data_storages::SchemaType::String,
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
                SchemaField {
                    name: column_name.to_string(),
                    type_: parse_pg_type(&type_str),
                    extra: HashMap::new(),
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

impl data_storages::DataStorage for PgSqlStorage {
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<super::data_storages::Schema, Box<dyn std::error::Error>> {
        Err(NoneErr {}.into())
    }

    async fn chunk_read(
        &mut self,
        cursor: Option<&str>,
        limit: u32,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        Box<dyn std::error::Error>,
    > {
        let parsed_options = parse_options(options);
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
        options: &std::collections::HashMap<String, String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Err(NoneErr {}.into())
    }

    async fn read(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        Box<dyn std::error::Error>,
    > {
        Err(NoneErr {}.into())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::data_storages::data_storages::DataStorage;

    use super::PgSqlStorage;
    #[tokio::test]
    async fn testtest() {
        let mut sql_storage = PgSqlStorage::new("postgres://test:test@localhost:5432/test")
            .await
            .unwrap();
        sql_storage
            .chunk_read(None, 100, &HashMap::new())
            .await
            .unwrap();
    }
}
