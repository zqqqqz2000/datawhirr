use std::{borrow::BorrowMut, ops::Deref};

use super::{data_storages::DataStorage, none::NoneErr};
use futures::TryStreamExt;
use sqlx::{error::Error as SqlXError, postgres::PgConnection, Column, Connection, Row};

struct PgSqlStorage {
    connection: PgConnection,
}

impl PgSqlStorage {
    async fn new(uri: String) -> Result<Self, SqlXError> {
        Ok(PgSqlStorage {
            connection: PgConnection::connect(uri.as_str()).await?,
        })
    }
}

impl DataStorage for PgSqlStorage {
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<super::data_storages::Schema, Box<dyn std::error::Error>> {
        Err(NoneErr {}.into())
    }

    async fn chunk_read(
        &mut self,
        cursor: Option<String>,
        limit: u32,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        Box<dyn std::error::Error>,
    > {
        let mut rows = sqlx::query("select * from xxx").fetch(&mut self.connection);
        while let Some(row) = rows.try_next().await? {
            for ele in row.columns() {
                let type_info = ele.type_info();
                println!("{}", type_info.to_string());
                println!("{}, {:?}", ele.name(), type_info.kind());
            }
        }
        Err(NoneErr {}.into())
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
        let mut sql_storage =
            PgSqlStorage::new("postgres://test:test@localhost:5432/test".to_string())
                .await
                .unwrap();
        sql_storage
            .chunk_read(None, 100, &HashMap::new())
            .await
            .unwrap();
    }
}
