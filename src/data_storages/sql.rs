use futures::{stream, StreamExt};
use sqlx::any::AnyRow;
use sqlx::error::Error;
use sqlx::{AnyConnection, Column, Connection, Executor, Row, TypeInfo};

use super::data_storages::{DataReader, DataStorage};

#[derive(Debug)]
struct SqlStorage {
    connection: sqlx::AnyConnection,
}

impl SqlStorage {
    async fn new(uri: String) -> Result<Self, Error> {
        match AnyConnection::connect(uri.as_str()).await {
            Ok(conn) => Ok(Self { connection: conn }),
            Err(err) => Err(err),
        }
    }
}

struct SqlReader<'c> {
    connection: &'c mut AnyConnection,
}

impl<'c> DataReader for SqlReader<'c> {
    async fn read_all(
        &self,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        impl std::error::Error,
    > {
        let a = sqlx::query("select * from xxx")
            .map(|row: AnyRow| {
                for ele in row.columns() {
                    ele.name();
                    let i = ele.type_info();
                    i.name();
                }
                row.get("asdfdf")
            })
            .fetch(self.connection);
        let b = a.collect::<Vec<_>>().await;
        b
    }
    async fn chunk_read(
        &self,
    ) -> Result<impl super::data_storages::ChunkReader, impl std::error::Error> {
    }
}

impl DataStorage for SqlStorage {
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<super::data_storages::Schema, Error> {
        // TODO: full schema
    }

    async fn read(
        &mut self,
        _: &std::collections::HashMap<String, String>,
    ) -> Result<SqlReader, std::io::Error> {
        Ok(SqlReader {
            connection: &mut self.connection,
        })
    }
}
