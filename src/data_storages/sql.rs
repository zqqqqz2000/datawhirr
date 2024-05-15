use core::panic;
use std::borrow::BorrowMut;
use std::io;

use futures::StreamExt;
use sqlx::any::{self, AnyRow};
use sqlx::error::Error;
use sqlx::{AnyConnection, Column, Connection, Executor, Row, TypeInfo};

use super::data_storages::{ChunkReader, ChunkWriter, DataReader, DataStorage, DataWriter};

#[derive(Debug)]
struct SqlStorage {
    connection: sqlx::AnyConnection,
}

impl SqlStorage {
    async fn new(uri: String) -> Result<Self, Error> {
        sqlx::any::install_default_drivers();
        match AnyConnection::connect(uri.as_str()).await {
            Ok(conn) => Ok(Self { connection: conn }),
            Err(err) => Err(err),
        }
    }
}

pub struct SqlReader<'c> {
    connection: &'c mut AnyConnection,
}

pub struct SqlChunkReader<'c> {
    connection: &'c mut AnyConnection,
}

impl<'c> ChunkReader for SqlChunkReader<'c> {
    async fn next(
        &mut self,
        chunk_size: u32,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        impl std::error::Error,
    > {
        Err(io::Error::new(io::ErrorKind::Other, "fuck"))
    }

    async fn close(&mut self) {}
}

impl<'c> DataReader for SqlReader<'c> {
    async fn read_all(
        &mut self,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        impl std::error::Error,
    > {
        println!("asdfdfdsfaf");
        let a = sqlx::query("select * from xxx")
            .map(|row: AnyRow| {
                for ele in row.columns() {
                    println!("ffff fuck");
                    ele.name();
                    let i = ele.type_info();
                    println!("{} test test test", i.name());
                }
                1
            })
            .fetch(self.connection.borrow_mut());
        let b = a.collect::<Vec<_>>().await;
        println!("{:?}", b);
        Err(io::Error::new(io::ErrorKind::Other, "fuck"))
    }

    async fn chunk_read(&mut self) -> Result<SqlChunkReader, impl std::error::Error> {
        let a = sqlx::query("select * from xxx")
            .map(|row: AnyRow| {
                for ele in row.columns() {
                    ele.name();
                    let i = ele.type_info();
                    println!("{}", i.name());
                }
                1
            })
            .fetch(self.connection.borrow_mut());
        let b = a.collect::<Vec<_>>().await;
        Err(io::Error::new(io::ErrorKind::Other, "fuck"))
    }
}

struct SqlWriter<'c> {
    connection: &'c mut AnyConnection,
}

struct SqlChunkWriter<'c> {
    connection: &'c mut AnyConnection,
}

impl<'c> ChunkWriter for SqlChunkWriter<'c> {
    async fn write(
        &mut self,
        rows: Vec<super::data_storages::Row>,
    ) -> Result<(), impl std::error::Error> {
        Err(io::Error::new(io::ErrorKind::Other, "fuck"))
    }

    async fn close(&mut self) {}
}

impl<'c> DataWriter for SqlWriter<'c> {
    async fn chunk_write(&mut self) -> Result<SqlChunkWriter, impl std::error::Error> {
        Err(io::Error::new(io::ErrorKind::Other, "fuck"))
    }

    async fn write_all(&mut self, rows: Vec<super::data_storages::Row>) -> Result<(), io::Error> {
        Ok(())
    }
}

impl DataStorage for SqlStorage {
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<super::data_storages::Schema, Error> {
        Err(Error::Io(io::Error::new(io::ErrorKind::Other, "fuck")))
    }

    async fn read(
        &mut self,
        _: &std::collections::HashMap<String, String>,
    ) -> Result<SqlReader, std::io::Error> {
        Ok(SqlReader {
            connection: &mut self.connection,
        })
    }

    async fn write(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<SqlWriter, impl std::error::Error> {
        Err(Error::Io(io::Error::new(io::ErrorKind::Other, "fuck")))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::data_storages::data_storages::DataReader;
    use crate::data_storages::data_storages::DataStorage;

    use super::SqlStorage;

    #[tokio::test(flavor = "current_thread")]
    async fn testtest() {
        let mut store = SqlStorage::new("postgres://test@localhost:5432/test".to_string())
            .await
            .unwrap();
        let mut reader = store.read(&HashMap::new()).await.unwrap();
        reader.read_all().await.unwrap();
    }
}
