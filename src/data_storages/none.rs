use std::fmt;

use super::data_storages::{
    ChunkReader, ChunkWriter, DataReader, DataStorage, DataWriter, Row, Schema,
};

#[derive(Debug)]
pub struct NoneStorage {}

#[derive(Debug)]
struct NoneErr {}
impl std::error::Error for NoneErr {}

impl fmt::Display for NoneErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "none error, not impl.")
    }
}

struct NoneReaderWriter {}

impl ChunkReader for NoneReaderWriter {
    async fn next(&self, chunk_size: u32) -> Result<(Vec<Row>, Schema), impl std::error::Error> {
        Result::Err(NoneErr {})
    }
    async fn close(&mut self) {}
}

impl ChunkWriter for NoneReaderWriter {
    async fn write(&self, rows: Vec<Row>) -> Result<(), impl std::error::Error> {
        Result::Err(NoneErr {})
    }
    async fn close(&mut self) {}
}

impl DataReader for NoneReaderWriter {
    async fn chunk_read(&self) -> Result<NoneReaderWriter, impl std::error::Error> {
        Result::Err(NoneErr {})
    }

    async fn read_all(&self) -> Result<(Vec<Row>, Schema), impl std::error::Error> {
        Result::Err(NoneErr {})
    }
}

impl DataWriter for NoneReaderWriter {
    async fn chunk_write(&self) -> Result<NoneReaderWriter, impl std::error::Error> {
        Result::Err(NoneErr {})
    }

    async fn write_all(&self, rows: Vec<Row>) -> Result<(), impl std::error::Error> {
        Result::Err(NoneErr {})
    }
}

impl DataStorage for NoneStorage {
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<super::data_storages::Schema, impl std::error::Error> {
        Result::Err(NoneErr {})
    }

    async fn read(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<NoneReaderWriter, impl std::error::Error> {
        Result::Err(NoneErr {})
    }

    async fn write(
        &mut self,
        options: &std::collections::HashMap<String, String>,
    ) -> Result<NoneReaderWriter, impl std::error::Error> {
        Result::Err(NoneErr {})
    }
}
