use anyhow::Result;
use async_trait::async_trait;
use std::fmt;

use super::data_storages::{DataStorage, ReadResult, Row, Schema, SchemaTypeWithValue};

#[derive(Debug)]
pub struct NoneStorage {}

#[derive(Debug)]
pub struct NoneErr {}
impl std::error::Error for NoneErr {}

impl fmt::Display for NoneErr {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error! not impl.")
    }
}

#[async_trait]
impl DataStorage for NoneStorage {
    async fn read_schema(
        &mut self,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<super::data_storages::Schema> {
        Err(NoneErr {}.into())
    }

    async fn read(&mut self, _: &std::collections::HashMap<&str, &str>) -> Result<ReadResult> {
        Err(NoneErr {}.into())
    }

    async fn write(
        &mut self,
        _: Vec<Row>,
        _: Option<Schema>,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<()> {
        Result::Err(NoneErr {}.into())
    }

    async fn chunk_read(
        &mut self,
        _: Option<SchemaTypeWithValue>,
        _: u32,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<ReadResult> {
        Err(NoneErr {}.into())
    }
}
