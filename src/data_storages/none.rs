use std::fmt;

use super::data_storages::DataStorage;

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

impl DataStorage for NoneStorage {
    async fn read_schema(
        &mut self,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<super::data_storages::Schema, Box<dyn std::error::Error>> {
        Err(NoneErr {}.into())
    }

    async fn read(
        &mut self,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        Box<dyn std::error::Error>,
    > {
        Err(NoneErr {}.into())
    }

    async fn write(
        &mut self,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        Result::Err(NoneErr {}.into())
    }

    async fn chunk_read(
        &mut self,
        _: Option<&str>,
        _: u32,
        _: &std::collections::HashMap<&str, &str>,
    ) -> Result<
        (Vec<super::data_storages::Row>, super::data_storages::Schema),
        Box<dyn std::error::Error>,
    > {
        Err(NoneErr {}.into())
    }
}
