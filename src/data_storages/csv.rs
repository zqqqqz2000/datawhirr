use core::panic;

use super::DataStorage;
use csv;
use std::path;

#[derive(Debug)]
pub struct CSVDataStorage {
    file: String,
}

fn extract_csv_uri_filepath(uri: &str) -> String {
    if uri.starts_with("file+csv:///") {
        uri.replace("file+csv://", "")
            .replace("$PWD", path::absolute(".").unwrap().to_str().unwrap())
    } else {
        panic!("not support auth/host uri, only `file+csv:///absoult/path` or `file+csv:///$PWD/relative/path` has been supported")
    }
}

impl CSVDataStorage {
    pub fn new(uri: &str) -> Self {
        CSVDataStorage {
            file: extract_csv_uri_filepath(uri),
        }
    }
}

impl DataStorage for CSVDataStorage {
    async fn read(
        &mut self,
        options: &std::collections::HashMap<&str, &str>,
    ) -> anyhow::Result<super::data_storages::ReadResult> {
    }
    async fn write(
        &mut self,
        data: Vec<super::data_storages::Row>,
        schema: Option<super::data_storages::Schema>,
        options: &std::collections::HashMap<&str, &str>,
    ) -> anyhow::Result<()> {
    }
    async fn chunk_read(
        &mut self,
        cursor: Option<super::data_storages::SchemaTypeWithValue>,
        limit: u32,
        options: &std::collections::HashMap<&str, &str>,
    ) -> anyhow::Result<super::data_storages::ReadResult> {
    }
    async fn read_schema(
        &mut self,
        options: &std::collections::HashMap<&str, &str>,
    ) -> anyhow::Result<super::data_storages::Schema> {
    }
}
