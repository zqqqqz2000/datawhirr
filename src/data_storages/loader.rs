use super::{csv::CSVDataStorage, data_storages::DataStorage, pgsql::PgSqlStorage};
use core::panic;
use fluent_uri::Uri;
use std::collections::HashMap;

pub async fn load_data_storage(
    storage_uri: &str,
    options: &HashMap<String, String>,
) -> Box<dyn DataStorage + Send> {
    match Uri::parse(storage_uri)
        .expect("cannot parse uri")
        .scheme()
        .expect("cannot extract schema from uri")
        .to_lowercase()
        .as_str()
    {
        "postgres" => {
            Box::new(PgSqlStorage::new(storage_uri).await.unwrap()) as Box<dyn DataStorage + Send>
        }
        "file+csv" => Box::new(CSVDataStorage::new(storage_uri)) as Box<dyn DataStorage + Send>,
        _ => panic!("not supported this type of uri yet"),
    }
}
