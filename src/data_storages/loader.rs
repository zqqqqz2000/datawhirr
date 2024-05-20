use super::{data_storages::DataStorage, none::NoneStorage, pgsql::PgSqlStorage};
use core::panic;
use std::collections::HashMap;

pub async fn load_data_storage(
    storage_uri: &str,
    options: &HashMap<String, String>,
) -> impl DataStorage {
    if storage_uri.starts_with("postgres://") {
        PgSqlStorage::new(storage_uri).await.unwrap()
    } else {
        panic!("");
    }
}
