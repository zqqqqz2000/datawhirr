use core::panic;
use std::collections::HashMap;

use super::{data_storages::DataStorage, none::NoneStorage};

pub fn load_data_storage(
    storage_uri: String,
    options: &HashMap<String, String>,
) -> impl DataStorage {
    NoneStorage {}
}
