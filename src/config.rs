use crate::data_storages::data_storages::{Schema, SchemaField, SchemaType};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct DataStorageConfig {
    pub uri: String,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub data_storages: HashMap<String, DataStorageConfig>,
    pub schemas: HashMap<String, Schema>,
}

impl Config {
    pub fn example() -> Config {
        Config {
            data_storages: HashMap::from([(
                "example_storage1".to_string(),
                DataStorageConfig {
                    uri: "mysql://test:test@localhost/test".to_string(),
                    options: HashMap::new(),
                },
            )]),
            schemas: HashMap::from([(
                "example_schema".to_string(),
                Schema(vec![SchemaField {
                    name: "field1".to_string(),
                    type_: SchemaType::String,
                    extra: HashMap::new(),
                }]),
            )]),
        }
    }
}
