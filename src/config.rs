use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::data_storages::data_storages::{Schema, SchemaField, SchemaType};

#[derive(Debug, Serialize, Deserialize)]
pub struct DataStorageConfig {
    pub uri: String,
    pub options: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub data_storages: HashMap<String, DataStorageConfig>,
    pub schemas: HashMap<String, Schema>,
}

impl Config {
    fn example() -> Config {
        Config {
            data_storages: HashMap::from([(
                "example_storage1".to_string(),
                DataStorageConfig {
                    uri: "mysql://xxxx:xxxx@xxxx/xxxx".to_string(),
                    options: HashMap::new(),
                },
            )]),
            schemas: HashMap::from([(
                "example_schema".to_string(),
                Schema(vec![SchemaField {
                    name: "field1".to_string(),
                    type_: SchemaType::String,
                    extra_: HashMap::new(),
                }]),
            )]),
        }
    }
}
