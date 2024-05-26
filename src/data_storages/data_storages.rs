use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, error::Error};

#[derive(Debug, Serialize, Deserialize)]
pub enum SchemaType {
    String,
    Int32,
    Int64,
    Binary,
    Boolean,
    Timestamp,
    Date,
    Datetime,
    Double,
    Float,
    None,
}

#[derive(Clone, Debug)]
pub enum SchemaTypeWithValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Binary(Vec<char>),
    Boolean(bool),
    Timestamp(u32),
    Date(DateTime<Utc>),
    Datetime(DateTime<Utc>),
    Double(f64),
    Float(f32),
    None,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SchemaField {
    pub name: String,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    pub type_: SchemaType,
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Schema(pub Vec<SchemaField>);

#[derive(Clone, Debug)]
pub struct Column {
    pub name: String,
    pub value: SchemaTypeWithValue,
}

#[derive(Clone, Debug)]
pub struct Row(pub Vec<Column>);
impl Row {
    pub fn normalize(&self, schema: Schema) -> Row {
        // TODO: normailze
        self.clone()
    }
}

pub struct ReadResult {
    pub data: Vec<Row>,
    pub schema: Schema,
    pub cursor: Option<SchemaTypeWithValue>,
}

pub trait DataStorage {
    async fn read_schema(
        &mut self,
        options: &HashMap<&str, &str>,
    ) -> Result<Schema, Box<dyn Error>>;

    async fn read(&mut self, options: &HashMap<&str, &str>) -> Result<ReadResult, Box<dyn Error>>;

    async fn chunk_read(
        &mut self,
        cursor: Option<SchemaTypeWithValue>,
        limit: u32,
        options: &HashMap<&str, &str>,
    ) -> Result<ReadResult, Box<dyn Error>>;

    async fn write(&mut self, options: &HashMap<&str, &str>) -> Result<(), Box<dyn Error>>;
}
