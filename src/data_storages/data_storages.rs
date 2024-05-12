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
}

#[derive(Clone)]
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

#[derive(Clone)]
struct Column {
    name: String,
    value: SchemaTypeWithValue,
}

#[derive(Clone)]
pub struct Row(Vec<Column>);
impl Row {
    fn normalize(&self, schema: Schema) -> Row {
        // TODO: normailze
        self.clone()
    }
}

pub trait ChunkReader {
    async fn next(&self, chunk_size: u32) -> Result<Vec<Row>, impl Error>;
    async fn close(&mut self);
}

pub trait DataReader {
    async fn chunk_read(&self) -> Result<impl ChunkReader, impl Error>;
    async fn read_all(&self) -> Result<Vec<Row>, impl Error>;
}

pub trait ChunkWriter {
    async fn write(&self, rows: Vec<Row>) -> Result<(), impl Error>;
    async fn close(&mut self);
}

pub trait DataWriter {
    async fn chunk_write(&self) -> Result<impl ChunkWriter, impl Error>;
    async fn write_all(&self, rows: Vec<Row>) -> Result<(), impl Error>;
}

pub trait DataStorage {
    async fn read_schema(&self, options: &HashMap<String, String>) -> Result<Schema, impl Error>;
    async fn read(&self, options: &HashMap<String, String>) -> Result<impl DataReader, impl Error>;
    async fn write(&self, options: &HashMap<String, String>)
        -> Result<impl DataWriter, impl Error>;
}
