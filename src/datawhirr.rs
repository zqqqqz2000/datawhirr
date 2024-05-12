use std::{collections::HashMap, error::Error};

use chrono::{Date, DateTime, Utc};

#[derive(Debug)]
enum SchemaType {
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
enum SchemaTypeWithValue {
    String(String),
    Int32(i32),
    Int64(i64),
    Binary(Vec<char>),
    Boolean(bool),
    Timestamp(u32),
    Date(Date<Utc>),
    Datetime(DateTime<Utc>),
    Double(f64),
    Float(f32),
}

#[derive(Debug)]
struct SchemaField {
    name: String,
    type_: SchemaType,
    extra: HashMap<String, String>,
}

struct Schema(Vec<SchemaField>);
struct ExtraOptions(HashMap<String, String>);

#[derive(Clone)]
struct Column {
    name: String,
    value: SchemaTypeWithValue,
}

#[derive(Clone)]
struct Row(Vec<Column>);
impl Row {
    fn normalize(&self, schema: Schema) -> Row {
        // TODO: normailze
        self.clone()
    }
}

trait ChunkReader {
    async fn next(&self, chunk_size: u32) -> Result<Vec<Row>, impl Error>;
    async fn close(&mut self);
}

trait DataReader {
    async fn chunk_read(&self) -> Result<impl ChunkReader, impl Error>;
    async fn read_all(&self) -> Result<Vec<Row>, impl Error>;
}

trait ChunkWriter {
    async fn write(&self, rows: Vec<Row>) -> Result<(), impl Error>;
    async fn close(&mut self);
}

trait DataWriter {
    async fn chunk_write(&self) -> Result<impl ChunkWriter, impl Error>;
    async fn write_all(&self, rows: Vec<Row>) -> Result<(), impl Error>;
}

trait DataStoreage {
    async fn read_schema(&self, options: &ExtraOptions) -> Result<Schema, impl Error>;
    async fn read(&self, options: &ExtraOptions) -> Result<impl DataReader, impl Error>;
    async fn write(&self, options: &ExtraOptions) -> Result<impl DataWriter, impl Error>;
}
