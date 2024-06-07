use std::collections::{HashMap, HashSet};

use crate::data_storages::data_storages::{Schema, SchemaField};

pub fn bool_str(b: bool) -> String {
    if b { "true" } else { "false" }.to_string()
}

fn schema_fieldname_to_field(schema: &Schema) -> HashMap<&str, SchemaField> {
    schema
        .0
        .iter()
        .map(|schema| (schema.name.as_str(), schema.clone()))
        .collect::<_>()
}

fn schema_get_fieldnames(schema: &Schema) -> HashSet<&str> {
    schema
        .0
        .iter()
        .map(|schema| schema.name.as_str())
        .collect::<HashSet<_>>()
}

pub fn merge_schema(schema1: &Schema, schema2: &Schema) -> Schema {
    let schema2_name_to_schema = schema_fieldname_to_field(schema2);

    let schema1_columns = schema_get_fieldnames(schema1);
    let schema2_columns = schema_get_fieldnames(schema2);

    let schema2_more_columns = &schema2_columns - &schema1_columns;

    let mut res: Vec<SchemaField> = vec![];
    for col in &schema1.0 {
        // TODO: merge schema
    }
    for schema2_more in schema2_more_columns {
        res.push(schema2_name_to_schema.get(schema2_more).unwrap().clone());
    }
    Schema(res)
}
