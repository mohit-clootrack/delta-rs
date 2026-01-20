//! Delta table schema

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

pub use delta_kernel::schema::{
    ArrayType, ColumnMetadataKey, DataType, DecimalType, MapType, MetadataValue, PrimitiveType,
    StructField, StructType,
};
use serde_json::Value;
use uuid::Uuid;

use crate::kernel::error::Error;
use crate::schema::DataCheck;
use crate::table::GeneratedColumn;

/// Type alias for a top level schema
pub type Schema = StructType;
/// Schema reference type
pub type SchemaRef = Arc<StructType>;

/// An invariant for a column that is enforced on all writes to a Delta table.
#[derive(Eq, PartialEq, Debug, Default, Clone)]
pub struct Invariant {
    /// The full path to the field.
    pub field_name: String,
    /// The SQL string that must always evaluate to true.
    pub invariant_sql: String,
}

impl Invariant {
    /// Create a new invariant
    pub fn new(field_name: &str, invariant_sql: &str) -> Self {
        Self {
            field_name: field_name.to_string(),
            invariant_sql: invariant_sql.to_string(),
        }
    }
}

impl DataCheck for Invariant {
    fn get_name(&self) -> &str {
        &self.field_name
    }

    fn get_expression(&self) -> &str {
        &self.invariant_sql
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Trait to add convenience functions to struct type
pub trait StructTypeExt {
    /// Get all invariants in the schemas
    fn get_invariants(&self) -> Result<Vec<Invariant>, Error>;

    /// Get all generated column expressions
    fn get_generated_columns(&self) -> Result<Vec<GeneratedColumn>, Error>;

    /// Add column mapping metadata to all fields in the schema.
    /// This generates unique physical names (UUIDs) and assigns sequential column IDs.
    /// Returns a new schema with column mapping metadata added to all fields.
    fn with_column_mapping_metadata(&self) -> Result<StructType, Error>;

    /// Build a mapping from logical column names to physical column names
    /// based on the column mapping metadata in this schema.
    fn get_logical_to_physical_mapping(&self) -> HashMap<String, String>;
}

impl StructTypeExt for StructType {
    /// Get all get_generated_columns in the schemas
    fn get_generated_columns(&self) -> Result<Vec<GeneratedColumn>, Error> {
        let mut remaining_fields: Vec<(String, StructField)> = self
            .fields()
            .map(|field| (field.name.clone(), field.clone()))
            .collect();
        let mut generated_cols: Vec<GeneratedColumn> = Vec::new();

        while let Some((field_path, field)) = remaining_fields.pop() {
            if let Some(MetadataValue::String(generated_col_string)) = field
                .metadata
                .get(ColumnMetadataKey::GenerationExpression.as_ref())
            {
                generated_cols.push(GeneratedColumn::new(
                    &field_path,
                    generated_col_string,
                    field.data_type(),
                ));
            }
        }
        Ok(generated_cols)
    }

    /// Get all invariants in the schemas
    fn get_invariants(&self) -> Result<Vec<Invariant>, Error> {
        let mut remaining_fields: Vec<(String, StructField)> = self
            .fields()
            .map(|field| (field.name.clone(), field.clone()))
            .collect();
        let mut invariants: Vec<Invariant> = Vec::new();

        let add_segment = |prefix: &str, segment: &str| -> String {
            if prefix.is_empty() {
                segment.to_owned()
            } else {
                format!("{prefix}.{segment}")
            }
        };

        while let Some((field_path, field)) = remaining_fields.pop() {
            match field.data_type() {
                DataType::Struct(inner) => {
                    remaining_fields.extend(
                        inner
                            .fields()
                            .map(|field| {
                                let new_prefix = add_segment(&field_path, &field.name);
                                (new_prefix, field.clone())
                            })
                            .collect::<Vec<(String, StructField)>>(),
                    );
                }
                DataType::Array(inner) => {
                    let element_field_name = add_segment(&field_path, "element");
                    remaining_fields.push((
                        element_field_name,
                        StructField::new("".to_string(), inner.element_type.clone(), false),
                    ));
                }
                DataType::Map(inner) => {
                    let key_field_name = add_segment(&field_path, "key");
                    remaining_fields.push((
                        key_field_name,
                        StructField::new("".to_string(), inner.key_type.clone(), false),
                    ));
                    let value_field_name = add_segment(&field_path, "value");
                    remaining_fields.push((
                        value_field_name,
                        StructField::new("".to_string(), inner.value_type.clone(), false),
                    ));
                }
                _ => {}
            }
            // JSON format: {"expression": {"expression": "<SQL STRING>"} }
            if let Some(MetadataValue::String(invariant_json)) =
                field.metadata.get(ColumnMetadataKey::Invariants.as_ref())
            {
                let json: Value = serde_json::from_str(invariant_json).map_err(|e| {
                    Error::InvalidInvariantJson {
                        json_err: e,
                        line: invariant_json.to_string(),
                    }
                })?;
                if let Value::Object(json) = json
                    && let Some(Value::Object(expr1)) = json.get("expression")
                    && let Some(Value::String(sql)) = expr1.get("expression")
                {
                    invariants.push(Invariant::new(&field_path, sql));
                }
            }
        }
        Ok(invariants)
    }

    /// Add column mapping metadata to all fields in the schema.
    /// This generates unique physical names (UUIDs) and assigns sequential column IDs.
    fn with_column_mapping_metadata(&self) -> Result<StructType, Error> {
        let mut column_id_counter = 1i64;

        fn add_metadata_to_field(
            field: &StructField,
            counter: &mut i64,
        ) -> Result<StructField, Error> {
            // Generate physical name and column ID
            let physical_name = format!("col-{}", Uuid::new_v4());
            let column_id = *counter;
            *counter += 1;

            // Build new metadata with column mapping info
            let mut metadata: Vec<(String, MetadataValue)> = field
                .metadata()
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            metadata.push((
                "delta.columnMapping.id".to_string(),
                MetadataValue::Number(column_id),
            ));
            metadata.push((
                "delta.columnMapping.physicalName".to_string(),
                MetadataValue::String(physical_name),
            ));

            // Handle nested types recursively
            let new_data_type = match field.data_type() {
                DataType::Struct(inner) => {
                    let new_fields: Result<Vec<StructField>, Error> = inner
                        .fields()
                        .map(|f| add_metadata_to_field(f, counter))
                        .collect();
                    DataType::Struct(Box::new(
                        StructType::try_new(new_fields?).map_err(|e| Error::Generic(e.to_string()))?,
                    ))
                }
                DataType::Array(inner) => {
                    // For arrays, the element type might need metadata if it's a struct
                    if let DataType::Struct(elem_struct) = inner.element_type() {
                        let new_fields: Result<Vec<StructField>, Error> = elem_struct
                            .fields()
                            .map(|f| add_metadata_to_field(f, counter))
                            .collect();
                        DataType::Array(Box::new(ArrayType::new(
                            DataType::Struct(Box::new(
                                StructType::try_new(new_fields?)
                                    .map_err(|e| Error::Generic(e.to_string()))?,
                            )),
                            inner.contains_null(),
                        )))
                    } else {
                        field.data_type().clone()
                    }
                }
                DataType::Map(inner) => {
                    // For maps, value type might need metadata if it's a struct
                    let new_key_type = inner.key_type().clone();
                    let new_value_type = if let DataType::Struct(val_struct) = inner.value_type() {
                        let new_fields: Result<Vec<StructField>, Error> = val_struct
                            .fields()
                            .map(|f| add_metadata_to_field(f, counter))
                            .collect();
                        DataType::Struct(Box::new(
                            StructType::try_new(new_fields?)
                                .map_err(|e| Error::Generic(e.to_string()))?,
                        ))
                    } else {
                        inner.value_type().clone()
                    };
                    DataType::Map(Box::new(MapType::new(
                        new_key_type,
                        new_value_type,
                        inner.value_contains_null(),
                    )))
                }
                _ => field.data_type().clone(),
            };

            // Create new field with updated data type and metadata
            let new_field = if field.is_nullable() {
                StructField::nullable(field.name(), new_data_type)
            } else {
                StructField::not_null(field.name(), new_data_type)
            };

            Ok(new_field.with_metadata(metadata))
        }

        let new_fields: Result<Vec<StructField>, Error> = self
            .fields()
            .map(|f| add_metadata_to_field(f, &mut column_id_counter))
            .collect();

        StructType::try_new(new_fields?).map_err(|e| Error::Generic(e.to_string()))
    }

    /// Build a mapping from logical column names to physical column names
    fn get_logical_to_physical_mapping(&self) -> HashMap<String, String> {
        fn collect_mappings(schema: &StructType, result: &mut HashMap<String, String>) {
            for field in schema.fields() {
                let logical_name = field.name().to_string();

                // Get physical name from metadata if present
                if let Some(MetadataValue::String(physical_name)) =
                    field.metadata().get("delta.columnMapping.physicalName")
                {
                    if &logical_name != physical_name {
                        result.insert(logical_name.clone(), physical_name.clone());
                    }
                }

                // Recursively handle nested structs
                if let DataType::Struct(nested) = field.data_type() {
                    collect_mappings(nested.as_ref(), result);
                }
            }
        }

        let mut mappings = HashMap::new();
        collect_mappings(self, &mut mappings);
        mappings
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use serde_json::json;

    #[test]
    fn test_get_generated_columns() {
        let schema: StructType = serde_json::from_value(json!(
            {
                "type":"struct",
                "fields":[
                    {"name":"id","type":"integer","nullable":true,"metadata":{}},
                    {"name":"gc","type":"integer","nullable":true,"metadata":{}}]
            }
        ))
        .unwrap();
        let cols = schema.get_generated_columns().unwrap();
        assert_eq!(cols.len(), 0);

        let schema: StructType = serde_json::from_value(json!(
            {
                "type":"struct",
                "fields":[
                    {"name":"id","type":"integer","nullable":true,"metadata":{}},
                    {"name":"gc","type":"integer","nullable":true,"metadata":{"delta.generationExpression":"5"}}]
            }
        )).unwrap();
        let cols = schema.get_generated_columns().unwrap();
        assert_eq!(cols.len(), 1);
        assert_eq!(cols[0].data_type, DataType::INTEGER);
        assert_eq!(cols[0].validation_expr, "gc <=> 5");

        let schema: StructType = serde_json::from_value(json!(
            {
                "type":"struct",
                "fields":[
                    {"name":"id","type":"integer","nullable":true,"metadata":{}},
                    {"name":"gc","type":"integer","nullable":true,"metadata":{"delta.generationExpression":"5"}},
                    {"name":"id2","type":"integer","nullable":true,"metadata":{"delta.generationExpression":"id * 10"}},]
            }
        )).unwrap();
        let cols = schema.get_generated_columns().unwrap();
        assert_eq!(cols.len(), 2);
    }

    #[test]
    fn test_get_invariants() {
        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [{"name": "x", "type": "string", "nullable": true, "metadata": {}}]
        }))
        .unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 0);

        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [
                {"name": "x", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"x > 2\"} }"
                }},
                {"name": "y", "type": "integer", "nullable": true, "metadata": {
                    "delta.invariants": "{\"expression\": { \"expression\": \"y < 4\"} }"
                }}
            ]
        }))
        .unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 2);
        assert!(invariants.contains(&Invariant::new("x", "x > 2")));
        assert!(invariants.contains(&Invariant::new("y", "y < 4")));

        let schema: StructType = serde_json::from_value(json!({
            "type": "struct",
            "fields": [{
                "name": "a_map",
                "type": {
                    "type": "map",
                    "keyType": "string",
                    "valueType": {
                        "type": "array",
                        "elementType": {
                            "type": "struct",
                            "fields": [{
                                "name": "d",
                                "type": "integer",
                                "metadata": {
                                    "delta.invariants": "{\"expression\": { \"expression\": \"a_map.value.element.d < 4\"} }"
                                },
                                "nullable": false
                            }]
                        },
                        "containsNull": false
                    },
                    "valueContainsNull": false
                },
                "nullable": false,
                "metadata": {}
            }]
        })).unwrap();
        let invariants = schema.get_invariants().unwrap();
        assert_eq!(invariants.len(), 1);
        assert_eq!(
            invariants[0],
            Invariant::new("a_map.value.element.d", "a_map.value.element.d < 4")
        );
    }

    /// <https://github.com/delta-io/delta-rs/issues/2152>
    #[test]
    fn test_identity_columns() {
        let buf = r#"{"type":"struct","fields":[{"name":"ID_D_DATE","type":"long","nullable":true,"metadata":{"delta.identity.start":1,"delta.identity.step":1,"delta.identity.allowExplicitInsert":false}},{"name":"TXT_DateKey","type":"string","nullable":true,"metadata":{}}]}"#;
        let _schema: StructType = serde_json::from_str(buf).expect("Failed to load");
    }
}
