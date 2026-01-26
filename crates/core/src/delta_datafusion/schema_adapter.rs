use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

use crate::kernel::schema::{COLUMN_MAPPING_PHYSICAL_NAME_KEY, cast_record_batch};
use arrow_array::RecordBatch;
use arrow_schema::{Field, Schema, SchemaRef};
use datafusion::common::{ColumnStatistics, Result, not_impl_err};
use datafusion::datasource::schema_adapter::{SchemaAdapter, SchemaAdapterFactory, SchemaMapper};

/// A Schema Adapter Factory which provides casting record batches from parquet to meet
/// delta lake conventions.
#[derive(Debug)]
pub(crate) struct DeltaSchemaAdapterFactory {}

impl SchemaAdapterFactory for DeltaSchemaAdapterFactory {
    fn create(
        &self,
        projected_table_schema: SchemaRef,
        table_schema: SchemaRef,
    ) -> Box<dyn SchemaAdapter> {
        Box::new(DeltaSchemaAdapter {
            projected_table_schema,
            table_schema,
        })
    }
}

pub(crate) struct DeltaSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by
    /// the mapping.
    projected_table_schema: SchemaRef,
    /// Schema for the table
    table_schema: SchemaRef,
}

impl SchemaAdapter for DeltaSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.table_schema.field(index);
        // First try to find by physical name (for column mapping tables)
        if let Some(physical_name) = field.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
            if let Some((idx, _)) = file_schema.fields.find(physical_name) {
                return Some(idx);
            }
        }
        // Fall back to logical name
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(&self, file_schema: &Schema) -> Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(file_schema.fields().len());
        // Build a mapping from physical column names to logical column names
        let mut physical_to_logical: HashMap<String, String> = HashMap::new();

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            // Check if this file field's name matches any projected table field's physical or logical name
            let file_field_name = file_field.name();
            let matched_field = self.projected_table_schema.fields().iter().find(|table_field| {
                // Check physical name first (for column mapping tables)
                if let Some(physical_name) = table_field.metadata().get(COLUMN_MAPPING_PHYSICAL_NAME_KEY) {
                    if physical_name == file_field_name {
                        return true;
                    }
                }
                // Fall back to logical name
                table_field.name() == file_field_name
            });
            if let Some(table_field) = matched_field {
                projection.push(file_idx);
                // If the file field name differs from the table field name (due to column mapping),
                // record the mapping from physical to logical name
                if file_field_name != table_field.name() {
                    physical_to_logical.insert(file_field_name.to_string(), table_field.name().to_string());
                }
            }
        }

        Ok((
            Arc::new(SchemaMapping {
                projected_schema: self.projected_table_schema.clone(),
                physical_to_logical,
            }),
            projection,
        ))
    }
}

#[derive(Debug)]
pub(crate) struct SchemaMapping {
    projected_schema: SchemaRef,
    /// Mapping from physical column names (in parquet files) to logical column names (in table schema).
    /// Only contains entries for columns where the physical name differs from the logical name.
    physical_to_logical: HashMap<String, String>,
}

impl SchemaMapper for SchemaMapping {
    fn map_batch(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // If there are column mappings, rename physical columns to logical names before casting
        let batch = if !self.physical_to_logical.is_empty() {
            let schema = batch.schema();
            let new_fields: Vec<_> = schema
                .fields()
                .iter()
                .map(|field| {
                    if let Some(logical_name) = self.physical_to_logical.get(field.name()) {
                        Arc::new(Field::new(
                            logical_name,
                            field.data_type().clone(),
                            field.is_nullable(),
                        ))
                    } else {
                        field.clone()
                    }
                })
                .collect();
            let new_schema = Arc::new(Schema::new(new_fields));
            RecordBatch::try_new(new_schema, batch.columns().to_vec())?
        } else {
            batch
        };
        let record_batch = cast_record_batch(&batch, self.projected_schema.clone(), false, true)?;
        Ok(record_batch)
    }

    fn map_column_statistics(
        &self,
        _file_col_statistics: &[ColumnStatistics],
    ) -> Result<Vec<ColumnStatistics>> {
        not_impl_err!("Mapping column statistics is not implemented for DeltaSchemaAdapter")
    }
}
