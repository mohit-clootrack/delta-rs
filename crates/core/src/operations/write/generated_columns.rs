use crate::table::state::DeltaTableState;
use datafusion::{execution::SessionState, prelude::DataFrame};
use datafusion_common::ScalarValue;
use datafusion_expr::{col, when, Expr, ExprSchemable};
use delta_kernel::engine::arrow_conversion::TryIntoArrow as _;
use tracing::debug;

use crate::{kernel::{StructField, StructType, ColumnMetadataKey, DataCheck}, table::GeneratedColumn, DeltaResult};

/// check if the writer version is able to write generated columns
pub fn able_to_gc(snapshot: &DeltaTableState) -> DeltaResult<bool> {
    if let Some(features) = &snapshot.protocol().writer_features {
        if snapshot.protocol().min_writer_version < 4 {
            return Ok(false);
        }
        if snapshot.protocol().min_writer_version == 7
            && !features.contains(&delta_kernel::table_features::WriterFeature::GeneratedColumns)
        {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Add generated column expressions to a dataframe
pub fn add_missing_generated_columns(
    mut df: DataFrame,
    generated_cols: &Vec<GeneratedColumn>,
) -> DeltaResult<(DataFrame, Vec<String>)> {
    let mut missing_cols = vec![];
    for generated_col in generated_cols {
        let col_name = generated_col.get_name();

        if df
            .clone()
            .schema()
            .field_with_unqualified_name(col_name)
            .is_err()
        // implies it doesn't exist
        {
            debug!("Adding missing generated column {col_name} in source as placeholder");
            // If column doesn't exist, we add a null column, later we will generate the values after
            // all the merge is projected.
            // Other generated columns that were provided upon the start we only validate during write
            missing_cols.push(col_name.to_string());
            df = df
                .clone()
                .with_column(col_name, Expr::Literal(ScalarValue::Null))?;
        }
    }
    Ok((df, missing_cols))
}

/// Add generated column expressions to a dataframe
pub fn add_generated_columns(
    mut df: DataFrame,
    generated_cols: &Vec<GeneratedColumn>,
    generated_cols_missing_in_source: &[String],
    state: &SessionState,
) -> DeltaResult<DataFrame> {
    debug!("Generating columns in dataframe");
    for generated_col in generated_cols {
        // We only validate columns that were missing from the start. We don't update
        // update generated columns that were provided during runtime
        if !generated_cols_missing_in_source.contains(&generated_col.name) {
            continue;
        }

        let generation_expr = state.create_logical_expr(
            generated_col.get_generation_expression(),
            df.clone().schema(),
        )?;
        let col_name = generated_col.get_name();

        df = df.clone().with_column(
            generated_col.get_name(),
            when(col(col_name).is_null(), generation_expr)
                .otherwise(col(col_name))?
                .cast_to(&((&generated_col.data_type).try_into_arrow()?), df.schema())?,
        )?
    }
    Ok(df)
}

/// Add column mapping metadata to schema fields
pub fn add_column_mapping_metadata(schema: StructType, is_first_time: bool) -> DeltaResult<StructType> {
    let mut max_column_id = if is_first_time { 1i64 } else {
        get_max_column_id(&schema) + 1
    };
    let mut new_fields = Vec::new();

    for field in schema.fields() {
        let mut metadata = field.metadata().clone();
        
        // Only add metadata if it doesn't already exist
        if !metadata.contains_key(ColumnMetadataKey::ColumnMappingId.as_ref()) {
            metadata.insert(
                ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                max_column_id.into(),
            );
            max_column_id += 1;
        }

        if !metadata.contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()) {
            let column_id = metadata.get(ColumnMetadataKey::ColumnMappingId.as_ref())
                .and_then(|v| match v {
                    crate::kernel::MetadataValue::Number(n) => Some(*n as i64),
                    _ => None,
                })
                .unwrap_or(max_column_id - 1);
            metadata.insert(
                ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().to_string(),
                format!("col-{}", column_id).into(),
            );
        }

        let new_field = StructField::new(
            field.name().clone(),
            field.data_type().clone(),
            field.is_nullable(),
        ).with_metadata(metadata);

        new_fields.push(new_field);
    }

    Ok(StructType::new(new_fields))
}

/// Get the maximum column ID from the schema
pub fn get_max_column_id(schema: &StructType) -> i64 {
    let mut max_id = 0i64;
    
    for field in schema.fields() {
        if let Some(column_id) = field.metadata()
            .get(ColumnMetadataKey::ColumnMappingId.as_ref())
            .and_then(|v| match v {
                crate::kernel::MetadataValue::Number(n) => Some(*n as i64),
                _ => None,
            })
        {
            max_id = max_id.max(column_id);
        }
    }
    
    // If no existing column IDs found, return 0 (will be incremented to 1)
    max_id
}
