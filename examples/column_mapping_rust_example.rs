use std::sync::Arc;
use deltalake_core::{DeltaOps, DeltaTable};
use delta_kernel::table_features::ColumnMappingMode;
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use tokio;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a test Delta table
    let ops = DeltaOps::new_in_memory();
    
    // Create a schema with three columns
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int64, true),
    ]);
    
    // Create the table
    let table = ops
        .create()
        .with_columns_from_schema(&schema)
        .with_table_name("example_table")
        .with_configuration_property("delta.minReaderVersion", Some("2".to_string()))
        .with_configuration_property("delta.minWriterVersion", Some("5".to_string()))
        .await?;
    
    println!("Initial table created");
    
    // Create some data
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
            Arc::new(Int64Array::from(vec![30, 25, 35])),
        ],
    )?;
    
    // Write the data
    let table = DeltaOps::from(table)
        .write(vec![batch])
        .await?;
    
    println!("Initial schema:");
    println!("{:#?}", table.schema()?);
    
    // Enable column mapping with Name mode
    let table = DeltaOps::from(table)
        .column_mapping()
        .with_mode(ColumnMappingMode::Name)
        .await?;
    
    println!("\nColumn mapping enabled");
    
    // Rename a column
    let table = DeltaOps::from(table)
        .column_mapping()
        .with_rename_column("name".to_string(), "full_name".to_string())
        .await?;
    
    println!("\nRenamed 'name' to 'full_name'");
    println!("Schema after renaming:");
    println!("{:#?}", table.schema()?);
    
    // Drop a column
    let table = DeltaOps::from(table)
        .column_mapping()
        .with_drop_column("age".to_string())
        .await?;
    
    println!("\nDropped 'age' column");
    println!("Schema after dropping column:");
    println!("{:#?}", table.schema()?);
    
    // Create new data with updated schema
    let schema = Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("full_name", DataType::Utf8, true),
    ]);
    
    let batch = RecordBatch::try_new(
        Arc::new(schema),
        vec![
            Arc::new(Int64Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["Dave", "Eve"])),
        ],
    )?;
    
    // Write new data
    let table = DeltaOps::from(table)
        .write(vec![batch])
        .await?;
    
    println!("\nAppended new data with new schema");
    println!("Final table version: {}", table.version().unwrap());
    
    Ok(())
} 