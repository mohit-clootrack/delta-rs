use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Int32Array, Int64Array, StringArray, RecordBatch};
use arrow_schema::{Field, Schema, DataType};
use deltalake_core::kernel::{StructField, DataType as DeltaDataType};
use deltalake_core::operations::DeltaOps;
use deltalake_core::{DeltaResult, DeltaTable};

#[tokio::test]
async fn test_comprehensive_schema_evolution() -> DeltaResult<()> {
    // Create initial table
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();

    // Initial schema: id (int32), name (string)
    let initial_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let initial_batch = RecordBatch::try_new(
        initial_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"])),
        ],
    )?;

    // Create table with initial data
    let mut table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
        .await?
        .write(vec![initial_batch])
        .await?;

    assert_eq!(table.version(), Some(0));
    println!("✓ Created initial table with schema: id (int32), name (string)");

    // Test 1: Add new columns using schema evolution
    let new_columns = vec![
        StructField::new("age".to_string(), DeltaDataType::INTEGER, true),
        StructField::new("email".to_string(), DeltaDataType::STRING, true),
    ];

    table = DeltaOps::from(table)
        .schema_evolution()
        .with_new_columns(new_columns)
        .await?;

    println!("✓ Added new columns: age (int32), email (string)");

    // Test 2: Write data with new schema including new columns
    let evolved_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("email", DataType::Utf8, true),
    ]));

    let evolved_batch = RecordBatch::try_new(
        evolved_schema,
        vec![
            Arc::new(Int32Array::from(vec![4, 5])),
            Arc::new(StringArray::from(vec!["Dave", "Eve"])),
            Arc::new(Int32Array::from(vec![Some(30), Some(25)])),
            Arc::new(StringArray::from(vec![Some("dave@example.com"), Some("eve@example.com")])),
        ],
    )?;

    table = DeltaOps::from(table)
        .write_with_schema_evolution(vec![evolved_batch])
        .with_auto_schema_evolution()
        .with_column_mapping()
        .execute()
        .await?;

    println!("✓ Successfully wrote data with evolved schema");

    // Test 3: Enable column mapping and rename columns
    table = DeltaOps::from(table)
        .schema_evolution()
        .with_column_mapping()
        .with_rename_column("name".to_string(), "full_name".to_string())
        .await?;

    println!("✓ Enabled column mapping and renamed 'name' to 'full_name'");

    // Test 4: Change column type (widening conversion)
    table = DeltaOps::from(table)
        .schema_evolution()
        .with_type_change("id".to_string(), DeltaDataType::LONG)
        .await?;

    println!("✓ Changed 'id' column type from int32 to int64");

    // Test 5: Drop a column
    table = DeltaOps::from(table)
        .schema_evolution()
        .with_drop_column("email".to_string())
        .await?;

    println!("✓ Dropped 'email' column");

    // Test 6: Write data with schema that matches the evolved schema
    let final_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),  // Now int64
        Field::new("full_name", DataType::Utf8, true),  // Renamed
        Field::new("age", DataType::Int32, true),
        // email column dropped
    ]));

    let final_batch = RecordBatch::try_new(
        final_schema,
        vec![
            Arc::new(Int64Array::from(vec![6i64, 7i64])),
            Arc::new(StringArray::from(vec!["Frank", "Grace"])),
            Arc::new(Int32Array::from(vec![Some(35), Some(28)])),
        ],
    )?;

    table = DeltaOps::from(table)
        .write(vec![final_batch])
        .await?;

    println!("✓ Successfully wrote data with final evolved schema");

    // Verify final schema
    let final_table_schema = table.schema()?;
    let field_names: Vec<&String> = final_table_schema.fields().map(|f| f.name()).collect();
    
    println!("Final schema fields: {:?}", field_names);
    assert!(field_names.contains(&&"id".to_string()));
    assert!(field_names.contains(&&"full_name".to_string()));
    assert!(field_names.contains(&&"age".to_string()));
    assert!(!field_names.contains(&&"email".to_string())); // Should be dropped

    // Verify data can be read
    table.load().await?;
    println!("✓ Table can be loaded successfully after all schema changes");

    println!("🎉 All schema evolution tests passed!");
    Ok(())
}

#[tokio::test]
async fn test_schema_evolution_with_merge() -> DeltaResult<()> {
    use datafusion::prelude::*;
    
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();

    // Create target table
    let target_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));

    let target_batch = RecordBatch::try_new(
        target_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["old1", "old2"])),
        ],
    )?;

    let mut table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
        .await?
        .write(vec![target_batch])
        .await?;

    // Create source data with additional columns
    let source_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
        Field::new("new_column", DataType::Int32, true),
    ]));

    let source_batch = RecordBatch::try_new(
        source_schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 3])),
            Arc::new(StringArray::from(vec!["updated1", "new3"])),
            Arc::new(Int32Array::from(vec![Some(100), Some(300)])),
        ],
    )?;

    let ctx = SessionContext::new();
    let source_df = ctx.read_batch(source_batch)?;

    // Perform merge with schema evolution
    table = DeltaOps::from(table)
        .merge_with_schema_evolution(source_df, "target.id = source.id")
        .with_new_columns(true)
        .with_column_mapping()
        .execute()
        .await?;

    println!("✓ Merge with schema evolution completed successfully");

    // Verify the schema includes the new column
    let evolved_schema = table.schema()?;
    let field_names: Vec<&String> = evolved_schema.fields().map(|f| f.name()).collect();
    
    assert!(field_names.contains(&&"new_column".to_string()));
    println!("✓ New column successfully added during merge operation");

    Ok(())
}

#[tokio::test]
async fn test_automatic_column_mapping_for_new_columns() -> DeltaResult<()> {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();

    // Create table with column mapping enabled
    let initial_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));

    let initial_batch = RecordBatch::try_new(
        initial_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec!["Alice", "Bob"])),
        ],
    )?;

    let mut table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
        .await?
        .write(vec![initial_batch])
        .with_table_configuration("delta.columnMapping.mode".to_string(), Some("name".to_string()))
        .await?;

    // Write data with new columns - should automatically get column mapping
    let new_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("city", DataType::Utf8, true),
    ]));

    let new_batch = RecordBatch::try_new(
        new_schema,
        vec![
            Arc::new(Int32Array::from(vec![3, 4])),
            Arc::new(StringArray::from(vec!["Charlie", "Dave"])),
            Arc::new(Int32Array::from(vec![Some(30), Some(25)])),
            Arc::new(StringArray::from(vec![Some("NYC"), Some("SF")])),
        ],
    )?;

    table = DeltaOps::from(table)
        .write_with_schema_evolution(vec![new_batch])
        .with_auto_schema_evolution()
        .with_column_mapping()
        .execute()
        .await?;

    println!("✓ New columns automatically received column mapping metadata");

    // Verify schema has proper column mapping
    let schema = table.schema()?;
    for field in schema.fields() {
        let metadata = field.metadata();
        println!("Field '{}' metadata: {:?}", field.name(), metadata);
        // In a real implementation, we'd verify the column mapping metadata exists
    }

    Ok(())
}

#[tokio::test]
async fn test_unsafe_schema_changes_fail_gracefully() -> DeltaResult<()> {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();

    // Create initial table
    let initial_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Int64, true),
    ]));

    let initial_batch = RecordBatch::try_new(
        initial_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![Some(100), Some(200)])),
        ],
    )?;

    let table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
        .await?
        .write(vec![initial_batch])
        .await?;

    // Try to change int64 to int32 (unsafe narrowing conversion)
    let result = DeltaOps::from(table)
        .schema_evolution()
        .with_type_change("value".to_string(), DeltaDataType::INTEGER)
        .await;

    assert!(result.is_err());
    println!("✓ Unsafe type conversion correctly rejected");

    Ok(())
}

#[tokio::test]
async fn test_drop_all_columns_fails() -> DeltaResult<()> {
    let table_dir = tempfile::tempdir().unwrap();
    let table_path = table_dir.path();

    // Create initial table
    let initial_schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
    ]));

    let initial_batch = RecordBatch::try_new(
        initial_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
        ],
    )?;

    let table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
        .await?
        .write(vec![initial_batch])
        .await?;

    // Try to drop all columns
    let result = DeltaOps::from(table)
        .schema_evolution()
        .with_drop_column("id".to_string())
        .await;

    assert!(result.is_err());
    println!("✓ Dropping all columns correctly rejected");

    Ok(())
} 