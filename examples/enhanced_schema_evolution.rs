/*!
Enhanced Schema Evolution Example for Delta-RS

This example demonstrates comprehensive schema evolution capabilities including:
- Adding new columns with automatic column mapping
- Renaming columns safely 
- Changing column types with validation
- Dropping columns
- Handling schema changes during writes and merges
*/

use std::sync::Arc;
use arrow_array::{Int32Array, Int64Array, StringArray, RecordBatch, Float64Array};
use arrow_schema::{Field, Schema, DataType};
use deltalake_core::kernel::{StructField, DataType as DeltaDataType};
use deltalake_core::operations::DeltaOps;
use deltalake_core::{DeltaResult, DeltaTable};

#[tokio::main]
async fn main() -> DeltaResult<()> {
    println!("🚀 Enhanced Schema Evolution Example\n");

    // Scenario: Building a customer data pipeline that needs to evolve over time
    let table_path = tempfile::tempdir().unwrap().into_path();
    println!("📁 Working with table at: {}\n", table_path.display());

    // Phase 1: Initial customer table with basic info
    println!("📊 Phase 1: Creating initial customer table");
    let initial_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("signup_date", DataType::Utf8, true),
    ]));

    let initial_batch = RecordBatch::try_new(
        initial_schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["Alice Johnson", "Bob Smith", "Carol Davis"])),
            Arc::new(StringArray::from(vec!["2023-01-15", "2023-02-20", "2023-03-10"])),
        ],
    )?;

    let mut table = DeltaOps::try_from_uri(table_path.to_str().unwrap())
        .await?
        .write(vec![initial_batch])
        .await?;

    println!("✅ Initial table created with {} customers", 3);
    print_schema_info(&table, "Initial schema").await?;

    // Phase 2: Business grows, need to add more customer attributes
    println!("\n📊 Phase 2: Adding new customer attributes");
    
    let new_columns = vec![
        StructField::new("email".to_string(), DeltaDataType::STRING, true),
        StructField::new("phone".to_string(), DeltaDataType::STRING, true),
        StructField::new("age".to_string(), DeltaDataType::INTEGER, true),
        StructField::new("subscription_tier".to_string(), DeltaDataType::STRING, true),
    ];

    table = DeltaOps::from(table)
        .schema_evolution()
        .with_new_columns(new_columns)
        .with_column_mapping() // Enable column mapping for future flexibility
        .await?;

    println!("✅ Added email, phone, age, and subscription_tier columns");
    print_schema_info(&table, "After adding columns").await?;

    // Phase 3: Write new customer data with the evolved schema
    println!("\n📊 Phase 3: Adding new customers with complete data");
    
    let evolved_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
        Field::new("signup_date", DataType::Utf8, true),
        Field::new("email", DataType::Utf8, true),
        Field::new("phone", DataType::Utf8, true),
        Field::new("age", DataType::Int32, true),
        Field::new("subscription_tier", DataType::Utf8, true),
    ]));

    let new_customers_batch = RecordBatch::try_new(
        evolved_schema,
        vec![
            Arc::new(Int32Array::from(vec![4, 5, 6])),
            Arc::new(StringArray::from(vec!["David Wilson", "Eve Brown", "Frank Miller"])),
            Arc::new(StringArray::from(vec!["2023-04-05", "2023-04-15", "2023-04-20"])),
            Arc::new(StringArray::from(vec![
                Some("david@example.com"), 
                Some("eve@example.com"), 
                Some("frank@example.com")
            ])),
            Arc::new(StringArray::from(vec![
                Some("+1-555-0104"), 
                Some("+1-555-0105"), 
                Some("+1-555-0106")
            ])),
            Arc::new(Int32Array::from(vec![Some(28), Some(34), Some(42)])),
            Arc::new(StringArray::from(vec![Some("premium"), Some("basic"), Some("premium")])),
        ],
    )?;

    table = DeltaOps::from(table)
        .write_with_schema_evolution(vec![new_customers_batch])
        .with_auto_schema_evolution()
        .with_column_mapping()
        .execute()
        .await?;

    println!("✅ Added 3 new customers with complete profile data");

    // Phase 4: Compliance requirement - rename PII columns for clarity
    println!("\n📊 Phase 4: Renaming columns for compliance clarity");
    
    table = DeltaOps::from(table)
        .schema_evolution()
        .with_rename_column("name".to_string(), "full_name".to_string())
        .with_rename_column("phone".to_string(), "phone_number".to_string())
        .await?;

    println!("✅ Renamed 'name' → 'full_name' and 'phone' → 'phone_number'");
    print_schema_info(&table, "After renaming columns").await?;

    // Phase 5: Scale up - customer IDs need to be bigger
    println!("\n📊 Phase 5: Scaling customer IDs to handle growth");
    
    table = DeltaOps::from(table)
        .schema_evolution()
        .with_type_change("customer_id".to_string(), DeltaDataType::LONG)
        .await?;

    println!("✅ Changed customer_id from int32 to int64 for scalability");

    // Phase 6: Remove obsolete data due to privacy regulations
    println!("\n📊 Phase 6: Removing signup_date for privacy compliance");
    
    table = DeltaOps::from(table)
        .schema_evolution()
        .with_drop_column("signup_date".to_string())
        .await?;

    println!("✅ Dropped signup_date column for privacy compliance");
    print_schema_info(&table, "Final schema").await?;

    // Phase 7: Demonstrate writing with final evolved schema
    println!("\n📊 Phase 7: Writing new customers with evolved schema");
    
    let final_schema = Arc::new(Schema::new(vec![
        Field::new("customer_id", DataType::Int64, false),  // Now int64
        Field::new("full_name", DataType::Utf8, true),      // Renamed
        Field::new("email", DataType::Utf8, true),
        Field::new("phone_number", DataType::Utf8, true),   // Renamed
        Field::new("age", DataType::Int32, true),
        Field::new("subscription_tier", DataType::Utf8, true),
        // signup_date removed
    ]));

    let final_batch = RecordBatch::try_new(
        final_schema,
        vec![
            Arc::new(Int64Array::from(vec![1000i64, 1001i64])), // Large customer IDs
            Arc::new(StringArray::from(vec!["Grace Chen", "Henry Rodriguez"])),
            Arc::new(StringArray::from(vec![
                Some("grace@example.com"), 
                Some("henry@example.com")
            ])),
            Arc::new(StringArray::from(vec![
                Some("+1-555-1000"), 
                Some("+1-555-1001")
            ])),
            Arc::new(Int32Array::from(vec![Some(31), Some(29)])),
            Arc::new(StringArray::from(vec![Some("enterprise"), Some("premium")])),
        ],
    )?;

    table = DeltaOps::from(table)
        .write(vec![final_batch])
        .await?;

    println!("✅ Successfully added customers with large IDs using evolved schema");

    // Final verification
    println!("\n🔍 Final Verification:");
    table.load().await?;
    println!("✅ Table loads successfully with version {}", table.version().unwrap());
    print_schema_info(&table, "Production-ready schema").await?;

    println!("\n🎉 Schema evolution pipeline completed successfully!");
    println!("   • Started with 3 columns, ended with 6 columns");
    println!("   • Safely renamed 2 columns with column mapping");
    println!("   • Upgraded customer_id data type for scalability");
    println!("   • Removed PII column for compliance");
    println!("   • All data remains accessible throughout evolution");

    Ok(())
}

async fn print_schema_info(table: &DeltaTable, description: &str) -> DeltaResult<()> {
    let schema = table.schema()?;
    println!("   {}: {:?}", description, 
        schema.fields().map(|f| format!("{}({})", f.name(), format_data_type(f.data_type()))).collect::<Vec<_>>()
    );
    Ok(())
}

fn format_data_type(data_type: &DeltaDataType) -> &'static str {
    match data_type {
        DeltaDataType::STRING => "string",
        DeltaDataType::INTEGER => "int32",
        DeltaDataType::LONG => "int64",
        DeltaDataType::DOUBLE => "float64",
        DeltaDataType::BOOLEAN => "bool",
        _ => "other",
    }
} 