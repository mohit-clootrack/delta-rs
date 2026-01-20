//! Integration tests for Delta Lake column mapping support
//!
//! These tests verify that delta-rs correctly handles tables with column mapping
//! enabled (both 'name' and 'id' modes).

#![cfg(feature = "datafusion")]

use datafusion::assert_batches_sorted_eq;
use deltalake_core::delta_datafusion::create_session;
use deltalake_core::{ensure_table_uri, open_table};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error + 'static>>;

fn test_table_uri(path: &str) -> url::Url {
    ensure_table_uri(path).expect("Failed to create table URI")
}

/// Test reading a table with column mapping (name mode)
#[tokio::test]
async fn test_read_table_with_column_mapping() -> TestResult {
    // Use the existing test table with column mapping
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;

    // Check that the table has column mapping enabled
    let config = table.snapshot()?.snapshot().table_configuration();
    let mode = config.column_mapping_mode();
    assert!(
        mode != delta_kernel::table_features::ColumnMappingMode::None,
        "Expected column mapping to be enabled"
    );

    // Get the schema - should have logical column names
    let schema = table.snapshot()?.schema();
    let field_names: Vec<_> = schema.fields().map(|f| f.name().as_str()).collect();

    // The test table should have columns with special characters
    assert!(
        field_names.iter().any(|n| n.contains(' ')),
        "Expected column names with spaces, got: {:?}",
        field_names
    );

    Ok(())
}

/// Test DataFusion query on table with column mapping
#[tokio::test]
async fn test_datafusion_query_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Query using logical column names (with special characters)
    let df = ctx
        .sql(r#"SELECT "Company Very Short", "Super Name" FROM test_table ORDER BY "Super Name" LIMIT 3"#)
        .await?;

    let batches = df.collect().await?;

    // Verify we got results
    assert!(!batches.is_empty(), "Expected non-empty result");
    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows > 0, "Expected at least one row");

    // Verify column names in result schema are logical names
    let schema = batches[0].schema();
    assert!(schema.field_with_name("Company Very Short").is_ok());
    assert!(schema.field_with_name("Super Name").is_ok());

    Ok(())
}

/// Test filtering on partition columns with column mapping
#[tokio::test]
async fn test_partition_filter_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Filter on partition column using logical name
    let df = ctx
        .sql(r#"SELECT "Super Name" FROM test_table WHERE "Company Very Short" = 'BMS'"#)
        .await?;

    let batches = df.collect().await?;

    // Verify we got results
    assert!(!batches.is_empty(), "Expected non-empty result");

    let expected = vec![
        "+------------------------+",
        "| Super Name             |",
        "+------------------------+",
        "| Anthony Johnson        |",
        "| Mr. Daniel Ferguson MD |",
        "| Nathan Bennett         |",
        "| Stephanie Mcgrath      |",
        "+------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}

/// Test statistics with column mapping
#[tokio::test]
async fn test_statistics_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;

    // Get file count from snapshot
    let snapshot = table.snapshot()?;
    let num_files = snapshot.log_data().num_files();

    assert!(num_files > 0, "Expected at least one file");

    Ok(())
}

/// Test scan with projection and column mapping
#[tokio::test]
async fn test_projection_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();
    ctx.register_table("test_table", provider)?;

    // Select only one column
    let df = ctx
        .sql(r#"SELECT "Super Name" FROM test_table LIMIT 2"#)
        .await?;

    let batches = df.collect().await?;

    // Verify schema only has requested column
    let schema = batches[0].schema();
    assert_eq!(schema.fields().len(), 1);
    assert!(schema.field_with_name("Super Name").is_ok());

    Ok(())
}

/// Test that we can get physical column names from schema
#[tokio::test]
async fn test_physical_name_access() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;

    let config = table.snapshot()?.snapshot().table_configuration();
    let schema = config.schema();
    let mapping_mode = config.column_mapping_mode();

    // Verify physical names are different from logical names
    for field in schema.fields() {
        let logical = field.name();
        let physical = field.physical_name(mapping_mode);

        // For tables with column mapping, physical names should be UUIDs
        if mapping_mode != delta_kernel::table_features::ColumnMappingMode::None {
            assert_ne!(
                logical, physical,
                "Physical name should differ from logical name with column mapping"
            );
            assert!(
                physical.starts_with("col-"),
                "Physical name should start with 'col-', got: {}",
                physical
            );
        }
    }

    Ok(())
}

/// Test end-to-end: read full table content
#[tokio::test]
async fn test_full_table_scan_with_column_mapping() -> TestResult {
    let table_path = "../test/tests/data/table_with_column_mapping";

    let table = open_table(test_table_uri(table_path)).await?;
    let provider = table.table_provider().await?;

    let ctx = create_session().into_inner();

    let batches = ctx.read_table(provider)?.collect().await?;

    let expected = vec![
        "+--------------------+------------------------+",
        "| Company Very Short | Super Name             |",
        "+--------------------+------------------------+",
        "| BME                | Timothy Lamb           |",
        "| BMS                | Anthony Johnson        |",
        "| BMS                | Mr. Daniel Ferguson MD |",
        "| BMS                | Nathan Bennett         |",
        "| BMS                | Stephanie Mcgrath      |",
        "+--------------------+------------------------+",
    ];
    assert_batches_sorted_eq!(&expected, &batches);

    Ok(())
}
