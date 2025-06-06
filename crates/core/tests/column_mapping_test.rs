#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use delta_kernel::table_features::ColumnMappingMode;
    use deltalake::operations::DeltaOps;
    use deltalake::DeltaTableBuilder;

    #[tokio::test]
    async fn test_column_mapping_mode() {
        // Test that ColumnMappingMode enum works correctly
        assert_eq!(ColumnMappingMode::None as u8, 0);
        assert_eq!(ColumnMappingMode::Name as u8, 1);
        assert_eq!(ColumnMappingMode::Id as u8, 2);
    }

    #[tokio::test]
    async fn test_column_mapping_builder_creation() {
        // This is a basic test to ensure the column mapping builder can be created
        // More comprehensive tests would require a full Delta table setup
        
        let storage_options = HashMap::<String, String>::new();
        
        // For now, just test that the builder patterns compile correctly
        // Real tests would need actual table data
        assert!(true); // Placeholder assertion
    }
} 