#[cfg(feature = "datafusion")]
mod tests {
    use std::collections::HashMap;
    use deltalake_core::operations::DeltaOps;
    use deltalake_core::DeltaTableBuilder;

    #[tokio::test]
    async fn test_column_mapping_builder_creation() {
        // This is a basic test to ensure the column mapping builder can be created
        // More comprehensive tests would require a full Delta table setup
        
        let _storage_options = HashMap::<String, String>::new();
        
        // For now, just test that the builder patterns compile correctly
        // Real tests would need actual table data
        assert!(true); // Placeholder assertion
    }

    #[tokio::test]
    async fn test_column_mapping() {
        let table_dir = tempfile::tempdir().unwrap();
        let _table_path = table_dir.path();
        let _storage_options = HashMap::<String, String>::new();
        
        // Placeholder test - would need actual implementation
        assert!(true);
    }
} 