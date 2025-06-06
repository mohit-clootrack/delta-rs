//! Column mapping operations for Delta tables
use std::collections::HashSet;
use std::sync::Arc;

use delta_kernel::table_features::{ColumnMappingMode, ReaderFeature, WriterFeature};
use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{StructField, StructType, ColumnMetadataKey};
use crate::logstore::LogStoreRef;
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable};

/// Builder for column mapping operations
pub struct ColumnMappingBuilder {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    mode: Option<ColumnMappingMode>,
    rename_columns: Vec<(String, String)>,
    drop_columns: Vec<String>,
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl Operation<()> for ColumnMappingBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl ColumnMappingBuilder {
    /// Create a new column mapping builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            mode: None,
            rename_columns: Vec::new(),
            drop_columns: Vec::new(),
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Set the column mapping mode
    pub fn with_mode(mut self, mode: ColumnMappingMode) -> Self {
        self.mode = Some(mode);
        self
    }

    /// Add a column rename operation
    pub fn with_rename_column(mut self, old_name: String, new_name: String) -> Self {
        self.rename_columns.push((old_name, new_name));
        self
    }

    /// Add a column drop operation
    pub fn with_drop_column(mut self, column_name: String) -> Self {
        self.drop_columns.push(column_name);
        self
    }

    /// Set commit properties
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Set custom execute handler
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    async fn enable_column_mapping(self, mode: ColumnMappingMode) -> DeltaResult<DeltaTable> {
        let mut metadata = self.snapshot.metadata().clone();
        let mut protocol = self.snapshot.protocol().clone();
        
        // Check current column mapping mode
        let current_mode = metadata.configuration
            .get("delta.columnMapping.mode")
            .and_then(|v| v.as_ref())
            .map(|v| match v.as_str() {
                "name" => ColumnMappingMode::Name,
                "id" => ColumnMappingMode::Id,
                _ => ColumnMappingMode::None,
            })
            .unwrap_or(ColumnMappingMode::None);

        if current_mode == ColumnMappingMode::None && mode != ColumnMappingMode::None {
            // Enable column mapping
            let mut configuration = metadata.configuration.clone();
            
            // Convert ColumnMappingMode to string representation
            let mode_str = match mode {
                ColumnMappingMode::None => "none",
                ColumnMappingMode::Name => "name",
                ColumnMappingMode::Id => "id",
            };

            configuration.insert("delta.columnMapping.mode".to_string(), Some(mode_str.to_string()));
            
            let max_column_id = self.get_max_column_id()?;
            configuration.insert("delta.columnMapping.maxColumnId".to_string(), Some(max_column_id.to_string()));

            metadata.configuration = configuration;

            // Update protocol to support column mapping
            let mut reader_features = HashSet::new();
            reader_features.insert(ReaderFeature::ColumnMapping);
            
            let mut writer_features = HashSet::new();
            writer_features.insert(WriterFeature::ColumnMapping);
            
            protocol.reader_features = Some(reader_features);
            protocol.writer_features = Some(writer_features);
            protocol.min_reader_version = 2;
            protocol.min_writer_version = 5;

            // Add column mapping metadata to schema
            if current_mode == ColumnMappingMode::None {
                let new_schema = self.add_column_mapping_metadata(&self.snapshot.schema(), mode)?;
                metadata.schema_string = serde_json::to_string(&new_schema)?;
            }

            let operation = DeltaOperation::SetTableProperties {
                properties: std::collections::HashMap::from([
                    ("delta.columnMapping.mode".to_string(), mode_str.to_string()),
                    ("delta.columnMapping.maxColumnId".to_string(), max_column_id.to_string()),
                ]),
            };

            let actions = vec![metadata.into(), protocol.into()];

            let commit = CommitBuilder::from(self.commit_properties)
                .with_actions(actions)
                .build(
                    Some(&self.snapshot),
                    self.log_store.clone(),
                    operation,
                ).await?;

            Ok(DeltaTable::new_with_state(self.log_store, commit.snapshot))
        } else {
            // Column mapping already enabled or no change needed
            Ok(DeltaTable::new_with_state(self.log_store, self.snapshot))
        }
    }

    fn get_max_column_id(&self) -> DeltaResult<i64> {
        let current_max = self.snapshot.metadata().configuration
            .get("delta.columnMapping.maxColumnId")
            .and_then(|v| v.as_ref())
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(15); // Default to 15 if not found

        Ok(current_max)
    }

    fn add_column_mapping_metadata(&self, schema: &StructType, _mode: ColumnMappingMode) -> DeltaResult<StructType> {
        let mut max_column_id = 1i64;
        let mut new_fields = Vec::new();

        for field in schema.fields() {
            let mut metadata = field.metadata().clone();
            
            if !metadata.contains_key(ColumnMetadataKey::ColumnMappingId.as_ref()) {
                metadata.insert(
                    ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                    max_column_id.into(),
                );
                max_column_id += 1;
            }

            if !metadata.contains_key(ColumnMetadataKey::ColumnMappingPhysicalName.as_ref()) {
                metadata.insert(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().to_string(),
                    format!("col-{}", max_column_id - 1).into(),
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
}

impl std::future::IntoFuture for ColumnMappingBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let mode = self.mode.unwrap_or(ColumnMappingMode::Name);
        
        Box::pin(async move {
            self.enable_column_mapping(mode).await
        })
    }
}

impl From<ColumnMappingBuilder> for DeltaOperation {
    fn from(_builder: ColumnMappingBuilder) -> Self {
        DeltaOperation::SetTableProperties {
            properties: std::collections::HashMap::new(),
        }
    }
} 