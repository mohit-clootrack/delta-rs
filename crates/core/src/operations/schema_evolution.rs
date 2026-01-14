//! Schema evolution operations for Delta tables

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use delta_kernel::schema::ColumnMetadataKey;
use delta_kernel::table_features::{ReaderFeature, WriterFeature};
use futures::future::BoxFuture;

use super::{CustomExecuteHandler, Operation};
use crate::kernel::transaction::{CommitBuilder, CommitProperties};
use crate::kernel::{StructField, StructType, DataType};
use crate::logstore::LogStoreRef;
use crate::operations::cast::merge_schema::merge_delta_struct;
use crate::operations::write::generated_columns::{add_column_mapping_metadata, get_max_column_id};
use crate::protocol::DeltaOperation;
use crate::table::state::DeltaTableState;
use crate::{DeltaResult, DeltaTable, DeltaTableError};

/// Schema evolution builder for comprehensive schema changes
pub struct SchemaEvolutionBuilder {
    snapshot: DeltaTableState,
    log_store: LogStoreRef,
    new_columns: Vec<StructField>,
    rename_columns: Vec<(String, String)>,
    drop_columns: Vec<String>,
    type_changes: Vec<(String, DataType)>,
    enable_column_mapping: bool,
    commit_properties: CommitProperties,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl Operation<()> for SchemaEvolutionBuilder {
    fn log_store(&self) -> &LogStoreRef {
        &self.log_store
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl SchemaEvolutionBuilder {
    /// Create a new schema evolution builder
    pub fn new(log_store: LogStoreRef, snapshot: DeltaTableState) -> Self {
        Self {
            snapshot,
            log_store,
            new_columns: Vec::new(),
            rename_columns: Vec::new(),
            drop_columns: Vec::new(),
            type_changes: Vec::new(),
            enable_column_mapping: false,
            commit_properties: CommitProperties::default(),
            custom_execute_handler: None,
        }
    }

    /// Add a new column to the schema
    pub fn with_new_column(mut self, field: StructField) -> Self {
        self.new_columns.push(field);
        self
    }

    /// Add multiple new columns to the schema
    pub fn with_new_columns(mut self, fields: Vec<StructField>) -> Self {
        self.new_columns.extend(fields);
        self
    }

    /// Rename a column
    pub fn with_rename_column(mut self, old_name: String, new_name: String) -> Self {
        self.rename_columns.push((old_name, new_name));
        self.enable_column_mapping = true; // Column mapping required for renames
        self
    }

    /// Drop a column
    pub fn with_drop_column(mut self, column_name: String) -> Self {
        self.drop_columns.push(column_name);
        self
    }

    /// Change a column's data type
    pub fn with_type_change(mut self, column_name: String, new_type: DataType) -> Self {
        self.type_changes.push((column_name, new_type));
        self
    }

    /// Enable column mapping explicitly
    pub fn with_column_mapping(mut self) -> Self {
        self.enable_column_mapping = true;
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

    async fn execute_schema_evolution(self) -> DeltaResult<DeltaTable> {
        let mut metadata = self.snapshot.metadata().clone();
        let mut protocol = self.snapshot.protocol().clone();
        let mut schema = self.snapshot.schema().clone();
        let mut actions = Vec::new();

        // Check if column mapping is currently enabled
        let current_column_mapping = metadata.configuration
            .get("delta.columnMapping.mode")
            .and_then(|v| v.as_ref())
            .map(|v| v.as_str())
            .unwrap_or("none");

        let column_mapping_enabled = current_column_mapping != "none";

        // Enable column mapping if needed
        if self.enable_column_mapping && !column_mapping_enabled {
            let mut configuration = metadata.configuration.clone();
            configuration.insert("delta.columnMapping.mode".to_string(), Some("name".to_string()));
            
            let max_column_id = get_max_column_id(&schema);
            configuration.insert("delta.columnMapping.maxColumnId".to_string(), Some(max_column_id.to_string()));
            
            metadata.configuration = configuration;

            // Update protocol for column mapping
            let mut reader_features = protocol.reader_features.unwrap_or_default();
            reader_features.insert(ReaderFeature::ColumnMapping);
            
            let mut writer_features = protocol.writer_features.unwrap_or_default();
            writer_features.insert(WriterFeature::ColumnMapping);
            
            protocol.reader_features = Some(reader_features);
            protocol.writer_features = Some(writer_features);
            protocol.min_reader_version = 2;
            protocol.min_writer_version = 5;

            // Add column mapping metadata to existing schema
            schema = add_column_mapping_metadata(schema, true)?;
        }

        // Process schema changes
        if !self.new_columns.is_empty() {
            schema = self.add_new_columns(schema)?;
        }

        if !self.rename_columns.is_empty() {
            schema = self.rename_columns_in_schema(schema)?;
        }

        if !self.drop_columns.is_empty() {
            schema = self.drop_columns_from_schema(schema)?;
        }

        if !self.type_changes.is_empty() {
            schema = self.change_column_types(schema)?;
        }

        // Update metadata with new schema
        metadata.schema_string = serde_json::to_string(&schema)?;
        
        // Store configuration for protocol update
        let configuration = metadata.configuration.clone();
        actions.push(metadata.into());

        // Update protocol if needed
        let current_protocol = self.snapshot.protocol();
        let new_protocol = protocol
            .apply_column_metadata_to_protocol(&schema)?
            .move_table_properties_into_features(&configuration);

        if current_protocol != &new_protocol {
            actions.push(new_protocol.into());
        }

        // Create operation description
        let operation = DeltaOperation::SetTableProperties {
            properties: HashMap::new(), // Could be enhanced with specific operation details
        };

        let commit = CommitBuilder::from(self.commit_properties)
            .with_actions(actions)
            .build(
                Some(&self.snapshot),
                self.log_store.clone(),
                operation,
            ).await?;

        Ok(DeltaTable::new_with_state(self.log_store, commit.snapshot))
    }

    fn add_new_columns(&self, mut schema: StructType) -> DeltaResult<StructType> {
        let mut max_column_id = if self.enable_column_mapping || 
            self.snapshot.metadata().configuration
                .get("delta.columnMapping.mode")
                .and_then(|v| v.as_ref())
                .map(|v| v.as_str())
                .unwrap_or("none") != "none" {
            get_max_column_id(&schema) + 1
        } else {
            0
        };

        let new_fields_struct = StructType::new(self.new_columns.clone());
        
        // Add column mapping metadata to new columns if needed
        let new_fields = if self.enable_column_mapping || 
            self.snapshot.metadata().configuration
                .get("delta.columnMapping.mode")
                .and_then(|v| v.as_ref())
                .map(|v| v.as_str())
                .unwrap_or("none") != "none" {
            
            let mut fields_with_mapping = Vec::new();
            for field in new_fields_struct.fields() {
                let mut metadata = field.metadata().clone();
                
                metadata.insert(
                    ColumnMetadataKey::ColumnMappingId.as_ref().to_string(),
                    max_column_id.into(),
                );
                
                metadata.insert(
                    ColumnMetadataKey::ColumnMappingPhysicalName.as_ref().to_string(),
                    format!("col-{}", max_column_id).into(),
                );
                
                max_column_id += 1;
                
                let new_field = StructField::new(
                    field.name().clone(),
                    field.data_type().clone(),
                    field.is_nullable(),
                ).with_metadata(metadata);
                
                fields_with_mapping.push(new_field);
            }
            fields_with_mapping
        } else {
            self.new_columns.clone()
        };

        // Merge new columns into the schema
        let fields_struct = StructType::new(new_fields);
        schema = merge_delta_struct(&schema, &fields_struct)?;

        Ok(schema)
    }

    fn rename_columns_in_schema(&self, schema: StructType) -> DeltaResult<StructType> {
        let mut new_fields = Vec::new();
        
        for field in schema.fields() {
            let mut new_field = field.clone();
            
            // Check if this field should be renamed
            for (old_name, new_name) in &self.rename_columns {
                if field.name() == old_name {
                    new_field = StructField::new(
                        new_name.clone(),
                        field.data_type().clone(),
                        field.is_nullable(),
                    ).with_metadata(field.metadata().clone());
                    break;
                }
            }
            
            new_fields.push(new_field);
        }
        
        Ok(StructType::new(new_fields))
    }

    fn drop_columns_from_schema(&self, schema: StructType) -> DeltaResult<StructType> {
        let drop_set: HashSet<&String> = self.drop_columns.iter().collect();
        let remaining_fields: Vec<StructField> = schema.fields()
            .filter(|field| !drop_set.contains(field.name()))
            .cloned()
            .collect();
        
        if remaining_fields.is_empty() {
            return Err(DeltaTableError::Generic(
                "Cannot drop all columns from the table".to_string()
            ));
        }
        
        Ok(StructType::new(remaining_fields))
    }

    fn change_column_types(&self, schema: StructType) -> DeltaResult<StructType> {
        let type_changes: HashMap<&String, &DataType> = self.type_changes.iter()
            .map(|(name, data_type)| (name, data_type))
            .collect();
        
        let mut new_fields = Vec::new();
        
        for field in schema.fields() {
            let new_field = if let Some(new_type) = type_changes.get(field.name()) {
                // Validate type compatibility
                if !self.is_type_change_safe(field.data_type(), new_type) {
                    return Err(DeltaTableError::Generic(
                        format!("Unsafe type change from {:?} to {:?} for column {}", 
                               field.data_type(), new_type, field.name())
                    ));
                }
                
                StructField::new(
                    field.name().clone(),
                    (*new_type).clone(),
                    field.is_nullable(),
                ).with_metadata(field.metadata().clone())
            } else {
                field.clone()
            };
            
            new_fields.push(new_field);
        }
        
        Ok(StructType::new(new_fields))
    }

    fn is_type_change_safe(&self, from: &DataType, to: &DataType) -> bool {
        match (from, to) {
            // Safe widening conversions
            (&DataType::BYTE, &DataType::SHORT) | 
            (&DataType::BYTE, &DataType::INTEGER) | 
            (&DataType::BYTE, &DataType::LONG) => true,
            (&DataType::SHORT, &DataType::INTEGER) | 
            (&DataType::SHORT, &DataType::LONG) => true,
            (&DataType::INTEGER, &DataType::LONG) => true,
            (&DataType::FLOAT, &DataType::DOUBLE) => true,
            
            // String conversions (generally safe for display)
            (_, &DataType::STRING) => true,
            
            // Same type conversions
            (t1, t2) if t1 == t2 => true,
            
            // All other conversions are potentially unsafe
            _ => false,
        }
    }
}

impl std::future::IntoFuture for SchemaEvolutionBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            self.execute_schema_evolution().await
        })
    }
}

/// Enhanced write operations with automatic schema evolution
pub struct SchemaAwareWriteBuilder {
    write_builder: crate::operations::write::WriteBuilder,
    auto_evolve_schema: bool,
    allow_type_widening: bool,
    enable_column_mapping: bool,
}

impl SchemaAwareWriteBuilder {
    pub fn new(write_builder: crate::operations::write::WriteBuilder) -> Self {
        Self {
            write_builder,
            auto_evolve_schema: false,
            allow_type_widening: false,
            enable_column_mapping: false,
        }
    }

    /// Enable automatic schema evolution for new columns
    pub fn with_auto_schema_evolution(mut self) -> Self {
        self.auto_evolve_schema = true;
        self
    }

    /// Allow automatic type widening (e.g., int32 -> int64)
    pub fn with_type_widening(mut self) -> Self {
        self.allow_type_widening = true;
        self
    }

    /// Enable column mapping for schema changes
    pub fn with_column_mapping(mut self) -> Self {
        self.enable_column_mapping = true;
        self
    }

    /// Execute the write with schema evolution
    pub async fn execute(mut self) -> DeltaResult<DeltaTable> {
        if self.auto_evolve_schema {
            self.write_builder = self.write_builder.with_schema_mode(crate::operations::write::SchemaMode::Merge);
        }

        if self.enable_column_mapping {
            // Add column mapping configuration
            self.write_builder = self.write_builder.with_table_configuration(
                "delta.columnMapping.mode".to_string(),
                Some("name".to_string()),
            );
        }

        // Execute the write operation
        self.write_builder.await
    }
}

/// Merge operations with enhanced schema evolution
pub struct SchemaAwareMergeBuilder {
    merge_builder: crate::operations::merge::MergeBuilder,
    handle_new_columns: bool,
    handle_type_changes: bool,
    enable_column_mapping: bool,
}

impl SchemaAwareMergeBuilder {
    pub fn new(merge_builder: crate::operations::merge::MergeBuilder) -> Self {
        Self {
            merge_builder,
            handle_new_columns: true,
            handle_type_changes: false,
            enable_column_mapping: false,
        }
    }

    /// Enable handling of new columns during merge
    pub fn with_new_columns(mut self, enabled: bool) -> Self {
        self.handle_new_columns = enabled;
        self
    }

    /// Enable handling of type changes during merge
    pub fn with_type_changes(mut self, enabled: bool) -> Self {
        self.handle_type_changes = enabled;
        self
    }

    /// Enable column mapping for schema changes
    pub fn with_column_mapping(mut self) -> Self {
        self.enable_column_mapping = true;
        self
    }

    /// Execute the merge with schema evolution
    pub async fn execute(mut self) -> DeltaResult<DeltaTable> {
        if self.handle_new_columns {
            self.merge_builder = self.merge_builder.with_merge_schema(true);
        }

        // Execute the merge operation and return just the table
        let (table, _metrics) = self.merge_builder.await?;
        Ok(table)
    }
} 