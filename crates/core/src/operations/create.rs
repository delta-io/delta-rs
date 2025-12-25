//! Command for creating a new delta table
// https://github.com/delta-io/delta/blob/master/core/src/main/scala/org/apache/spark/sql/delta/commands/CreateDeltaTableCommand.scala

use std::collections::HashMap;
use std::sync::Arc;

use delta_kernel::schema::MetadataValue;
use futures::TryStreamExt as _;
use futures::future::BoxFuture;
use serde_json::Value;
use uuid::Uuid;

use super::{CustomExecuteHandler, Operation};
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::transaction::{CommitBuilder, CommitProperties, PROTOCOL, TableReference};
use crate::kernel::{
    Action, DataType, MetadataExt, ProtocolExt as _, ProtocolInner, StructField, StructType,
    new_metadata,
};
use crate::logstore::LogStoreRef;
use crate::protocol::{DeltaOperation, SaveMode};
use crate::table::builder::ensure_table_uri;
use crate::table::config::TableProperty;
use crate::table::normalize_table_url;
use crate::{DeltaTable, DeltaTableBuilder};

#[derive(thiserror::Error, Debug)]
enum CreateError {
    #[error("Location must be provided to create a table.")]
    MissingLocation,

    #[error("At least one column must be defined to create a table.")]
    MissingSchema,

    #[error("Please configure table meta data via the CreateBuilder.")]
    MetadataSpecified,

    #[error("A Delta Lake table already exists at that location.")]
    TableAlreadyExists,

    #[error("SaveMode `append` is not allowed for create operation.")]
    AppendNotAllowed,
}

impl From<CreateError> for DeltaTableError {
    fn from(err: CreateError) -> Self {
        DeltaTableError::GenericError {
            source: Box::new(err),
        }
    }
}

/// Build an operation to create a new [DeltaTable]
#[derive(Clone)]
pub struct CreateBuilder {
    name: Option<String>,
    location: Option<String>,
    mode: SaveMode,
    comment: Option<String>,
    columns: Vec<StructField>,
    partition_columns: Option<Vec<String>>,
    storage_options: Option<HashMap<String, String>>,
    actions: Vec<Action>,
    log_store: Option<LogStoreRef>,
    configuration: HashMap<String, Option<String>>,
    /// Additional information to add to the commit
    commit_properties: CommitProperties,
    raise_if_key_not_exists: bool,
    custom_execute_handler: Option<Arc<dyn CustomExecuteHandler>>,
}

impl super::Operation for CreateBuilder {
    fn log_store(&self) -> &LogStoreRef {
        self.log_store
            .as_ref()
            .expect("Logstore shouldn't be none at this stage.")
    }
    fn get_custom_execute_handler(&self) -> Option<Arc<dyn CustomExecuteHandler>> {
        self.custom_execute_handler.clone()
    }
}

impl Default for CreateBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CreateBuilder {
    /// Create a new [`CreateBuilder`]
    pub fn new() -> Self {
        Self {
            name: None,
            location: None,
            mode: SaveMode::ErrorIfExists,
            comment: None,
            columns: Default::default(),
            partition_columns: None,
            storage_options: None,
            actions: Default::default(),
            log_store: None,
            configuration: Default::default(),
            commit_properties: CommitProperties::default(),
            raise_if_key_not_exists: true,
            custom_execute_handler: None,
        }
    }

    /// Specify the table name. Optionally qualified with
    /// a database name [database_name.] table_name.
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Specify the path to the location where table data is stored,
    /// which could be a path on distributed storage.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Specify the behavior when a table exists at location
    pub fn with_save_mode(mut self, save_mode: SaveMode) -> Self {
        self.mode = save_mode;
        self
    }

    /// Comment to describe the table.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
        self
    }

    /// Specify a column in the table
    pub fn with_column(
        mut self,
        name: impl Into<String>,
        data_type: DataType,
        nullable: bool,
        metadata: Option<HashMap<String, Value>>,
    ) -> Self {
        let mut field = StructField::new(name.into(), data_type, nullable);
        if let Some(meta) = metadata {
            field = field.with_metadata(meta.iter().map(|(k, v)| {
                (
                    k,
                    if let Value::Number(n) = v {
                        n.as_i64().map_or_else(
                            || MetadataValue::String(v.to_string()),
                            MetadataValue::Number,
                        )
                    } else {
                        MetadataValue::String(v.to_string())
                    },
                )
            }));
        };
        self.columns.push(field);
        self
    }

    /// Specify columns to append to schema
    pub fn with_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<StructField>>,
    ) -> Self {
        self.columns.extend(columns.into_iter().map(|c| c.into()));
        self
    }

    /// Specify table partitioning
    pub fn with_partition_columns(
        mut self,
        partition_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.partition_columns = Some(partition_columns.into_iter().map(|s| s.into()).collect());
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables.
    ///
    /// [crate::table::builder::s3_storage_options] describes the available options for the AWS or S3-compliant backend.
    /// If an object store is also passed using `with_object_store()` these options will be ignored.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    /// Set configuration on created table
    pub fn with_configuration(
        mut self,
        configuration: impl IntoIterator<Item = (impl Into<String>, Option<impl Into<String>>)>,
    ) -> Self {
        self.configuration = configuration
            .into_iter()
            .map(|(k, v)| (k.into(), v.map(|s| s.into())))
            .collect();
        self
    }

    /// Specify a table property in the table configuration
    pub fn with_configuration_property(
        mut self,
        key: TableProperty,
        value: Option<impl Into<String>>,
    ) -> Self {
        self.configuration
            .insert(key.as_ref().into(), value.map(|v| v.into()));
        self
    }

    /// Additional metadata to be added to commit info
    pub fn with_commit_properties(mut self, commit_properties: CommitProperties) -> Self {
        self.commit_properties = commit_properties;
        self
    }

    /// Specify whether to raise an error if the table properties in the configuration are not TableProperties
    pub fn with_raise_if_key_not_exists(mut self, raise_if_key_not_exists: bool) -> Self {
        self.raise_if_key_not_exists = raise_if_key_not_exists;
        self
    }

    /// Specify additional actions to be added to the commit.
    ///
    /// This method is mainly meant for internal use. Manually adding inconsistent
    /// actions to a create operation may have undesired effects - use with caution.
    pub fn with_actions(mut self, actions: impl IntoIterator<Item = Action>) -> Self {
        self.actions.extend(actions);
        self
    }

    /// Provide a [`LogStore`] instance
    pub fn with_log_store(mut self, log_store: LogStoreRef) -> Self {
        self.log_store = Some(log_store);
        self
    }

    /// Set a custom execute handler, for pre and post execution
    pub fn with_custom_execute_handler(mut self, handler: Arc<dyn CustomExecuteHandler>) -> Self {
        self.custom_execute_handler = Some(handler);
        self
    }

    /// Consume self into uninitialized table with corresponding create actions and operation meta
    pub(crate) async fn into_table_and_actions(
        mut self,
    ) -> DeltaResult<(DeltaTable, Vec<Action>, DeltaOperation, Uuid)> {
        if self
            .actions
            .iter()
            .any(|a| matches!(a, Action::Metadata(_)))
        {
            return Err(CreateError::MetadataSpecified.into());
        }
        if self.columns.is_empty() {
            return Err(CreateError::MissingSchema.into());
        }

        let (storage_url, table) = if let Some(log_store) = self.log_store {
            (
                normalize_table_url(log_store.root_url()),
                DeltaTable::new(log_store, Default::default()),
            )
        } else {
            let storage_url =
                ensure_table_uri(self.location.clone().ok_or(CreateError::MissingLocation)?)?;
            (
                storage_url.clone(),
                DeltaTableBuilder::from_url(storage_url)?
                    .with_storage_options(self.storage_options.clone().unwrap_or_default())
                    .build()?,
            )
        };

        self.log_store = Some(table.log_store());
        let operation_id = self.get_operation_id();
        self.pre_execute(operation_id).await?;

        let configuration = self
            .configuration
            .iter()
            .filter_map(|(k, v)| Some((k.to_string(), v.as_ref()?.to_string())))
            .collect();

        let current_protocol = ProtocolInner {
            min_reader_version: PROTOCOL.default_reader_version(),
            min_writer_version: PROTOCOL.default_writer_version(),
            reader_features: None,
            writer_features: None,
        }
        .as_kernel();

        let protocol = self
            .actions
            .iter()
            .find(|a| matches!(a, Action::Protocol(_)))
            .map(|a| match a {
                Action::Protocol(p) => p.clone(),
                _ => unreachable!(),
            })
            .unwrap_or_else(|| current_protocol);

        let schema = StructType::try_new(self.columns)?;

        let protocol = protocol
            .apply_properties_to_protocol(&configuration, self.raise_if_key_not_exists)?
            .apply_column_metadata_to_protocol(&schema)?
            .move_table_properties_into_features(&configuration);

        let mut metadata = new_metadata(
            &schema,
            self.partition_columns.unwrap_or_default(),
            configuration,
        )?;
        if let Some(name) = self.name {
            metadata = metadata.with_name(name)?;
        }
        if let Some(comment) = self.comment {
            metadata = metadata.with_description(comment)?;
        }

        let operation = DeltaOperation::Create {
            mode: self.mode,
            metadata: metadata.clone(),
            location: storage_url,
            protocol: protocol.clone(),
        };

        let mut actions = vec![Action::Protocol(protocol), Action::Metadata(metadata)];

        actions.extend(
            self.actions
                .into_iter()
                .filter(|a| !matches!(a, Action::Protocol(_))),
        );

        Ok((table, actions, operation, operation_id))
    }
}

impl std::future::IntoFuture for CreateBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;
        Box::pin(async move {
            let handler = this.custom_execute_handler.clone();
            let mode = &this.mode;
            let (mut table, mut actions, operation, operation_id) =
                this.clone().into_table_and_actions().await?;

            let table_state = if table.log_store.is_delta_table_location().await? {
                match mode {
                    SaveMode::ErrorIfExists => return Err(CreateError::TableAlreadyExists.into()),
                    SaveMode::Append => return Err(CreateError::AppendNotAllowed.into()),
                    SaveMode::Ignore => {
                        table.load().await?;
                        return Ok(table);
                    }
                    SaveMode::Overwrite => {
                        table.load().await?;
                        let remove_actions = table
                            .snapshot()?
                            .snapshot()
                            .file_views(&table.log_store(), None)
                            .map_ok(|p| p.remove_action(true).into())
                            .try_collect::<Vec<_>>()
                            .await?;
                        actions.extend(remove_actions);
                        Some(table.snapshot()?)
                    }
                }
            } else {
                None
            };

            let version = CommitBuilder::from(this.commit_properties.clone())
                .with_actions(actions)
                .with_operation_id(operation_id)
                .with_post_commit_hook_handler(handler.clone())
                .build(
                    table_state.map(|f| f as &dyn TableReference),
                    table.log_store.clone(),
                    operation,
                )
                .await?
                .version();
            table.load_version(version).await?;

            if let Some(handler) = handler {
                handler
                    .post_execute(&table.log_store(), operation_id)
                    .await?;
            }
            Ok(table)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table::config::TableProperty;
    use crate::writer::test_utils::get_delta_schema;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_create() {
        let table_schema = get_delta_schema();

        let table = DeltaTable::new_in_memory()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.snapshot().unwrap().schema().as_ref(), &table_schema)
    }

    #[tokio::test]
    async fn test_create_local_relative_path() {
        let table_schema = get_delta_schema();
        let tmp_dir = TempDir::new_in(".").unwrap();
        let relative_path = format!(
            "./{}",
            tmp_dir.path().file_name().unwrap().to_str().unwrap()
        );
        let table_path = std::path::Path::new(&relative_path).canonicalize().unwrap();
        let table_url = url::Url::from_directory_path(table_path)
            .map_err(|_| DeltaTableError::InvalidTableLocation(relative_path.clone()))
            .unwrap();
        let table = DeltaTable::try_from_url(table_url)
            .await
            .unwrap()
            .create()
            .with_columns(table_schema.fields().cloned())
            .with_save_mode(SaveMode::Ignore)
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
        assert_eq!(table.snapshot().unwrap().schema().as_ref(), &table_schema)
    }

    #[tokio::test]
    async fn test_create_table_local_path() {
        let schema = get_delta_schema();
        let tmp_dir = TempDir::new_in(".").unwrap();
        let relative_path = format!(
            "./{}",
            tmp_dir.path().file_name().unwrap().to_str().unwrap()
        );
        let table = CreateBuilder::new()
            .with_location(format!("./{relative_path}"))
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        assert_eq!(table.version(), Some(0));
    }

    #[tokio::test]
    async fn test_create_table_metadata() {
        let schema = get_delta_schema();
        let table = CreateBuilder::new()
            .with_location("memory:///")
            .with_columns(schema.fields().cloned())
            .await
            .unwrap();
        let snapshot = table.snapshot().unwrap();
        assert_eq!(snapshot.version(), 0);
        assert_eq!(
            snapshot.protocol().min_reader_version(),
            PROTOCOL.default_reader_version()
        );
        assert_eq!(
            snapshot.protocol().min_writer_version(),
            PROTOCOL.default_writer_version()
        );
        assert_eq!(snapshot.schema().as_ref(), &schema);

        // check we can overwrite default settings via adding actions
        let protocol = ProtocolInner {
            min_reader_version: 1,
            min_writer_version: 2,
            writer_features: None,
            reader_features: None,
        }
        .as_kernel();
        let table = CreateBuilder::new()
            .with_location("memory:///")
            .with_columns(schema.fields().cloned())
            .with_actions(vec![Action::Protocol(protocol)])
            .await
            .unwrap();
        let snapshot = table.snapshot().unwrap();
        assert_eq!(snapshot.protocol().min_reader_version(), 1);
        assert_eq!(snapshot.protocol().min_writer_version(), 2);

        let table = CreateBuilder::new()
            .with_location("memory:///")
            .with_columns(schema.fields().cloned())
            .with_configuration_property(TableProperty::AppendOnly, Some("true"))
            .await
            .unwrap();
        let append = table
            .snapshot()
            .unwrap()
            .metadata()
            .configuration()
            .get(TableProperty::AppendOnly.as_ref())
            .unwrap()
            .clone();
        assert_eq!(String::from("true"), append)
    }

    #[cfg(feature = "datafusion")]
    mod datafusion_tests {
        use super::*;

        use crate::writer::test_utils::get_record_batch;
        #[tokio::test]
        async fn test_create_table_save_mode() {
            let tmp_dir = tempfile::tempdir().unwrap();

            let schema = get_delta_schema();
            let table = CreateBuilder::new()
                .with_location(tmp_dir.path().to_str().unwrap())
                .with_columns(schema.fields().cloned())
                .await
                .unwrap();
            assert_eq!(table.version(), Some(0));
            let first_id = table.snapshot().unwrap().metadata().id().to_string();

            let log_store = table.log_store;

            // Check an error is raised when a table exists at location
            let table = CreateBuilder::new()
                .with_log_store(log_store.clone())
                .with_columns(schema.fields().cloned())
                .with_save_mode(SaveMode::ErrorIfExists)
                .await;
            assert!(table.is_err());

            // Check current table is returned when ignore option is chosen.
            let table = CreateBuilder::new()
                .with_log_store(log_store.clone())
                .with_columns(schema.fields().cloned())
                .with_save_mode(SaveMode::Ignore)
                .await
                .unwrap();
            assert_eq!(table.snapshot().unwrap().metadata().id(), first_id);

            // Check table is overwritten
            let table = CreateBuilder::new()
                .with_log_store(log_store)
                .with_columns(schema.fields().cloned())
                .with_save_mode(SaveMode::Overwrite)
                .await
                .unwrap();
            assert_ne!(table.snapshot().unwrap().metadata().id(), first_id)
        }

        #[tokio::test]
        async fn test_create_or_replace_existing_table() {
            let batch = get_record_batch(None, false);
            let schema = get_delta_schema();
            let table = DeltaTable::new_in_memory()
                .write(vec![batch.clone()])
                .with_save_mode(SaveMode::ErrorIfExists)
                .await
                .unwrap();
            let state = table.snapshot().unwrap();
            assert_eq!(state.version(), 0);
            assert_eq!(state.log_data().num_files(), 1);

            let mut table = table
                .create()
                .with_columns(schema.fields().cloned())
                .with_save_mode(SaveMode::Overwrite)
                .await
                .unwrap();
            table.load().await.unwrap();
            let state = table.snapshot().unwrap();
            assert_eq!(state.version(), 1);
            // Checks if files got removed after overwrite
            assert_eq!(state.log_data().num_files(), 0);
        }

        #[tokio::test]
        async fn test_create_or_replace_existing_table_partitioned() {
            let batch = get_record_batch(None, false);
            let schema = get_delta_schema();
            let table = DeltaTable::new_in_memory()
                .write(vec![batch.clone()])
                .with_save_mode(SaveMode::ErrorIfExists)
                .await
                .unwrap();
            let state = table.snapshot().unwrap();
            assert_eq!(state.version(), 0);
            assert_eq!(state.log_data().num_files(), 1);

            let mut table = table
                .create()
                .with_columns(schema.fields().cloned())
                .with_save_mode(SaveMode::Overwrite)
                .with_partition_columns(vec!["id"])
                .await
                .unwrap();
            table.load().await.unwrap();
            let state = table.snapshot().unwrap();
            assert_eq!(state.version(), 1);
            // Checks if files got removed after overwrite
            assert_eq!(state.log_data().num_files(), 0);
        }

        #[tokio::test]
        async fn test_create_table_metadata_raise_if_key_not_exists() {
            let schema = get_delta_schema();
            let config: HashMap<String, Option<String>> =
                vec![("key".to_string(), Some("value".to_string()))]
                    .into_iter()
                    .collect();

            // Fail to create table with unknown Delta key
            let table = CreateBuilder::new()
                .with_location("memory:///")
                .with_columns(schema.fields().cloned())
                .with_configuration(config.clone())
                .await;
            assert!(table.is_err());

            // Succeed in creating table with unknown Delta key since we set raise_if_key_not_exists to false
            let table = CreateBuilder::new()
                .with_location("memory:///")
                .with_columns(schema.fields().cloned())
                .with_raise_if_key_not_exists(false)
                .with_configuration(config)
                .await;
            assert!(table.is_ok());

            // Ensure the non-Delta key was set correctly
            let value = table
                .unwrap()
                .snapshot()
                .unwrap()
                .metadata()
                .configuration()
                .get("key")
                .unwrap()
                .clone();
            assert_eq!(String::from("value"), value);
        }
    }
}
