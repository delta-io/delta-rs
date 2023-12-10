//! Command for converting a Parquet table to a Delta table in place
// https://github.com/delta-io/delta/blob/1d5dd774111395b0c4dc1a69c94abc169b1c83b6/spark/src/main/scala/org/apache/spark/sql/delta/commands/ConvertToDeltaCommand.scala

use crate::{
    kernel::{Action, Add, Schema, StructField},
    logstore::{LogStore, LogStoreRef},
    operations::create::CreateBuilder,
    protocol::SaveMode,
    storage::config::configure_log_store,
    table::builder::ensure_table_uri,
    table::config::DeltaConfigKey,
    DeltaResult, DeltaTable, DeltaTableError, DeltaTablePartition, ObjectStoreError,
    NULL_PARTITION_VALUE_DATA_PATH,
};
use arrow::{datatypes::Schema as ArrowSchema, error::ArrowError};
use futures::{
    future::{self, BoxFuture},
    TryStreamExt,
};
use log::{debug, info};
use parquet::{
    arrow::async_reader::{ParquetObjectReader, ParquetRecordBatchStreamBuilder},
    errors::ParquetError,
};
use percent_encoding::percent_decode_str;
use serde_json::{Map, Value};
use std::{
    collections::{HashMap, HashSet},
    num::TryFromIntError,
    str::{FromStr, Utf8Error},
    sync::Arc,
};

/// Error converting a Parquet table to a Delta table
#[derive(Debug, thiserror::Error)]
enum Error {
    #[error("Object store error: {0}")]
    ObjectStore(#[from] ObjectStoreError),
    #[error("Arrow error: {0}")]
    Arrow(#[from] ArrowError),
    #[error("Parquet error: {0}")]
    Parquet(#[from] ParquetError),
    #[error("DeltaTable error: {0}")]
    DeltaTable(#[from] DeltaTableError),
    #[error("Error percent-decoding as UTF-8: {0}")]
    PercentDecode(#[from] Utf8Error),
    #[error("Error converting usize to i64: {0}")]
    TryFromUsize(#[from] TryFromIntError),
    #[error("No parquet file is found in the given location")]
    ParquetFileNotFound,
    #[error("The schema of partition columns must be provided to convert a Parquet table to a Delta table")]
    MissingPartitionSchema,
    #[error("Partition column provided by the user does not exist in the parquet files")]
    PartitionColumnNotExist(HashSet<StructField>),
    #[error("The given location is already a delta table location")]
    DeltaTableAlready,
    #[error("Location must be provided to convert a Parquet table to a Delta table")]
    MissingLocation,
}

impl From<Error> for DeltaTableError {
    fn from(err: Error) -> Self {
        match err {
            Error::ObjectStore(e) => DeltaTableError::ObjectStore { source: e },
            Error::Arrow(e) => DeltaTableError::Arrow { source: e },
            Error::Parquet(e) => DeltaTableError::Parquet { source: e },
            Error::DeltaTable(e) => e,
            _ => DeltaTableError::GenericError {
                source: Box::new(err),
            },
        }
    }
}

/// The partition strategy used by the Parquet table
/// Currently only hive-partitioning is supproted for Parquet paths
#[non_exhaustive]
#[derive(Default)]
pub enum PartitionStrategy {
    /// Hive-partitioning
    #[default]
    Hive,
}

impl FromStr for PartitionStrategy {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "hive" => Ok(PartitionStrategy::Hive),
            _ => Err(DeltaTableError::Generic(format!(
                "Invalid partition strategy provided {}",
                s
            ))),
        }
    }
}

/// Build an operation to convert a Parquet table to a [`DeltaTable`] in place
pub struct ConvertToDeltaBuilder {
    log_store: Option<LogStoreRef>,
    location: Option<String>,
    storage_options: Option<HashMap<String, String>>,
    partition_schema: HashSet<StructField>,
    partition_strategy: PartitionStrategy,
    mode: SaveMode,
    name: Option<String>,
    comment: Option<String>,
    configuration: HashMap<String, Option<String>>,
    metadata: Option<Map<String, Value>>,
}

impl Default for ConvertToDeltaBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl ConvertToDeltaBuilder {
    /// Create a new [`ConvertToDeltaBuilder`]
    pub fn new() -> Self {
        Self {
            log_store: None,
            location: None,
            storage_options: None,
            partition_schema: Default::default(),
            partition_strategy: Default::default(),
            mode: SaveMode::ErrorIfExists,
            name: None,
            comment: None,
            configuration: Default::default(),
            metadata: Default::default(),
        }
    }

    /// Provide a [`LogStore`] instance, that points at table location
    pub fn with_log_store(mut self, log_store: Arc<dyn LogStore>) -> Self {
        self.log_store = Some(log_store);
        self
    }

    /// Specify the path to the location where table data is stored,
    /// which could be a path on distributed storage.
    ///
    /// If an object store is also passed using `with_log_store()`, this path will be ignored.
    pub fn with_location(mut self, location: impl Into<String>) -> Self {
        self.location = Some(location.into());
        self
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables.
    ///
    /// [crate::table::builder::s3_storage_options] describes the available options for the AWS or S3-compliant backend.
    /// [dynamodb_lock::DynamoDbLockClient] describes additional options for the AWS atomic rename client.
    ///
    /// If an object store is also passed using `with_log_store()`, these options will be ignored.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        self.storage_options = Some(storage_options);
        self
    }

    /// Specify the partition schema of the Parquet table
    pub fn with_partition_schema(
        mut self,
        partition_schema: impl IntoIterator<Item = StructField>,
    ) -> Self {
        self.partition_schema = HashSet::from_iter(partition_schema);
        self
    }

    /// Specify the partition strategy of the Parquet table
    /// Currently only hive-partitioning is supproted for Parquet paths
    pub fn with_partition_strategy(mut self, strategy: PartitionStrategy) -> Self {
        self.partition_strategy = strategy;
        self
    }
    /// Specify the behavior when a table exists at location
    pub fn with_save_mode(mut self, save_mode: SaveMode) -> Self {
        self.mode = save_mode;
        self
    }

    /// Specify the table name. Optionally qualified with
    /// a database name [database_name.] table_name.
    pub fn with_table_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Comment to describe the table.
    pub fn with_comment(mut self, comment: impl Into<String>) -> Self {
        self.comment = Some(comment.into());
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
        key: DeltaConfigKey,
        value: Option<impl Into<String>>,
    ) -> Self {
        self.configuration
            .insert(key.as_ref().into(), value.map(|v| v.into()));
        self
    }

    /// Append custom (application-specific) metadata to the commit.
    ///
    /// This might include provenance information such as an id of the
    /// user that made the commit or the program that created it.
    pub fn with_metadata(mut self, metadata: Map<String, Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Consume self into CreateBuilder with corresponding add actions, schemas and operation meta
    async fn into_create_builder(mut self) -> Result<CreateBuilder, Error> {
        // Use the specified log store. If a log store is not provided, create a new store from the specified path.
        // Return an error if neither log store nor path is provided
        let log_store = if let Some(log_store) = self.log_store {
            log_store
        } else if let Some(location) = self.location {
            configure_log_store(
                ensure_table_uri(location)?,
                self.storage_options.unwrap_or_default(),
            )?
        } else {
            return Err(Error::MissingLocation);
        };

        // Return an error if the location is already a Delta table location
        if log_store.is_delta_table_location().await? {
            return Err(Error::DeltaTableAlready);
        }
        info!(
            "Converting Parquet table in log store location: {:?}",
            log_store.root_uri()
        );

        // Get all the parquet files in the location
        let object_store = log_store.object_store();
        let mut files = Vec::new();
        object_store
            .list(None)
            .await?
            .try_for_each_concurrent(10, |meta| {
                if Some("parquet") == meta.location.extension() {
                    debug!("Found parquet file {:#?}", meta.location);
                    files.push(meta);
                }
                future::ready(Ok(()))
            })
            .await?;
        if files.is_empty() {
            return Err(Error::ParquetFileNotFound);
        }

        // Iterate over the parquet files. Parse partition columns, generate add actions and collect parquet file schemas
        let mut arrow_schemas = Vec::new();
        let mut actions = Vec::new();
        // A HashSet of all unique partition columns in a Parquet table
        let mut partition_columns = HashSet::new();
        // A vector of StructField of all unique partition columns in a Parquet table
        let mut partition_schema_fields = Vec::new();
        for file in files {
            // A HashMap from partition column to value for this parquet file only
            let mut partition_values = HashMap::new();
            let mut iter = file.location.as_ref().split('/').peekable();
            let mut subpath = iter.next();
            // Get partitions from subpaths. Skip the last subpath
            while iter.peek().is_some() {
                if let Some(subpath) = subpath {
                    // Return an error if the partition is not hive-partitioning
                    let partition = DeltaTablePartition::try_from(
                        percent_decode_str(subpath).decode_utf8()?.as_ref(),
                    )?;
                    debug!(
                        "Found partition {partition:#?} in parquet file {:#?}",
                        file.location
                    );
                    let (key, val) = (partition.key, partition.value);
                    partition_values.insert(
                        key.clone(),
                        if val == NULL_PARTITION_VALUE_DATA_PATH {
                            None
                        } else {
                            Some(val)
                        },
                    );
                    if partition_columns.insert(key.clone()) {
                        if let Some(schema) = self.partition_schema.take(key.as_str()) {
                            partition_schema_fields.push(schema);
                        } else {
                            // Return an error if the schema of a partition column is not provided by user
                            return Err(Error::MissingPartitionSchema);
                        }
                    }
                } else {
                    // This error shouldn't happen. The while condition ensures that subpath is not none
                    panic!("Subpath iterator index overflows");
                }
                subpath = iter.next();
            }

            actions.push(Action::Add(Add {
                path: percent_decode_str(file.location.as_ref())
                    .decode_utf8()?
                    .to_string(),
                size: i64::try_from(file.size)?,
                partition_values,
                modification_time: file.last_modified.timestamp_millis(),
                data_change: true,
                ..Default::default()
            }));

            let mut arrow_schema = ParquetRecordBatchStreamBuilder::new(ParquetObjectReader::new(
                object_store.clone(),
                file,
            ))
            .await?
            .schema()
            .as_ref()
            .clone();
            // Arrow schema of Parquet files may have conflicting metatdata
            // Since Arrow schema metadata is not used to generate Delta table schema, we set the metadata field to an empty HashMap
            arrow_schema.metadata = HashMap::new();
            arrow_schemas.push(arrow_schema);
        }

        if !self.partition_schema.is_empty() {
            // Partition column provided by the user does not exist in the parquet files
            return Err(Error::PartitionColumnNotExist(self.partition_schema));
        }

        // Merge parquet file schemas
        // This step is needed because timestamp will not be preserved when copying files in S3. We can't use the schema of the latest parqeut file as Delta table's schema
        let mut schema_fields = Schema::try_from(&ArrowSchema::try_merge(arrow_schemas)?)?
            .fields()
            .clone();
        schema_fields.append(&mut partition_schema_fields);
        debug!("Schema fields for the parquet table: {schema_fields:#?}");

        // Generate CreateBuilder with corresponding add actions, schemas and operation meta
        let mut builder = CreateBuilder::new()
            .with_log_store(log_store)
            .with_columns(schema_fields)
            .with_partition_columns(partition_columns.into_iter())
            .with_actions(actions)
            .with_save_mode(self.mode)
            .with_configuration(self.configuration);
        if let Some(name) = self.name {
            builder = builder.with_table_name(name);
        }
        if let Some(comment) = self.comment {
            builder = builder.with_comment(comment);
        }
        if let Some(metadata) = self.metadata {
            builder = builder.with_metadata(metadata);
        }
        Ok(builder)
    }
}

impl std::future::IntoFuture for ConvertToDeltaBuilder {
    type Output = DeltaResult<DeltaTable>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(self) -> Self::IntoFuture {
        let this = self;

        Box::pin(async move {
            let builder = this
                .into_create_builder()
                .await
                .map_err(DeltaTableError::from)?;
            let table = builder.await?;
            Ok(table)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        configure_log_store, ensure_table_uri, ConvertToDeltaBuilder, DeltaTable, LogStoreRef,
        StructField,
    };
    use crate::{
        kernel::schema::{DataType, PrimitiveType},
        open_table,
        storage::config::StorageOptions,
        Path,
    };
    use itertools::Itertools;
    use pretty_assertions::assert_eq;
    use std::fs;
    use tempfile::tempdir;

    fn schema_field(key: &str, primitive: PrimitiveType, nullable: bool) -> StructField {
        StructField::new(key.to_string(), DataType::Primitive(primitive), nullable)
    }

    // Copy all Parquet files in the source location to a temp dir (with Delta log removed)
    fn copy_files(src: impl AsRef<std::path::Path>, dst: impl AsRef<std::path::Path>) {
        fs::create_dir_all(&dst).expect("Failed to create all directories");
        let files = fs::read_dir(src).expect("Failed to read source directory");
        for file in files {
            let file = file.expect("Failed to read file");
            let name = file.file_name();
            // Skip Delta log
            if name.to_str() != Some("_delta_log") {
                if file.file_type().expect("Failed to get file type").is_dir() {
                    copy_files(file.path(), dst.as_ref().join(name));
                } else {
                    fs::copy(file.path(), dst.as_ref().join(name)).expect("Failed to copy file");
                }
            }
        }
    }

    fn log_store(path: impl Into<String>) -> LogStoreRef {
        configure_log_store(
            ensure_table_uri(path.into()).expect("Failed to convert to table URI"),
            StorageOptions::default(),
        )
        .expect("Failed to create an object store")
    }

    async fn create_delta_table(
        path: &str,
        partition_schema: Vec<StructField>,
        // Whether testing on object store or path
        from_path: bool,
    ) -> DeltaTable {
        let temp_dir = tempdir().expect("Failed to create a temp directory");
        let temp_dir = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert Path to string slice");
        // Copy all files to a temp directory to perform testing. Skip Delta log
        copy_files(format!("{}/{}", env!("CARGO_MANIFEST_DIR"), path), temp_dir);
        let builder = if from_path {
            ConvertToDeltaBuilder::new().with_location(temp_dir)
        } else {
            ConvertToDeltaBuilder::new().with_log_store(log_store(temp_dir))
        };
        builder
            .with_partition_schema(partition_schema)
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to convert to Delta table. Location: {path}. Error: {e}")
            })
    }

    async fn open_created_delta_table(
        path: &str,
        partition_schema: Vec<StructField>,
    ) -> DeltaTable {
        let temp_dir = tempdir().expect("Failed to create a temp directory");
        let temp_dir = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert to string slice");
        // Copy all files to a temp directory to perform testing. Skip Delta log
        copy_files(format!("{}/{}", env!("CARGO_MANIFEST_DIR"), path), temp_dir);
        ConvertToDeltaBuilder::new()
            .with_log_store(log_store(temp_dir))
            .with_partition_schema(partition_schema)
            .await
            .unwrap_or_else(|e| {
                panic!("Failed to convert to Delta table. Location: {path}. Error: {e}")
            });
        open_table(temp_dir).await.expect("Failed to open table")
    }

    fn assert_delta_table(
        table: DeltaTable,
        // Test data location in the repo
        test_data_from: &str,
        expected_version: i64,
        expected_paths: Vec<Path>,
        expected_schema: Vec<StructField>,
        expected_partition_values: &[(String, Option<String>)],
    ) {
        assert_eq!(
            table.version(),
            expected_version,
            "Testing location: {test_data_from:?}"
        );

        let mut files = table.get_files_iter().collect_vec();
        files.sort();
        assert_eq!(
            files, expected_paths,
            "Testing location: {test_data_from:?}"
        );

        let mut schema_fields = table
            .get_schema()
            .expect("Failed to get schema")
            .fields()
            .clone();
        schema_fields.sort_by(|a, b| a.name().cmp(b.name()));
        assert_eq!(
            schema_fields, expected_schema,
            "Testing location: {test_data_from:?}"
        );

        let mut partition_values = table
            .get_partition_values()
            .flat_map(|map| map.clone())
            .collect::<Vec<_>>();
        partition_values.sort();
        assert_eq!(partition_values, expected_partition_values);
    }

    // Test Parquet files in object store location
    #[tokio::test]
    async fn test_convert_to_delta() {
        let path = "tests/data/delta-0.8.0-date";
        let table = create_delta_table(path, Vec::new(), false).await;
        let action = table
            .get_active_add_actions_by_partitions(&[])
            .expect("Failed to get Add actions")
            .next()
            .expect("Iterator index overflows");
        assert_eq!(
            action.path,
            "part-00000-d22c627d-9655-4153-9527-f8995620fa42-c000.snappy.parquet"
        );
        assert!(action.data_change);
        assert_delta_table(
            table,
            path,
            0,
            vec![Path::from(
                "part-00000-d22c627d-9655-4153-9527-f8995620fa42-c000.snappy.parquet",
            )],
            vec![
                schema_field("date", PrimitiveType::Date, true),
                schema_field("dayOfYear", PrimitiveType::Integer, true),
            ],
            &[],
        );

        let path = "tests/data/delta-0.8.0-null-partition";
        let table = create_delta_table(
            path,
            vec![schema_field("k", PrimitiveType::String, true)],
            false,
        )
        .await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                    Path::from("k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet"),
                    Path::from("k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet")
            ],
            vec![
                schema_field("k", PrimitiveType::String, true),
                schema_field("v", PrimitiveType::Long, true),
            ],
            &[
                ("k".to_string(), None),
                ("k".to_string(), Some("A".to_string())),
            ],
        );

        let path = "tests/data/delta-0.8.0-special-partition";
        let table = create_delta_table(
            path,
            vec![schema_field("x", PrimitiveType::String, true)],
            false,
        )
        .await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from_url_path(
                    "x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet",
                )
                .expect("Invalid URL path"),
                Path::from_url_path(
                    "x=B%20B/part-00015-e9abbc6f-85e9-457b-be8e-e9f5b8a22890.c000.snappy.parquet",
                )
                .expect("Invalid URL path"),
            ],
            vec![
                schema_field("x", PrimitiveType::String, true),
                schema_field("y", PrimitiveType::Long, true),
            ],
            &[
                ("x".to_string(), Some("A/A".to_string())),
                ("x".to_string(), Some("B B".to_string())),
            ],
        );

        let path = "tests/data/delta-0.8.0-partitioned";
        let table = create_delta_table(
            path,
            vec![
                schema_field("day", PrimitiveType::String, true),
                schema_field("month", PrimitiveType::String, true),
                schema_field("year", PrimitiveType::String, true),
            ],
            false,
        )
        .await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from(
                    "year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet",
                ),
                Path::from(
                    "year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet",
                ),
                Path::from(
                    "year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet",
                ),
                Path::from(
                    "year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet",
                ),
                Path::from(
                    "year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet",
                ),
                Path::from(
                    "year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet",
                ),
            ],
            vec![
                schema_field("day", PrimitiveType::String, true),
                schema_field("month", PrimitiveType::String, true),
                schema_field("value", PrimitiveType::String, true),
                schema_field("year", PrimitiveType::String, true),
            ],
            &[
                ("day".to_string(), Some("1".to_string())),
                ("day".to_string(), Some("20".to_string())),
                ("day".to_string(), Some("3".to_string())),
                ("day".to_string(), Some("4".to_string())),
                ("day".to_string(), Some("5".to_string())),
                ("day".to_string(), Some("5".to_string())),
                ("month".to_string(), Some("1".to_string())),
                ("month".to_string(), Some("12".to_string())),
                ("month".to_string(), Some("12".to_string())),
                ("month".to_string(), Some("2".to_string())),
                ("month".to_string(), Some("2".to_string())),
                ("month".to_string(), Some("4".to_string())),
                ("year".to_string(), Some("2020".to_string())),
                ("year".to_string(), Some("2020".to_string())),
                ("year".to_string(), Some("2020".to_string())),
                ("year".to_string(), Some("2021".to_string())),
                ("year".to_string(), Some("2021".to_string())),
                ("year".to_string(), Some("2021".to_string())),
            ],
        );
    }

    // Test opening the newly created Delta table
    #[tokio::test]
    async fn test_open_created_delta_table() {
        let path = "tests/data/delta-0.2.0";
        let table = open_created_delta_table(path, Vec::new()).await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from("part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet"),
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet"),
                Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
                Path::from("part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet"),
                Path::from("part-00001-4327c977-2734-4477-9507-7ccf67924649-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ],
            vec![schema_field("value", PrimitiveType::Integer, false)],
            &[],
        );

        let path = "tests/data/delta-0.8-empty";
        let table = open_created_delta_table(path, Vec::new()).await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from("part-00000-b0cc5102-6177-4d60-80d3-b5d170011621-c000.snappy.parquet"),
                Path::from("part-00007-02b8c308-e5a7-41a8-a653-cb5594582017-c000.snappy.parquet"),
            ],
            vec![schema_field("column", PrimitiveType::Long, true)],
            &[],
        );

        let path = "tests/data/delta-0.8.0";
        let table = open_created_delta_table(path, Vec::new()).await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"),
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
                Path::from("part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"),
            ],
            vec![schema_field("value", PrimitiveType::Integer, true)],
            &[],
        );
    }

    // Test Parquet files in path
    #[tokio::test]
    async fn test_convert_to_delta_from_path() {
        let path = "tests/data/delta-2.2.0-partitioned-types";
        let table = create_delta_table(
            path,
            vec![
                schema_field("c1", PrimitiveType::Integer, true),
                schema_field("c2", PrimitiveType::String, true),
            ],
            true,
        )
        .await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from(
                    "c1=4/c2=c/part-00003-f525f459-34f9-46f5-82d6-d42121d883fd.c000.snappy.parquet",
                ),
                Path::from(
                    "c1=5/c2=b/part-00007-4e73fa3b-2c88-424a-8051-f8b54328ffdb.c000.snappy.parquet",
                ),
                Path::from(
                    "c1=6/c2=a/part-00011-10619b10-b691-4fd0-acc4-2a9608499d7c.c000.snappy.parquet",
                ),
            ],
            vec![
                schema_field("c1", PrimitiveType::Integer, true),
                schema_field("c2", PrimitiveType::String, true),
                schema_field("c3", PrimitiveType::Integer, true),
            ],
            &[
                ("c1".to_string(), Some("4".to_string())),
                ("c1".to_string(), Some("5".to_string())),
                ("c1".to_string(), Some("6".to_string())),
                ("c2".to_string(), Some("a".to_string())),
                ("c2".to_string(), Some("b".to_string())),
                ("c2".to_string(), Some("c".to_string())),
            ],
        );

        let path = "tests/data/delta-0.8.0-numeric-partition";
        let table = create_delta_table(
            path,
            vec![
                schema_field("x", PrimitiveType::Long, true),
                schema_field("y", PrimitiveType::Double, true),
            ],
            true,
        )
        .await;
        assert_delta_table(
            table,
            path,
            0,
            vec![
                Path::from(
                    "x=10/y=10.0/part-00015-24eb4845-2d25-4448-b3bb-5ed7f12635ab.c000.snappy.parquet",
                ),
                Path::from(
                    "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet",
                ),
            ],
            vec![
                schema_field("x", PrimitiveType::Long, true),
                schema_field("y", PrimitiveType::Double, true),
                schema_field("z", PrimitiveType::String, true),
            ],
            &[
                ("x".to_string(), Some("10".to_string())),
                ("x".to_string(), Some("9".to_string())),
                ("y".to_string(), Some("10.0".to_string())),
                ("y".to_string(), Some("9.9".to_string())),
            ],
        );
    }

    #[tokio::test]
    async fn test_missing_location() {
        let _table = ConvertToDeltaBuilder::new()
            .await
            .expect_err("Location is missing. Should error");
    }

    #[tokio::test]
    async fn test_empty_dir() {
        let temp_dir = tempdir().expect("Failed to create a temp directory");
        let temp_dir = temp_dir
            .path()
            .to_str()
            .expect("Failed to convert to string slice");
        let _table = ConvertToDeltaBuilder::new()
            .with_location(temp_dir)
            .await
            .expect_err("Parquet file does not exist. Should error");
    }

    #[tokio::test]
    async fn test_partition_column_not_exist() {
        let _table = ConvertToDeltaBuilder::new()
            .with_location("tests/data/delta-0.8.0-null-partition")
            .with_partition_schema(vec![schema_field("foo", PrimitiveType::String, true)])
            .await
            .expect_err(
            "Partition column provided by user does not exist in the parquet files. Should error",
        );
    }

    #[tokio::test]
    async fn test_missing_partition_schema() {
        let _table = ConvertToDeltaBuilder::new()
            .with_location("tests/data/delta-0.8.0-numeric-partition")
            .await
            .expect_err("The schema of a partition column is not provided by user. Should error");
    }

    #[tokio::test]
    async fn test_delta_table_already() {
        let _table = ConvertToDeltaBuilder::new()
            .with_location("tests/data/delta-0.2.0")
            .await
            .expect_err("The given location is already a delta table location. Should error");
    }
}
