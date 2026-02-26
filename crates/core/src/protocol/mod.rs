//! Actions included in Delta table transaction logs

#![allow(non_camel_case_types)]

use crate::table::Constraint;
use std::borrow::Borrow;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::mem::take;
use std::str::FromStr;

use arrow::compute::filter_record_batch;
use arrow_array::{BooleanArray, RecordBatch};
use delta_kernel::FilteredEngineData;
use delta_kernel::engine::arrow_data::ArrowEngineData;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use url::Url;

use crate::crate_version;
use crate::errors::{DeltaResult, DeltaTableError};
use crate::kernel::{Add, CommitInfo, Metadata, Protocol, Remove, StructField, TableFeatures};

pub mod checkpoints;
pub mod log_compaction;

pub(crate) use checkpoints::{cleanup_expired_logs_for, create_checkpoint_for};

/// Struct used to represent minValues and maxValues in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnValueStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnValueStat>),
    /// Json representation of statistics.
    Value(Value),
}

impl ColumnValueStat {
    /// Returns the HashMap representation of the ColumnValueStat.
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnValueStat>> {
        match self {
            ColumnValueStat::Column(m) => Some(m),
            _ => None,
        }
    }

    /// Returns the serde_json representation of the ColumnValueStat.
    pub fn as_value(&self) -> Option<&Value> {
        match self {
            ColumnValueStat::Value(v) => Some(v),
            _ => None,
        }
    }
}

/// Struct used to represent nullCount in add action statistics.
#[derive(Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(untagged)]
pub enum ColumnCountStat {
    /// Composite HashMap representation of statistics.
    Column(HashMap<String, ColumnCountStat>),
    /// Json representation of statistics.
    Value(i64),
}

impl ColumnCountStat {
    /// Returns the HashMap representation of the ColumnCountStat.
    pub fn as_column(&self) -> Option<&HashMap<String, ColumnCountStat>> {
        match self {
            ColumnCountStat::Column(m) => Some(m),
            _ => None,
        }
    }

    /// Returns the serde_json representation of the ColumnCountStat.
    pub fn as_value(&self) -> Option<i64> {
        match self {
            ColumnCountStat::Value(v) => Some(*v),
            _ => None,
        }
    }
}

/// Statistics associated with Add actions contained in the Delta log.
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
    /// Number of records in the file associated with the log action.
    pub num_records: i64,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, ColumnValueStat>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, ColumnValueStat>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, ColumnCountStat>,
}

/// Statistics associated with Add actions contained in the Delta log.
/// min_values, max_values and null_count are optional to allow them to be missing
#[derive(Serialize, Deserialize, Debug, Default, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct PartialStats {
    /// Number of records in the file associated with the log action.
    pub num_records: i64,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: Option<HashMap<String, ColumnValueStat>>,
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: Option<HashMap<String, ColumnValueStat>>,
    /// The number of null values for all columns.
    pub null_count: Option<HashMap<String, ColumnCountStat>>,
}

impl PartialStats {
    /// Fills in missing HashMaps
    pub fn as_stats(&mut self) -> Stats {
        let min_values = take(&mut self.min_values);
        let max_values = take(&mut self.max_values);
        let null_count = take(&mut self.null_count);
        Stats {
            num_records: self.num_records,
            min_values: min_values.unwrap_or_default(),
            max_values: max_values.unwrap_or_default(),
            null_count: null_count.unwrap_or_default(),
        }
    }
}

/// File stats parsed from raw parquet format.
#[derive(Debug, Default)]
pub struct StatsParsed {
    /// Number of records in the file associated with the log action.
    pub num_records: i64,

    // start of per column stats
    /// Contains a value smaller than all values present in the file for all columns.
    pub min_values: HashMap<String, parquet::record::Field>,
    /// Contains a value larger than all values present in the file for all columns.
    /// Contains a value larger than all values present in the file for all columns.
    pub max_values: HashMap<String, parquet::record::Field>,
    /// The number of null values for all columns.
    pub null_count: HashMap<String, i64>,
}

impl Hash for Add {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

impl PartialEq for Add {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.size == other.size
            && self.partition_values == other.partition_values
            && self.modification_time == other.modification_time
            && self.data_change == other.data_change
            && self.stats == other.stats
            && self.tags == other.tags
            && self.deletion_vector == other.deletion_vector
    }
}

impl Eq for Add {}

impl Add {
    /// Get whatever stats are available. Uses (parquet struct) parsed_stats if present falling back to json stats.
    pub fn get_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        self.get_json_stats()
    }

    /// Returns the serde_json representation of stats contained in the action if present.
    /// Since stats are defined as optional in the protocol, this may be None.
    fn get_json_stats(&self) -> Result<Option<Stats>, serde_json::error::Error> {
        self.stats
            .as_ref()
            .map(|stats| serde_json::from_str(stats).map(|mut ps: PartialStats| ps.as_stats()))
            .transpose()
    }
}

impl Hash for Remove {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.path.hash(state);
    }
}

/// Borrow `Remove` as str so we can look at tombstones hashset in `DeltaTableState` by using
/// a path from action `Add`.
impl Borrow<str> for Remove {
    fn borrow(&self) -> &str {
        self.path.as_ref()
    }
}

impl PartialEq for Remove {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.deletion_timestamp == other.deletion_timestamp
            && self.data_change == other.data_change
            && self.extended_file_metadata == other.extended_file_metadata
            && self.partition_values == other.partition_values
            && self.size == other.size
            && self.tags == other.tags
            && self.deletion_vector == other.deletion_vector
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
/// Used to record the operations performed to the Delta Log
pub struct MergePredicate {
    /// The type of merge operation performed
    pub action_type: String,
    /// The predicate used for the merge operation
    #[serde(skip_serializing_if = "Option::is_none")]
    pub predicate: Option<String>,
}

/// Operation performed when creating a new log entry with one or more actions.
/// This is a key element of the `CommitInfo` action.
#[allow(clippy::large_enum_variant)]
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub enum DeltaOperation {
    /// Represents a Delta `Add Column` operation.
    /// Used to add new columns or field in a struct
    AddColumn {
        /// Fields added to existing schema
        fields: Vec<StructField>,
    },

    /// Represents a Delta `Create` operation.
    /// Would usually only create the table, if also data is written,
    /// a `Write` operations is more appropriate
    Create {
        /// The save mode used during the create.
        mode: SaveMode,
        /// The storage location of the new table
        location: Url,
        /// The min reader and writer protocol versions of the table
        protocol: Protocol,
        /// Metadata associated with the new table
        metadata: Metadata,
    },

    /// Represents a Delta `Write` operation.
    /// Write operations will typically only include `Add` actions.
    #[serde(rename_all = "camelCase")]
    Write {
        /// The save mode used during the write.
        mode: SaveMode,
        /// The columns the write is partitioned by.
        partition_by: Option<Vec<String>>,
        /// The predicate used during the write.
        predicate: Option<String>,
    },

    /// Delete data matching predicate from delta table
    Delete {
        /// The condition the to be deleted data must match
        predicate: Option<String>,
    },

    /// Update data matching predicate from delta table
    Update {
        /// The update predicate
        predicate: Option<String>,
    },
    /// Add constraints to a table
    AddConstraint {
        /// Constraints with Name and Expression
        constraints: Vec<Constraint>,
    },

    /// Add table features to a table
    AddFeature {
        /// Name of the feature
        name: Vec<TableFeatures>,
    },

    /// Drops constraints from a table
    DropConstraint {
        /// Constraints name
        name: String,
    },

    /// Merge data with a source data with the following predicate
    #[serde(rename_all = "camelCase")]
    Merge {
        /// Cleaned merge predicate for conflict checks
        predicate: Option<String>,

        /// The original merge predicate
        merge_predicate: Option<String>,

        /// Match operations performed
        matched_predicates: Vec<MergePredicate>,

        /// Not Match operations performed
        not_matched_predicates: Vec<MergePredicate>,

        /// Not Match by Source operations performed
        not_matched_by_source_predicates: Vec<MergePredicate>,
    },

    /// Represents a Delta `StreamingUpdate` operation.
    #[serde(rename_all = "camelCase")]
    StreamingUpdate {
        /// The output mode the streaming writer is using.
        output_mode: OutputMode,
        /// The query id of the streaming writer.
        query_id: String,
        /// The epoch id of the written micro-batch.
        epoch_id: i64,
    },

    /// Set table properties operations
    #[serde(rename_all = "camelCase")]
    SetTableProperties {
        /// Table properties that were added
        properties: HashMap<String, String>,
    },

    #[serde(rename_all = "camelCase")]
    /// Represents a `Optimize` operation
    Optimize {
        // TODO: Create a string representation of the filter passed to optimize
        /// The filter used to determine which partitions to filter
        predicate: Option<String>,
        /// Target optimize size
        target_size: i64,
    },
    #[serde(rename_all = "camelCase")]
    /// Represents a `FileSystemCheck` operation
    FileSystemCheck {},

    /// Represents a `Restore` operation
    Restore {
        /// Version to restore
        version: Option<i64>,
        ///Datetime to restore
        datetime: Option<i64>,
    }, // TODO: Add more operations

    #[serde(rename_all = "camelCase")]
    /// Represents the start of `Vacuum` operation
    VacuumStart {
        /// Whether the retention check is enforced
        retention_check_enabled: bool,
        /// The specified retetion period in milliseconds
        specified_retention_millis: Option<i64>,
        /// The default delta table retention milliseconds policy
        default_retention_millis: i64,
    },

    /// Represents the end of `Vacuum` operation
    VacuumEnd {
        /// The status of the operation
        status: String,
    },
    /// Set table field metadata operations
    #[serde(rename_all = "camelCase")]
    UpdateFieldMetadata {
        /// Fields added to existing schema
        fields: Vec<StructField>,
    },
    /// Update table metadata operations
    #[serde(rename_all = "camelCase")]
    UpdateTableMetadata {
        /// The metadata update to apply
        metadata_update: crate::operations::update_table_metadata::TableMetadataUpdate,
    },
}

impl DeltaOperation {
    /// A human readable name for the operation
    pub fn name(&self) -> &str {
        // operation names taken from https://learn.microsoft.com/en-us/azure/databricks/delta/history#--operation-metrics-keys
        match &self {
            DeltaOperation::AddColumn { .. } => "ADD COLUMN",
            DeltaOperation::Create {
                mode: SaveMode::Overwrite,
                ..
            } => "CREATE OR REPLACE TABLE",
            DeltaOperation::Create { .. } => "CREATE TABLE",
            DeltaOperation::Write { .. } => "WRITE",
            DeltaOperation::Delete { .. } => "DELETE",
            DeltaOperation::Update { .. } => "UPDATE",
            DeltaOperation::Merge { .. } => "MERGE",
            DeltaOperation::StreamingUpdate { .. } => "STREAMING UPDATE",
            DeltaOperation::SetTableProperties { .. } => "SET TBLPROPERTIES",
            DeltaOperation::Optimize { .. } => "OPTIMIZE",
            DeltaOperation::FileSystemCheck { .. } => "FSCK",
            DeltaOperation::Restore { .. } => "RESTORE",
            DeltaOperation::VacuumStart { .. } => "VACUUM START",
            DeltaOperation::VacuumEnd { .. } => "VACUUM END",
            DeltaOperation::AddConstraint { .. } => "ADD CONSTRAINT",
            DeltaOperation::DropConstraint { .. } => "DROP CONSTRAINT",
            DeltaOperation::AddFeature { .. } => "ADD FEATURE",
            DeltaOperation::UpdateFieldMetadata { .. } => "UPDATE FIELD METADATA",
            DeltaOperation::UpdateTableMetadata { .. } => "UPDATE TABLE METADATA",
        }
    }

    /// Parameters configured for operation.
    pub fn operation_parameters(&self) -> DeltaResult<HashMap<String, Value>> {
        if let Some(Some(Some(map))) = serde_json::to_value(self)?
            .as_object()
            .map(|p| p.values().next().map(|q| q.as_object()))
        {
            Ok(map
                .iter()
                .filter(|item| !item.1.is_null())
                .map(|(k, v)| {
                    (
                        k.to_owned(),
                        serde_json::Value::String(if v.is_string() {
                            String::from(v.as_str().unwrap())
                        } else {
                            v.to_string()
                        }),
                    )
                })
                .collect())
        } else {
            Err(DeltaTableError::Generic(
                "Operation parameters serialized into unexpected shape".into(),
            ))
        }
    }

    /// Denotes if the operation changes the data contained in the table
    pub fn changes_data(&self) -> bool {
        match self {
            Self::Optimize { .. }
            | Self::UpdateFieldMetadata { .. }
            | Self::UpdateTableMetadata { .. }
            | Self::SetTableProperties { .. }
            | Self::AddColumn { .. }
            | Self::AddFeature { .. }
            | Self::VacuumStart { .. }
            | Self::VacuumEnd { .. }
            | Self::AddConstraint { .. }
            | Self::DropConstraint { .. } => false,
            Self::Create { .. }
            | Self::FileSystemCheck {}
            | Self::StreamingUpdate { .. }
            | Self::Write { .. }
            | Self::Delete { .. }
            | Self::Merge { .. }
            | Self::Update { .. }
            | Self::Restore { .. } => true,
        }
    }

    /// Retrieve basic commit information to be added to Delta commits
    pub fn get_commit_info(&self) -> CommitInfo {
        // TODO infer additional info from operation parameters ...
        CommitInfo {
            operation: Some(self.name().into()),
            operation_parameters: self.operation_parameters().ok(),
            engine_info: Some(format!("delta-rs:{}", crate_version())),
            ..Default::default()
        }
    }

    /// Get predicate expression applied when the operation reads data from the table.
    pub fn read_predicate(&self) -> Option<String> {
        match self {
            // TODO add more operations
            Self::Write { predicate, .. } => predicate.clone(),
            Self::Delete { predicate, .. } => predicate.clone(),
            Self::Update { predicate, .. } => predicate.clone(),
            Self::Merge { predicate, .. } => predicate.clone(),
            _ => None,
        }
    }

    /// Denotes if the operation reads the entire table
    pub fn read_whole_table(&self) -> bool {
        match self {
            // Predicate is none -> Merge operation had to join full source and target
            Self::Merge { predicate, .. } if predicate.is_none() => true,
            _ => false,
        }
    }
}

/// The SaveMode used when performing a DeltaOperation
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq)]
pub enum SaveMode {
    /// Files will be appended to the target location.
    Append,
    /// The target location will be overwritten.
    Overwrite,
    /// If files exist for the target, the operation must fail.
    ErrorIfExists,
    /// If files exist for the target, the operation must not proceed or change any data.
    Ignore,
}

impl FromStr for SaveMode {
    type Err = DeltaTableError;

    fn from_str(s: &str) -> DeltaResult<Self> {
        match s.to_ascii_lowercase().as_str() {
            "append" => Ok(SaveMode::Append),
            "overwrite" => Ok(SaveMode::Overwrite),
            "error" => Ok(SaveMode::ErrorIfExists),
            "ignore" => Ok(SaveMode::Ignore),
            _ => Err(DeltaTableError::Generic(format!(
                "Invalid save mode provided: {s}, only these are supported: ['append', 'overwrite', 'error', 'ignore']"
            ))),
        }
    }
}

/// The OutputMode used in streaming operations.
#[derive(Serialize, Deserialize, Debug, Copy, Clone)]
pub enum OutputMode {
    /// Only new rows will be written when new data is available.
    Append,
    /// The full output (all rows) will be written whenever new data is available.
    Complete,
    /// Only rows with updates will be written when new or changed data is available.
    Update,
}

pub(crate) fn to_rb(data: FilteredEngineData) -> DeltaResult<RecordBatch> {
    let (underlying_data, selection_vector) = data.into_parts();
    let engine_data = ArrowEngineData::try_from_engine_data(underlying_data)?;
    let predicate = BooleanArray::from(selection_vector);
    let batch = filter_record_batch(engine_data.record_batch(), &predicate)?;
    Ok(batch)
}

#[cfg(test)]
mod tests {
    use pretty_assertions::assert_eq;

    use super::*;
    use crate::kernel::Action;

    #[test]
    fn test_load_table_stats() {
        let action = Add {
            stats: Some(
                serde_json::json!({
                    "numRecords": 22,
                    "minValues": {"a": 1, "nested": {"b": 2, "c": "a"}},
                    "maxValues": {"a": 10, "nested": {"b": 20, "c": "z"}},
                    "nullCount": {"a": 1, "nested": {"b": 0, "c": 1}},
                })
                .to_string(),
            ),
            path: Default::default(),
            data_change: true,
            deletion_vector: None,
            partition_values: Default::default(),
            tags: None,
            size: 0,
            modification_time: 0,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let stats = action.get_stats().unwrap().unwrap();

        assert_eq!(stats.num_records, 22);

        assert_eq!(
            stats.min_values["a"].as_value().unwrap(),
            &serde_json::json!(1)
        );
        assert_eq!(
            stats.min_values["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(2)
        );
        assert_eq!(
            stats.min_values["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("a")
        );

        assert_eq!(
            stats.max_values["a"].as_value().unwrap(),
            &serde_json::json!(10)
        );
        assert_eq!(
            stats.max_values["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            &serde_json::json!(20)
        );
        assert_eq!(
            stats.max_values["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            &serde_json::json!("z")
        );

        assert_eq!(stats.null_count["a"].as_value().unwrap(), 1);
        assert_eq!(
            stats.null_count["nested"].as_column().unwrap()["b"]
                .as_value()
                .unwrap(),
            0
        );
        assert_eq!(
            stats.null_count["nested"].as_column().unwrap()["c"]
                .as_value()
                .unwrap(),
            1
        );
    }

    #[test]
    fn test_load_table_partial_stats() {
        let action = Add {
            stats: Some(
                serde_json::json!({
                    "numRecords": 22
                })
                .to_string(),
            ),
            path: Default::default(),
            data_change: true,
            deletion_vector: None,
            partition_values: Default::default(),
            tags: None,
            size: 0,
            modification_time: 0,
            base_row_id: None,
            default_row_commit_version: None,
            clustering_provider: None,
        };

        let stats = action.get_stats().unwrap().unwrap();

        assert_eq!(stats.num_records, 22);
        assert_eq!(stats.min_values.len(), 0);
        assert_eq!(stats.max_values.len(), 0);
        assert_eq!(stats.null_count.len(), 0);
    }

    #[test]
    fn test_read_commit_info() {
        let raw = r#"
        {
            "timestamp": 1670892998177,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[\"c1\",\"c2\"]"
            },
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": "3",
                "numOutputRows": "3",
                "numOutputBytes": "1356"
            },
            "engineInfo": "Apache-Spark/3.3.1 Delta-Lake/2.2.0",
            "txnId": "046a258f-45e3-4657-b0bf-abfb0f76681c"
        }"#;

        let info = serde_json::from_str::<CommitInfo>(raw);
        assert!(info.is_ok());

        // assert that commit info has no required fields
        let raw = "{}";
        let info = serde_json::from_str::<CommitInfo>(raw);
        assert!(info.is_ok());

        // arbitrary field data may be added to commit
        let raw = r#"
        {
            "timestamp": 1670892998177,
            "operation": "WRITE",
            "operationParameters": {
                "mode": "Append",
                "partitionBy": "[\"c1\",\"c2\"]"
            },
            "isolationLevel": "Serializable",
            "isBlindAppend": true,
            "operationMetrics": {
                "numFiles": "3",
                "numOutputRows": "3",
                "numOutputBytes": "1356"
            },
            "engineInfo": "Apache-Spark/3.3.1 Delta-Lake/2.2.0",
            "txnId": "046a258f-45e3-4657-b0bf-abfb0f76681c",
            "additionalField": "more data",
            "additionalStruct": {
                "key": "value",
                "otherKey": 123
            }
        }"#;

        let info = serde_json::from_str::<CommitInfo>(raw).expect("should parse");
        assert!(info.info.contains_key("additionalField"));
        assert!(info.info.contains_key("additionalStruct"));
    }

    #[test]
    fn test_read_domain_metadata() {
        let buf = r#"{"domainMetadata":{"domain":"delta.liquid","configuration":"{\"clusteringColumns\":[{\"physicalName\":[\"id\"]}],\"domainName\":\"delta.liquid\"}","removed":false}}"#;
        let _action: Action =
            serde_json::from_str(buf).expect("Expected to be able to deserialize");
    }

    mod arrow_tests {
        use crate::DeltaResult;
        use arrow::array::{self, ArrayRef, StructArray};
        use arrow::compute::kernels::cast_utils::Parser;
        use arrow::compute::sort_to_indices;
        use arrow::datatypes::{Date32Type, TimestampMicrosecondType};
        use arrow::record_batch::RecordBatch;
        use futures::TryStreamExt;
        use pretty_assertions::assert_eq;
        use std::path::Path;
        use std::sync::Arc;
        use url::Url;
        fn sort_batch_by(batch: &RecordBatch, column: &str) -> arrow::error::Result<RecordBatch> {
            let sort_column = batch.column(batch.schema().column_with_name(column).unwrap().0);
            let sort_indices = sort_to_indices(sort_column, None, None)?;
            let schema = batch.schema();
            let sorted_columns: Vec<(&String, ArrayRef)> = schema
                .fields()
                .iter()
                .zip(batch.columns().iter())
                .map(|(field, column)| {
                    Ok((
                        field.name(),
                        arrow::compute::take(column, &sort_indices, None)?,
                    ))
                })
                .collect::<arrow::error::Result<_>>()?;
            RecordBatch::try_from_iter(sorted_columns)
        }

        #[tokio::test]
        async fn test_with_partitions() {
            // test table with partitions
            let path = "../test/tests/data/delta-0.8.0-null-partition";
            let table_uri =
                Url::from_directory_path(std::fs::canonicalize(Path::new(path)).unwrap()).unwrap();
            let table = crate::open_table(table_uri).await.unwrap();
            let actions = table.snapshot().unwrap().add_actions_table(true).unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet",
                        "k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet",
                    ])),
                ),
                (
                    "size_bytes",
                    Arc::new(array::Int64Array::from(vec![460, 460])),
                ),
                (
                    "modification_time",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        1627990384000,
                        1627990384000,
                    ])),
                ),
                (
                    "num_records",
                    Arc::new(array::Int64Array::from(vec![None, None])),
                ),
                (
                    "null_count.v",
                    Arc::new(array::Int64Array::from(vec![None, None])),
                ),
                ("min.v", Arc::new(array::Int64Array::from(vec![None, None]))),
                ("max.v", Arc::new(array::Int64Array::from(vec![None, None]))),
                (
                    "partition.k",
                    Arc::new(array::StringArray::from(vec![Some("A"), None])),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);

            // let actions = table.snapshot().unwrap().add_actions_table(false).unwrap();
            // let actions = sort_batch_by(&actions, "path").unwrap();

            // expected_columns[4] = (
            //     "partition_values",
            //     Arc::new(array::StructArray::new(
            //         Fields::from(vec![Field::new("k", DataType::Utf8, true)]),
            //         vec![Arc::new(array::StringArray::from(vec![Some("A"), None])) as ArrayRef],
            //         None,
            //     )),
            // );
            // let expected = RecordBatch::try_from_iter(expected_columns).unwrap();

            // assert_eq!(expected, actions);
        }

        #[tokio::test]
        async fn test_with_deletion_vector() -> DeltaResult<()> {
            // test table with partitions
            let path = "../test/tests/data/table_with_deletion_logs";
            let table_uri = Url::from_directory_path(
                std::fs::canonicalize(path).expect("Failed to canonicalize"),
            )
            .expect("Failed to create URL");
            let _table = crate::open_table(table_uri).await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_without_partitions() {
            // test table without partitions
            let path = "../test/tests/data/simple_table";
            let table_uri = Url::from_directory_path(std::fs::canonicalize(path).unwrap()).unwrap();
            let table = crate::open_table(table_uri).await.unwrap();

            let actions = table.snapshot().unwrap().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet",
                        "part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet",
                        "part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet",
                        "part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet",
                        "part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet",
                    ])),
                ),
                (
                    "size_bytes",
                    Arc::new(array::Int64Array::from(vec![262, 262, 429, 429, 429])),
                ),
                (
                    "modification_time",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        1587968626000,
                        1587968602000,
                        1587968602000,
                        1587968602000,
                        1587968602000,
                    ])),
                ),
                (
                    "num_records",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        None, None, None, None, None,
                    ])),
                ),
                (
                    "null_count.id",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        None, None, None, None, None,
                    ])),
                ),
                (
                    "min.id",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        None, None, None, None, None,
                    ])),
                ),
                (
                    "max.id",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        None, None, None, None, None,
                    ])),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();
            assert_eq!(expected, actions);
        }

        #[tokio::test]
        async fn test_with_column_mapping() -> DeltaResult<()> {
            // test table with column mapping and partitions
            let path = "../test/tests/data/table_with_column_mapping";
            let table_uri = Url::from_directory_path(
                std::fs::canonicalize(path).expect("Failed to canonicalize"),
            )
            .expect("Failed to create URL");
            let _table = crate::open_table(table_uri).await?;
            Ok(())
        }

        #[tokio::test]
        async fn test_with_stats() {
            // test table with stats
            let path = "../test/tests/data/delta-0.8.0";
            let table_uri =
                Url::from_directory_path(std::fs::canonicalize(Path::new(path)).unwrap()).unwrap();
            let table = crate::open_table(table_uri).await.unwrap();
            let actions = table.snapshot().unwrap().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet",
                        "part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet",
                    ])),
                ),
                (
                    "size_bytes",
                    Arc::new(array::Int64Array::from(vec![440, 440])),
                ),
                (
                    "modification_time",
                    Arc::new(arrow::array::Int64Array::from(vec![
                        1615043776000,
                        1615043767000,
                    ])),
                ),
                ("num_records", Arc::new(array::Int64Array::from(vec![2, 2]))),
                (
                    "null_count.value",
                    Arc::new(array::Int64Array::from(vec![0, 0])),
                ),
                ("min.value", Arc::new(array::Int32Array::from(vec![2, 0]))),
                ("max.value", Arc::new(array::Int32Array::from(vec![4, 2]))),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(expected, actions);
        }

        #[tokio::test]
        async fn test_table_not_always_with_stats() {
            let path = "../test/tests/data/delta-stats-optional";
            let table_uri =
                Url::from_directory_path(std::fs::canonicalize(Path::new(path)).unwrap()).unwrap();
            let mut table = crate::open_table(table_uri).await.unwrap();
            table.load().await.unwrap();
            let actions = table.snapshot().unwrap().add_actions_table(true).unwrap();
            let actions = sort_batch_by(&actions, "path").unwrap();
            // get column-0 path, and column-4 num_records, and column_5 null_count.integer
            let expected_path: ArrayRef = Arc::new(array::StringArray::from(vec![
                "part-00000-28925d3a-bdf2-411e-bca9-b067444cbcb0-c000.snappy.parquet",
                "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
            ]));
            let expected_num_records: ArrayRef =
                Arc::new(array::Int64Array::from(vec![None, Some(1)]));
            let expected_null_count: ArrayRef =
                Arc::new(array::Int64Array::from(vec![None, Some(0)]));

            let path_column = actions.column_by_name("path").unwrap();
            let num_records_column = actions.column_by_name("num_records").unwrap();
            let null_count_column = actions.column_by_name("null_count.integer").unwrap();

            assert_eq!(&expected_path, path_column);
            assert_eq!(&expected_num_records, num_records_column);
            assert_eq!(&expected_null_count, null_count_column);
        }

        #[tokio::test]
        async fn test_table_checkpoint_not_always_with_stats() {
            let path = "../test/tests/data/delta-checkpoint-stats-optional";
            let table_uri =
                Url::from_directory_path(std::fs::canonicalize(Path::new(path)).unwrap()).unwrap();
            let mut table = crate::open_table(table_uri).await.unwrap();
            table.load().await.unwrap();

            let snapshot = table.snapshot().unwrap().snapshot();
            let files: Vec<_> = snapshot
                .file_views(&table.log_store, None)
                .try_collect()
                .await
                .unwrap();

            assert_eq!(2, files.len());
        }

        #[tokio::test]
        #[ignore = "re-enable once https://github.com/delta-io/delta-kernel-rs/issues/1075 is resolved."]
        async fn test_only_struct_stats() {
            // test table with no json stats
            let path = "../test/tests/data/delta-1.2.1-only-struct-stats";
            let table_uri = Url::from_directory_path(Path::new(path)).unwrap();
            let mut table = crate::open_table(table_uri).await.unwrap();
            table.load_version(1).await.unwrap();

            let actions = table.snapshot().unwrap().add_actions_table(true).unwrap();

            let expected_columns: Vec<(&str, ArrayRef)> = vec![
                (
                    "path",
                    Arc::new(array::StringArray::from(vec![
                        "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet",
                    ])),
                ),
                ("size_bytes", Arc::new(array::Int64Array::from(vec![5489]))),
                (
                    "modification_time",
                    Arc::new(arrow::array::Int64Array::from(vec![1666652373000])),
                ),
                ("num_records", Arc::new(array::Int64Array::from(vec![1]))),
                (
                    "null_count.integer",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.null",
                    Arc::new(array::Int64Array::from(vec![1])),
                ),
                (
                    "null_count.boolean",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.double",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.decimal",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.string",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.timestamp",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.struct.struct_element",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                ("null_count.map", Arc::new(array::Int64Array::from(vec![0]))),
                (
                    "null_count.array",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.nested_struct.struct_element.nested_struct_element",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "null_count.struct_of_array_of_map.struct_element",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                ("min.integer", Arc::new(array::Int32Array::from(vec![0]))),
                ("max.integer", Arc::new(array::Int32Array::from(vec![0]))),
                ("min.null", Arc::new(array::NullArray::new(1))),
                ("max.null", Arc::new(array::NullArray::new(1))),
                ("min.boolean", Arc::new(array::NullArray::new(1))),
                ("max.boolean", Arc::new(array::NullArray::new(1))),
                (
                    "min.double",
                    Arc::new(array::Float64Array::from(vec![1.234])),
                ),
                (
                    "max.double",
                    Arc::new(array::Float64Array::from(vec![1.234])),
                ),
                (
                    "min.decimal",
                    Arc::new(
                        array::Decimal128Array::from_iter_values([-567800])
                            .with_precision_and_scale(8, 5)
                            .unwrap(),
                    ),
                ),
                (
                    "max.decimal",
                    Arc::new(
                        array::Decimal128Array::from_iter_values([-567800])
                            .with_precision_and_scale(8, 5)
                            .unwrap(),
                    ),
                ),
                (
                    "min.string",
                    Arc::new(array::StringArray::from(vec!["string"])),
                ),
                (
                    "max.string",
                    Arc::new(array::StringArray::from(vec!["string"])),
                ),
                ("min.binary", Arc::new(array::NullArray::new(1))),
                ("max.binary", Arc::new(array::NullArray::new(1))),
                (
                    "null_count.date",
                    Arc::new(array::Int64Array::from(vec![0])),
                ),
                (
                    "min.date",
                    Arc::new(array::Date32Array::from(vec![Date32Type::parse(
                        "2022-10-24",
                    )])),
                ),
                (
                    "max.date",
                    Arc::new(array::Date32Array::from(vec![Date32Type::parse(
                        "2022-10-24",
                    )])),
                ),
                (
                    "min.timestamp",
                    Arc::new(
                        array::TimestampMicrosecondArray::from(vec![
                            TimestampMicrosecondType::parse("2022-10-24T22:59:32.846Z"),
                        ])
                        .with_timezone("UTC"),
                    ),
                ),
                (
                    "max.timestamp",
                    Arc::new(
                        array::TimestampMicrosecondArray::from(vec![
                            TimestampMicrosecondType::parse("2022-10-24T22:59:32.846Z"),
                        ])
                        .with_timezone("UTC"),
                    ),
                ),
                (
                    "min.struct.struct_element",
                    Arc::new(array::StringArray::from(vec!["struct_value"])),
                ),
                (
                    "max.struct.struct_element",
                    Arc::new(array::StringArray::from(vec!["struct_value"])),
                ),
                (
                    "min.nested_struct.struct_element.nested_struct_element",
                    Arc::new(array::StringArray::from(vec!["nested_struct_value"])),
                ),
                (
                    "max.nested_struct.struct_element.nested_struct_element",
                    Arc::new(array::StringArray::from(vec!["nested_struct_value"])),
                ),
            ];
            let expected = RecordBatch::try_from_iter(expected_columns.clone()).unwrap();

            assert_eq!(
                expected
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<&str>>(),
                actions
                    .schema()
                    .fields()
                    .iter()
                    .map(|field| field.name().as_str())
                    .collect::<Vec<&str>>()
            );
            assert_eq!(expected, actions);

            let actions = table.snapshot().unwrap().add_actions_table(false).unwrap();
            // For brevity, just checking a few nested columns in stats

            assert_eq!(
                actions
                    .get_field_at_path(&[
                        "null_count",
                        "nested_struct",
                        "struct_element",
                        "nested_struct_element"
                    ])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::Int64Array>()
                    .unwrap(),
                &array::Int64Array::from(vec![0]),
            );

            assert_eq!(
                actions
                    .get_field_at_path(&[
                        "min",
                        "nested_struct",
                        "struct_element",
                        "nested_struct_element"
                    ])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap(),
                &array::StringArray::from(vec!["nested_struct_value"]),
            );

            assert_eq!(
                actions
                    .get_field_at_path(&[
                        "max",
                        "nested_struct",
                        "struct_element",
                        "nested_struct_element"
                    ])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap(),
                &array::StringArray::from(vec!["nested_struct_value"]),
            );

            assert_eq!(
                actions
                    .get_field_at_path(&["null_count", "struct_of_array_of_map", "struct_element"])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::Int64Array>()
                    .unwrap(),
                &array::Int64Array::from(vec![0])
            );

            assert_eq!(
                actions
                    .get_field_at_path(&["tags", "OPTIMIZE_TARGET_SIZE"])
                    .unwrap()
                    .as_any()
                    .downcast_ref::<array::StringArray>()
                    .unwrap(),
                &array::StringArray::from(vec!["268435456"])
            );
        }

        /// Trait to make it easier to access nested fields
        trait NestedTabular {
            fn get_field_at_path(&self, path: &[&str]) -> Option<ArrayRef>;
        }

        impl NestedTabular for RecordBatch {
            fn get_field_at_path(&self, path: &[&str]) -> Option<ArrayRef> {
                // First, get array in the batch
                let (first_key, remainder) = path.split_at(1);
                let mut col = self.column(self.schema().column_with_name(first_key[0])?.0);

                if remainder.is_empty() {
                    return Some(Arc::clone(col));
                }

                for segment in remainder {
                    col = col
                        .as_any()
                        .downcast_ref::<StructArray>()?
                        .column_by_name(segment)?;
                }

                Some(Arc::clone(col))
            }
        }
    }
}
