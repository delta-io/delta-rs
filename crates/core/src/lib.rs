//! Native Delta Lake implementation in Rust
//!
//! # Usage
//!
//! Load a Delta Table by path:
//!
//! ```rust
//! async {
//!   let table = deltalake_core::open_table("../test/tests/data/simple_table").await.unwrap();
//!   let version = table.version();
//! };
//! ```
//!
//! Load a specific version of Delta Table by path then filter files by partitions:
//!
//! ```rust
//! async {
//!   let table = deltalake_core::open_table_with_version("../test/tests/data/simple_table", 0).await.unwrap();
//!   let files = table.get_files_by_partitions(&[deltalake_core::PartitionFilter {
//!       key: "month".to_string(),
//!       value: deltalake_core::PartitionValue::Equal("12".to_string()),
//!   }]);
//! };
//! ```
//!
//! Load a specific version of Delta Table by path and datetime:
//!
//! ```rust
//! async {
//!   let table = deltalake_core::open_table_with_ds(
//!       "../test/tests/data/simple_table",
//!       "2020-05-02T23:47:31-07:00",
//!   ).await.unwrap();
//!   let version = table.version();
//! };
//! ```
//!
//! # Optional cargo package features
//!
//! - `s3`, `gcs`, `azure` - enable the storage backends for AWS S3, Google Cloud Storage (GCS),
//!   or Azure Blob Storage / Azure Data Lake Storage Gen2 (ADLS2). Use `s3-native-tls` to use native TLS
//!   instead of Rust TLS implementation.
//! - `datafusion` - enable the `datafusion::datasource::TableProvider` trait implementation
//!   for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow-datafusion).
//! - `datafusion-ext` - DEPRECATED: alias for `datafusion` feature.
//!
//! # Querying Delta Tables with Datafusion
//!
//! Querying from local filesystem:
//! ```
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//!
//! async {
//!   let mut ctx = SessionContext::new();
//!   let table = deltalake_core::open_table("../test/tests/data/simple_table")
//!       .await
//!       .unwrap();
//!   ctx.register_table("demo", Arc::new(table)).unwrap();
//!
//!   let batches = ctx
//!       .sql("SELECT * FROM demo").await.unwrap()
//!       .collect()
//!       .await.unwrap();
//! };
//! ```

#![deny(missing_docs)]
#![allow(rustdoc::invalid_html_tags)]
#![allow(clippy::nonminimal_bool)]

pub mod data_catalog;
pub mod errors;
pub mod kernel;
pub mod logstore;
pub mod operations;
pub mod protocol;
pub mod schema;
pub mod storage;
pub mod table;

#[cfg(test)]
pub mod test_utils;

#[cfg(feature = "datafusion")]
pub mod delta_datafusion;
pub mod writer;

use std::collections::HashMap;

pub use self::data_catalog::{DataCatalog, DataCatalogError};
pub use self::errors::*;
pub use self::schema::partitions::*;
pub use self::schema::*;
pub use self::table::builder::{DeltaTableBuilder, DeltaTableConfig, DeltaVersion};
pub use self::table::config::TableProperty;
pub use self::table::DeltaTable;
pub use object_store::{path::Path, Error as ObjectStoreError, ObjectMeta, ObjectStore};
pub use operations::DeltaOps;

// convenience exports for consumers to avoid aligning crate versions
pub use arrow;
#[cfg(feature = "datafusion")]
pub use datafusion;
pub use parquet;
pub use protocol::checkpoints;

/// Creates and loads a DeltaTable from the given path with current metadata.
/// Infers the storage backend to use from the scheme in the given table path.
///
/// Will fail fast if specified `table_uri` is a local path but doesn't exist.
pub async fn open_table(table_uri: impl AsRef<str>) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_valid_uri(table_uri)?.load().await?;
    Ok(table)
}

/// Same as `open_table`, but also accepts storage options to aid in building the table for a deduced
/// `StorageService`.
///
/// Will fail fast if specified `table_uri` is a local path but doesn't exist.
pub async fn open_table_with_storage_options(
    table_uri: impl AsRef<str>,
    storage_options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_valid_uri(table_uri)?
        .with_storage_options(storage_options)
        .load()
        .await?;
    Ok(table)
}

/// Creates a DeltaTable from the given path and loads it with the metadata from the given version.
/// Infers the storage backend to use from the scheme in the given table path.
///
/// Will fail fast if specified `table_uri` is a local path but doesn't exist.
pub async fn open_table_with_version(
    table_uri: impl AsRef<str>,
    version: i64,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_valid_uri(table_uri)?
        .with_version(version)
        .load()
        .await?;
    Ok(table)
}

/// Creates a DeltaTable from the given path.
/// Loads metadata from the version appropriate based on the given ISO-8601/RFC-3339 timestamp.
/// Infers the storage backend to use from the scheme in the given table path.
///
/// Will fail fast if specified `table_uri` is a local path but doesn't exist.
pub async fn open_table_with_ds(
    table_uri: impl AsRef<str>,
    ds: impl AsRef<str>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_valid_uri(table_uri)?
        .with_datestring(ds)?
        .load()
        .await?;
    Ok(table)
}

/// Returns rust crate version, can be use used in language bindings to expose Rust core version
pub fn crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;

    use super::*;
    use crate::table::PeekCommit;
    use std::collections::HashMap;

    #[tokio::test]
    async fn read_delta_2_0_table_without_version() {
        let table = crate::open_table("../test/tests/data/delta-0.2.0")
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ]
        );
        let tombstones = table
            .snapshot()
            .unwrap()
            .all_tombstones(table.object_store().clone())
            .await
            .unwrap()
            .collect_vec();
        assert_eq!(tombstones.len(), 4);
        assert!(tombstones.contains(&crate::kernel::Remove {
            path: "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1564524298213),
            data_change: false,
            extended_file_metadata: None,
            deletion_vector: None,
            partition_values: None,
            tags: None,
            base_row_id: None,
            default_row_commit_version: None,
            size: None,
        }));
    }

    #[tokio::test]
    async fn read_delta_table_with_update() {
        let path = "../test/tests/data/simple_table_with_checkpoint/";
        let table_newest_version = crate::open_table(path).await.unwrap();
        let mut table_to_update = crate::open_table_with_version(path, 0).await.unwrap();
        // calling update several times should not produce any duplicates
        table_to_update.update().await.unwrap();
        table_to_update.update().await.unwrap();
        table_to_update.update().await.unwrap();

        assert_eq!(
            table_newest_version.get_files_iter().unwrap().collect_vec(),
            table_to_update.get_files_iter().unwrap().collect_vec()
        );
    }
    #[tokio::test]
    async fn read_delta_2_0_table_with_version() {
        let mut table = crate::open_table_with_version("../test/tests/data/delta-0.2.0", 0)
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet"),
                Path::from("part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet"),
            ],
        );

        table = crate::open_table_with_version("../test/tests/data/delta-0.2.0", 2)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ]
        );

        table = crate::open_table_with_version("../test/tests/data/delta-0.2.0", 3)
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ]
        );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_without_version() {
        let table = crate::open_table("../test/tests/data/delta-0.8.0")
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"),
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
            ]
        );
        assert_eq!(table.get_files_count(), 2);

        let stats = table.snapshot().unwrap().add_actions_table(true).unwrap();

        let num_records = stats.column_by_name("num_records").unwrap();
        let num_records = num_records
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        let total_records = num_records.values().iter().sum::<i64>();
        assert_eq!(total_records, 4);

        let null_counts = stats.column_by_name("null_count.value").unwrap();
        let null_counts = null_counts
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        null_counts.values().iter().for_each(|x| assert_eq!(*x, 0));

        let tombstones = table
            .snapshot()
            .unwrap()
            .all_tombstones(table.object_store().clone())
            .await
            .unwrap()
            .collect_vec();
        assert_eq!(tombstones.len(), 1);
        assert!(tombstones.contains(&crate::kernel::Remove {
            path: "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1615043776198),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::new()),
            size: Some(445),
            base_row_id: None,
            default_row_commit_version: None,
            deletion_vector: None,
            tags: Some(HashMap::new()),
        }));
    }

    #[tokio::test]
    async fn read_delta_8_0_table_with_load_version() {
        let mut table = crate::open_table("../test/tests/data/delta-0.8.0")
            .await
            .unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"),
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
            ]
        );
        table.load_version(0).await.unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.protocol().unwrap().min_writer_version, 2);
        assert_eq!(table.protocol().unwrap().min_reader_version, 1);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
                Path::from("part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"),
            ]
        );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_with_partitions() {
        let table = crate::open_table("../test/tests/data/delta-0.8.0-partitioned")
            .await
            .unwrap();

        let filters = vec![
            crate::PartitionFilter {
                key: "month".to_string(),
                value: crate::PartitionValue::Equal("2".to_string()),
            },
            crate::PartitionFilter {
                key: "year".to_string(),
                value: crate::PartitionValue::Equal("2020".to_string()),
            },
        ];

        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![
                Path::from("year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet"),
                Path::from("year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet")
            ]
        );
        assert_eq!(
            table.get_file_uris_by_partitions(&filters).unwrap().into_iter().map(|p| std::fs::canonicalize(p).unwrap()).collect::<Vec<_>>(),
            vec![
                std::fs::canonicalize("../test/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet").unwrap(),
                std::fs::canonicalize("../test/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet").unwrap(),
            ]
        );

        let filters = vec![crate::PartitionFilter {
            key: "month".to_string(),
            value: crate::PartitionValue::NotEqual("2".to_string()),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![
                Path::from("year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet"),
                Path::from("year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet"),
                Path::from("year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet"),
                Path::from("year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet")
            ]
        );

        let filters = vec![crate::PartitionFilter {
            key: "month".to_string(),
            value: crate::PartitionValue::In(vec!["2".to_string(), "12".to_string()]),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![
                Path::from("year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet"),
                Path::from("year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet"),
                Path::from("year=2021/month=12/day=20/part-00000-9275fdf4-3961-4184-baa0-1c8a2bb98104.c000.snappy.parquet"),
                Path::from("year=2021/month=12/day=4/part-00000-6dc763c0-3e8b-4d52-b19e-1f92af3fbb25.c000.snappy.parquet")
            ]
        );

        let filters = vec![crate::PartitionFilter {
            key: "month".to_string(),
            value: crate::PartitionValue::NotIn(vec!["2".to_string(), "12".to_string()]),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![
                Path::from("year=2020/month=1/day=1/part-00000-8eafa330-3be9-4a39-ad78-fd13c2027c7e.c000.snappy.parquet"),
                Path::from("year=2021/month=4/day=5/part-00000-c5856301-3439-4032-a6fc-22b7bc92bebb.c000.snappy.parquet")
            ]
        );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_with_null_partition() {
        let table = crate::open_table("../test/tests/data/delta-0.8.0-null-partition")
            .await
            .unwrap();

        let filters = vec![crate::PartitionFilter {
            key: "k".to_string(),
            value: crate::PartitionValue::Equal("A".to_string()),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![Path::from(
                "k=A/part-00000-b1f1dbbb-70bc-4970-893f-9bb772bf246e.c000.snappy.parquet"
            )]
        );

        let filters = vec![crate::PartitionFilter {
            key: "k".to_string(),
            value: crate::PartitionValue::Equal("".to_string()),
        }];
        assert_eq!(
        table.get_files_by_partitions(&filters).unwrap(),
        vec![
            Path::from("k=__HIVE_DEFAULT_PARTITION__/part-00001-8474ac85-360b-4f58-b3ea-23990c71b932.c000.snappy.parquet")
        ]
    );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_with_special_partition() {
        let table = crate::open_table("../test/tests/data/delta-0.8.0-special-partition")
            .await
            .unwrap();

        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![
                Path::parse(
                    "x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
                )
                .unwrap(),
                Path::parse(
                    "x=B%20B/part-00015-e9abbc6f-85e9-457b-be8e-e9f5b8a22890.c000.snappy.parquet"
                )
                .unwrap()
            ]
        );

        let filters = vec![crate::PartitionFilter {
            key: "x".to_string(),
            value: crate::PartitionValue::Equal("A/A".to_string()),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![Path::parse(
                "x=A%2FA/part-00007-b350e235-2832-45df-9918-6cab4f7578f7.c000.snappy.parquet"
            )
            .unwrap()]
        );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_partition_with_compare_op() {
        let table = crate::open_table("../test/tests/data/delta-0.8.0-numeric-partition")
            .await
            .unwrap();

        let filters = vec![crate::PartitionFilter {
            key: "x".to_string(),
            value: crate::PartitionValue::LessThanOrEqual("9".to_string()),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![Path::from(
                "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet"
            )]
        );

        let filters = vec![crate::PartitionFilter {
            key: "y".to_string(),
            value: crate::PartitionValue::LessThan("10.0".to_string()),
        }];
        assert_eq!(
            table.get_files_by_partitions(&filters).unwrap(),
            vec![Path::from(
                "x=9/y=9.9/part-00007-3c50fba1-4264-446c-9c67-d8e24a1ccf83.c000.snappy.parquet"
            )]
        );
    }

    #[tokio::test]
    async fn test_table_history() {
        let path = "../test/tests/data/simple_table_with_checkpoint";
        let latest_table = crate::open_table(path).await.unwrap();

        let table = crate::open_table_with_version(path, 1).await.unwrap();

        let history1 = table.history(None).await.expect("Cannot get table history");
        let history2 = latest_table
            .history(None)
            .await
            .expect("Cannot get table history");

        assert_eq!(history1, history2);

        let history3 = latest_table
            .history(Some(5))
            .await
            .expect("Cannot get table history");
        assert_eq!(history3.len(), 5);
    }

    #[tokio::test]
    async fn test_poll_table_commits() {
        let path = "../test/tests/data/simple_table_with_checkpoint";
        let mut table = crate::open_table_with_version(path, 9).await.unwrap();
        let peek = table.peek_next_commit(table.version()).await.unwrap();
        assert!(matches!(peek, PeekCommit::New(..)));

        if let PeekCommit::New(version, actions) = peek {
            assert_eq!(table.version(), 9);
            assert!(!table.get_files_iter().unwrap().any(|f| f
                == Path::from(
                    "part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet"
                )));

            assert_eq!(version, 10);
            assert_eq!(actions.len(), 2);

            table.update_incremental(None).await.unwrap();

            assert_eq!(table.version(), 10);
            assert!(table.get_files_iter().unwrap().any(|f| f
                == Path::from(
                    "part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet"
                )));
        };

        let peek = table.peek_next_commit(table.version()).await.unwrap();
        assert!(matches!(peek, PeekCommit::UpToDate));
    }

    #[tokio::test]
    async fn test_read_vacuumed_log() {
        let path = "../test/tests/data/checkpoints_vacuumed";
        let table = crate::open_table(path).await.unwrap();
        assert_eq!(table.version(), 12);
    }

    #[tokio::test]
    async fn test_read_vacuumed_log_history() {
        let path = "../test/tests/data/checkpoints_vacuumed";
        let table = crate::open_table(path).await.unwrap();

        // load history for table version with available log file
        let history = table
            .history(Some(5))
            .await
            .expect("Cannot get table history");

        assert_eq!(history.len(), 5);

        // load history for table version without log file
        let history = table
            .history(Some(10))
            .await
            .expect("Cannot get table history");

        assert_eq!(history.len(), 8);
    }

    #[tokio::test]
    async fn read_empty_folder() {
        let dir = std::env::temp_dir();
        let result = crate::open_table(&dir.into_os_string().into_string().unwrap()).await;

        assert!(matches!(
            result.unwrap_err(),
            crate::errors::DeltaTableError::NotATable(_),
        ));

        let dir = std::env::temp_dir();
        let result = crate::open_table_with_ds(
            &dir.into_os_string().into_string().unwrap(),
            "2021-08-09T13:18:31+08:00",
        )
        .await;

        assert!(matches!(
            result.unwrap_err(),
            crate::errors::DeltaTableError::NotATable(_),
        ));
    }

    #[tokio::test]
    async fn read_delta_table_with_cdc() {
        let table = crate::open_table("../test/tests/data/simple_table_with_cdc")
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(
            table.get_files_iter().unwrap().collect_vec(),
            vec![Path::from(
                "part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet"
            ),]
        );
    }

    #[tokio::test()]
    async fn test_version_zero_table_load() {
        let path = "../test/tests/data/COVID-19_NYT";
        let latest_table: DeltaTable = crate::open_table(path).await.unwrap();

        let version_0_table = crate::open_table_with_version(path, 0).await.unwrap();

        let version_0_history = version_0_table
            .history(None)
            .await
            .expect("Cannot get table history");
        let latest_table_history = latest_table
            .history(None)
            .await
            .expect("Cannot get table history");

        assert_eq!(latest_table_history, version_0_history);
    }

    #[tokio::test()]
    async fn test_fail_fast_on_not_existing_path() {
        use std::path::Path as FolderPath;

        let non_existing_path_str = "../test/tests/data/folder_doesnt_exist";

        // Check that there is no such path at the beginning
        let path_doesnt_exist = !FolderPath::new(non_existing_path_str).exists();
        assert!(path_doesnt_exist);

        let error = crate::open_table(non_existing_path_str).await.unwrap_err();
        let _expected_error_msg = format!(
            "Local path \"{}\" does not exist or you don't have access!",
            non_existing_path_str
        );
        assert!(matches!(
            error,
            DeltaTableError::InvalidTableLocation(_expected_error_msg),
        ))
    }

    /// <https://github.com/delta-io/delta-rs/issues/2152>
    #[tokio::test]
    async fn test_identity_column() {
        let path = "../test/tests/data/issue-2152";
        let _ = crate::open_table(path)
            .await
            .expect("Failed to load the table");
    }
}
