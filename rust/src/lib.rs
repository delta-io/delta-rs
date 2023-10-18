//! Native Delta Lake implementation in Rust
//!
//! # Usage
//!
//! Load a Delta Table by path:
//!
//! ```rust
//! async {
//!   let table = deltalake::open_table("./tests/data/simple_table").await.unwrap();
//!   let files = table.get_files();
//! };
//! ```
//!
//! Load a specific version of Delta Table by path then filter files by partitions:
//!
//! ```rust
//! async {
//!   let table = deltalake::open_table_with_version("./tests/data/simple_table", 0).await.unwrap();
//!   let files = table.get_files_by_partitions(&[deltalake::PartitionFilter {
//!       key: "month".to_string(),
//!       value: deltalake::PartitionValue::Equal("12".to_string()),
//!   }]);
//! };
//! ```
//!
//! Load a specific version of Delta Table by path and datetime:
//!
//! ```rust
//! async {
//!   let table = deltalake::open_table_with_ds(
//!       "./tests/data/simple_table",
//!       "2020-05-02T23:47:31-07:00",
//!   ).await.unwrap();
//!   let files = table.get_files();
//! };
//! ```
//!
//! # Optional cargo package features
//!
//! - `s3`, `gcs`, `azure` - enable the storage backends for AWS S3, Google Cloud Storage (GCS),
//!   or Azure Blob Storage / Azure Data Lake Storage Gen2 (ADLS2). Use `s3-native-tls` to use native TLS
//!   instead of Rust TLS implementation.
//! - `glue` - enable the Glue data catalog to work with Delta Tables with AWS Glue.
//! - `datafusion` - enable the `datafusion::datasource::TableProvider` trait implementation
//!   for Delta Tables, allowing them to be queried using [DataFusion](https://github.com/apache/arrow-datafusion).
//! - `datafusion-ext` - DEPRECATED: alias for `datafusion` feature.
//! - `parquet2` - use parquet2 for checkpoint deserialization. Since `arrow` and `parquet` features
//!   are enabled by default for backwards compatibility, this feature needs to be used with `--no-default-features`.
//!
//! # Querying Delta Tables with Datafusion
//!
//! Querying from local filesystem:
//! ```ignore
//! use std::sync::Arc;
//! use datafusion::prelude::SessionContext;
//!
//! async {
//!   let mut ctx = SessionContext::new();
//!   let table = deltalake::open_table("./tests/data/simple_table")
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

#![deny(warnings)]
#![deny(missing_docs)]
#![allow(rustdoc::invalid_html_tags)]

#[cfg(all(feature = "parquet", feature = "parquet2"))]
compile_error!(
    "Features parquet and parquet2 are mutually exclusive and cannot be enabled together"
);

#[cfg(all(feature = "s3", feature = "s3-native-tls"))]
compile_error!(
    "Features s3 and s3-native-tls are mutually exclusive and cannot be enabled together"
);

pub mod data_catalog;
pub mod errors;
pub mod operations;
pub mod protocol;
pub mod schema;
pub mod storage;
pub mod table;

#[cfg(feature = "datafusion")]
pub mod delta_datafusion;
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub mod writer;

use std::collections::HashMap;

pub use self::data_catalog::{get_data_catalog, DataCatalog, DataCatalogError};
pub use self::errors::*;
pub use self::schema::partitions::*;
pub use self::schema::*;
pub use self::table::builder::{
    DeltaTableBuilder, DeltaTableConfig, DeltaTableLoadOptions, DeltaVersion,
};
pub use self::table::config::DeltaConfigKey;
pub use self::table::DeltaTable;
pub use object_store::{path::Path, Error as ObjectStoreError, ObjectMeta, ObjectStore};
pub use operations::DeltaOps;

// convenience exports for consumers to avoid aligning crate versions
#[cfg(feature = "arrow")]
pub use arrow;
#[cfg(feature = "datafusion")]
pub use datafusion;
#[cfg(feature = "parquet")]
pub use parquet;
#[cfg(feature = "parquet2")]
pub use parquet2;
#[cfg(all(feature = "arrow", feature = "parquet"))]
pub use protocol::checkpoints;

// needed only for integration tests
// TODO can / should we move this into the test crate?
#[cfg(feature = "integration_test")]
pub mod test_utils;

/// Creates and loads a DeltaTable from the given path with current metadata.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table(table_uri: impl AsRef<str>) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri).load().await?;
    Ok(table)
}

/// Same as `open_table`, but also accepts storage options to aid in building the table for a deduced
/// `StorageService`.
pub async fn open_table_with_storage_options(
    table_uri: impl AsRef<str>,
    storage_options: HashMap<String, String>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_storage_options(storage_options)
        .load()
        .await?;
    Ok(table)
}

/// Creates a DeltaTable from the given path and loads it with the metadata from the given version.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table_with_version(
    table_uri: impl AsRef<str>,
    version: i64,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_version(version)
        .load()
        .await?;
    Ok(table)
}

/// Creates a DeltaTable from the given path.
/// Loads metadata from the version appropriate based on the given ISO-8601/RFC-3339 timestamp.
/// Infers the storage backend to use from the scheme in the given table path.
pub async fn open_table_with_ds(
    table_uri: impl AsRef<str>,
    ds: impl AsRef<str>,
) -> Result<DeltaTable, DeltaTableError> {
    let table = DeltaTableBuilder::from_uri(table_uri)
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
    use super::*;
    use crate::table::PeekCommit;
    use std::collections::HashMap;

    #[tokio::test]
    async fn read_delta_2_0_table_without_version() {
        let table = crate::open_table("./tests/data/delta-0.2.0").await.unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ]
        );
        let tombstones = table.get_state().all_tombstones();
        assert_eq!(tombstones.len(), 4);
        assert!(tombstones.contains(&crate::protocol::Remove {
            path: "part-00000-512e1537-8aaa-4193-b8b4-bef3de0de409-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1564524298213),
            data_change: false,
            extended_file_metadata: Some(false),
            ..Default::default()
        }));
    }

    #[tokio::test]
    async fn read_delta_table_with_update() {
        let path = "./tests/data/simple_table_with_checkpoint/";
        let table_newest_version = crate::open_table(path).await.unwrap();
        let mut table_to_update = crate::open_table_with_version(path, 0).await.unwrap();
        // calling update several times should not produce any duplicates
        table_to_update.update().await.unwrap();
        table_to_update.update().await.unwrap();
        table_to_update.update().await.unwrap();

        assert_eq!(
            table_newest_version.get_files(),
            table_to_update.get_files()
        );
    }
    #[tokio::test]
    async fn read_delta_2_0_table_with_version() {
        let mut table = crate::open_table_with_version("./tests/data/delta-0.2.0", 0)
            .await
            .unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-b44fcdb0-8b06-4f3a-8606-f8311a96f6dc-c000.snappy.parquet"),
                Path::from("part-00001-185eca06-e017-4dea-ae49-fc48b973e37e-c000.snappy.parquet"),
            ],
        );

        table = crate::open_table_with_version("./tests/data/delta-0.2.0", 2)
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ]
        );

        table = crate::open_table_with_version("./tests/data/delta-0.2.0", 3)
            .await
            .unwrap();
        assert_eq!(table.version(), 3);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-cb6b150b-30b8-4662-ad28-ff32ddab96d2-c000.snappy.parquet"),
                Path::from("part-00000-7c2deba3-1994-4fb8-bc07-d46c948aa415-c000.snappy.parquet"),
                Path::from("part-00001-c373a5bd-85f0-4758-815e-7eb62007a15c-c000.snappy.parquet"),
            ]
        );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_without_version() {
        let table = crate::open_table("./tests/data/delta-0.8.0").await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
                Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet")
            ]
        );
        assert_eq!(table.get_stats().count(), 2);

        assert_eq!(
            table
                .get_stats()
                .map(|x| x.unwrap().unwrap().num_records)
                .sum::<i64>(),
            4
        );

        assert_eq!(
            table
                .get_stats()
                .map(|x| x.unwrap().unwrap().null_count["value"].as_value().unwrap())
                .collect::<Vec<i64>>(),
            vec![0, 0]
        );
        let tombstones = table.get_state().all_tombstones();
        assert_eq!(tombstones.len(), 1);
        assert!(tombstones.contains(&crate::protocol::Remove {
            path: "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1615043776198),
            data_change: true,
            extended_file_metadata: Some(true),
            partition_values: Some(HashMap::new()),
            size: Some(445),
            ..Default::default()
        }));
    }

    #[tokio::test]
    async fn read_delta_8_0_table_with_load_version() {
        let mut table = crate::open_table("./tests/data/delta-0.8.0").await.unwrap();
        assert_eq!(table.version(), 1);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
                Path::from("part-00000-04ec9591-0b73-459e-8d18-ba5711d6cbe1-c000.snappy.parquet"),
            ]
        );
        table.load_version(0).await.unwrap();
        assert_eq!(table.version(), 0);
        assert_eq!(table.get_min_writer_version(), 2);
        assert_eq!(table.get_min_reader_version(), 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-c9b90f86-73e6-46c8-93ba-ff6bfaf892a1-c000.snappy.parquet"),
                Path::from("part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet"),
            ]
        );
    }

    #[tokio::test]
    async fn read_delta_8_0_table_with_partitions() {
        let current_dir = Path::from_filesystem_path(std::env::current_dir().unwrap()).unwrap();
        let table = crate::open_table("./tests/data/delta-0.8.0-partitioned")
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

        #[cfg(unix)]
        assert_eq!(
        table.get_file_uris_by_partitions(&filters).unwrap(),
        vec![
            format!("/{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet", current_dir.as_ref()),
            format!("/{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet", current_dir.as_ref())
        ]
    );
        #[cfg(windows)]
        assert_eq!(
        table.get_file_uris_by_partitions(&filters).unwrap(),
        vec![
            format!("{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=3/part-00000-94d16827-f2fd-42cd-a060-f67ccc63ced9.c000.snappy.parquet", current_dir.as_ref()),
            format!("{}/tests/data/delta-0.8.0-partitioned/year=2020/month=2/day=5/part-00000-89cdd4c8-2af7-4add-8ea3-3990b2f027b5.c000.snappy.parquet", current_dir.as_ref())
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
        let table = crate::open_table("./tests/data/delta-0.8.0-null-partition")
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
        let table = crate::open_table("./tests/data/delta-0.8.0-special-partition")
            .await
            .unwrap();

        assert_eq!(
            table.get_files(),
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
        let table = crate::open_table("./tests/data/delta-0.8.0-numeric-partition")
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

    // TODO: enable this for parquet2
    #[cfg(feature = "parquet")]
    #[tokio::test]
    async fn read_delta_1_2_1_struct_stats_table() {
        let table_uri = "./tests/data/delta-1.2.1-only-struct-stats";
        let table_from_struct_stats = crate::open_table(table_uri).await.unwrap();
        let table_from_json_stats = crate::open_table_with_version(table_uri, 1).await.unwrap();

        fn get_stats_for_file(
            table: &crate::DeltaTable,
            file_name: &str,
        ) -> crate::protocol::Stats {
            table
                .get_file_uris()
                .zip(table.get_stats())
                .filter_map(|(file_uri, file_stats)| {
                    if file_uri.ends_with(file_name) {
                        file_stats.unwrap()
                    } else {
                        None
                    }
                })
                .next()
                .unwrap()
        }

        let file_to_compare = "part-00000-7a509247-4f58-4453-9202-51d75dee59af-c000.snappy.parquet";

        assert_eq!(
            get_stats_for_file(&table_from_struct_stats, file_to_compare),
            get_stats_for_file(&table_from_json_stats, file_to_compare),
        );
    }

    #[tokio::test]
    async fn test_table_history() {
        let path = "./tests/data/simple_table_with_checkpoint";
        let mut latest_table = crate::open_table(path).await.unwrap();

        let mut table = crate::open_table_with_version(path, 1).await.unwrap();

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
        let path = "./tests/data/simple_table_with_checkpoint";
        let mut table = crate::open_table_with_version(path, 9).await.unwrap();
        let peek = table.peek_next_commit(table.version()).await.unwrap();
        assert!(matches!(peek, PeekCommit::New(..)));

        if let PeekCommit::New(version, actions) = peek {
            assert_eq!(table.version(), 9);
            assert!(!table.get_files_iter().any(|f| f
                == Path::from(
                    "part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet"
                )));

            assert_eq!(version, 10);
            assert_eq!(actions.len(), 2);

            table.update_incremental(None).await.unwrap();

            assert_eq!(table.version(), 10);
            assert!(table.get_files_iter().any(|f| f
                == Path::from(
                    "part-00000-f0e955c5-a1e3-4eec-834e-dcc098fc9005-c000.snappy.parquet"
                )));
        };

        let peek = table.peek_next_commit(table.version()).await.unwrap();
        assert!(matches!(peek, PeekCommit::UpToDate));
    }

    #[tokio::test]
    async fn test_read_vacuumed_log() {
        let path = "./tests/data/checkpoints_vacuumed";
        let table = crate::open_table(path).await.unwrap();
        assert_eq!(table.version(), 12);
    }

    #[tokio::test]
    async fn test_read_vacuumed_log_history() {
        let path = "./tests/data/checkpoints_vacuumed";
        let mut table = crate::open_table(path).await.unwrap();

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
        let table = crate::open_table("./tests/data/simple_table_with_cdc")
            .await
            .unwrap();
        assert_eq!(table.version(), 2);
        assert_eq!(
            table.get_files(),
            vec![Path::from(
                "part-00000-7444aec4-710a-4a4c-8abe-3323499043e9.c000.snappy.parquet"
            ),]
        );
    }
}
