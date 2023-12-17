#![cfg(feature = "integration_test")]

use deltalake_core::test_utils::{IntegrationContext, StorageIntegration, TestResult, TestTables};
use deltalake_core::{DeltaTableBuilder, ObjectStore};
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
use maplit::hashmap;
use object_store::path::Path;
use serial_test::serial;

#[allow(dead_code)]
mod fs_common;

static TEST_PREFIXES: &[&str] = &["my table", "你好/😊"];

/// TEST_PREFIXES as they should appear in object stores.
static TEST_PREFIXES_ENCODED: &[&str] = &["my%20table", "%E4%BD%A0%E5%A5%BD/%F0%9F%98%8A"];

#[cfg(feature = "azure")]
mod azure {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_read_tables_azure() -> TestResult {
        read_tables(StorageIntegration::Microsoft).await?;

        for (prefix, prefix_encoded) in TEST_PREFIXES.iter().zip(TEST_PREFIXES_ENCODED.iter()) {
            read_table_paths(StorageIntegration::Microsoft, prefix, prefix_encoded).await?;
        }

        Ok(())
    }
}

mod local {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_read_tables_local() -> TestResult {
        read_tables(StorageIntegration::Local).await?;

        for prefix in TEST_PREFIXES {
            read_table_paths(StorageIntegration::Local, prefix, prefix).await?;
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_action_reconciliation() {
        let path = "./tests/data/action_reconciliation";
        let mut table = fs_common::create_table(path, None).await;

        // Add a file.
        let a = fs_common::add(3 * 60 * 1000);
        assert_eq!(1, fs_common::commit_add(&mut table, &a).await);
        assert_eq!(table.get_files(), vec![Path::from(a.path.clone())]);

        // Remove added file.
        let r = deltalake_core::kernel::Remove {
            path: a.path.clone(),
            deletion_timestamp: Some(chrono::Utc::now().timestamp_millis()),
            data_change: false,
            extended_file_metadata: None,
            partition_values: None,
            size: None,
            tags: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
        };

        assert_eq!(2, fs_common::commit_removes(&mut table, vec![&r]).await);
        assert_eq!(table.get_files().len(), 0);
        assert_eq!(
            table
                .get_state()
                .all_tombstones()
                .iter()
                .map(|r| r.path.as_str())
                .collect::<Vec<_>>(),
            vec![a.path.as_str()]
        );

        // Add removed file back.
        assert_eq!(3, fs_common::commit_add(&mut table, &a).await);
        assert_eq!(table.get_files(), vec![Path::from(a.path)]);
        // tombstone is removed.
        assert_eq!(table.get_state().all_tombstones().len(), 0);
    }
}

#[cfg(feature = "hdfs")]
mod hdfs {
    use super::*;
    #[tokio::test]
    #[serial]
    async fn test_read_tables_hdfs() -> TestResult {
        Ok(read_tables(StorageIntegration::Hdfs).await?)
    }
}

#[cfg(any(feature = "s3", feature = "s3-native-tls"))]
mod s3 {
    use super::*;
    #[tokio::test]
    #[serial]
    async fn test_read_tables_aws() -> TestResult {
        read_tables(StorageIntegration::Amazon).await?;

        for (prefix, prefix_encoded) in TEST_PREFIXES.iter().zip(TEST_PREFIXES_ENCODED.iter()) {
            read_table_paths(StorageIntegration::Amazon, prefix, prefix_encoded).await?;
        }

        Ok(())
    }
}

async fn read_tables(storage: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(storage)?;
    context.load_table(TestTables::Simple).await?;
    context.load_table(TestTables::Golden).await?;
    context
        .load_table(TestTables::Delta0_8_0SpecialPartitioned)
        .await?;

    read_simple_table(&context).await?;
    read_simple_table_with_version(&context).await?;
    read_golden(&context).await?;

    Ok(())
}

async fn read_table_paths(
    storage: StorageIntegration,
    table_root: &str,
    upload_path: &str,
) -> TestResult {
    let context = IntegrationContext::new(storage)?;
    context
        .load_table_with_name(TestTables::Delta0_8_0SpecialPartitioned, upload_path)
        .await?;

    verify_store(&context, table_root).await?;

    read_encoded_table(&context, table_root).await?;

    Ok(())
}

async fn verify_store(integration: &IntegrationContext, root_path: &str) -> TestResult {
    let table_uri = format!("{}/{}", integration.root_uri(), root_path);
    let storage = DeltaTableBuilder::from_uri(table_uri.clone())
        .with_allow_http(true)
        .build_storage()?
        .object_store();

    let files = storage.list_with_delimiter(None).await?;
    assert_eq!(
        vec![
            Path::parse("_delta_log").unwrap(),
            Path::parse("x=A%2FA").unwrap(),
            Path::parse("x=B%20B").unwrap(),
        ],
        files.common_prefixes
    );

    Ok(())
}

async fn read_encoded_table(integration: &IntegrationContext, root_path: &str) -> TestResult {
    let table_uri = format!("{}/{}", integration.root_uri(), root_path);

    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .load()
        .await?;

    assert_eq!(table.version(), 0);
    assert_eq!(table.get_files().len(), 2);

    Ok(())
}

async fn read_simple_table(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Simple);
    // the s3 options don't hurt us for other integrations ...
    #[cfg(any(feature = "s3", feature = "s3-native-tls"))]
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .load()
        .await?;
    #[cfg(not(any(feature = "s3", feature = "s3-native-tls")))]
    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .load()
        .await?;

    assert_eq!(table.version(), 4);
    assert_eq!(table.protocol().min_writer_version, 2);
    assert_eq!(table.protocol().min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet"),
            Path::from("part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet"),
            Path::from("part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet"),
            Path::from("part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"),
            Path::from("part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet"),
        ]
    );
    let tombstones = table.get_state().all_tombstones();
    assert_eq!(tombstones.len(), 31);
    assert!(tombstones.contains(&deltalake_core::kernel::Remove {
        path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
        deletion_timestamp: Some(1587968596250),
        data_change: true,
        extended_file_metadata: None,
        deletion_vector: None,
        base_row_id: None,
        default_row_commit_version: None,
        size: None,
        partition_values: None,
        tags: None,
    }));

    Ok(())
}

async fn read_simple_table_with_version(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Simple);

    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .with_version(3)
        .load()
        .await?;

    assert_eq!(table.version(), 3);
    assert_eq!(table.protocol().min_writer_version, 2);
    assert_eq!(table.protocol().min_reader_version, 1);
    assert_eq!(
        table.get_files(),
        vec![
            Path::from("part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet"),
            Path::from("part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet"),
            Path::from("part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet"),
            Path::from("part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"),
            Path::from("part-00000-f17fcbf5-e0dc-40ba-adae-ce66d1fcaef6-c000.snappy.parquet"),
            Path::from("part-00001-bb70d2ba-c196-4df2-9c85-f34969ad3aa9-c000.snappy.parquet"),
        ]
    );
    let tombstones = table.get_state().all_tombstones();
    assert_eq!(tombstones.len(), 29);
    assert!(tombstones.contains(&deltalake_core::kernel::Remove {
        path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
        deletion_timestamp: Some(1587968596250),
        data_change: true,
        tags: None,
        partition_values: None,
        base_row_id: None,
        default_row_commit_version: None,
        size: None,
        deletion_vector: None,
        extended_file_metadata: None,
    }));

    Ok(())
}

async fn read_golden(integration: &IntegrationContext) -> TestResult {
    let table_uri = integration.uri_for_table(TestTables::Golden);

    let table = DeltaTableBuilder::from_uri(table_uri)
        .with_allow_http(true)
        .load()
        .await
        .unwrap();

    assert_eq!(table.version(), 0);
    assert_eq!(table.protocol().min_writer_version, 2);
    assert_eq!(table.protocol().min_reader_version, 1);

    Ok(())
}

// TODO we keep teh gcs test around until we can also integrate with CI
#[cfg(feature = "gcs")]
mod gcs {
    use object_store::path::Path;
    /*
     * The storage account to run this test must be provided by the developer and test are executed locally.
     *
     * To prepare test execution, create a gcs bucket and upload the contents of ./rust/tests/data/simple_table
     * into that bucket.
     *
     * Set the environment variables used for authentication as outlined in rust/src/storage/gcs/mod.rs
     * Also set GCS_DELTA_BUCKET for the created bucket name.
     *
     * remove the ignore statement below and execute tests via 'cargo test --features gcs'
     */
    #[ignore]
    #[tokio::test]
    async fn test_gcs_simple() {
        let bucket = std::env::var("GCS_DELTA_BUCKET").unwrap();
        let table = deltalake_core::open_table(format!("gs://{}/simple_table", bucket).as_str())
            .await
            .unwrap();
        assert_eq!(table.version(), 4);
        assert_eq!(table.protocol().min_writer_version, 2);
        assert_eq!(table.protocol().min_reader_version, 1);
        assert_eq!(
            table.get_files(),
            vec![
                Path::from("part-00000-c1777d7d-89d9-4790-b38a-6ee7e24456b1-c000.snappy.parquet"),
                Path::from("part-00001-7891c33d-cedc-47c3-88a6-abcfb049d3b4-c000.snappy.parquet"),
                Path::from("part-00004-315835fe-fb44-4562-98f6-5e6cfa3ae45d-c000.snappy.parquet"),
                Path::from("part-00007-3a0e4727-de0d-41b6-81ef-5223cf40f025-c000.snappy.parquet"),
                Path::from("part-00000-2befed33-c358-4768-a43c-3eda0d2a499d-c000.snappy.parquet"),
            ]
        );
        let tombstones = table.get_state().all_tombstones();
        assert_eq!(tombstones.len(), 31);
        assert!(tombstones.contains(&deltalake_core::kernel::Remove {
            path: "part-00006-63ce9deb-bc0f-482d-b9a1-7e717b67f294-c000.snappy.parquet".to_string(),
            deletion_timestamp: Some(1587968596250),
            data_change: true,
            extended_file_metadata: None,
            deletion_vector: None,
            base_row_id: None,
            default_row_commit_version: None,
            size: None,
            partition_values: None,
            tags: None,
        }));
    }
}
