#![cfg(feature = "integration_test")]

use bytes::Bytes;
use deltalake_core::storage::utils::flatten_list_stream;
use deltalake_core::test_utils::{IntegrationContext, StorageIntegration, TestResult};
use deltalake_core::{DeltaTableBuilder, ObjectStore};
use object_store::{path::Path, DynObjectStore, Error as ObjectStoreError};
use serial_test::serial;

#[tokio::test]
#[serial]
async fn test_object_store_local() -> TestResult {
    test_object_store(StorageIntegration::Local, false).await?;
    Ok(())
}

#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_object_store_azure() -> TestResult {
    test_object_store(StorageIntegration::Microsoft, false).await?;
    Ok(())
}

// NOTE: This test is ignored based on [this
// comment](https://github.com/delta-io/delta-rs/pull/1564#issuecomment-1721048753) and we should
// figure out a way to re-enable this test at least in the GitHub Actions CI environment
#[ignore]
#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_object_store_onelake() -> TestResult {
    let path = Path::from("17d3977c-d46e-4bae-8fed-ff467e674aed/Files/SampleCustomerList.csv");
    read_write_test_onelake(StorageIntegration::Onelake, &path).await?;
    Ok(())
}

// NOTE: This test is ignored based on [this
// comment](https://github.com/delta-io/delta-rs/pull/1564#issuecomment-1721048753) and we should
// figure out a way to re-enable this test at least in the GitHub Actions CI environment
#[ignore]
#[cfg(feature = "azure")]
#[tokio::test]
#[serial]
async fn test_object_store_onelake_abfs() -> TestResult {
    let path = Path::from("17d3977c-d46e-4bae-8fed-ff467e674aed/Files/SampleCustomerList.csv");
    read_write_test_onelake(StorageIntegration::OnelakeAbfs, &path).await?;
    Ok(())
}

#[cfg(feature = "s3")]
#[tokio::test]
#[ignore = "S3 does not support rename_if_not_exists"]
#[serial]
async fn test_object_store_aws() -> TestResult {
    test_object_store(StorageIntegration::Amazon, true).await?;
    Ok(())
}

// TODO pending emulator support in object store crate
#[ignore]
#[cfg(feature = "gcs")]
#[tokio::test]
#[serial]
async fn test_object_store_google() -> TestResult {
    test_object_store(StorageIntegration::Google, false).await?;
    Ok(())
}

#[cfg(feature = "hdfs")]
#[tokio::test]
#[serial]
async fn test_object_store_hdfs() -> TestResult {
    test_object_store(StorageIntegration::Hdfs, false).await?;
    Ok(())
}

async fn read_write_test_onelake(integration: StorageIntegration, path: &Path) -> TestResult {
    let context = IntegrationContext::new(integration)?;

    //println!("line 102-{:#?}",context.root_uri());

    let delta_store = DeltaTableBuilder::from_uri(&context.root_uri())
        .with_allow_http(true)
        .build_storage()?
        .object_store();

    //println!("{:#?}",delta_store);

    let expected = Bytes::from_static(b"test world from delta-rs on friday");

    delta_store.put(path, expected.clone()).await.unwrap();
    let fetched = delta_store.get(path).await.unwrap().bytes().await.unwrap();
    assert_eq!(expected, fetched);

    for range in [0..10, 3..5, 0..expected.len()] {
        let data = delta_store.get_range(path, range.clone()).await.unwrap();
        assert_eq!(&data[..], &expected[range])
    }

    Ok(())
}

async fn test_object_store(integration: StorageIntegration, skip_copy: bool) -> TestResult {
    let context = IntegrationContext::new(integration)?;
    let delta_store = DeltaTableBuilder::from_uri(context.root_uri())
        .with_allow_http(true)
        .build_storage()?
        .object_store();

    put_get_delete_list(delta_store.as_ref()).await?;
    list_with_delimiter(delta_store.as_ref()).await?;
    rename_and_copy(delta_store.as_ref()).await?;
    if !skip_copy {
        copy_if_not_exists(delta_store.as_ref()).await?;
    }
    rename_if_not_exists(delta_store.as_ref()).await?;
    // get_nonexistent_object(store, None).await?;
    Ok(())
}

async fn put_get_delete_list(storage: &DynObjectStore) -> TestResult {
    delete_fixtures(storage).await?;

    let content_list = flatten_list_stream(storage, None).await?;
    assert!(
        content_list.is_empty(),
        "Expected list to be empty; found: {:?}",
        content_list
    );

    let location = Path::from("test_dir/test_file.json");

    let data = Bytes::from("arbitrary data");
    let expected_data = data.clone();
    storage.put(&location, data).await?;

    let root = Path::from("/");

    // List everything
    let content_list = flatten_list_stream(storage, None).await?;
    assert_eq!(content_list, &[location.clone()]);

    // Should behave the same as no prefix
    let content_list = flatten_list_stream(storage, Some(&root)).await?;
    assert_eq!(content_list, &[location.clone()]);

    // List with delimiter
    let result = storage.list_with_delimiter(None).await?;
    assert_eq!(&result.objects, &[]);
    assert_eq!(result.common_prefixes.len(), 1);
    assert_eq!(result.common_prefixes[0], Path::from("test_dir"));

    // Should behave the same as no prefix
    let result = storage.list_with_delimiter(Some(&root)).await?;
    assert!(result.objects.is_empty());
    assert_eq!(result.common_prefixes.len(), 1);
    assert_eq!(result.common_prefixes[0], Path::from("test_dir"));

    // List everything starting with a prefix that should return results
    let prefix = Path::from("test_dir");
    let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
    assert_eq!(content_list, &[location.clone()]);

    // List everything starting with a prefix that shouldn't return results
    let prefix = Path::from("something");
    let content_list = flatten_list_stream(storage, Some(&prefix)).await?;
    assert!(content_list.is_empty());

    let read_data = storage.get(&location).await?.bytes().await?;
    assert_eq!(&*read_data, expected_data);

    // Test range request
    let range = 3..7;
    let range_result = storage.get_range(&location, range.clone()).await;

    let out_of_range = 200..300;
    let out_of_range_result = storage.get_range(&location, out_of_range).await;

    let bytes = range_result?;
    assert_eq!(bytes, expected_data.slice(range));

    // Should be a non-fatal error
    out_of_range_result.unwrap_err();

    let ranges = vec![0..1, 2..3, 0..5];
    let bytes = storage.get_ranges(&location, &ranges).await?;
    for (range, bytes) in ranges.iter().zip(bytes) {
        assert_eq!(bytes, expected_data.slice(range.clone()))
    }

    let head = storage.head(&location).await?;
    assert_eq!(head.size, expected_data.len());

    storage.delete(&location).await?;

    let content_list = flatten_list_stream(storage, None).await?;
    assert!(content_list.is_empty());

    let err = storage.get(&location).await.unwrap_err();
    assert!(matches!(err, ObjectStoreError::NotFound { .. }), "{}", err);

    let err = storage.head(&location).await.unwrap_err();
    assert!(matches!(err, ObjectStoreError::NotFound { .. }), "{}", err);

    // Test handling of paths containing an encoded delimiter

    let file_with_delimiter = Path::from_iter(["a", "b/c", "foo.file"]);
    storage
        .put(&file_with_delimiter, Bytes::from("arbitrary"))
        .await?;

    let files = flatten_list_stream(storage, None).await?;
    assert_eq!(files, vec![file_with_delimiter.clone()]);

    let files = flatten_list_stream(storage, Some(&Path::from("a/b"))).await?;
    assert!(files.is_empty());

    let files = storage
        .list_with_delimiter(Some(&Path::from("a/b")))
        .await?;
    assert!(files.common_prefixes.is_empty());
    assert!(files.objects.is_empty());

    let files = storage.list_with_delimiter(Some(&Path::from("a"))).await?;
    assert_eq!(files.common_prefixes, vec![Path::from_iter(["a", "b/c"])]);
    assert!(files.objects.is_empty());

    let files = storage
        .list_with_delimiter(Some(&Path::from_iter(["a", "b/c"])))
        .await?;
    assert!(files.common_prefixes.is_empty());
    assert_eq!(files.objects.len(), 1);
    assert_eq!(files.objects[0].location, file_with_delimiter);

    storage.delete(&file_with_delimiter).await?;

    // Test handling of paths containing non-ASCII characters, e.g. emoji

    let emoji_prefix = Path::from("🙀");
    let emoji_file = Path::from("🙀/😀.parquet");
    storage.put(&emoji_file, Bytes::from("arbitrary")).await?;

    storage.head(&emoji_file).await?;
    storage.get(&emoji_file).await?.bytes().await?;

    let files = flatten_list_stream(storage, Some(&emoji_prefix)).await?;

    assert_eq!(files, vec![emoji_file.clone()]);

    let dst = Path::from("foo.parquet");
    storage.copy(&emoji_file, &dst).await?;
    let mut files = flatten_list_stream(storage, None).await?;
    files.sort_unstable();
    assert_eq!(files, vec![emoji_file.clone(), dst.clone()]);

    storage.delete(&emoji_file).await?;
    storage.delete(&dst).await?;
    let files = flatten_list_stream(storage, Some(&emoji_prefix)).await?;
    assert!(files.is_empty());

    // Test handling of paths containing percent-encoded sequences

    // "HELLO" percent encoded
    let hello_prefix = Path::parse("%48%45%4C%4C%4F")?;
    let path = hello_prefix.child("foo.parquet");

    storage.put(&path, Bytes::from(vec![0, 1])).await?;
    let files = flatten_list_stream(storage, Some(&hello_prefix)).await?;
    assert_eq!(files, vec![path.clone()]);

    // Cannot list by decoded representation
    let files = flatten_list_stream(storage, Some(&Path::from("HELLO"))).await?;
    assert!(files.is_empty());

    // Cannot access by decoded representation
    let err = storage
        .head(&Path::from("HELLO/foo.parquet"))
        .await
        .unwrap_err();
    assert!(matches!(err, ObjectStoreError::NotFound { .. }), "{}", err);

    storage.delete(&path).await?;

    // Can also write non-percent encoded sequences
    let path = Path::parse("%Q.parquet")?;
    storage.put(&path, Bytes::from(vec![0, 1])).await?;

    let files = flatten_list_stream(storage, None).await?;
    assert_eq!(files, vec![path.clone()]);

    storage.delete(&path).await?;
    Ok(())
}

async fn list_with_delimiter(storage: &DynObjectStore) -> TestResult {
    delete_fixtures(storage).await?;

    // ==================== check: store is empty ====================
    let content_list = flatten_list_stream(storage, None).await?;
    assert!(content_list.is_empty());

    // ==================== do: create files ====================
    let data = Bytes::from("arbitrary data");

    let files: Vec<_> = [
        "test_file",
        "mydb/wb/000/000/000.segment",
        "mydb/wb/000/000/001.segment",
        "mydb/wb/000/000/002.segment",
        "mydb/wb/001/001/000.segment",
        "mydb/wb/foo.json",
        "mydb/wbwbwb/111/222/333.segment",
        "mydb/data/whatevs",
    ]
    .iter()
    .map(|&s| Path::from(s))
    .collect();

    for f in &files {
        let data = data.clone();
        storage.put(f, data).await?;
    }

    // ==================== check: prefix-list `mydb/wb` (directory) ====================
    let prefix = Path::from("mydb/wb");

    let expected_000 = Path::from("mydb/wb/000");
    let expected_001 = Path::from("mydb/wb/001");
    let expected_location = Path::from("mydb/wb/foo.json");

    let result = storage.list_with_delimiter(Some(&prefix)).await?;

    assert_eq!(result.common_prefixes, vec![expected_000, expected_001]);
    assert_eq!(result.objects.len(), 1);

    let object = &result.objects[0];

    assert_eq!(object.location, expected_location);
    assert_eq!(object.size, data.len());

    // ==================== check: prefix-list `mydb/wb/000/000/001` (partial filename doesn't match) ====================
    let prefix = Path::from("mydb/wb/000/000/001");

    let result = storage.list_with_delimiter(Some(&prefix)).await?;
    assert!(result.common_prefixes.is_empty());
    assert_eq!(result.objects.len(), 0);

    // ==================== check: prefix-list `not_there` (non-existing prefix) ====================
    let prefix = Path::from("not_there");

    let result = storage.list_with_delimiter(Some(&prefix)).await?;
    assert!(result.common_prefixes.is_empty());
    assert!(result.objects.is_empty());

    // ==================== do: remove all files ====================
    for f in &files {
        storage.delete(f).await?;
    }

    // ==================== check: store is empty ====================
    let content_list = flatten_list_stream(storage, None).await?;
    assert!(content_list.is_empty());
    Ok(())
}

async fn rename_and_copy(storage: &DynObjectStore) -> TestResult {
    // Create two objects
    let path1 = Path::from("test1");
    let path2 = Path::from("test2");
    let contents1 = Bytes::from("cats");
    let contents2 = Bytes::from("dogs");

    // copy() make both objects identical
    storage.put(&path1, contents1.clone()).await?;
    storage.put(&path2, contents2.clone()).await?;
    storage.copy(&path1, &path2).await?;
    let new_contents = storage.get(&path2).await?.bytes().await?;
    assert_eq!(&new_contents, &contents1);

    // rename() copies contents and deletes original
    storage.put(&path1, contents1.clone()).await?;
    storage.put(&path2, contents2.clone()).await?;
    storage.rename(&path1, &path2).await?;
    let new_contents = storage.get(&path2).await?.bytes().await?;
    assert_eq!(&new_contents, &contents1);
    let result = storage.get(&path1).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ObjectStoreError::NotFound { .. }
    ));

    // Clean up
    storage.delete(&path2).await?;
    Ok(())
}

async fn copy_if_not_exists(storage: &DynObjectStore) -> TestResult {
    // Create two objects
    let path1 = Path::from("test1");
    let path2 = Path::from("test2");
    let contents1 = Bytes::from("cats");
    let contents2 = Bytes::from("dogs");

    // copy_if_not_exists() errors if destination already exists
    storage.put(&path1, contents1.clone()).await?;
    storage.put(&path2, contents2.clone()).await?;
    let result = storage.copy_if_not_exists(&path1, &path2).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ObjectStoreError::AlreadyExists { .. }
    ));

    // copy_if_not_exists() copies contents and allows deleting original
    storage.delete(&path2).await?;
    storage.copy_if_not_exists(&path1, &path2).await?;
    storage.delete(&path1).await?;
    let new_contents = storage.get(&path2).await?.bytes().await?;
    assert_eq!(&new_contents, &contents1);
    let result = storage.get(&path1).await;
    assert!(result.is_err());
    assert!(matches!(
        result.unwrap_err(),
        ObjectStoreError::NotFound { .. }
    ));

    // Clean up
    storage.delete(&path2).await?;
    Ok(())
}

async fn rename_if_not_exists(storage: &DynObjectStore) -> TestResult {
    let path1 = Path::from("tmp_file1");
    let path2 = Path::from("tmp_file2");
    storage.put(&path1, bytes::Bytes::new()).await?;

    // delete objects
    let result = storage.rename_if_not_exists(&path1, &path2).await;
    assert!(result.is_ok());
    assert!(storage.head(&path1).await.is_err());
    assert!(storage.head(&path2).await.is_ok());

    storage.put(&path1, bytes::Bytes::new()).await?;
    let result = storage.rename_if_not_exists(&path1, &path2).await;
    assert!(result.is_err());
    assert!(storage.head(&path1).await.is_ok());
    assert!(storage.head(&path2).await.is_ok());
    Ok(())
}

// pub(crate) async fn get_nonexistent_object(
//     storage: &DynObjectStore,
//     location: Option<Path>,
// ) -> ObjectStoreResult<Bytes> {
//     let location = location.unwrap_or_else(|| Path::from("this_file_should_not_exist"));

//     let err = storage.head(&location).await.unwrap_err();
//     assert!(matches!(err, ObjectStoreError::NotFound { .. }));

//     storage.get(&location).await?.bytes().await
// }

async fn delete_fixtures(storage: &DynObjectStore) -> TestResult {
    let paths = flatten_list_stream(storage, None).await?;

    for f in &paths {
        storage.delete(f).await?;
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_object_store_prefixes_local() -> TestResult {
    test_object_store_prefixes(StorageIntegration::Local).await?;
    Ok(())
}

async fn test_object_store_prefixes(integration: StorageIntegration) -> TestResult {
    let context = IntegrationContext::new(integration)?;
    let prefixes = &["table path", "table path/hello%3F", "你好/😊"];
    for prefix in prefixes {
        let rooturi = format!("{}/{}", context.root_uri(), prefix);
        let delta_store = DeltaTableBuilder::from_uri(&rooturi)
            .with_allow_http(true)
            .build_storage()?
            .object_store();

        let contents = Bytes::from("cats");
        let path = Path::from("test");
        delta_store.put(&path, contents.clone()).await?;
        let data = delta_store.get(&path).await?.bytes().await?;
        assert_eq!(&data, &contents);
    }

    Ok(())
}
