use chrono::Duration;
use deltalake::action;
use deltalake::action::Add;
use deltalake::get_backend_for_uri_with_options;
use deltalake::storage::file::FileStorageBackend;
use deltalake::vacuum::Vacuum;
use deltalake::StorageBackend;
use deltalake::{
    DeltaTable, DeltaTableConfig, DeltaTableMetaData, Schema, SchemaDataType, SchemaField,
};
use serde_json::{json, Map, Value};
use serial_test::serial;
use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::SystemTime;
use tempdir::TempDir;

use deltalake::storage::StorageError;

use chrono::Utc;
use deltalake::vacuum::Clock;

#[tokio::test]
async fn vacuum_delta_8_0_table() {
    let backend = FileStorageBackend::new("");
    let mut table = deltalake::open_table(&backend.join_paths(&["tests", "data", "delta-0.8.0"]))
        .await
        .unwrap();

    let retention_hours = 1;
    let dry_run = true;

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(1))
        .dry_run(true)
        .execute(&mut table)
        .await;

    assert!(matches!(result.unwrap_err(),
        deltalake::vacuum::VacuumError::InvalidVacuumRetentionPeriod {
            provided,
            min,
        } if provided == Duration::hours(1).num_milliseconds()
            && min == table.get_state().tombstone_retention_millis(),
    ));

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(0))
        .dry_run(true)
        .enforce_retention_duration(false)
        .execute(&mut table)
        .await
        .unwrap();
    // do not enforce retention duration check with 0 hour will purge all files
    assert_eq!(
        result.files_deleted,
        vec![backend.join_paths(&[
            "tests",
            "data",
            "delta-0.8.0",
            "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet",
        ])]
    );

    let result = Vacuum::default()
        .with_retention_period(Duration::hours(169))
        .dry_run(true)
        .execute(&mut table)
        .await
        .unwrap();

    assert_eq!(
        result.files_deleted,
        vec![backend.join_paths(&[
            "tests",
            "data",
            "delta-0.8.0",
            "part-00001-911a94a2-43f6-4acb-8620-5e68c2654989-c000.snappy.parquet",
        ])]
    );

    let retention_hours = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        / 3600;
    let empty: Vec<String> = Vec::new();
    let result = Vacuum::default()
        .with_retention_period(Duration::hours(retention_hours as i64))
        .dry_run(true)
        .execute(&mut table)
        .await
        .unwrap();

    assert_eq!(result.files_deleted, empty);
}

#[tokio::test]
// Validate that files and directories that start with '.' or '_' are ignored
async fn test_ignored_files() {
    let mut context = setup_local_context();
    context
        .create_table_from_schema(get_schema(), &["date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        ".dotfile",
        "_underscore",
        "nested/.dotfile",
        "nested2/really/deep/_underscore",
        // Directories
        "_underscoredir/dont_delete_me",
        "_dotdir/dont_delete_me",
        "nested3/_underscoredir/dont_delete_me",
        "nested4/really/deep/.dotdir/dont_delete_me",
    ];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                &[],
                clock.current_timestamp_millis(),
                false,
            )
            .await;
    }

    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 0);
    for path in paths {
        assert!(!is_deleted(&mut context, path).await);
    } 
}

#[tokio::test]
// TODO: Partitions that start with _ are not ignored
async fn test_partitions_included() {}

#[tokio::test]
// files that are not managed by the delta log and have a last_modified greater than the retention period should be deleted
async fn test_non_managed_files() {
    let mut context = setup_local_context();
    context
        .create_table_from_schema(get_schema(), &["date"])
        .await;
    let clock = TestClock::from_systemtime();

    let paths = [
        "garbage_file",
        "nested/garbage_file",
        "nested2/really/garbage_file",
    ];

    for path in paths {
        context
            .add_file(
                path,
                "random junk".as_ref(),
                &[],
                clock.current_timestamp_millis(),
                false,
            )
            .await;
    }


    // Validate unmanaged files are not deleted within the retention period

    let res = {
        clock.tick(Duration::hours(1));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), 0);
    for path in paths {
        assert!(!is_deleted(&mut context, path).await);
    } 

    // Validate unmanaged files are deleted after the retention period
    let res = {
        clock.tick(Duration::days(8));
        let table = context.table.as_mut().unwrap();
        let mut plan = Vacuum::default();
        plan.clock = Some(Arc::new(clock.clone()));
        plan.execute(table).await.unwrap()
    };

    assert_eq!(res.files_deleted.len(), paths.len());
    for path in paths {
        assert!(is_deleted(&mut context, path).await);
    } 
}

async fn is_deleted(context: &mut Context, path: &str) -> bool {
    let uri = context.table.as_ref().unwrap().table_uri.to_string();
    let backend = context.get_storage();
    let path = uri + "/" + path;
    let res = backend.head_obj(&path).await;
    match res {
        Err(StorageError::NotFound) => true,
        _ => false,
    }
}

#[derive(Clone, Debug)]
struct TestClock {
    //TODO: mutex might be overkill. Maybe just use an atomic i64..
    now: Arc<Mutex<i64>>,
}

impl Clock for TestClock {
    fn current_timestamp_millis(&self) -> i64 {
        let inner = self.now.lock().unwrap();
        return *inner;
    }
}

impl TestClock {
    pub fn new(start: i64) -> Self {
        TestClock {
            now: Arc::new(Mutex::new(start)),
        }
    }

    pub fn from_systemtime() -> Self {
        TestClock {
            now: Arc::new(Mutex::new(Utc::now().timestamp_millis())),
        }
    }

    pub fn set_timestamp(&self, timestamp: i64) {
        let mut inner = self.now.lock().unwrap();
        *inner = timestamp;
    }

    pub fn tick(&self, duration: Duration) {
        let mut inner = self.now.lock().unwrap();
        *inner = *inner + duration.num_milliseconds();
    }
}

pub fn get_schema() -> Schema {
    return Schema::new(vec![
        SchemaField::new(
            "x".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "y".to_owned(),
            SchemaDataType::primitive("integer".to_owned()),
            false,
            HashMap::new(),
        ),
        SchemaField::new(
            "date".to_owned(),
            SchemaDataType::primitive("string".to_owned()),
            false,
            HashMap::new(),
        ),
    ]);
}

#[derive(Default)]
pub struct Context {
    pub table: Option<DeltaTable>,
    pub backend: Option<Box<dyn StorageBackend>>,
    pub config: HashMap<String, String>,
    pub storage_context: Option<Box<dyn Any>>,
}

pub struct LocalFS {
    pub tmp_dir: TempDir,
}

impl Context {
    pub fn get_storage(&mut self) -> &Box<dyn StorageBackend> {
        if self.backend.is_none() {
            self.backend = Some(self.new_storage())
        }

        return self.backend.as_ref().unwrap();
    }

    fn new_storage(&self) -> Box<dyn StorageBackend> {
        let config = self.config.clone();
        let uri = config.get("URI").unwrap().to_string();
        get_backend_for_uri_with_options(&uri, config).unwrap()
    }

    pub async fn add_file(
        &mut self,
        path: &str,
        data: &[u8],
        partition_values: &[(&str, Option<&str>)],
        create_time: i64,
        commit_to_log: bool,
    ) {
        let uri = self.table.as_ref().unwrap().table_uri.to_string();
        let backend = self.get_storage();
        let remote_path = uri + "/" + path;

        backend.put_obj(&remote_path, data).await.unwrap();

        if commit_to_log {
            let mut part_values = HashMap::new();
            for v in partition_values {
                part_values.insert(v.0.to_string(), v.1.map(|v| v.to_string()));
            }

            let add = Add {
                path: path.into(),
                size: data.len() as i64,
                partition_values: part_values,
                data_change: true,
                ..Default::default()
            };
            let table = self.table.as_mut().unwrap();
            let mut transaction = table.create_transaction(None);
            transaction.add_action(action::Action::add(add));
            transaction.commit(None, None).await.unwrap();
        }
    }

    //Create and set a new table from the provided schema
    pub async fn create_table_from_schema(&mut self, schema: Schema, partitions: &[&str]) {
        let p = partitions
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let table_meta = DeltaTableMetaData::new(
            Some("delta-rs_test_table".to_owned()),
            Some("Table created by delta-rs tests".to_owned()),
            None,
            schema.clone(),
            p,
            HashMap::new(),
        );

        let backend = self.new_storage();
        let p = self.config.get("URI").unwrap().to_string();
        let mut dt = DeltaTable::new(&p, backend, DeltaTableConfig::default()).unwrap();
        let mut commit_info = Map::<String, Value>::new();

        let protocol = action::Protocol {
            min_reader_version: 1,
            min_writer_version: 2,
        };

        commit_info.insert(
            "operation".to_string(),
            serde_json::Value::String("CREATE TABLE".to_string()),
        );
        let _res = dt
            .create(
                table_meta.clone(),
                protocol.clone(),
                Some(commit_info),
                None,
            )
            .await
            .unwrap();

        self.table = Some(dt);
    }
}

pub fn setup_local_context() -> Context {
    let tmp_dir = tempdir::TempDir::new("delta-rs_tests").unwrap();
    let mut config = HashMap::new();
    config.insert(
        "URI".to_owned(),
        tmp_dir.path().to_str().to_owned().unwrap().to_string(),
    );

    let localfs = LocalFS { tmp_dir };

    Context {
        storage_context: Some(Box::new(localfs)),
        config,
        ..Context::default()
    }
}
