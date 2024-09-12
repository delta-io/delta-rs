//! AWS S3 and similar tooling for delta-rs
//!
//! This module also contains the [S3DynamoDbLogStore] implemtnation for concurrent writer support
//! with AWS S3 specifically.

pub mod constants;
mod credentials;
pub mod errors;
pub mod logstore;
#[cfg(feature = "native-tls")]
mod native;
pub mod storage;
use aws_config::SdkConfig;
use aws_sdk_dynamodb::error::SdkError;
use aws_sdk_dynamodb::{
    operation::{
        create_table::CreateTableError, delete_item::DeleteItemError, get_item::GetItemError,
        put_item::PutItemError, query::QueryError, update_item::UpdateItemError,
    },
    types::{
        AttributeDefinition, AttributeValue, BillingMode, KeySchemaElement, KeyType,
        ScalarAttributeType,
    },
    Client,
};
use lazy_static::lazy_static;
use object_store::aws::AmazonS3ConfigKey;
use regex::Regex;
use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tracing::debug;

use deltalake_core::logstore::{default_logstore, logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{factories, url_prefix_handler, ObjectStoreRef, StorageOptions};
use deltalake_core::{DeltaResult, Path};
use url::Url;

use errors::{DynamoDbConfigError, LockClientError};
use storage::{S3ObjectStoreFactory, S3StorageOptions};

#[derive(Clone, Debug, Default)]
pub struct S3LogStoreFactory {}

impl LogStoreFactory for S3LogStoreFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let store = url_prefix_handler(store, Path::parse(location.path())?);

        // With conditional put in S3-like API we can use the deltalake default logstore which use PutIfAbsent
        if options
            .0
            .contains_key(AmazonS3ConfigKey::ConditionalPut.as_ref())
        {
            debug!("S3LogStoreFactory has been asked to create a default LogStore where the underlying store has Conditonal Put enabled - no locking provider required");
            return Ok(default_logstore(store, location, options));
        }

        if options
            .0
            .contains_key(AmazonS3ConfigKey::CopyIfNotExists.as_ref())
        {
            debug!("S3LogStoreFactory has been asked to create a LogStore where the underlying store has copy-if-not-exists enabled - no locking provider required");
            return Ok(logstore::default_s3_logstore(store, location, options));
        }

        let s3_options = S3StorageOptions::from_map(&options.0)?;

        if s3_options.locking_provider.as_deref() != Some("dynamodb") {
            debug!("S3LogStoreFactory has been asked to create a LogStore without the dynamodb locking provider");
            return Ok(logstore::default_s3_logstore(store, location, options));
        }

        Ok(Arc::new(logstore::S3DynamoDbLogStore::try_new(
            location.clone(),
            options.clone(),
            &s3_options,
            store,
        )?))
    }
}

/// Register an [ObjectStoreFactory] for common S3 [Url] schemes
pub fn register_handlers(_additional_prefixes: Option<Url>) {
    let object_stores = Arc::new(S3ObjectStoreFactory::default());
    let log_stores = Arc::new(S3LogStoreFactory::default());
    for scheme in ["s3", "s3a"].iter() {
        let url = Url::parse(&format!("{}://", scheme)).unwrap();
        factories().insert(url.clone(), object_stores.clone());
        logstores().insert(url.clone(), log_stores.clone());
    }
}

/// Representation of a log entry stored in DynamoDb
/// dynamo db item consists of:
/// - table_path: String - tracked in the log store implementation
/// - file_name: String - commit version.json (part of primary key), stored as i64 in this struct
/// - temp_path: String - name of temporary file containing commit info
/// - complete: bool - operation completed, i.e. atomic rename from `tempPath` to `fileName` succeeded
/// - expire_time: `Option<SystemTime>` - epoch seconds at which this external commit entry is safe to be deleted
#[derive(Debug, PartialEq)]
pub struct CommitEntry {
    /// Commit version, stored as file name (e.g., 00000N.json) in dynamodb (relative to `_delta_log/`
    pub version: i64,
    /// Path to temp file for this commit, relative to the `_delta_log
    pub temp_path: Path,
    /// true if delta json file is successfully copied to its destination location, else false
    pub complete: bool,
    /// If complete = true, epoch seconds at which this external commit entry is safe to be deleted
    pub expire_time: Option<SystemTime>,
}

impl CommitEntry {
    /// Create a new log entry for the given version.
    /// Initial log entry state is incomplete.
    pub fn new(version: i64, temp_path: Path) -> CommitEntry {
        Self {
            version,
            temp_path,
            complete: false,
            expire_time: None,
        }
    }
}

/// Lock client backed by DynamoDb.
pub struct DynamoDbLockClient {
    /// DynamoDb client
    dynamodb_client: Client,
    /// configuration of the
    config: DynamoDbConfig,
}

impl std::fmt::Debug for DynamoDbLockClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(f, "DynamoDbLockClient(config: {:?})", self.config)
    }
}

impl DynamoDbLockClient {
    /// Creates a new DynamoDbLockClient from the supplied storage options.
    pub fn try_new(
        sdk_config: &SdkConfig,
        lock_table_name: Option<String>,
        billing_mode: Option<String>,
        max_elapsed_request_time: Option<String>,
        dynamodb_override_endpoint: Option<String>,
    ) -> Result<Self, DynamoDbConfigError> {
        let dynamodb_sdk_config =
            Self::create_dynamodb_sdk_config(sdk_config, dynamodb_override_endpoint);

        let dynamodb_client = aws_sdk_dynamodb::Client::new(&dynamodb_sdk_config);

        let lock_table_name = lock_table_name
            .or_else(|| std::env::var(constants::LOCK_TABLE_KEY_NAME).ok())
            .unwrap_or(constants::DEFAULT_LOCK_TABLE_NAME.to_owned());

        let billing_mode = if let Some(bm) = billing_mode
            .or_else(|| std::env::var(constants::BILLING_MODE_KEY_NAME).ok())
            .as_ref()
        {
            BillingMode::try_parse(bm.to_ascii_uppercase().as_str())
                .map_err(|_| DynamoDbConfigError::InvalidBillingMode(String::default()))?
        } else {
            BillingMode::PayPerRequest
        };

        let max_elapsed_request_time = max_elapsed_request_time
            .or_else(|| std::env::var(constants::MAX_ELAPSED_REQUEST_TIME_KEY_NAME).ok())
            .map_or_else(
                || Ok(Duration::from_secs(60)),
                |secs| u64::from_str(&secs).map(Duration::from_secs),
            )
            .map_err(|err| DynamoDbConfigError::ParseMaxElapsedRequestTime { source: err })?;

        let config = DynamoDbConfig {
            billing_mode,
            lock_table_name,
            max_elapsed_request_time,
            sdk_config: sdk_config.clone(),
        };
        Ok(Self {
            dynamodb_client,
            config,
        })
    }
    fn create_dynamodb_sdk_config(
        sdk_config: &SdkConfig,
        dynamodb_override_endpoint: Option<String>,
    ) -> SdkConfig {
        /*
        if dynamodb_override_endpoint exists/AWS_ENDPOINT_URL_DYNAMODB is specified by user
        use dynamodb_override_endpoint to create dynamodb client
        */

        match dynamodb_override_endpoint {
            Some(dynamodb_endpoint_url) => sdk_config
                .to_owned()
                .to_builder()
                .endpoint_url(dynamodb_endpoint_url)
                .build(),
            None => sdk_config.to_owned(),
        }
    }

    /// Create the lock table where DynamoDb stores the commit information for all delta tables.
    ///
    /// Transparently handles the case where that table already exists, so it's safe to call.
    /// After `create_table` operation is executed, the table state in DynamoDb is `creating`, and
    /// it's not immediately useable. This method does not wait for the table state to become
    /// `active`, so transient failures might occurr when immediately using the lock client.
    pub async fn try_create_lock_table(&self) -> Result<CreateLockTableResult, LockClientError> {
        let attribute_definitions = vec![
            AttributeDefinition::builder()
                .attribute_name(constants::ATTR_TABLE_PATH)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
            AttributeDefinition::builder()
                .attribute_name(constants::ATTR_FILE_NAME)
                .attribute_type(ScalarAttributeType::S)
                .build()
                .unwrap(),
        ];
        let request = self
            .dynamodb_client
            .create_table()
            .set_attribute_definitions(Some(attribute_definitions))
            .set_key_schema(Some(vec![
                KeySchemaElement::builder()
                    .attribute_name(constants::ATTR_TABLE_PATH.to_owned())
                    .key_type(KeyType::Hash)
                    .build()
                    .unwrap(),
                KeySchemaElement::builder()
                    .attribute_name(constants::ATTR_FILE_NAME.to_owned())
                    .key_type(KeyType::Range)
                    .build()
                    .unwrap(),
            ]))
            .billing_mode(self.config.billing_mode.clone())
            .table_name(&self.config.lock_table_name)
            .send();
        match request.await {
            Ok(_) => Ok(CreateLockTableResult::TableCreated),
            Err(sdk_err) => match sdk_err.as_service_error() {
                Some(CreateTableError::ResourceInUseException(_)) => {
                    Ok(CreateLockTableResult::TableAlreadyExists)
                }
                Some(_) => Err(LockClientError::LockTableCreateFailure {
                    name: self.config.lock_table_name.clone(),
                    source: Box::new(sdk_err.into_service_error()),
                }),
                _ => Err(LockClientError::GenericDynamoDb {
                    source: Box::new(sdk_err),
                }),
            },
        }
    }

    /// Get the name of the lock table for transactional commits used by the DynamoDb lock client.
    pub fn get_lock_table_name(&self) -> String {
        self.config.lock_table_name.clone()
    }

    pub fn get_dynamodb_config(&self) -> &DynamoDbConfig {
        &self.config
    }

    fn get_primary_key(&self, version: i64, table_path: &str) -> HashMap<String, AttributeValue> {
        maplit::hashmap! {
            constants::ATTR_TABLE_PATH.to_owned()  => string_attr(table_path),
            constants::ATTR_FILE_NAME.to_owned()   => string_attr(format!("{:020}.json", version)),
        }
    }

    /// Read a log entry from DynamoDb.
    pub async fn get_commit_entry(
        &self,
        table_path: &str,
        version: i64,
    ) -> Result<Option<CommitEntry>, LockClientError> {
        let item = self
            .retry(
                || async {
                    self.dynamodb_client
                        .get_item()
                        .consistent_read(true)
                        .table_name(&self.config.lock_table_name)
                        .set_key(Some(self.get_primary_key(version, table_path)))
                        .send()
                        .await
                },
                |err| match err.as_service_error() {
                    Some(GetItemError::ProvisionedThroughputExceededException(_)) => true,
                    _ => false,
                },
            )
            .await
            .map_err(|err| match err.as_service_error() {
                Some(GetItemError::ProvisionedThroughputExceededException(_)) => {
                    LockClientError::ProvisionedThroughputExceeded
                }
                _ => err.into(),
            })?;
        item.item.as_ref().map(CommitEntry::try_from).transpose()
    }

    /// write new entry to to DynamoDb lock table.
    pub async fn put_commit_entry(
        &self,
        table_path: &str,
        entry: &CommitEntry,
    ) -> Result<(), LockClientError> {
        self.retry(
            || async {
                let item = create_value_map(entry, table_path);
                let _ = self
                    .dynamodb_client
                    .put_item()
                    .condition_expression(constants::CONDITION_EXPR_CREATE.as_str())
                    .table_name(self.get_lock_table_name())
                    .set_item(Some(item))
                    .send()
                    .await?;
                Ok(())
            },
            |err: &SdkError<_, _>| match err.as_service_error() {
                Some(PutItemError::ProvisionedThroughputExceededException(_)) => true,
                _ => false,
            },
        )
        .await
        .map_err(|err| match err.as_service_error() {
            Some(PutItemError::ProvisionedThroughputExceededException(_)) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            Some(PutItemError::ConditionalCheckFailedException(_)) => {
                LockClientError::VersionAlreadyExists {
                    table_path: table_path.to_owned(),
                    version: entry.version,
                }
            }
            Some(PutItemError::ResourceNotFoundException(_)) => LockClientError::LockTableNotFound,
            _ => err.into(),
        })
    }

    /// Get the latest entry (entry with highest version).
    pub async fn get_latest_entry(
        &self,
        table_path: &str,
    ) -> Result<Option<CommitEntry>, LockClientError> {
        Ok(self
            .get_latest_entries(table_path, 1)
            .await?
            .into_iter()
            .next())
    }

    /// Find the latest entry in the lock table for the delta table on the specified `table_path`.
    pub async fn get_latest_entries(
        &self,
        table_path: &str,
        limit: i64,
    ) -> Result<Vec<CommitEntry>, LockClientError> {
        let query_result = self
            .retry(
                || async {
                    self.dynamodb_client
                        .query()
                        .table_name(self.get_lock_table_name())
                        .consistent_read(true)
                        .limit(limit.try_into().unwrap_or(i32::MAX))
                        .scan_index_forward(false)
                        .key_condition_expression(format!("{} = :tn", constants::ATTR_TABLE_PATH))
                        .set_expression_attribute_values(Some(
                            maplit::hashmap!(":tn".into() => string_attr(table_path)),
                        ))
                        .send()
                        .await
                },
                |err: &SdkError<_, _>| match err.as_service_error() {
                    Some(QueryError::ProvisionedThroughputExceededException(_)) => true,
                    _ => false,
                },
            )
            .await
            .map_err(|err| match err.as_service_error() {
                Some(QueryError::ProvisionedThroughputExceededException(_)) => {
                    LockClientError::ProvisionedThroughputExceeded
                }
                _ => err.into(),
            })?;

        query_result
            .items
            .unwrap()
            .iter()
            .map(CommitEntry::try_from)
            .collect()
    }

    /// Update existing log entry
    pub async fn update_commit_entry(
        &self,
        version: i64,
        table_path: &str,
    ) -> Result<UpdateLogEntryResult, LockClientError> {
        let seconds_since_epoch = (SystemTime::now()
            + constants::DEFAULT_COMMIT_ENTRY_EXPIRATION_DELAY)
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let res = self
            .retry(
                || async {
                    let _ = self
                        .dynamodb_client
                        .update_item()
                        .table_name(self.get_lock_table_name())
                        .set_key(Some(self.get_primary_key(version, table_path)))
                        .update_expression("SET complete = :c, expireTime = :e".to_owned())
                        .set_expression_attribute_values(Some(maplit::hashmap! {
                            ":c".to_owned() => string_attr("true"),
                            ":e".to_owned() => num_attr(seconds_since_epoch),
                            ":f".into() => string_attr("false"),
                        }))
                        .condition_expression(constants::CONDITION_UPDATE_INCOMPLETE)
                        .send()
                        .await?;
                    Ok(())
                },
                |err: &SdkError<_, _>| match err.as_service_error() {
                    Some(UpdateItemError::ProvisionedThroughputExceededException(_)) => true,
                    _ => false,
                },
            )
            .await;

        match res {
            Ok(()) => Ok(UpdateLogEntryResult::UpdatePerformed),
            Err(err) => match err.as_service_error() {
                Some(UpdateItemError::ProvisionedThroughputExceededException(_)) => {
                    Err(LockClientError::ProvisionedThroughputExceeded)
                }
                Some(UpdateItemError::ConditionalCheckFailedException(_)) => {
                    Ok(UpdateLogEntryResult::AlreadyCompleted)
                }
                _ => Err(err.into()),
            },
        }
    }

    /// Delete existing log entry if it is not already complete
    pub async fn delete_commit_entry(
        &self,
        version: i64,
        table_path: &str,
    ) -> Result<(), LockClientError> {
        self.retry(
            || async {
                let _ = self
                    .dynamodb_client
                    .delete_item()
                    .table_name(self.get_lock_table_name())
                    .set_key(Some(self.get_primary_key(version, table_path)))
                    .set_expression_attribute_values(Some(maplit::hashmap! {
                        ":f".into() => string_attr("false"),
                    }))
                    .condition_expression(constants::CONDITION_DELETE_INCOMPLETE.as_str())
                    .send()
                    .await?;
                Ok(())
            },
            |err: &SdkError<_, _>| match err.as_service_error() {
                Some(DeleteItemError::ProvisionedThroughputExceededException(_)) => true,
                _ => false,
            },
        )
        .await
        .map_err(|err| match err.as_service_error() {
            Some(DeleteItemError::ProvisionedThroughputExceededException(_)) => {
                LockClientError::ProvisionedThroughputExceeded
            }
            Some(DeleteItemError::ConditionalCheckFailedException(_)) => {
                LockClientError::VersionAlreadyCompleted {
                    table_path: table_path.to_owned(),
                    version,
                }
            }
            _ => err.into(),
        })
    }

    async fn retry<I, E, F, Fut, Wn>(&self, operation: F, when: Wn) -> Result<I, E>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<I, E>>,
        Wn: Fn(&E) -> bool,
    {
        use backon::Retryable;
        let backoff = backon::ExponentialBuilder::default()
            .with_factor(2.)
            .with_max_delay(self.config.max_elapsed_request_time);
        operation.retry(backoff).when(when).await
    }
}

#[derive(Debug, PartialEq)]
pub enum UpdateLogEntryResult {
    UpdatePerformed,
    AlreadyCompleted,
}

impl TryFrom<&HashMap<String, AttributeValue>> for CommitEntry {
    type Error = LockClientError;

    fn try_from(item: &HashMap<String, AttributeValue>) -> Result<Self, Self::Error> {
        let version_str = extract_required_string_field(item, constants::ATTR_FILE_NAME)?;
        let version = extract_version_from_filename(version_str).ok_or_else(|| {
            LockClientError::InconsistentData {
                description: format!(
                    "invalid log file name: can't extract version number from '{version_str}'"
                ),
            }
        })?;
        let temp_path = extract_required_string_field(item, constants::ATTR_TEMP_PATH)?;
        let temp_path =
            Path::from_iter(DELTA_LOG_PATH.parts().chain(Path::from(temp_path).parts()));
        let expire_time: Option<SystemTime> =
            extract_optional_number_field(item, constants::ATTR_EXPIRE_TIME)?
                .map(|s| {
                    s.parse::<u64>()
                        .map_err(|err| LockClientError::InconsistentData {
                            description: format!("conversion to number failed, {err}"),
                        })
                })
                .transpose()?
                .map(epoch_to_system_time);
        Ok(Self {
            version,
            temp_path,
            complete: extract_required_string_field(item, constants::ATTR_COMPLETE)? == "true",
            expire_time,
        })
    }
}

fn system_time_to_epoch(t: &SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs()
}

fn epoch_to_system_time(s: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + Duration::from_secs(s)
}

fn create_value_map(
    commit_entry: &CommitEntry,
    table_path: &str,
) -> HashMap<String, AttributeValue> {
    // cut off `_delta_log` part: temp_path in DynamoDb is relative to `_delta_log` not table root.
    let temp_path = Path::from_iter(commit_entry.temp_path.parts().skip(1));
    let mut value_map = maplit::hashmap! {
        constants::ATTR_TABLE_PATH.to_owned()  => string_attr(table_path),
        constants::ATTR_FILE_NAME.to_owned()   => string_attr(format!("{:020}.json", commit_entry.version)),
        constants::ATTR_TEMP_PATH.to_owned()   => string_attr(temp_path),
        constants::ATTR_COMPLETE.to_owned()    => string_attr(if commit_entry.complete { "true" } else { "false" }),
    };
    commit_entry.expire_time.as_ref().map(|t| {
        value_map.insert(
            constants::ATTR_EXPIRE_TIME.to_owned(),
            num_attr(system_time_to_epoch(t)),
        )
    });
    value_map
}

#[derive(Debug)]
pub struct DynamoDbConfig {
    pub billing_mode: BillingMode,
    pub lock_table_name: String,
    pub max_elapsed_request_time: Duration,
    pub sdk_config: SdkConfig,
}

impl Eq for DynamoDbConfig {}
impl PartialEq for DynamoDbConfig {
    fn eq(&self, other: &Self) -> bool {
        self.billing_mode == other.billing_mode
            && self.lock_table_name == other.lock_table_name
            && self.max_elapsed_request_time == other.max_elapsed_request_time
            && self.sdk_config.endpoint_url() == other.sdk_config.endpoint_url()
            && self.sdk_config.region() == other.sdk_config.region()
    }
}

/// Represents the possible, positive outcomes of calling `DynamoDbClient::try_create_lock_table()`
#[derive(Debug, PartialEq)]
pub enum CreateLockTableResult {
    /// Table created successfully.
    TableCreated,
    /// Table was not created because it already exists.
    /// Does not imply that the table has the correct schema.
    TableAlreadyExists,
}

/// Extract a field from an item's attribute value map, producing a descriptive error
/// of the various failure cases.
fn extract_required_string_field<'a>(
    fields: &'a HashMap<String, AttributeValue>,
    field_name: &str,
) -> Result<&'a str, LockClientError> {
    fields
        .get(field_name)
        .ok_or_else(|| LockClientError::InconsistentData {
            description: format!("mandatory string field '{field_name}' missing"),
        })?
        .as_s()
        .map_err(|v| LockClientError::InconsistentData {
            description: format!(
                "mandatory string field '{field_name}' exists, but is not a string: {:#?}",
                v,
            ),
        })
        .map(|s| s.as_str())
}

/// Extract an optional String field from an item's attribute value map.
/// This call fails if the field exists, but is not of type string.
fn extract_optional_number_field<'a>(
    fields: &'a HashMap<String, AttributeValue>,
    field_name: &str,
) -> Result<Option<&'a String>, LockClientError> {
    fields
        .get(field_name)
        .map(|attr| {
            attr.as_n().map_err(|_| LockClientError::InconsistentData {
                description: format!(
                    "field with name '{field_name}' exists, but is not of type number"
                ),
            })
        })
        .transpose()
}

fn string_attr<T: ToString>(s: T) -> AttributeValue {
    AttributeValue::S(s.to_string())
}

fn num_attr<T: ToString>(n: T) -> AttributeValue {
    AttributeValue::N(n.to_string())
}

lazy_static! {
    static ref DELTA_LOG_PATH: Path = Path::from("_delta_log");
    static ref DELTA_LOG_REGEX: Regex = Regex::new(r"(\d{20})\.(json|checkpoint).*$").unwrap();
}

/// Extract version from a file name in the delta log
fn extract_version_from_filename(name: &str) -> Option<i64> {
    DELTA_LOG_REGEX
        .captures(name)
        .map(|captures| captures.get(1).unwrap().as_str().parse().unwrap())
}

#[cfg(test)]
mod tests {
    use super::*;
    use aws_config::Region;
    use object_store::memory::InMemory;
    use serial_test::serial;

    fn commit_entry_roundtrip(c: &CommitEntry) -> Result<(), LockClientError> {
        let item_data: HashMap<String, AttributeValue> = create_value_map(c, "some_table");
        let c_parsed = CommitEntry::try_from(&item_data)?;
        assert_eq!(c, &c_parsed);
        Ok(())
    }

    #[test]
    fn commit_entry_roundtrip_test() -> Result<(), LockClientError> {
        let system_time = SystemTime::UNIX_EPOCH
            + Duration::from_secs(
                SystemTime::now()
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            );
        commit_entry_roundtrip(&CommitEntry {
            version: 0,
            temp_path: Path::from("_delta_log/tmp/0_abc.json"),
            complete: true,
            expire_time: Some(system_time),
        })?;
        commit_entry_roundtrip(&CommitEntry {
            version: 139,
            temp_path: Path::from("_delta_log/tmp/0_abc.json"),
            complete: false,
            expire_time: None,
        })?;
        Ok(())
    }

    /// In cases where there is no dynamodb specified locking provider, this should get a default
    /// logstore
    #[test]
    #[serial]
    fn test_logstore_factory_default() {
        let factory = S3LogStoreFactory::default();
        let store = InMemory::new();
        let url = Url::parse("s3://test-bucket").unwrap();
        std::env::remove_var(storage::s3_constants::AWS_S3_LOCKING_PROVIDER);
        let logstore = factory
            .with_options(Arc::new(store), &url, &StorageOptions::from(HashMap::new()))
            .unwrap();
        assert_eq!(logstore.name(), "S3LogStore");
    }

    #[test]
    #[serial]
    fn test_create_dynamodb_sdk_config() {
        let sdk_config = SdkConfig::builder()
            .region(Region::from_static("eu-west-1"))
            .endpoint_url("http://localhost:1234")
            .build();
        let dynamodb_sdk_config = DynamoDbLockClient::create_dynamodb_sdk_config(
            &sdk_config,
            Some("http://localhost:2345".to_string()),
        );
        assert_eq!(
            dynamodb_sdk_config.endpoint_url(),
            Some("http://localhost:2345"),
        );
        assert_eq!(
            dynamodb_sdk_config.region().unwrap().to_string(),
            "eu-west-1".to_string(),
        );
        let dynamodb_sdk_no_override_config =
            DynamoDbLockClient::create_dynamodb_sdk_config(&sdk_config, None);
        assert_eq!(
            dynamodb_sdk_no_override_config.endpoint_url(),
            Some("http://localhost:1234"),
        );
    }
}
