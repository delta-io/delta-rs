//! Lock client implementation based on DynamoDb.

pub mod errors;
pub mod logstore;
pub mod storage;

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

use deltalake_core::logstore::{logstores, LogStore, LogStoreFactory};
use deltalake_core::storage::{factories, url_prefix_handler, ObjectStoreRef, StorageOptions};
use deltalake_core::{DeltaResult, Path};
use rusoto_core::{HttpClient, Region, RusotoError};
use rusoto_credential::AutoRefreshingProvider;
use rusoto_dynamodb::{
    AttributeDefinition, AttributeValue, CreateTableError, CreateTableInput, DynamoDb,
    DynamoDbClient, GetItemError, GetItemInput, KeySchemaElement, PutItemError, PutItemInput,
    QueryError, QueryInput, UpdateItemError, UpdateItemInput,
};
use rusoto_sts::WebIdentityProvider;
use url::Url;

use errors::{DynamoDbConfigError, LockClientError};
use storage::{S3ObjectStoreFactory, S3StorageOptions};

#[derive(Clone, Debug, Default)]
struct S3LogStoreFactory {}

impl LogStoreFactory for S3LogStoreFactory {
    fn with_options(
        &self,
        store: ObjectStoreRef,
        location: &Url,
        options: &StorageOptions,
    ) -> DeltaResult<Arc<dyn LogStore>> {
        let store = url_prefix_handler(store, Path::parse(location.path())?)?;

        if options
            .0
            .contains_key(AmazonS3ConfigKey::CopyIfNotExists.as_ref())
        {
            debug!("S3LogStoreFactory has been asked to create a LogStore where the underlying store has copy-if-not-exists enabled - no locking provider required");
            return Ok(deltalake_core::logstore::default_logstore(
                store, location, options,
            ));
        }

        let s3_options = S3StorageOptions::from_map(&options.0);

        if s3_options.locking_provider.as_deref() != Some("dynamodb") {
            debug!("S3LogStoreFactory has been asked to create a LogStore without the dynamodb locking provider");
            return Ok(deltalake_core::logstore::default_logstore(
                store, location, options,
            ));
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
    dynamodb_client: DynamoDbClient,
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
        lock_table_name: Option<String>,
        billing_mode: Option<String>,
        max_elapsed_request_time: Option<String>,
        region: Region,
        use_web_identity: bool,
    ) -> Result<Self, DynamoDbConfigError> {
        let dynamodb_client = create_dynamodb_client(region.clone(), use_web_identity)?;

        let lock_table_name = lock_table_name
            .or_else(|| std::env::var(constants::LOCK_TABLE_KEY_NAME).ok())
            .unwrap_or(constants::DEFAULT_LOCK_TABLE_NAME.to_owned());

        let billing_mode = billing_mode
            .or_else(|| std::env::var(constants::BILLING_MODE_KEY_NAME).ok())
            .map_or_else(
                || Ok(BillingMode::PayPerRequest),
                |bm| BillingMode::from_str(&bm),
            )?;

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
            use_web_identity,
            region,
        };
        Ok(Self {
            dynamodb_client,
            config,
        })
    }

    /// Create the lock table where DynamoDb stores the commit information for all delta tables.
    ///
    /// Transparently handles the case where that table already exists, so it's safe to call.
    /// After `create_table` operation is executed, the table state in DynamoDb is `creating`, and
    /// it's not immediately useable. This method does not wait for the table state to become
    /// `active`, so transient failures might occurr when immediately using the lock client.
    pub async fn try_create_lock_table(&self) -> Result<CreateLockTableResult, LockClientError> {
        let attribute_definitions = vec![
            AttributeDefinition {
                attribute_name: constants::ATTR_TABLE_PATH.to_owned(),
                attribute_type: constants::STRING_TYPE.to_owned(),
            },
            AttributeDefinition {
                attribute_name: constants::ATTR_FILE_NAME.to_owned(),
                attribute_type: constants::STRING_TYPE.to_owned(),
            },
        ];
        let input = CreateTableInput {
            attribute_definitions,
            key_schema: vec![
                KeySchemaElement {
                    attribute_name: constants::ATTR_TABLE_PATH.to_owned(),
                    key_type: constants::KEY_TYPE_HASH.to_owned(),
                },
                KeySchemaElement {
                    attribute_name: constants::ATTR_FILE_NAME.to_owned(),
                    key_type: constants::KEY_TYPE_RANGE.to_owned(),
                },
            ],
            billing_mode: Some(self.config.billing_mode.to_str()),
            table_name: self.config.lock_table_name.clone(),
            ..Default::default()
        };
        match self.dynamodb_client.create_table(input).await {
            Ok(_) => Ok(CreateLockTableResult::TableCreated),
            Err(RusotoError::Service(CreateTableError::ResourceInUse(_))) => {
                Ok(CreateLockTableResult::TableAlreadyExists)
            }
            Err(reason) => Err(LockClientError::LockTableCreateFailure {
                name: self.config.lock_table_name.clone(),
                source: reason,
            }),
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
        let input = GetItemInput {
            consistent_read: Some(true),
            table_name: self.config.lock_table_name.clone(),
            key: self.get_primary_key(version, table_path),
            ..Default::default()
        };
        let item = self
            .retry(|| async {
                match self.dynamodb_client.get_item(input.clone()).await {
                    Ok(x) => Ok(x),
                    Err(RusotoError::Service(GetItemError::ProvisionedThroughputExceeded(_))) => {
                        Err(backoff::Error::transient(
                            LockClientError::ProvisionedThroughputExceeded,
                        ))
                    }
                    Err(err) => Err(backoff::Error::permanent(err.into())),
                }
            })
            .await?;
        item.item.as_ref().map(CommitEntry::try_from).transpose()
    }

    /// write new entry to to DynamoDb lock table.
    pub async fn put_commit_entry(
        &self,
        table_path: &str,
        entry: &CommitEntry,
    ) -> Result<(), LockClientError> {
        let item = create_value_map(entry, table_path);
        let input = PutItemInput {
            condition_expression: Some(constants::CONDITION_EXPR_CREATE.to_owned()),
            table_name: self.get_lock_table_name(),
            item,
            ..Default::default()
        };
        self.retry(|| async {
            match self.dynamodb_client.put_item(input.clone()).await {
                Ok(_) => Ok(()),
                Err(RusotoError::Service(PutItemError::ProvisionedThroughputExceeded(_))) => Err(
                    backoff::Error::transient(LockClientError::ProvisionedThroughputExceeded),
                ),
                Err(RusotoError::Service(PutItemError::ConditionalCheckFailed(_))) => Err(
                    backoff::Error::permanent(LockClientError::VersionAlreadyExists {
                        table_path: table_path.to_owned(),
                        version: entry.version,
                    }),
                ),
                Err(RusotoError::Service(PutItemError::ResourceNotFound(_))) => Err(
                    backoff::Error::permanent(LockClientError::LockTableNotFound),
                ),
                Err(err) => Err(backoff::Error::permanent(err.into())),
            }
        })
        .await
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
        let input = QueryInput {
            table_name: self.get_lock_table_name(),
            consistent_read: Some(true),
            limit: Some(limit),
            scan_index_forward: Some(false),
            key_condition_expression: Some(format!("{} = :tn", constants::ATTR_TABLE_PATH)),
            expression_attribute_values: Some(
                maplit::hashmap!(":tn".into() => string_attr(table_path)),
            ),
            ..Default::default()
        };
        let query_result = self
            .retry(|| async {
                match self.dynamodb_client.query(input.clone()).await {
                    Ok(result) => Ok(result),
                    Err(RusotoError::Service(QueryError::ProvisionedThroughputExceeded(_))) => Err(
                        backoff::Error::transient(LockClientError::ProvisionedThroughputExceeded),
                    ),
                    Err(err) => Err(backoff::Error::permanent(err.into())),
                }
            })
            .await?;

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
        let input = UpdateItemInput {
            table_name: self.get_lock_table_name(),
            key: self.get_primary_key(version, table_path),
            update_expression: Some("SET complete = :c, expireTime = :e".to_owned()),
            expression_attribute_values: Some(maplit::hashmap! {
                ":c".to_owned() => string_attr("true"),
                ":e".to_owned() => num_attr(seconds_since_epoch),
                ":f".into() => string_attr("false"),
            }),
            condition_expression: Some(constants::CONDITION_UPDATE_INCOMPLETE.to_owned()),
            ..Default::default()
        };

        self.retry(|| async {
            match self.dynamodb_client.update_item(input.clone()).await {
                Ok(_) => Ok(UpdateLogEntryResult::UpdatePerformed),
                Err(RusotoError::Service(UpdateItemError::ConditionalCheckFailed(_))) => {
                    Ok(UpdateLogEntryResult::AlreadyCompleted)
                }
                Err(RusotoError::Service(UpdateItemError::ProvisionedThroughputExceeded(_))) => {
                    Err(backoff::Error::transient(
                        LockClientError::ProvisionedThroughputExceeded,
                    ))
                }
                Err(err) => Err(backoff::Error::permanent(err.into())),
            }
        })
        .await
    }

    async fn retry<I, E, Fn, Fut>(&self, operation: Fn) -> Result<I, E>
    where
        Fn: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<I, backoff::Error<E>>>,
    {
        let backoff = backoff::ExponentialBackoffBuilder::new()
            .with_multiplier(2.)
            .with_max_interval(Duration::from_secs(15))
            .with_max_elapsed_time(Some(self.config.max_elapsed_request_time))
            .build();
        backoff::future::retry(backoff, operation).await
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

#[derive(Debug, PartialEq)]
pub enum BillingMode {
    PayPerRequest,
    Provisioned,
}

impl BillingMode {
    fn to_str(&self) -> String {
        match self {
            Self::PayPerRequest => "PAY_PER_REQUEST".to_owned(),
            Self::Provisioned => "PROVISIONED".to_owned(),
        }
    }
}

impl FromStr for BillingMode {
    type Err = DynamoDbConfigError;

    fn from_str(s: &str) -> Result<Self, DynamoDbConfigError> {
        match s.to_ascii_lowercase().as_str() {
            "provisioned" => Ok(BillingMode::Provisioned),
            "pay_per_request" => Ok(BillingMode::PayPerRequest),
            _ => Err(DynamoDbConfigError::InvalidBillingMode(s.to_owned())),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct DynamoDbConfig {
    pub billing_mode: BillingMode,
    pub lock_table_name: String,
    pub max_elapsed_request_time: Duration,
    pub use_web_identity: bool,
    pub region: Region,
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

pub mod constants {
    use std::time::Duration;

    use lazy_static::lazy_static;

    pub const DEFAULT_LOCK_TABLE_NAME: &str = "delta_log";
    pub const LOCK_TABLE_KEY_NAME: &str = "DELTA_DYNAMO_TABLE_NAME";
    pub const BILLING_MODE_KEY_NAME: &str = "DELTA_DYNAMO_BILLING_MODE";
    pub const MAX_ELAPSED_REQUEST_TIME_KEY_NAME: &str = "DELTA_DYNAMO_MAX_ELAPSED_REQUEST_TIME";

    pub const ATTR_TABLE_PATH: &str = "tablePath";
    pub const ATTR_FILE_NAME: &str = "fileName";
    pub const ATTR_TEMP_PATH: &str = "tempPath";
    pub const ATTR_COMPLETE: &str = "complete";
    pub const ATTR_EXPIRE_TIME: &str = "expireTime";

    pub const STRING_TYPE: &str = "S";

    pub const KEY_TYPE_HASH: &str = "HASH";
    pub const KEY_TYPE_RANGE: &str = "RANGE";

    lazy_static! {
        pub static ref CONDITION_EXPR_CREATE: String = format!(
            "attribute_not_exists({ATTR_TABLE_PATH}) and attribute_not_exists({ATTR_FILE_NAME})"
        );
    }

    pub const CONDITION_UPDATE_INCOMPLETE: &str = "complete = :f";

    pub const DEFAULT_COMMIT_ENTRY_EXPIRATION_DELAY: Duration = Duration::from_secs(86_400);
}

fn create_dynamodb_client(
    region: Region,
    use_web_identity: bool,
) -> Result<DynamoDbClient, DynamoDbConfigError> {
    Ok(match use_web_identity {
        true => {
            let dispatcher = HttpClient::new()?;
            rusoto_dynamodb::DynamoDbClient::new_with(
                dispatcher,
                get_web_identity_provider()?,
                region,
            )
        }
        false => rusoto_dynamodb::DynamoDbClient::new(region),
    })
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
        .s
        .as_ref()
        .ok_or_else(|| LockClientError::InconsistentData {
            description: format!(
                "mandatory string field '{field_name}' exists, but is not a string: {:#?}",
                fields.get(field_name)
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
            attr.n
                .as_ref()
                .ok_or_else(|| LockClientError::InconsistentData {
                    description: format!(
                        "field with name '{field_name}' exists, but is not of type number"
                    ),
                })
        })
        .transpose()
}

fn string_attr<T: ToString>(s: T) -> AttributeValue {
    AttributeValue {
        s: Some(s.to_string()),
        ..Default::default()
    }
}

fn num_attr<T: ToString>(n: T) -> AttributeValue {
    AttributeValue {
        n: Some(n.to_string()),
        ..Default::default()
    }
}

fn get_web_identity_provider(
) -> Result<AutoRefreshingProvider<WebIdentityProvider>, DynamoDbConfigError> {
    let provider = WebIdentityProvider::from_k8s_env();
    Ok(AutoRefreshingProvider::new(provider)?)
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
        assert_eq!(logstore.name(), "DefaultLogStore");
    }
}
