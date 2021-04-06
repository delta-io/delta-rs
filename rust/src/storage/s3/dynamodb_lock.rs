use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusoto_core::RusotoError;
use rusoto_dynamodb::*;
use uuid::Uuid;

mod options {
    /// Environment variable for `partition_key_value` option.
    pub const PARTITION_KEY_VALUE: &str = "DYNAMO_LOCK_PARTITION_KEY_VALUE";
    /// Environment variable for `table_name` option.
    pub const TABLE_NAME: &str = "DYNAMO_LOCK_TABLE_NAME";
    /// Environment variable for `owner_name` option.
    pub const OWNER_NAME: &str = "DYNAMO_LOCK_OWNER_NAME";
    /// Environment variable for `lease_duration` option.
    pub const LEASE_DURATION: &str = "DYNAMO_LOCK_LEASE_DURATION";
    /// Environment variable for `refresh_period` option.
    pub const REFRESH_PERIOD_SECS: &str = "DYNAMO_LOCK_REFRESH_PERIOD_SECS";
    /// Environment variable for `additional_time_to_wait_for_lock` option.
    pub const ADDITIONAL_TIME_TO_WAIT_SECS: &str = "DYNAMO_LOCK_ADDITIONAL_TIME_TO_WAIT_SECS";
}

/// Configuration options for [`DynamoDbLockClient`].
#[derive(Clone, Debug)]
pub struct Options {
    /// Partition key value of DynamoDB table,
    /// should be the same among the clients which work with the lock.
    pub partition_key_value: String,
    /// The DynamoDB table name, should be the same among the clients which work with the lock.
    /// The table has to be created if it not exists before using it with DynamoDB locking API.
    pub table_name: String,
    /// Owner name, should be unique among the clients which work with the lock.
    pub owner_name: String,
    /// The amount of time (in seconds) that the owner has for the acquired lock.
    pub lease_duration: u64,
    /// The amount of time to wait before trying to get the lock again.
    pub refresh_period: Duration,
    /// The amount of time to wait in addition to `lease_duration`.
    pub additional_time_to_wait_for_lock: Duration,
}

impl Default for Options {
    fn default() -> Self {
        fn str_env(key: &str, default: String) -> String {
            std::env::var(key).unwrap_or(default)
        }

        fn u64_env(key: &str, default: u64) -> u64 {
            std::env::var(key)
                .ok()
                .and_then(|e| e.parse::<u64>().ok())
                .unwrap_or(default)
        }

        let refresh_period = Duration::from_secs(u64_env(options::REFRESH_PERIOD_SECS, 1000));
        let additional_time_to_wait_for_lock =
            Duration::from_secs(u64_env(options::ADDITIONAL_TIME_TO_WAIT_SECS, 1000));

        Self {
            partition_key_value: str_env(options::PARTITION_KEY_VALUE, "delta-rs".to_string()),
            table_name: str_env(options::TABLE_NAME, "delta_rs_lock_table".to_string()),
            owner_name: str_env(options::OWNER_NAME, Uuid::new_v4().to_string()),
            lease_duration: u64_env(options::LEASE_DURATION, 20),
            refresh_period,
            additional_time_to_wait_for_lock,
        }
    }
}

/// A lock that has been successfully acquired
#[derive(Clone, Debug)]
pub struct LockItem {
    /// The name of the owner that owns this lock.
    pub owner_name: String,
    /// Current version number of the lock in DynamoDB. This is what tells the lock client
    /// when the lock is stale.
    pub record_version_number: String,
    /// The amount of time (in seconds) that the owner has this lock for.
    pub lease_duration: u64,
    /// Tells whether or not the lock was marked as released when loaded from DynamoDB.
    pub is_released: bool,
    /// Optional data associated with this lock.
    pub data: Option<String>,
    /// The last time this lock was updated or retrieved.
    pub lookup_time: u128,
}

impl LockItem {
    fn is_expired(&self) -> bool {
        if self.is_released {
            return true;
        }
        now_millis() - self.lookup_time > (self.lease_duration as u128) * 1000
    }
}

/// Error returned by the [`DynamoDbLockClient`] API.
#[derive(thiserror::Error, Debug)]
pub enum DynamoError {
    /// Error caused by the DynamoDB table not being created.
    #[error("Dynamo table not found")]
    TableNotFound,

    /// Error that indicates the condition in the DynamoDB operation could not be evaluated.
    /// Mostly used by [`DynamoDbLockClient::acquire_lock`] to handle unsuccessful retries
    /// of acquiring the lock.
    #[error("Conditional check failed")]
    ConditionalCheckFailed,

    /// Error that is returned by [`DynamoDbLockClient::release_lock`] when the given lock
    /// has been already expired and could not be released.
    #[error("Lock is expired")]
    LockIsExpired,

    /// The required field of [`LockItem`] is missing in DynamoDB record or has incompatible type.
    #[error("DynamoDB item has invalid schema")]
    InvalidItemSchema,

    /// Error returned by [`DynamoDbLockClient::acquire_lock`] which indicates that the lock could
    /// not be acquired for more that returned number of seconds.
    #[error("Could not acquire lock for {0} sec")]
    TimedOut(u64),

    /// Error caused by the [`DynamoDbClient::put_item`] request.
    #[error("Put item error: {0}")]
    PutItemError(RusotoError<PutItemError>),

    /// Error caused by the [`DynamoDbClient::update_item`] request.
    #[error("Update item error: {0}")]
    UpdateItemError(#[from] RusotoError<UpdateItemError>),

    /// Error caused by the [`DynamoDbClient::get_item`] request.
    #[error("Get item error: {0}")]
    GetItemError(RusotoError<GetItemError>),
}

impl From<RusotoError<PutItemError>> for DynamoError {
    fn from(error: RusotoError<PutItemError>) -> Self {
        match error {
            RusotoError::Service(PutItemError::ConditionalCheckFailed(_)) => {
                DynamoError::ConditionalCheckFailed
            }
            _ => DynamoError::PutItemError(error),
        }
    }
}

impl From<RusotoError<GetItemError>> for DynamoError {
    fn from(error: RusotoError<GetItemError>) -> Self {
        match error {
            RusotoError::Service(GetItemError::ResourceNotFound(_)) => DynamoError::TableNotFound,
            _ => DynamoError::GetItemError(error),
        }
    }
}

/// The partition key field name in DynamoDB
pub const PARTITION_KEY_NAME: &str = "key";
/// The field name of `owner_name` in DynamoDB
pub const OWNER_NAME: &str = "ownerName";
/// The field name of `record_version_number` in DynamoDB
pub const RECORD_VERSION_NUMBER: &str = "recordVersionNumber";
/// The field name of `is_released` in DynamoDB
pub const IS_RELEASED: &str = "isReleased";
/// The field name of `lease_duration` in DynamoDB
pub const LEASE_DURATION: &str = "leaseDuration";
/// The field name of `data` in DynamoDB
pub const DATA: &str = "data";

mod expressions {
    /// The expression that checks whether the lock record does not exists.
    pub const ACQUIRE_LOCK_THAT_DOESNT_EXIST: &str = "attribute_not_exists(#pk)";

    /// The expression that checks whether the lock record exists and it is marked as released.
    pub const PK_EXISTS_AND_IS_RELEASED: &str = "attribute_exists(#pk) AND #ir = :ir";

    /// The expression that checks whether the lock record exists
    /// and its record version number matches with the given one.
    pub const PK_EXISTS_AND_RVN_MATCHES: &str = "attribute_exists(#pk) AND #rvn = :rvn";

    /// The expression that checks whether the lock record exists,
    /// its record version number matches with the given one
    /// and its owner name matches with the given one.
    pub const PK_EXISTS_AND_OWNER_RVN_MATCHES: &str =
        "attribute_exists(#pk) AND #rvn = :rvn AND #on = :on";

    /// The expression that updates is_released and data fields of DynamoDB record.
    pub const UPDATE_IS_RELEASED_AND_DATA: &str = "SET #ir = :ir, #d = :d";

    /// The expression that updates is_released field of DynamoDB record.
    pub const UPDATE_IS_RELEASED: &str = "SET #ir = :ir";
}

mod vars {
    pub const PK_PATH: &str = "#pk";
    pub const RVN_PATH: &str = "#rvn";
    pub const RVN_VALUE: &str = ":rvn";
    pub const IS_RELEASED_PATH: &str = "#ir";
    pub const IS_RELEASED_VALUE: &str = ":ir";
    pub const OWNER_NAME_PATH: &str = "#on";
    pub const OWNER_NAME_VALUE: &str = ":on";
    pub const DATA_PATH: &str = "#d";
    pub const DATA_VALUE: &str = ":d";
}

/// Provides a simple library for using DynamoDB's consistent read/write feature
/// to use it for managing distributed locks.
pub struct DynamoDbLockClient {
    client: DynamoDbClient,
    opts: Options,
}

impl DynamoDbLockClient {
    /// Creates new DynamoDB lock client
    pub fn new(client: DynamoDbClient, opts: Options) -> Self {
        Self { client, opts }
    }

    /// Attempts to acquire a lock until it either acquires the lock or a specified
    /// `additional_time_to_wait_for_lock` is reached. This function will poll DynamoDB based
    /// on the `refresh_period`. If it does not see the lock in DynamoDB, it will immediately
    /// return the lock to the caller. If it does see the lock, it will note the lease
    /// expiration on the lock. If the lock is deemed stale, then this will acquire and return it.
    /// Otherwise, if it waits for as long as `additional_time_to_wait_for_lock` without acquiring
    /// the lock, then it will a [`DynamoError::TimedOut].
    ///
    /// Note that this method will wait for at least as long as the `lease_duration` in order
    /// to acquire a lock that already exists. If the lock is not acquired in that time,
    /// it will wait an additional amount of time specified in `additional_time_to_wait_for_lock`
    /// before giving up.
    pub async fn acquire_lock(&self) -> Result<LockItem, DynamoError> {
        let mut state = AcquireLockState {
            client: self,
            cached_lock: None,
            started: Instant::now(),
            timeout_in: self.opts.additional_time_to_wait_for_lock,
        };

        loop {
            match state.try_acquire_lock().await {
                Ok(lock) => return Ok(lock),
                Err(DynamoError::ConditionalCheckFailed) => {
                    if state.has_timed_out() {
                        return Err(DynamoError::TimedOut(state.started.elapsed().as_secs()));
                    }
                    tokio::time::sleep(self.opts.refresh_period).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Returns current lock from DynamoDB (if any).
    pub async fn get_lock(&self) -> Result<Option<LockItem>, DynamoError> {
        let output = self
            .client
            .get_item(GetItemInput {
                consistent_read: Some(true),
                table_name: self.opts.table_name.clone(),
                key: hashmap! {
                    PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone())
                },
                ..Default::default()
            })
            .await?;

        if let Some(item) = output.item {
            let get_value = |key| -> Result<String, DynamoError> {
                Ok(item
                    .get(key)
                    .and_then(|r| r.s.as_ref())
                    .ok_or(DynamoError::InvalidItemSchema)?
                    .clone())
            };

            let lease_duration = get_value(LEASE_DURATION)?
                .parse::<u64>()
                .map_err(|_| DynamoError::InvalidItemSchema)?;

            return Ok(Some(LockItem {
                owner_name: get_value(OWNER_NAME)?,
                record_version_number: get_value(RECORD_VERSION_NUMBER)?,
                lease_duration,
                is_released: item.contains_key(IS_RELEASED),
                data: get_value(DATA).ok(),
                lookup_time: now_millis(),
            }));
        }

        Ok(None)
    }

    /// Releases the given lock if the current user still has it, returning true if the lock was
    /// successfully released, and false if someone else already stole the lock
    pub async fn release_lock(&self, lock: &LockItem) -> Result<bool, DynamoError> {
        let mut names = hashmap! {
            vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
            vars::RVN_PATH.to_string() => RECORD_VERSION_NUMBER.to_string(),
            vars::OWNER_NAME_PATH.to_string() => OWNER_NAME.to_string(),
            vars::IS_RELEASED_PATH.to_string() => IS_RELEASED.to_string(),
        };
        let mut values = hashmap! {
            vars::IS_RELEASED_VALUE.to_string() => attr("1"),
            vars::RVN_VALUE.to_string() => attr(&lock.record_version_number),
            vars::OWNER_NAME_VALUE.to_string() => attr(&lock.owner_name),
        };

        let update: &str;
        if let Some(ref data) = lock.data {
            update = expressions::UPDATE_IS_RELEASED_AND_DATA;
            names.insert(vars::DATA_PATH.to_string(), DATA.to_string());
            values.insert(vars::DATA_VALUE.to_string(), attr(data));
        } else {
            update = expressions::UPDATE_IS_RELEASED;
        }

        self.client.update_item(UpdateItemInput {
            table_name: self.opts.table_name.clone(),
            key: hashmap! {
                PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone())
            },
            condition_expression: Some(expressions::PK_EXISTS_AND_OWNER_RVN_MATCHES.to_string()),
            update_expression: Some(update.to_string()),
            expression_attribute_names: Some(names),
            expression_attribute_values: Some(values),
            ..Default::default()
        });

        match result.await {
            Ok(_) => Ok(true),
            Err(RusotoError::Service(UpdateItemError::ConditionalCheckFailed(_))) => Ok(false),
            Err(e) => Err(DynamoError::UpdateItemError(e)),
        }
    }

    async fn upsert_item(
        &self,
        data: Option<String>,
        condition_expression: Option<String>,
        expression_attribute_names: Option<HashMap<String, String>>,
        expression_attribute_values: Option<HashMap<String, AttributeValue>>,
    ) -> Result<LockItem, DynamoError> {
        let lookup_time = now_millis();
        let rvn = Uuid::new_v4().to_string();

        let mut item = hashmap! {
            PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone()),
            OWNER_NAME.to_string() => attr(&self.opts.owner_name),
            RECORD_VERSION_NUMBER.to_string() => attr(&rvn),
            LEASE_DURATION.to_string() => attr(&self.opts.lease_duration),
        };

        if let Some(ref d) = data {
            item.insert(DATA.to_string(), attr(d));
        }

        self.client
            .put_item(PutItemInput {
                table_name: self.opts.table_name.clone(),
                item,
                condition_expression,
                expression_attribute_names,
                expression_attribute_values,
                ..Default::default()
            })
            .await?;

        Ok(LockItem {
            owner_name: self.opts.owner_name.clone(),
            record_version_number: rvn,
            lease_duration: self.opts.lease_duration,
            is_released: false,
            data,
            lookup_time,
        })
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

/// Converts Rust String into DynamoDB string AttributeValue
pub fn attr<T: ToString>(s: T) -> AttributeValue {
    AttributeValue {
        s: Some(s.to_string()),
        ..Default::default()
    }
}

struct AcquireLockState<'a> {
    client: &'a DynamoDbLockClient,
    cached_lock: Option<LockItem>,
    started: Instant,
    timeout_in: Duration,
}

impl<'a> AcquireLockState<'a> {
    fn has_timed_out(&self) -> bool {
        self.started.elapsed() > self.timeout_in
    }

    async fn try_acquire_lock(&mut self) -> Result<LockItem, DynamoError> {
        match self.client.get_lock().await? {
            None => {
                // there's no lock, we good to acquire it
                Ok(self.upsert_new_lock().await?)
            }
            Some(existing) if existing.is_released => {
                // lock is released by a caller, we good to acquire it
                Ok(self.upsert_released_lock(existing.data).await?)
            }
            Some(existing) => {
                let cached = match self.cached_lock.as_ref() {
                    // there's existing lock and it's out first attempt to acquire it
                    None => {
                        // first we store it, extend timeout period and try again later
                        self.timeout_in = Duration::from_secs(
                            self.timeout_in.as_secs() + existing.lease_duration,
                        );
                        self.cached_lock = Some(existing);

                        return Err(DynamoError::ConditionalCheckFailed);
                    }
                    Some(cached) => cached,
                };
                // there's existing lock and we've already tried to acquire it, let's try again
                let cached_rvn = &cached.record_version_number;

                // let's check store rvn against current lock from dynamo
                if cached_rvn == &existing.record_version_number {
                    // rvn matches
                    if cached.is_expired() {
                        // the lock is expired and we're safe to try to acquire it
                        self.upsert_expired_lock(cached_rvn, existing.data).await
                    } else {
                        // the lock is not yet expired, try again later
                        Err(DynamoError::ConditionalCheckFailed)
                    }
                } else {
                    // rvn doesn't match, meaning that other worker acquire it before us
                    // let's change cached lock with new one and extend timeout period
                    self.cached_lock = Some(existing);
                    return Err(DynamoError::ConditionalCheckFailed);
                }
            }
        }
    }

    async fn upsert_new_lock(&self) -> Result<LockItem, DynamoError> {
        self.client
            .upsert_item(
                None,
                Some(expressions::ACQUIRE_LOCK_THAT_DOESNT_EXIST.to_string()),
                Some(hashmap! {
                    vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                }),
                None,
            )
            .await
    }

    async fn upsert_released_lock(&self, data: Option<String>) -> Result<LockItem, DynamoError> {
        self.client
            .upsert_item(
                data,
                Some(expressions::PK_EXISTS_AND_IS_RELEASED.to_string()),
                Some(hashmap! {
                    vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                    vars::IS_RELEASED_PATH.to_string() => IS_RELEASED.to_string(),
                }),
                Some(hashmap! {
                    vars::IS_RELEASED_VALUE.to_string() => attr("1")
                }),
            )
            .await
    }

    async fn upsert_expired_lock(
        &self,
        existing_rvn: &str,
        data: Option<String>,
    ) -> Result<LockItem, DynamoError> {
        self.client
            .upsert_item(
                data,
                Some(expressions::PK_EXISTS_AND_RVN_MATCHES.to_string()),
                Some(hashmap! {
                  vars::PK_PATH.to_string() => PARTITION_KEY_NAME.to_string(),
                  vars::RVN_PATH.to_string() => RECORD_VERSION_NUMBER.to_string(),
                }),
                Some(hashmap! {
                    vars::RVN_VALUE.to_string() => attr(existing_rvn)
                }),
            )
            .await
    }
}
