use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use rusoto_core::RusotoError;
use rusoto_dynamodb::*;
use uuid::Uuid;

pub struct DynamoLock {
    client: DynamoDbClient,
    opts: Options,
}

#[derive(Clone, Debug)]
pub struct Options {
    pub partition_key_value: String,
    pub table_name: String,
    pub owner_name: String,
    pub lease_duration: u64,
    pub sleep_millis: u64,
}

#[derive(Clone, Debug)]
pub struct LockItem {
    pub owner_name: String,
    pub record_version_number: String,
    pub lease_duration: u64,
    pub is_released: bool,
    pub data: Option<String>,
    pub lookup_time: u128,
}

#[derive(thiserror::Error, Debug)]
pub enum DynamoError {
    #[error("Dynamo table not found")]
    TableNotFound,

    #[error("Conditional check failed")]
    ConditionalCheckFailed,

    #[error("DynamoDB item has invalid schema")]
    InvalidItemSchema,

    #[error("Could not acquire lock for {0} sec")]
    TimedOut(u64),

    #[error("Put item error: {0}")]
    PutItemError(RusotoError<PutItemError>),

    #[error("Update item error: {0}")]
    UpdateItemError(#[from] RusotoError<UpdateItemError>),

    #[error("Get item error: {0}")]
    GetItemError(RusotoError<GetItemError>),

    #[error("Delete item error: {0}")]
    DeleteItemError(#[from] RusotoError<DeleteItemError>),
}

pub const PARTITION_KEY_NAME: &str = "key";
pub const OWNER_NAME: &str = "ownerName";
pub const RECORD_VERSION_NUMBER: &str = "recordVersionNumber";
pub const IS_RELEASED: &str = "isReleased";
pub const LEASE_DURATION: &str = "leaseDuration";
pub const DATA: &str = "data";

mod expressions {
    pub const ACQUIRE_LOCK_THAT_DOESNT_EXIST: &str = "attribute_not_exists(#pk)";

    pub const PK_EXISTS_AND_IS_RELEASED: &str = "attribute_exists(#pk) AND #ir = :ir";

    pub const PK_EXISTS_AND_RVN_MATCHES: &str = "attribute_exists(#pk) AND #rvn = :rvn";

    pub const PK_EXISTS_AND_OWNER_RVN_MATCHES: &str =
        "attribute_exists(#pk) AND #rvn = :rvn AND #on = :on";

    pub const UPDATE_IS_RELEASED_AND_DATA: &str = "SET #ir = :ir, #d = :d";

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

mod options {
    pub const PARTITION_KEY_VALUE: &str = "DYNAMO_LOCK_PARTITION_KEY_VALUE";
    pub const TABLE_NAME: &str = "DYNAMO_LOCK_TABLE_NAME";
    pub const OWNER_NAME: &str = "DYNAMO_LOCK_OWNER_NAME";
    pub const LEASE_DURATION: &str = "DYNAMO_LOCK_LEASE_DURATION";
    pub const SLEEP_MILLIS: &str = "DYNAMO_LOCK_SLEEP_MILLIS";
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

        Self {
            partition_key_value: str_env(options::PARTITION_KEY_VALUE, "delta-rs".to_string()),
            table_name: str_env(options::TABLE_NAME, "delta_rs_lock_table".to_string()),
            owner_name: str_env(options::OWNER_NAME, Uuid::new_v4().to_string()),
            lease_duration: u64_env(options::LEASE_DURATION, 20),
            sleep_millis: u64_env(options::SLEEP_MILLIS, 1000),
        }
    }
}

impl DynamoLock {
    pub fn new(client: DynamoDbClient, opts: Options) -> Self {
        Self { client, opts }
    }

    pub async fn acquire_lock(&self) -> Result<LockItem, DynamoError> {
        let mut state = AcquireLockState {
            client: self,
            active_lock: None,
            started: Instant::now(),
            timeout_in: Duration::from_millis(self.opts.sleep_millis),
        };

        loop {
            match state.try_acquire_lock().await {
                Ok(lock) => return Ok(lock),
                Err(DynamoError::ConditionalCheckFailed) => {
                    if state.has_timed_out() {
                        return Err(DynamoError::TimedOut(state.started.elapsed().as_secs()));
                    }
                    tokio::time::sleep(Duration::from_millis(self.opts.sleep_millis)).await;
                }
                Err(e) => return Err(e),
            }
        }
    }

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

    pub async fn release_lock(&self, lock: &LockItem) -> Result<(), DynamoError> {
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

        self.client
            .update_item(UpdateItemInput {
                table_name: self.opts.table_name.clone(),
                key: hashmap! {
                    PARTITION_KEY_NAME.to_string() => attr(self.opts.partition_key_value.clone())
                },
                condition_expression: Some(
                    expressions::PK_EXISTS_AND_OWNER_RVN_MATCHES.to_string(),
                ),
                update_expression: Some(update.to_string()),
                expression_attribute_names: Some(names),
                expression_attribute_values: Some(values),
                ..Default::default()
            })
            .await?;
        Ok(())
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

impl LockItem {
    fn is_expired(&self) -> bool {
        if self.is_released {
            return true;
        }
        now_millis() - self.lookup_time > (self.lease_duration as u128) * 1000
    }
}

fn now_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn attr<T: ToString>(s: T) -> AttributeValue {
    AttributeValue {
        s: Some(s.to_string()),
        ..Default::default()
    }
}

struct AcquireLockState<'a> {
    client: &'a DynamoLock,
    active_lock: Option<LockItem>,
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
                // there's existing lock and it's out first attempt to acquire it
                if self.active_lock.is_none() {
                    // first we store it, extend timeout period and try again later
                    return self.set_new_active_lock_and_fail(existing);
                }

                // there's existing lock and we've already tried to acquire it, let's try again
                let active = self.active_lock.as_ref().unwrap();
                let active_rvn = &active.record_version_number;

                // let's check store rvn against current lock from dynamo
                if active_rvn == &existing.record_version_number {
                    // rvn matches
                    if active.is_expired() {
                        // the lock is expired and we're safe to try to acquire it
                        self.upsert_expired_lock(active_rvn, existing.data).await
                    } else {
                        // the lock is not yet expired, try again later
                        Err(DynamoError::ConditionalCheckFailed)
                    }
                } else {
                    // rvn doesn't match, meaning that other worker acquire it before us
                    // let's change active lock with new one and extend timeout period
                    self.set_new_active_lock_and_fail(existing)
                }
            }
        }
    }

    fn set_new_active_lock_and_fail(&mut self, lock: LockItem) -> Result<LockItem, DynamoError> {
        self.extend_timeout_by(lock.lease_duration);
        self.active_lock = Some(lock);

        Err(DynamoError::ConditionalCheckFailed)
    }

    fn extend_timeout_by(&mut self, seconds: u64) {
        self.timeout_in = Duration::from_secs(self.timeout_in.as_secs() + seconds)
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
