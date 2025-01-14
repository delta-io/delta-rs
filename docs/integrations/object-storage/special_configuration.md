# Advanced object storage configuration

Delta-rs provides some addition values to be set in the storage_options for advanced use case:

- Limit concurrent requests overall
- Set retry configuration
- Exponential backoff with decorrelated jitter algorithm details [See here for more details](https://docs.rs/object_store/latest/object_store/struct.BackoffConfig.html) 
- Mounted storage support


| Config key                            | Description                                                                   | 
| -----------------------               | ---------------------------------                                             |
| OBJECT_STORE_CONCURRENCY_LIMIT         | The number of concurrent connections the underlying object store can create  |
| max_retries | The maximum number of times to retry a request. Set to 0 to disable retries |
| retry_timeout | The maximum length of time from the initial request after which no further retries will be attempted |
| backoff_config.init_backoff | The initial backoff duration |
| backoff_config.max_backoff | The maximum backoff duration |
| backoff_config.base | The multiplier to use for the next backoff duration |
| MOUNT_ALLOW_UNSAFE_RENAME | If set it will allow unsafe renames on mounted storage |
