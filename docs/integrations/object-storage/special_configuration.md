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

## Common Client Options

The following configuration options from `ClientConfigKey` work across all storage backends (S3, Azure, GCS, etc.) and control HTTP client behavior. These can be passed via `storage_options` regardless of which cloud provider you're using.

| Config Key | Description |
|------------|-------------|
| `allow_http` | Allow HTTP connections (non-HTTPS). Set to `true` for local development or testing with services like MinIO or LocalStack. Default: `false` |
| `allow_invalid_certificates` | Skip TLS certificate validation. **WARNING**: This is dangerous and should only be used for testing. Default: `false` |
| `connect_timeout` | Maximum time to wait for a connection to be established. Accepts duration strings like `30s`, `5m`. |
| `timeout` | Maximum time for a complete request (including retries). Accepts duration strings like `60s`, `10m`. |
| `proxy_url` | HTTP proxy URL to route requests through. Example: `http://proxy.example.com:8080` |
| `proxy_ca_certificate` | PEM-encoded CA certificate for the proxy server (when using HTTPS proxy with custom CA) |
| `proxy_excludes` | Comma-separated list of hosts to exclude from proxying. Example: `localhost,127.0.0.1` |
| `pool_idle_timeout` | Maximum time a connection can remain idle in the connection pool before being closed. Accepts duration strings. |
| `pool_max_idle_per_host` | Maximum number of idle connections to maintain per host. Default varies by backend. |
| `http1_only` | Force HTTP/1.1 only, disable HTTP/2. Set to `true` if the server doesn't support HTTP/2. Default: `false` |
| `http2_only` | Force HTTP/2 only. Set to `true` to require HTTP/2. Default: `false` |
| `http2_keep_alive_interval` | Interval for HTTP/2 keep-alive pings. Accepts duration strings like `30s`. |
| `http2_keep_alive_timeout` | Timeout for HTTP/2 keep-alive ping responses. Accepts duration strings. |
| `http2_keep_alive_while_idle` | Send HTTP/2 keep-alive pings even when no streams are active. Set to `true` to enable. Default: `false` |
| `http2_max_frame_size` | Maximum HTTP/2 frame size in bytes. Must be between 16,384 and 16,777,215. |
| `user_agent` | Custom User-Agent header to send with requests. Example: `my-app/1.0` |
| `default_content_type` | Default Content-Type header for uploads when not otherwise specified. Example: `application/octet-stream` |

### Example Usage

```python
from deltalake import write_deltalake

storage_options = {
    # Cloud-specific credentials
    'AWS_ACCESS_KEY_ID': 'your-key',
    'AWS_SECRET_ACCESS_KEY': 'your-secret',
    'AWS_REGION': 'us-east-1',

    # Common client options (work with any backend)
    'timeout': '120s',
    'connect_timeout': '30s',
    'pool_max_idle_per_host': '10',
}

write_deltalake("s3://bucket/table", df, storage_options=storage_options)
```

### Development and Testing Options

For local development with services like MinIO, LocalStack, or Azurite:

```python
storage_options = {
    'AWS_ACCESS_KEY_ID': 'test',
    'AWS_SECRET_ACCESS_KEY': 'test',
    'AWS_ENDPOINT_URL': 'http://localhost:4566',
    'allow_http': 'true',  # Required for non-HTTPS endpoints
    'connect_timeout': '10s',
    'timeout': '60s',
}
```

!!! warning
    Never use `allow_invalid_certificates: true` in production environments. This disables critical security protections.

!!! note
    For the complete and authoritative list of client configuration options, refer to the [object_store ClientConfigKey documentation](https://docs.rs/object_store/latest/object_store/enum.ClientConfigKey.html).
