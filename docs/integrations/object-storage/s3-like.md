# CloudFlare R2 & Minio

`delta-rs` offers native support for using Cloudflare R2 or Minio as an S3-compatible storage backend. R2 and Minio support conditional puts, which removes the need for DynamoDB for safe concurrent writes.  However, we have to pass the `aws_conditional_put` flag into `storage_options`. See the example below.

You don’t need to install any extra dependencies to read/write Delta tables to S3 with engines that use `delta-rs`. You do need to configure your AWS access credentials correctly.

## Passing S3 Credentials

You can pass your AWS credentials explicitly by using:

- the `storage_options `kwarg
- Environment variables

## Example

Let's work through an example with Polars. The same logic applies to other Python engines like Pandas, Daft, Dask, etc.

Follow the steps below to use Delta Lake on S3 (R2/Minio) with Polars:

1. Install Polars and deltalake. For example, using:

   `pip install polars deltalake`

2. Create a dataframe with some toy data.

   `df = pl.DataFrame({'x': [1, 2, 3]})`

3. Set your `storage_options` correctly.

```python
storage_options = {
    'AWS_SECRET_ACCESS_KEY': <access_key>,
    'aws_conditional_put': 'etag', # Here we say to use conditional put, this provides safe concurrency.
}
```

4. Write data to Delta table using the `storage_options` kwarg.

   ```python
   df.write_delta(
       "s3://bucket/delta_table",
       storage_options=storage_options,
   )
   ```


## Minio and Docker

Minio is straightforward to host locally with Docker and docker-compose, via the following `docker-compose.yml` file - just run `docker-compose up`:

```yaml
version: '3.8'

services:
  minio:
    image: minio/minio
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      MINIO_ROOT_USER: ...
      MINIO_ROOT_PASSWORD: ...
    command: server /data --console-address ":9001"
```

With this configuration, Minio will host its S3-compatible API over HTTP, not HTTPS, on port 9000.  This requires an additional flag in `storage_options`, `AWS_ALLOW_HTTP`, to be set to `true`:

```python
storage_options = {
    'AWS_ACCESS_KEY_ID': ...,
    'AWS_SECRET_ACCESS_KEY': ...,
    'AWS_ENDPOINT_URL': 'http://localhost:9000',
    'AWS_ALLOW_HTTP': 'true',
    'aws_conditional_put': 'etag'
}
```

## Supported S3-Compatible Services

Delta Lake works with many S3-compatible object storage services. Below are the most commonly used services and their specific configuration requirements.

### Cloudflare R2

Cloudflare R2 supports conditional puts with ETags, which provides safe concurrent writes without requiring DynamoDB.

```python
from deltalake import write_deltalake

storage_options = {
    'AWS_ACCESS_KEY_ID': '<R2_ACCESS_KEY_ID>',
    'AWS_SECRET_ACCESS_KEY': '<R2_SECRET_ACCESS_KEY>',
    'AWS_ENDPOINT_URL': 'https://<account_id>.r2.cloudflarestorage.com',
    'AWS_REGION': 'auto',
    'aws_conditional_put': 'etag',  # Required for safe concurrent writes
}

write_deltalake(
    "s3://my-bucket/delta-table",
    df,
    storage_options=storage_options
)
```

### MinIO

MinIO is an open-source S3-compatible storage server that can be self-hosted.

```python
from deltalake import write_deltalake

storage_options = {
    'AWS_ACCESS_KEY_ID': '<MINIO_ACCESS_KEY>',
    'AWS_SECRET_ACCESS_KEY': '<MINIO_SECRET_KEY>',
    'AWS_ENDPOINT_URL': 'http://localhost:9000',  # Or your MinIO server URL
    'AWS_REGION': 'us-east-1',  # MinIO default region
    'allow_http': 'true',  # Required for non-HTTPS endpoints
    'aws_conditional_put': 'etag',  # Required for safe concurrent writes
}

write_deltalake(
    "s3://my-bucket/delta-table",
    df,
    storage_options=storage_options
)
```

### Alibaba Cloud OSS

Alibaba Cloud Object Storage Service (OSS) is S3-compatible and commonly used in China and Asia-Pacific regions.

```python
from deltalake import write_deltalake

storage_options = {
    'AWS_ACCESS_KEY_ID': '<OSS_ACCESS_KEY_ID>',
    'AWS_SECRET_ACCESS_KEY': '<OSS_SECRET_ACCESS_KEY>',
    'AWS_ENDPOINT_URL': 'https://oss-<region>.aliyuncs.com',  # e.g., oss-cn-hangzhou.aliyuncs.com
    'AWS_REGION': '<region>',  # e.g., cn-hangzhou
    'aws_virtual_hosted_style_request': 'true',
}

write_deltalake(
    "s3://my-bucket/delta-table",
    df,
    storage_options=storage_options
)
```

!!! note
    For Alibaba OSS issues, see [#2361](https://github.com/delta-io/delta-rs/issues/2361) for known compatibility considerations.

### LocalStack

LocalStack provides a local AWS cloud emulator for testing, including S3 emulation.

```python
from deltalake import write_deltalake

storage_options = {
    'AWS_ACCESS_KEY_ID': 'test',  # LocalStack accepts any credentials
    'AWS_SECRET_ACCESS_KEY': 'test',
    'AWS_ENDPOINT_URL': 'http://localhost:4566',  # Default LocalStack endpoint
    'AWS_REGION': 'us-east-1',
    'allow_http': 'true',  # Required for HTTP endpoint
    'aws_skip_signature': 'true',  # Optional: skip signing for faster testing
}

write_deltalake(
    "s3://test-bucket/delta-table",
    df,
    storage_options=storage_options
)
```

### Ceph / RADOS Gateway

Ceph's RADOS Gateway provides an S3-compatible interface to Ceph object storage.

```python
from deltalake import write_deltalake

storage_options = {
    'AWS_ACCESS_KEY_ID': '<CEPH_ACCESS_KEY>',
    'AWS_SECRET_ACCESS_KEY': '<CEPH_SECRET_KEY>',
    'AWS_ENDPOINT_URL': 'https://ceph.example.com',
    'AWS_REGION': 'default',  # Ceph typically uses 'default' or 'us-east-1'
    'aws_virtual_hosted_style_request': 'false',  # Ceph often requires path-style requests
}

write_deltalake(
    "s3://my-bucket/delta-table",
    df,
    storage_options=storage_options
)
```

!!! warning
    Different Ceph configurations may require different settings. Check your Ceph installation's documentation for specific requirements.

## Configuration Key Summary

When working with S3-compatible services, the most important configuration keys are:

| Key | Purpose | Common Values |
|-----|---------|---------------|
| `AWS_ENDPOINT_URL` | Custom S3 endpoint | Service-specific URL |
| `aws_conditional_put` | Safe concurrent writes | `etag` (for services supporting conditional puts) |
| `allow_http` | Allow non-HTTPS connections | `true` for local/testing environments |
| `aws_virtual_hosted_style_request` | URL style for requests | `true` for virtual-hosted, `false` for path-style |
| `aws_skip_signature` | Skip request signing | `true` for testing/unauthenticated access |

!!! tip
    Most S3-compatible services work best with `aws_conditional_put: 'etag'` to enable safe concurrent writes without requiring DynamoDB.
