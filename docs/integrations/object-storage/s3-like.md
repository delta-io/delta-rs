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
