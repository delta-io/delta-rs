# CloudFlare R2 & Minio

`delta-rs` offers native support for using Cloudflare R2 and Minio's as storage backend. R2 and Minio support conditional puts, however we have to pass this flag into the storage options. See the example below

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
    'conditional_put': 'etag', # Here we say to use conditional put, this provides safe concurrency.
}
```

4. Write data to Delta table using the `storage_options` kwarg.

   ```python
   df.write_delta(
       "s3://bucket/delta_table",
       storage_options=storage_options,
   )
   ```

## Delta Lake on S3: Safe Concurrent Writes

You need a locking provider to ensure safe concurrent writes when writing Delta tables to S3. This is because S3 does not guarantee mutual exclusion.

A locking provider guarantees that only one writer is able to create the same file. This prevents corrupted or conflicting data.

`delta-rs` uses DynamoDB to guarantee safe concurrent writes.

Run the code below in your terminal to create a DynamoDB table that will act as your locking provider.

```
    aws dynamodb create-table \
    --table-name delta_log \
    --attribute-definitions AttributeName=tablePath,AttributeType=S AttributeName=fileName,AttributeType=S \
    --key-schema AttributeName=tablePath,KeyType=HASH AttributeName=fileName,KeyType=RANGE \
    --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

If for some reason you don't want to use DynamoDB as your locking mechanism you can choose to set the `AWS_S3_ALLOW_UNSAFE_RENAME` variable to `true` in order to enable S3 unsafe writes.

Read more in the [Usage](../../usage/writing/writing-to-s3-with-locking-provider.md) section.

## Delta Lake on S3: Required permissions

You need to have permissions to get, put and delete objects in the S3 bucket you're storing your data in. Please note that you must be allowed to delete objects even if you're just appending to the Delta Lake, because there are temporary files into the log folder that are deleted after usage.

In AWS S3, you will need the following permissions:

- s3:GetObject
- s3:PutObject
- s3:DeleteObject

In DynamoDB, you will need the following permissions:

- dynamodb:GetItem
- dynamodb:Query
- dynamodb:PutItem
- dynamodb:UpdateItem
