# Writing to S3 with a locking provider

Delta lake guarantees [ACID transactions](../../how-delta-lake-works/delta-lake-acid-transactions.md) when writing data. This is done by default when writing to all supported object stores except AWS S3. (Some S3 clients like CloudFlare R2 or MinIO may enable atomic renames, refer to [this section](#enabling-concurrent-writes-for-alternative-clients) for more information).

When writing to S3, delta-rs provides a locking mechanism to ensure that concurrent writes are safe. This is done by default when writing to S3, but you can opt-out by setting the `AWS_S3_ALLOW_UNSAFE_RENAME` variable to ``true``.

To enable safe concurrent writes to AWS S3, we must provide an external locking mechanism.

### DynamoDB
DynamoDB is the only available locking provider at the moment in delta-rs. To enable DynamoDB as the locking provider, you need to set the ``AWS_S3_LOCKING_PROVIDER`` to 'dynamodb' as a ``storage_options`` or as an environment variable.

Additionally, you must create a DynamoDB table with the name ``delta_log``
so that it can be automatically recognized by delta-rs. Alternatively, you can
use a table name of your choice, but you must set the ``DELTA_DYNAMO_TABLE_NAME``
variable to match your chosen table name. The required schema for the DynamoDB
table is as follows:

```json
"Table": {
    "AttributeDefinitions": [
        {
            "AttributeName": "fileName",
            "AttributeType": "S"
        },
        {
            "AttributeName": "tablePath",
            "AttributeType": "S"
        }
    ],
    "TableName": "delta_log",
    "KeySchema": [
        {
            "AttributeName": "tablePath",
            "KeyType": "HASH"
        },
        {
            "AttributeName": "fileName",
            "KeyType": "RANGE"
        }
    ],
}
```

Here is an example writing to s3 using this mechanism:

```python
from deltalake import write_deltalake
df = pd.DataFrame({'x': [1, 2, 3]})
storage_options = {
    'AWS_S3_LOCKING_PROVIDER': 'dynamodb',
    'DELTA_DYNAMO_TABLE_NAME': 'custom_table_name'
}
write_deltalake(
    's3a://path/to/table',
    df,
    storage_options=storage_options
)
```

This locking mechanism is compatible with the one used by Apache Spark. The `tablePath` property, denoting the root url of the delta table itself, is part of the primary key, and all writers intending to write to the same table must match this property precisely. In Spark, S3 URLs are prefixed with `s3a://`, and a table in delta-rs must be configured accordingly.

The following code allows creating the necessary table from the AWS cli:

```sh
aws dynamodb create-table \
--table-name delta_log \
--attribute-definitions AttributeName=tablePath,AttributeType=S AttributeName=fileName,AttributeType=S \
--key-schema AttributeName=tablePath,KeyType=HASH AttributeName=fileName,KeyType=RANGE \
--provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5
```

You can find additional information in the [delta-rs-documentation](https://docs.delta.io/latest/delta-storage.html#multi-cluster-setup), which also includes recommendations on configuring a time-to-live (TTL) for the table to avoid growing the table indefinitely.


### Enable unsafe writes in S3 (opt-in)
If for some reason you don't want to use dynamodb as your locking mechanism you can
choose to set the `AWS_S3_ALLOW_UNSAFE_RENAME` variable to ``true`` in order to enable S3 unsafe writes.


### Required permissions
You need to have permissions to get, put and delete objects in the S3 bucket you're storing your data in. Please note that you must be allowed to delete objects even if you're just appending to the deltalake, because there are temporary files into the log folder that are deleted after usage.

In AWS, those would be the required permissions:

- s3:GetObject
- s3:PutObject
- s3:DeleteObject

In DynamoDB, you need those permissions:

- dynamodb:GetItem
- dynamodb:Query
- dynamodb:PutItem
- dynamodb:UpdateItem

### Enabling concurrent writes for alternative clients

Unlike AWS S3, some S3 clients support atomic renames by passing some headers
in requests.

For CloudFlare R2 passing this in the storage_options will enable concurrent writes:

```python
storage_options = {
    "copy_if_not_exists": "header: cf-copy-destination-if-none-match: *",
}
```

Something similar can be done with MinIO but the header to pass should be verified
in the MinIO documentation.