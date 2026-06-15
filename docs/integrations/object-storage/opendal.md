# OpenDAL Storage Backends

`delta-rs` can reach any storage service supported by
[Apache OpenDAL](https://opendal.apache.org/) through a single generic backend.
This complements the native backends (S3, ADLS, GCS, HDFS, LakeFS): when a
service has no dedicated native integration — or you simply want to use the
OpenDAL implementation — you can address it through an OpenDAL scheme without
any service-specific code in delta-rs.

The Python wheel ships with these OpenDAL services enabled: `fs`, `memory`,
`s3`, `gcs`, `azblob`, `azdls`, `oss`, `obs`, `cos`, `tos`, `b2`, `swift`,
`webhdfs`, `webdav`, `ftp`, `sftp`, and `hf` (HuggingFace Hub).

## URL schemes

Every enabled service is reachable under an unambiguous `opendal+<service>://`
scheme, for example `opendal+s3://`. When the service's natural scheme does
**not** collide with a native delta backend, it is *also* registered under that
bare scheme, so `hf://`, `fs://`, `gcs://`, `oss://`, … work directly.

Services whose natural scheme is owned by a native backend (`s3`, `memory`) are
reachable **only** via the `opendal+` form (`opendal+s3://`,
`opendal+memory://`), so enabling them never shadows the native `s3://` backend.

## Passing service configuration

All OpenDAL configuration is passed through `storage_options` using the
`opendal.<key>` convention. Each `opendal.<key> = <value>` entry is forwarded to
the OpenDAL operator as `<key> = <value>`, with the `opendal.` prefix stripped.
This keeps delta's own reserved option keys (retries, timeouts, …) separate from
the service config and lets you reach any documented OpenDAL service key without
a per-service allow-list. See the
[OpenDAL service docs](https://opendal.apache.org/docs/rust/opendal/services/index.html)
for the keys each service accepts.

How the URL maps onto the operator for the simple (bucket-rooted) services:

- the URL **host** becomes the `bucket` (unless you set `opendal.bucket`
  explicitly),
- the URL **path** becomes the table prefix within that bucket,
- the operator is scoped at the bucket root.

## Example: an S3-compatible service

Any S3-compatible object store (here, a self-hosted MinIO) via the OpenDAL S3
service. The bucket comes from the URL host, the table path from the URL path.

```python
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

storage_options = {
    "opendal.endpoint": "http://localhost:9000",
    "opendal.region": "us-east-1",
    "opendal.access_key_id": "minioadmin",
    "opendal.secret_access_key": "minioadmin",
}

df = pa.table({"x": [1, 2, 3]})
write_deltalake(
    "opendal+s3://my-bucket/my_table",
    df,
    storage_options=storage_options,
)

dt = DeltaTable("opendal+s3://my-bucket/my_table", storage_options=storage_options)
```

## Example: local filesystem

The `fs` service has no native counterpart, so it is available under its bare
`fs://` scheme (delta's core owns `file://`, not `fs://`). `opendal.root` scopes
the operator at a directory; the URL path is the table prefix within it.

```python
storage_options = {"opendal.root": "/tmp/delta-data"}
write_deltalake("fs://localhost/my_table", df, storage_options=storage_options)
```

## Example: HuggingFace Hub — datasets and models

Dataset and model repositories are addressed as
`hf://<repo_type>/<owner>/<repo>/<table_path>`. An optional revision
(branch, tag, or commit SHA) can be embedded in the URL as
`hf://<repo_type>/<owner>/<repo>@<revision>/<table_path>`.

```python
storage_options = {
    "opendal.token": "hf_...",  # a HuggingFace access token
}

# Write to the default branch
write_deltalake(
    "hf://datasets/my-org/my-dataset/my_table",
    df,
    storage_options=storage_options,
)

# Read from a specific revision
dt = DeltaTable(
    "hf://datasets/my-org/my-dataset@v1.0/my_table",
    storage_options=storage_options,
)
```

## Example: HuggingFace Hub — buckets

HuggingFace [Buckets](https://huggingface.co/docs/huggingface_hub/en/guides/buckets)
are S3-like object stores (no git history) addressed as
`hf://buckets/<owner>/<bucket>/<table_path>`. Buckets do not support
revisions.

```python
storage_options = {
    "opendal.token": "hf_...",  # a HuggingFace access token
}

write_deltalake(
    "hf://buckets/my-org/my-bucket/my_table",
    df,
    storage_options=storage_options,
)

dt = DeltaTable(
    "hf://buckets/my-org/my-bucket/my_table",
    storage_options=storage_options,
)
```
