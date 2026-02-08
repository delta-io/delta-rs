# LakeFS
`delta-rs` offers native support for using LakeFS as an object storage backend. Each 
deltalake operation is executed in a transaction branch and safely merged into your source branch.

You don’t need to install any extra dependencies to read/write Delta tables to LakeFS with engines that use `delta-rs`. You do need to configure your LakeFS access credentials correctly.

## Passing LakeFS Credentials

You can pass your LakeFS credentials explicitly by using:

- the `storage_options `kwarg
- Environment variables

## Example

Let's work through an example with Polars. The same logic applies to other Python engines like Pandas, Daft, Dask, etc.

Follow the steps below to use Delta Lake on LakeFS with Polars:

1. Install Polars and deltalake. For example, using:

   `pip install polars deltalake`

2. Create a dataframe with some toy data.

   `df = pl.DataFrame({'x': [1, 2, 3]})`

3. Set your `storage_options` correctly.

```python
storage_options = {
        "endpoint": "https://mylakefs.intranet.com", # LakeFS endpoint
        "access_key_id": "LAKEFSID",
        "secret_access_key": "LAKEFSKEY",
    }
```

4. Write data to Delta table using the `storage_options` kwarg. The subpath after the bucket is always the branch you want to write into.

   ```python
   df.write_delta(
       "lakefs://bucket/branch/table",
       storage_options=storage_options,
   )
   ```

## Cleaning up failed transaction branches

It might occur that a deltalake operation fails midway. At this point a lakefs transaction branch was created, but never destroyed. The branches are hidden in the UI, but each branch starts with `delta-tx`.

With the lakefs python library you can list these branches and delete stale ones.

```python
import lakefs

# Initialize LakeFS client
client = lakefs.Client(
    host="https://mylakefs.example.com",
    username="LAKEFSID",
    password="LAKEFSKEY",
)

# Access the repository
repo = lakefs.Repository("my-repo", client=client)

# List and delete stale transaction branches
for branch in repo.branches():
    if branch.id.startswith("delta-tx"):
        print(f"Deleting stale transaction branch: {branch.id}")
        branch.delete()
```

!!! tip
    You can add additional logic to check the branch creation time and only delete branches older than a certain threshold to avoid removing branches from operations that are still in progress.
