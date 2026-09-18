# LakeFS
`delta-rs` offers native support for using LakeFS as an object storage backend. Every writing
operation runs on a hidden LakeFS transaction branch and is squash-merged into your source branch.
Each Delta commit becomes exactly one LakeFS commit on the source branch, so a failed operation
never leaves partial data on the branch you work on.

You don’t need to install any extra dependencies to read/write Delta tables to LakeFS with engines that use `delta-rs`. You do need to configure your LakeFS access credentials correctly.

## Passing LakeFS Credentials

You can pass your LakeFS credentials explicitly by using:

- the `storage_options` kwarg
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

## How an operation reaches your branch

Every writing operation (`write`, `delete`, `update`, `merge`, `optimize`, `vacuum`, table
creation, schema changes, checkpoints, log compaction and metadata cleanup) does the following:

1. It creates a hidden branch named `delta-tx-<uuid>` from your source branch.
2. It uploads its data files and its `_delta_log/<version>.json` to that branch.
3. It makes a LakeFS commit of the branch and squash-merges the branch into your source branch.
   When another writer already committed the same Delta version, the merge is rejected, the
   operation checks for conflicts and retries with the next version on the same branch.
4. It deletes the transaction branch. When the operation fails at any point, the branch is
   deleted and your source branch is left unchanged.

Post-commit work such as checkpoints and expired log cleanup runs on a second transaction branch
and produces a separate LakeFS commit. To undo one Delta commit with `lakectl revert`, revert both
the commit and its checkpoint commit, or reset the branch to the version before the commit.

### Writing to a branch with uncommitted changes

LakeFS refuses to merge into a branch that has uncommitted (staged) changes, and no API flag
bypasses that check. An operation that hits this state fails with a `DirtyBranch` error, deletes
its transaction branch and leaves your source branch untouched. Commit or revert the staged
changes on the branch and retry the operation.

### Low-level writers

`create_write_transaction` in Python and `RecordBatchWriter` in Rust do not use a transaction
branch. They stage their files directly on the source branch and, when they commit, write
`_delta_log/<version>.json` with a conditional put and then make a LakeFS commit of the source
branch. That LakeFS commit contains everything that is staged on the branch at that moment, not
only the files of the Delta commit.

## Cleaning up transaction branches after a process kill

A transaction branch is deleted when the operation finishes, fails, or is cancelled. Only a
process that is killed while an operation is running leaves its `delta-tx-*` branch behind. The
branches are hidden in the UI.

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
