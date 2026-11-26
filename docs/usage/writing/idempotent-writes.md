# Idempotent writes with application transactions

A job that is retried after a failure, or a stream consumer that receives the same batch twice, will append the same rows twice unless the writer can tell that the batch is already in the table. The Delta transaction log supports this with the `txn` action: a commit can record an application id together with an application-defined version number, and later writers can read back the latest version committed for that id and skip work they have already done.

delta-rs exposes this through three primitives:

- `Transaction(app_id, version)` describes one application transaction. The meaning of `version` is up to your application (a batch number, a stream offset, a run id), but it must increase with every new batch for the check below to work.
- `CommitProperties(app_transactions=[...])` attaches one or more transactions to a commit. Every operation that accepts `commit_properties` can record them, including `write_deltalake`, `DeltaTable.create_write_transaction`, and the `DeltaTable` operations such as merge, update and delete.
- `DeltaTable.transaction_version(app_id)` returns the latest version committed for `app_id`, or `None` if the table has never recorded that id.

## Writing a batch exactly once

Before writing, read the latest version recorded for your application id. If it is already at or past the batch you are about to write, an earlier attempt committed it and the batch can be skipped. Otherwise write the data and record the batch's version in the same commit, so the data and the marker become visible atomically.

{{ code_example('idempotent_writes', 'idempotent_append', ['CommitProperties']) }}

If a writer fails after uploading data files but before the commit, no transaction is recorded and the retry writes the batch again, which is the intended outcome. If it fails after the commit, the retry finds the marker and does nothing.

## Concurrent writers

Checking and then writing leaves a gap: another process could commit the same batch between your check and your commit. delta-rs closes this gap in its conflict checker. If your commit records a transaction for an application id, and a commit that landed after the snapshot you read from also recorded a transaction for that id, your commit is rejected with a `ConcurrentTransaction` conflict. In Python this surfaces as `CommitFailedError`; in Rust as a `DeltaTableError::Transaction` whose source is `CommitConflictError::ConcurrentTransaction`.

For this protection to apply, the check and the write have to use the same snapshot. In Python, pass the `DeltaTable` object on which you called `transaction_version` to the write; passing the table URI instead loads a fresh snapshot and reopens the gap.

On a conflict, refresh the table, read the version again, and either skip the batch or retry it:

{{ code_example('idempotent_writes', 'recover_from_conflict', ['DeltaTable']) }}

Writers that use different application ids do not conflict with each other on this rule, and neither do commits that record no transaction at all.

## Choosing application ids and versions

- Use one application id per independent stream of writes into the table, for example one per pipeline or per source partition. Two writers that share an id serialize on each other.
- Versions are compared numerically, so they must be monotonically increasing. Batch sequence numbers, stream offsets, or the epoch seconds of a scheduler's logical run time all work. The wall-clock time of the attempt does not, because a retry would get a newer value and be written again.
- `Transaction` also accepts an optional `last_updated` timestamp in milliseconds since the Unix epoch. If the table property `delta.setTransactionRetentionDuration` is set, `transaction_version` treats a transaction whose `last_updated` is older than that interval as expired and returns `None` for it, so pick a retention period longer than the longest gap between runs of your application. Transactions without `last_updated` never expire.

See the [transaction API reference](../../api/transaction.md) for `CommitProperties` and `Transaction`.
