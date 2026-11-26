def idempotent_append():
    # --8<-- [start:idempotent_append]
    import pyarrow as pa

    from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake

    APP_ID = "orders-loader"

    def append_batch(table_uri: str, batch_id: int, data: pa.Table) -> bool:
        """Append `data` unless this batch, or a later one, was already committed."""
        if not DeltaTable.is_deltatable(table_uri):
            target = table_uri
        else:
            dt = DeltaTable(table_uri)
            last = dt.transaction_version(APP_ID)
            if last is not None and batch_id <= last:
                return False
            # Write through the same DeltaTable object that was used for the check,
            # so the commit is validated against that snapshot.
            target = dt
        write_deltalake(
            target,
            data,
            mode="append",
            commit_properties=CommitProperties(
                app_transactions=[Transaction(app_id=APP_ID, version=batch_id)]
            ),
        )
        return True

    data = pa.table({"order_id": [1, 2, 3], "amount": [10.0, 20.0, 30.0]})
    assert append_batch("/tmp/orders", batch_id=1, data=data)
    # Retrying the same batch is a no-op.
    assert not append_batch("/tmp/orders", batch_id=1, data=data)
    assert DeltaTable("/tmp/orders").transaction_version(APP_ID) == 1
    # --8<-- [end:idempotent_append]


def recover_from_conflict():
    # --8<-- [start:recover_from_conflict]
    import pyarrow as pa

    from deltalake import CommitProperties, DeltaTable, Transaction, write_deltalake
    from deltalake.exceptions import CommitFailedError

    APP_ID = "orders-loader"
    data = pa.table({"order_id": [4, 5], "amount": [40.0, 50.0]})

    dt = DeltaTable("/tmp/orders")
    try:
        write_deltalake(
            dt,
            data,
            mode="append",
            commit_properties=CommitProperties(
                app_transactions=[Transaction(app_id=APP_ID, version=2)]
            ),
        )
    except CommitFailedError:
        # Another writer committed a transaction for APP_ID after our snapshot
        # was taken. Refresh and check whether it already covered this batch.
        dt.update_incremental()
        last = dt.transaction_version(APP_ID)
        if last is None or last < 2:
            raise
    # --8<-- [end:recover_from_conflict]
