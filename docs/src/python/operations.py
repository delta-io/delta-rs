def replace_where():
    # --8<-- [start:replace_where]
    import pyarrow as pa
    from deltalake import write_deltalake

    # Assuming there is already a table in this location with some records where `id = '1'` which we want to overwrite
    table_path = "/tmp/my_table"
    data = pa.table(
        {
            "id": pa.array(["1", "1"], pa.string()),
            "value": pa.array([11, 12], pa.int64()),
        }
    )
    write_deltalake(
        table_path,
        data,
        mode="overwrite",
        predicate="id = '1'",
    )
    # --8<-- [end:replace_where]
