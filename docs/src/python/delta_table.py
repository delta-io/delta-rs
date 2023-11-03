def get_table_info():
    # --8<-- [start:get_table_info]
    from deltalake import DeltaTable

    dt = DeltaTable("../rust/tests/data/delta-0.2.0")
    print(f"Version: {dt.version()}")
    print(f"Files: {dt.files()}")
    # --8<-- [end:get_table_info]
