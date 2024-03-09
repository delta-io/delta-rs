def add_constraint():
    # --8<-- [start:add_constraint]
    from deltalake import DeltaTable

    dt = DeltaTable("../rust/tests/data/simple_table")

    # Check the schema before hand
    print(dt.schema())
    # Add the constraint to the table.
    dt.alter.add_constraint({"id_gt_0": "id > 0"})
    # --8<-- [end:add_constraint]


def add_data():
    # --8<-- [start:add_data]
    from deltalake import write_deltalake, DeltaTable
    import pandas as pd

    dt = DeltaTable("../rust/tests/data/simple_table")

    df = pd.DataFrame({"id": [-1]})
    write_deltalake(dt, df, mode="append", engine="rust")
    # _internal.DeltaProtocolError: Invariant violations: ["Check or Invariant (id > 0) violated by value in row: [-1]"]
    # --8<-- [end:add_data]
