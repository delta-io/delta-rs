# Usage

A [DeltaTable][deltalake.table.DeltaTable] represents the state of a
delta table at a particular version. This includes which files are
currently part of the table, the schema of the table, and other metadata
such as creation time.

{{ code_example('delta_table', 'get_table_info', ['DeltaTable'])}}