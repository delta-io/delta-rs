# Adding a Constraint to a table

Check constraints are a way to enforce that only data that meets the constraint is allowed to be added to the table.

delta-rs evaluates CHECK constraints using DataFusion SQL semantics. For example, `n / 2 = 3` accepts an integer `n` of `7` in delta-rs, while Spark SQL evaluates the expression to false. When writing from multiple engines, verify that the constraint accepts and rejects the same data in each engine. See also [generated column expressions](writing/index.md#generated-column-expressions).

## Add the Constraint

{{ code_example('check_constraints', 'add_constraint', ['DeltaTable']) }}

After you have added the constraint to the table attempting to append data to the table that violates the constraint
will instead throw an error.

## Verify the constraint by trying to add some data

{{ code_example('check_constraints', 'add_data', []) }}
