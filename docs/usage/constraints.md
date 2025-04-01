# Adding a Constraint to a table

Check constraints are a way to enforce that only data that meets the constraint is allowed to be added to the table.

## Add the Constraint

{{ code_example('check_constraints', 'add_constraint', ['DeltaTable']) }}

After you have added the constraint to the table attempting to append data to the table that violates the constraint
will instead throw an error.

## Verify the constraint by trying to add some data

{{ code_example('check_constraints', 'add_data', []) }}
