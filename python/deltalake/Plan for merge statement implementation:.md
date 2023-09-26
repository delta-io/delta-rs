Plan for merge statement implementation:

- create Merge class property on DeltaTable
    - Which returns a TableMergeBuilder class
- On TableMergeBuilder create multiple class methods
    See options in Spark-delta and what's available in Delta-RS for the naming

- in Lib.rs create merge statement that's called from the propertycall on the RawDeltaTable in python
    - This merge should return 
    
