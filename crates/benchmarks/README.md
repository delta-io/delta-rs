# Merge
The merge benchmarks are similar to the ones used by [Delta Spark](https://github.com/delta-io/delta/pull/1835).


## Dataset

Databricks maintains a public S3 bucket of the TPC-DS dataset with various factor where requesters must pay to download this dataset. Below is an example of how to list the 1gb scale factor 

```
aws s3api list-objects --bucket devrel-delta-datasets --request-payer requester --prefix tpcds-2.13/tpcds_sf1_parquet/web_returns/
```

You can generate the TPC-DS dataset yourself by downloading and compiling [the generator](https://www.tpc.org/tpc_documents_current_versions/current_specifications5.asp) 
You may need to update the CFLAGS to include `-fcommon` to compile on newer versions of GCC.

## Commands
These commands can be executed from the root of the benchmark crate. Some commands depend on the existance of the TPC-DS Dataset existing.

### Convert
Converts a TPC-DS web_returns csv into a Delta table
Assumes the dataset is pipe delimited and records do not have a trailing delimiter

```
 cargo run --release --bin merge -- convert data/tpcds/web_returns.dat data/web_returns
```

### Standard
Execute the standard merge bench suite.
Results can be saved to a delta table for further analysis.
This table has the following schema:

group_id: Used to group all tests that executed as a part of this call. Default value is the timestamp of execution
name: The benchmark name that was executed
sample: The iteration number for a given benchmark name
duration_ms: How long the benchmark took in ms
data: Free field to pack any additonal data

```
 cargo run --release --bin merge -- standard data/web_returns 1 data/merge_results 
```

### Compare
Compare the results of two different runs.
The a Delta table paths and the `group_id` of each run and obtain the speedup for each test case

```
 cargo run --release --bin merge -- compare data/benchmarks/ 1698636172801 data/benchmarks/ 1699759539902
```

### Show
Show all benchmarks results from a delta table

```
 cargo run --release --bin merge -- show data/benchmark
```
