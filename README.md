<p align="center">
  <a href="https://delta.io/">
    <img src="https://github.com/delta-io/delta-rs/blob/main/logo.png?raw=true" alt="delta-rs logo" height="250">
  </a>
</p>
<p align="center">
  A native Rust library for Delta Lake, with bindings into Python
  <br>
  <a href="https://delta-io.github.io/delta-rs/python/">Python docs</a>
  ·
  <a href="https://docs.rs/deltalake/latest/deltalake/">Rust docs</a>
  ·
  <a href="https://github.com/delta-io/delta-rs/issues/new?template=bug_report.md">Report a bug</a>
  ·
  <a href="https://github.com/delta-io/delta-rs/issues/new?template=feature_request.md">Request a feature</a>
  ·
  <a href="https://github.com/delta-io/delta-rs/issues/1128">Roadmap</a>
  <br>
  <br>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/l/deltalake.svg?style=flat-square&color=00ADD4&logo=apache">
  </a>
  <a target="_blank" href="https://github.com/delta-io/delta-rs" style="background:none">
    <img src="https://img.shields.io/github/stars/delta-io/delta-rs?logo=github&color=F75101">
  </a>
  <a target="_blank" href="https://crates.io/crates/deltalake" style="background:none">
    <img alt="Crate" src="https://img.shields.io/crates/v/deltalake.svg?style=flat-square&color=00ADD4&logo=rust" >
  </a>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/v/deltalake.svg?style=flat-square&color=F75101&logo=pypi" >
  </a>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/pyversions/deltalake.svg?style=flat-square&color=00ADD4&logo=python">
  </a>
  <a target="_blank" href="https://go.delta.io/slack">
    <img alt="#delta-rs in the Delta Lake Slack workspace" src="https://img.shields.io/badge/slack-delta-blue.svg?logo=slack&style=flat-square&color=F75101">
  </a>
</p>

The Delta Lake project aims to unlock the power of the Deltalake for as many users and projects as possible
by providing native low level APIs aimed at developers and integrators, as well as a high level operations
API that lets you query, inspect, and operate your Delta Lake with ease.

| Source                | Downloads                         | Installation Command    | Docs            |
| --------------------- | --------------------------------- | ----------------------- | --------------- |
| **[PyPi][pypi]**      | [![Downloads][pypi-dl]][pypi]     | `pip install deltalake` | [Docs][py-docs] |
| **[Crates.io][pypi]** | [![Downloads][crates-dl]][crates] | `cargo add deltalake`   | [Docs][rs-docs] |

[pypi]: https://pypi.org/project/deltalake/
[pypi-dl]: https://img.shields.io/pypi/dm/deltalake?style=flat-square&color=00ADD4
[py-docs]: https://delta-io.github.io/delta-rs/python/
[rs-docs]: https://docs.rs/deltalake/latest/deltalake/
[crates]: https://crates.io/crates/deltalake
[crates-dl]: https://img.shields.io/crates/d/deltalake?color=F75101

## Table of contents

- [Quick Start](#quick-start)
- [Get Involved](#get-involved)
- [Integartions](#integrations)
- [Features](#features)

## Quick Start

The `deltalake` library aim to adopt familiar patterns from other libraries in data processing,
so getting started should look famililiar.

```py3
from deltalake import DeltaTable
from deltalake.write import write_deltalake
import pandas as pd

# write some data into a delta table
df = pd.DataFrame({"id": [1, 2], "value": ["foo", "boo"]})
write_deltalake("./data/delta", df)

# load data from delta table
dt = DeltaTable("./data/delta")
df2 = dt.to_pandas()

assert df == df2
```

The same table written can also be loaded using the core Rust crate:

```rs
use deltalake::{open_table, DeltaTableError};

#[tokio::main]
async fn main() -> Result<(), DeltaTableError> {
    // open the table written in python
    let table = open_table("./data/delta").await?;

    // show all active files in the table
    let files = table.get_files();
    println!("{files}");

    Ok(())
}
```

## Get Involved

We encourage you to reach out, and are [commited](https://github.com/delta-io/delta-rs/blob/main/CODE_OF_CONDUCT.md)
to provide a welcoming community.

- [Join us in our Slack workspace](https://go.delta.io/slack)
- [Report an issue](https://github.com/delta-io/delta-rs/issues/new?template=bug_report.md)
- Looking to contribute? See our [good first issues](https://github.com/delta-io/delta-rs/contribute).

## Integrations

Libraries and framewors that interoperate with delta-rs - in alphabetical order.

- [AWS SDK for Pandas](https://github.com/aws/aws-sdk-pandas)
- [ballista][ballista]
- [datafusion][datafusion]
- [Dask](https://github.com/dask-contrib/dask-deltatable)
- [datahub](https://datahubproject.io/)
- [DuckDB](https://duckdb.org/)
- [polars](https://www.pola.rs/)
- [Ray](https://github.com/delta-incubator/deltaray)

## Features

The following section outline some core features like supported [storage backends](#cloud-integrations)
and [operations](#supported-operations) that can be performed against tables. The state of implementation
of features outlined in the Delta [protocol][protocol] is also [tracked](#protocol-support-level).

### Cloud Integrations

| Storage              |         Rust          |        Python         | Comment                             |
| -------------------- | :-------------------: | :-------------------: | ----------------------------------- |
| Local                |        ![done]        |        ![done]        |                                     |
| S3 - AWS             |        ![done]        |        ![done]        | requires lock for concurrent writes |
| S3 - MinIO           |        ![done]        |        ![done]        | requires lock for concurrent writes |
| S3 - R2              |        ![done]        |        ![done]        | requires lock for concurrent writes |
| Azure Blob           |        ![done]        |        ![done]        |                                     |
| Azure ADLS Gen2      |        ![done]        |        ![done]        |                                     |
| Micorosft OneLake    | [![open]][onelake-rs] | [![open]][onelake-rs] |                                     |
| Google Cloud Storage |        ![done]        |        ![done]        |                                     |

### Supported Operations

| Operation             |        Rust         |       Python        | Description                           |
| --------------------- | :-----------------: | :-----------------: | ------------------------------------- |
| Create                |       ![done]       |       ![done]       | Create a new table                    |
| Read                  |       ![done]       |       ![done]       | Read data from a table                |
| Vacuum                |       ![done]       |       ![done]       | Remove unused files and log entries   |
| Delete - partitions   |                     |       ![done]       | Delete a table partition              |
| Delete - predicates   |       ![done]       |                     | Delete data based on a predicate      |
| Optimize - compaction |       ![done]       |       ![done]       | Harmonize the size of data file       |
| Optimize - Z-order    |       ![done]       |       ![done]       | Place similar data into the same file |
| Merge                 | [![open]][merge-rs] | [![open]][merge-py] |                                       |
| FS check              |       ![done]       |                     | Remove corrupted files from table     |

### Protocol Support Level

| Writer Version | Requirement                                   |        Status        |
| -------------- | --------------------------------------------- | :------------------: |
| Version 2      | Append Only Tables                            |  [![open]][roadmap]  |
| Version 2      | Column Invariants                             |       ![done]        |
| Version 3      | Enforce `delta.checkpoint.writeStatsAsJson`   | [![open]][writer-rs] |
| Version 3      | Enforce `delta.checkpoint.writeStatsAsStruct` | [![open]][writer-rs] |
| Version 3      | CHECK constraints                             | [![open]][writer-rs] |
| Version 4      | Change Data Feed                              |                      |
| Version 4      | Generated Columns                             |                      |
| Version 5      | Column Mapping                                |                      |
| Version 6      | Identity Columns                              |                      |
| Version 7      | Table Features                                |                      |

| Reader Version | Requirement                         | Status |
| -------------- | ----------------------------------- | ------ |
| Version 2      | Collumn Mapping                     |        |
| Version 3      | Table Features (requires reader V7) |        |

[datafusion]: https://github.com/apache/arrow-datafusion
[ballista]: https://github.com/apache/arrow-ballista
[polars]: https://github.com/pola-rs/polars
[open]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueOpened.svg
[done]: https://cdn.jsdelivr.net/gh/Readme-Workflows/Readme-Icons@main/icons/octicons/IssueClosed.svg
[roadmap]: https://github.com/delta-io/delta-rs/issues/1128
[merge-py]: https://github.com/delta-io/delta-rs/issues/1357
[merge-rs]: https://github.com/delta-io/delta-rs/issues/850
[writer-rs]: https://github.com/delta-io/delta-rs/issues/851
[onelake-rs]: https://github.com/delta-io/delta-rs/issues/1418
[protocol]: https://github.com/delta-io/delta/blob/master/PROTOCOL.md
