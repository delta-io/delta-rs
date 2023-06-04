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
    <img alt="Deltalake" src="https://img.shields.io/pypi/l/deltalake.svg?style=flat-square&color=00ADD4&labelColor=F75101&logo=apache&label=&logoColor=00ADD4">
  </a>
  <a target="_blank" href="https://github.com/delta-io/delta-rs" style="background:none">
    <img src="https://img.shields.io/github/stars/delta-io/delta-rs?logo=github&color=00ADD4&labelColor=F75101&logoColor=00ADD4&label=&logoColor=00ADD4">
  </a>
  <a href="https://crates.io/crates/deltalake">
    <img alt="Crate" src="https://img.shields.io/crates/v/deltalake.svg?style=flat-square&color=00ADD4&labelColor=F75101&logo=rust&label=&logoColor=00ADD4" >
  </a>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/v/deltalake.svg?style=flat-square&color=00ADD4&labelColor=F75101&logo=python&label=&logoColor=00ADD4" >
  </a>
  <a target="_blank" href="https://pypistats.org/packages/deltalake" style="background:none">
    <img alt="PyPI - Downloads" src="https://img.shields.io/pypi/dm/deltalake?style=flat-square&color=00ADD4&labelColor=F75101&logo=pypi&label=&logoColor=00ADD4" >
  </a>
  <a href="https://pypi.python.org/pypi/deltalake">
    <img alt="Deltalake" src="https://img.shields.io/pypi/pyversions/deltalake.svg?style=flat-square&color=00ADD4&labelColor=F75101&logo=python&label=&logoColor=00ADD4">
  </a>
  <a target="_blank" href="https://go.delta.io/slack">
    <img alt="#delta-rs in the Delta Lake Slack workspace" src="https://img.shields.io/badge/slack-delta-blue.svg?logo=slack&style=flat-square&color=00ADD4&labelColor=F75101&label=&logoColor=00ADD4">
  </a>
</p>

This library provides low level access to Delta tables in Rust, which can be used standalone
or for integrating on other frameworks like [datafusion][datafusion], [polars][polars], or [ballista][ballista].
It also provider higher level APIs to perform more complex [operations](#supported-operations) that are
used to interact with Delta tables. These operations are also exposed as python bindings.

## Cloud Integrations

| Storage         |  Rust   | Python  | Comment                             |
| --------------- | :-----: | :-----: | ----------------------------------- |
| Local           | ![done] | ![done] |                                     |
| S3 - AWS        | ![done] | ![done] | requires lock for concurrent writes |
| S3 - MinIO      | ![done] | ![done] | requires lock for concurrent writes |
| S3 - R2         | ![done] | ![done] | requires lock for concurrent writes |
| Azure Blob      | ![done] | ![done] |                                     |
| Azure ADLS Gen2 | ![done] | ![done] |                                     |

## Supported Operations

| Operation             |        Rust         |       Python        | Description                           |
| --------------------- | :-----------------: | :-----------------: | ------------------------------------- |
| Create                |       ![done]       |       ![done]       | Create a new table                    |
| Read                  |       ![done]       |       ![done]       | Read data from a table                |
| Vacuum                |       ![done]       |       ![done]       | Remove unused files and log entries   |
| Delete - partitions   |                     |       ![done]       | Delete a table partition              |
| Delete - predicates   |       ![done]       |                     | Delete data based on a predicate      |
| Optimize - compaction |       ![done]       |       ![done]       | Harmonize the size of data file       |
| Optimize - Z-order    |       ![done]       |                     | Place similar data into the same file |
| Merge                 | [![open]][merge-rs] | [![open]][merge-py] |                                       |
| FS check              |       ![done]       |                     | Remove corrupted files from table     |

## Protocol Support

| Writer Version | Requirement                                   |        Status        |
| -------------- | --------------------------------------------- | :------------------: |
| Version 2      | Append Only Tables                            |  [![open]][roadmap]  |
| Version 2      | Column Invatiants                             |       ![done]        |
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
