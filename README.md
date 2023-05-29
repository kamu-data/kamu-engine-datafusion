# Apache Arrow DataFusion Engine for Open Data Fabric
This the implementation of the `Engine` contract of [Open Data Fabric](http://opendatafabric.org/) using the [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) data processing framework. It is currently in use in [kamu-cli](https://github.com/kamu-data/kamu-cli) data management tool.

This is implementation is in a **very early prototype phase** and not ready for use.


## Features
TBD


## Known Issues
- [Not following the spec for TIMESTAMP_MILLIS legacy converted types](https://github.com/apache/arrow-rs/issues/4308)
  - Using patched version
- [UNION ALL schema harmonization failure in subquery/view](https://github.com/apache/arrow-datafusion/issues/6463)


## Developing
This is a Rust-based project. You can follow similar steps as in [`kamu-cli` development guide](https://github.com/kamu-data/kamu-cli/blob/master/DEVELOPER.md).
