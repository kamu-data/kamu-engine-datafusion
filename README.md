# Open Data Fabric: Apache Arrow DataFusion Engine

<div align="center">

[![Release](https://img.shields.io/github/v/release/kamu-data/kamu-engine-datafusion?include_prereleases&logo=rust&logoColor=orange&style=for-the-badge)](https://github.com/kamu-data/kamu-engine-datafusion/releases/latest)
[![CI](https://img.shields.io/github/actions/workflow/status/kamu-data/kamu-engine-datafusion/build.yaml?logo=githubactions&label=CI&logoColor=white&style=for-the-badge&branch=master)](https://github.com/kamu-data/kamu-engine-datafusion/actions)
[![Dependencies](https://deps.rs/repo/github/kamu-data/kamu-engine-datafusion/status.svg?&style=for-the-badge)](https://deps.rs/repo/github/kamu-data/kamu-engine-datafusion)
[![Chat](https://shields.io/discord/898726370199359498?style=for-the-badge&logo=discord&label=Discord)](https://discord.gg/nU6TXRQNXC)

</div>

This the implementation of the `Engine` contract of [Open Data Fabric](http://opendatafabric.org/) using the [Apache Arrow DataFusion](https://github.com/apache/arrow-datafusion) data processing framework. It is currently in use in [kamu-cli](https://github.com/kamu-data/kamu-cli) data management tool.


## Features
This engine is experimental and has limited functionality due to being **batch-oriented**, but is extremely fast and low-footprint. There are [ongoing attempts](https://github.com/apache/arrow-datafusion/issues/4285) to add stream processing functionality.

We recommend using this engine only for **basic filter/map operations** that do not require temporal processing. If you need temporal JOINs, aggregations, windowing, and watermark semantics - take a look at [Apache Flink ODF Engine](https://github.com/kamu-data/kamu-engine-flink).

Also note that this engine does not automatically handle **retractions and corrections**. If you perform map/filter operations on the stream that can contain retractions and corrections - make sure to manually propagate the `op` column. If output does not contain an `op` column - all emitted records will be considered as appends.

More information and engine comparisons are [available here](https://docs.kamu.dev/cli/supported-engines/).


## Developing
This is a Rust-based project. You can follow similar steps as in [`kamu-cli` development guide](https://github.com/kamu-data/kamu-cli/blob/master/DEVELOPER.md).
