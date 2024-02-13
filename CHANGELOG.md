# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]
### Changed
- Updated Rust toolchain and minor dependencies

## [0.7.2] - 2024-02-08
### Added
- Building `arm64` version of the image to support Apple M-series silicon without QEMU overhead 

## [0.7.1] - 2024-01-15
### Changed
- Reverted to use `int32` and `int64` for `op` and `offset` respectively to preserve compatibility with Spark engine until Spark's Parquet version is upgraded.

## [0.7.0] - 2024-01-10
### Changed
- Migration to ODF changelog schema

## [0.6.0] - 2024-01-03
### Changed
- Upgraded to new ODF engine protocol

## [0.5.0] - 2024-01-03
### Changed
- Upgraded to `datafusion v34`
- Upgraded to new ODF schemas

## [0.4.0] - 2023-08-27
### Changed
- Upgraded to `datafusion v30.0.0`
- Enabled compression and using better encoding for Parquet files

## [0.3.0] - 2023-08-24
### Changed
- All timestamps will be coerced into `Timestamp(MILLIS,true)` for better Flink compatibility
### Fixed
- Watermark computation bug

## [0.2.0] - 2023-07-24
### Changed
- Upgraded to latest `datafusion` (fixes several known issues)

## [0.1.2] - 2023-05-26
### Changed
- Improved error handling
- Using patched `arrow` / `parquet` version that reads `TIMESTAMP_MILLIS` type as UTC.

## [0.1.1] - 2023-05-25
### Changed
- Writing `offset` as `Int64` type instead of `UInt64` for compatibility with other engines.

## [0.1.0] - 2023-05-25
### Added
- Initial engine prototype
- Keeping a CHANGELOG
