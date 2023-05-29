# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
