<!-- markdownlint-disable MD012 MD013 MD024 MD033 -->
# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/) and this project adheres to [Semantic Versioning](https://semver.org/)

## [0.6.0] 2026-02-23

### Changed

- move to github
- update template

### Fixed

- insecure dependencies


## [0.5.1] 2025-11-07

### Fixed

- add a sleep between call to avoid API overload
- fix a bug were the repos are not in the correct fetched


## [0.5.0] 2025-11-03

### Added

- initial version of the Search Logs task
  - Service URL, Account and token parameter for authn
  - Query, Time Range, Limit and Repositories parameter for limiting search scope and result
  - List of output paths paramter to specify the output schema

