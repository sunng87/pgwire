# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased] - ReleaseDate

### Changed

- Updated `QueryParser` API to provide parameter types for implementation.
- Updated `Portal` API, it now holds `Arc` reference to related
  `StoredStatement`.
- Updated `ExtendedQueryHandler::do_describe` arguments, it now takes a borrowed
  `StoredStatement`. Compared to previews API, parameter types are provided as
  well.
- Renamed `StoredStatement::type_oids()` to `parameter_types()`

## [0.8.0] - 2023-01-29

### Changed

- `ExtendedQueryHandler` trait now has two new component: `PortalStore` and
  `QueryParser`. `PortalStore` replaced statement and portal cache on
  `ClientInfo`, these data will now be cached with `PortalStore` from
  `ExtendedQueryHandler` implementation.
- A default `PortalStore` implementation, `MemPortalStore`, is provided for most
  usecase.
- `Statement` API now renamed to `StoredStatement`. The new API holds parsed
  statement from `QueryParser` implementation.
- `Portal` API, like it's statement counterpart, no longer stores statement as
  string, it shares same statement type with `StoredStatement`, `QueryParser`
  and `ExtendedQueryHandler`.
- `PasswordVerifier` and `AuthDB` are now merged into `AuthSource` API. The new
  API asks developer to fetch password and an optional salt. The password can
  either be hashed or cleattext, depends on which authentication mechanism is
  used.

### Added

- `SCRAM-SHA-256-PLUS` authentication is supported when you provide server
  certificate.
- `do_describe` is added to `ExtendedQueryHandler`, now it's possible to
  `Describe` a statement or portal without execution. The implementation is also
  required to be capable with this.
- `DefaultServerParameterProvider` implementation is provided to include minimal
  parameters for JDBC and psycopg2 working.
- `PlaceholderExtendedQueryHandler` to simplify example and demo when extended
  query is not supported.
- `Flush` message added. @penberg
