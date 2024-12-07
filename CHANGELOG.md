# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic
Versioning](https://semver.org/spec/v2.0.0.html).

## [0.28.0] - 2024-12-07

### Added

- `ErrorHandler` for centralized error processing [#222]

### Changed

- `PgWireHandlerFactory` is renamed to `PgWireServerHandlers` to avoid
  confusion. This trait doesn't require generating new instance for each
  call. [#226]

### Fixed

- Text encoding for bytea type [#224]

## [0.27.0] - 2024-11-30

### Added

- `on_flush` handler for extended query handler [#220]

### Fixed

- `Parse` message encoding [#219]

### Changed

- MSRV to 1.75
- Made TLS an optional feature. `server-api` now provides no TLS functionality
  [#218]


## [0.26.0] - 2024-11-02

### Added

- New `Response` type for transforming transaction state: `TransactionStart` and
  `TransactionEnd`. [#200]

### Fixed

- oid output for tags [#211]
- array type serialization [#212]

### Changed

- Update MSRV to 1.74
- Breaking: Rewrite `NoopStartupHandler` to be a trait. You can provide an empty
  implementation to keep original behaviour. And it's now possible to customize
  some behaviour by adding a `post_startup` implementation.

## [0.25.0] - 2024-09-28

### Fixed

- Dead-loop when client connects without send packets or sending insufficient
  bytes [#206], [#207]

### Changes

- Removed unneeded `Arc` for handler factory [#202]
- `PgWireFrontendMessage::SslRequest` now carries an optional `SslRequest`
  message where `None` indicates the client doesn't issue `SslRequest` and
  starts directly. [#207]
- Add `AwaitingSslRequest` state. [#207]

## [0.24.2] - 2024-09-14

### Fixed

- Array encoding issues for date and time types [#194]
- String array quote and escape [#198]

## [0.24.1] - 2024-08-06

### Fixed

- Codec error when decoding authentication messages [#190]
- Codec error when decoding parameters larger than 32767 [#192]
- Panic when client sends invalid authentication message type [#191]

## [0.24.0] - 2024-08-03

### Added

- `ToSqlText` support for `Decimal` [#186]
- Direct SSL startup, a new feature introduced in PostgreSQL 17. It allows
  ssl handshake once tcp connected, which saves a roundtrip on secure connection
  startup. [#189]

### Changed

- Improvement our API for COPY. [#185]
  - Added `CopyInProgress` for `PgWireConnectionState` to track connection in
    COPY
  - Changed `on_copy_fail` in `CopyHandler` to return an `Error` as Postgres
    document specified.
  - Corrected behaviour for error handling when COPY is initialized via extended
    subprotocol.

## [0.23.0] - 2024-06-14

### Added

- New `CopyHandler` API for processing postgres `COPY`-in operations.

### Changed

- Updated the API of `pgwire::tokio::process_socket` to accept new
  `PgWireHandlerFactory` instead of each subprotocol handler individually, to
  keep API clean, also reduce some of boilerplate code.
- `MakeHandler` trait and its implementation are removed. You will need to
  implement those logic with new `PgWireHandlerFactory`. For stateful handlers,
  like most `ExtenededQueryHandler` implementations, it need to be created for
  every call to `extended_query_handler`. For stateless ones, it can be cached.

### Fixed

- Include API usage on random nonce generate [#182]


## [0.22.0] - 2024-04-29

### Changed

- SCRAM authenticator is now an optional feature that is turned off by
  default. To use SCRAM authenticator, add feature flag `scram`. [#180]
- `aws-lc-rs` based crypto backend is the default when using TLS or SCRAM. Use
  `default-features=false` and `server-api-ring` to use the ring backend. [#179]
- Use `default-features=false` if you only uses message codecs from this
  library. [#177]
- BREAKING CHANGE: ready state code is now presented in enum
  `TransactionStatus`. There is an update of `ReadyForQuery`'s `new`. [#176]

### Fixed

- panic when decoding SASL messages [#175]

## [0.21.0] - 2024-04-18

### Added

- You can now customize command tag for `QueryResponse`. [#173]

### Fixed

- Text representation of NULL values in array, and bool values are now fixed to
  align with postgres. [#174]

## [0.20.0] - 2024-03-17

### Changed

- Changed `do_describe` API from `ExtendedQueryHandler` to more specific
  `do_descirbe_statement` and `do_describe_portal`. This is a breaking change,
  see sqlite example for how to migrate. [#164]
- Improved performance of `DataRowEncoder`, updated how we store data in
  `DataRow`. Breaking change for message layer users. [#165] [#166]

## [0.19.2] - 2024-01-31

### Fixed

- Error on `Parse` command when using pgcli client [#152]
- Remove `'static` bound for handler implementations [#149]

## [0.19.1] - 2024-01-14

### Fixed

- Simple query client blocks on error [#148]

## [0.19.0] - 2024-01-08

### Changed

- Updated `Tag` constructors for including ObjectID. [#147]
- Removed some getters and setters, use direct field access. [#144]

### Fixed

- Message sequences fixes [#145] [#146]

## [0.18.0] - 2023-12-23

### Changed

- Associated `PortalStore` to client as how PostgreSQL is designed. This is a
  regression design issue introduced in `0.8`.

## [0.17.0] - 2023-12-07

### Added

- Added `SslRequest` and `SslResponse` message [#116] [#117]
- Added `NotificationResponse` message for Postgres `LISTEN/NOTIFY` feature
  [#136]
- Added encoding support for array data type [#130]

### Changed

- Allow sending custom message from `SimpleQueryHandler` by changing `Client`
  reference to mutable [#133]
- Updated tokio-rustls to 0.25 [#135]
- Using `feed` for sending response [#128]

## [0.16.1] - 2023-09-28

### Fixed

- Fixed message decode for `flush` [#113]
- Add a fix for potential panic on `startup`. [#112]

## [0.16.0] - 2023-08-04

### Changed

- Changed `Portal::parameter` function to require a `&Type` argument for
  deserialization. Previously we relies on client specified types but it doesn't
  work when the client driver requires type inference on server-side. The new
  function signature allows you to provide an type. [#106]
- Changed `DefaultServerParameterProvider` API to allow modification of built-in
  parameters. [#97]

## [0.15.0] - 2023-06-23

### Added

- Codecs for `Copy` messages. [#91]

### Changed

- Made `parse_sql` from `QueryParser` an async function because some
  implementation requires async operation [#96]

## [0.14.1] - 2023-06-01

### Fixed

- Corrected how server handler responds `Sync` message. Fixed compatibility with
  rust sqlx client. #90

## [0.14.0] - 2023-05-04

### Added

- Exposed `send_describe_response` and `send_execution_response` as helper
  functions for custom `ExtendedQueryHandler` implementation.

### Changed

- `tcp_nodelay` is turned on by default within pgwire for performance
  consideration.
- Changed getters of `QueryResponse` to return owned data.

## [0.13.1] - 2023-04-30

### Added

- A new helper function `send_query_response` is added for convenience of custom
  query handler implementation

## [0.13.0] - 2023-04-08

### Added

- Message `NoData` that sends from backend when `Describe` sent on an empty
  query.
- Add `EmptyQuery` to `Response` enum to represent response for empty query.
- Add `no_data` constructor to `DescribeResponse` for empty query.

### Changed

- Improved empty query check for `SimpleQueryHandler`. #75

## [0.12.0] - 2023-03-26

### Added

- `ToSqlText` trait and default implementation for some types. This trait is
  similar to `ToSql` from `postgres-types` package. It provide text format
  encoding while `ToSql` are binary by default.

### Changed

- Updated `DataRowEncoder` encode API with unified `encode_field` for both
  binary and text encoding. `DataRowEncoder::new` now accepts
  `Arc<Vec<FieldInfo>>` instead of column count. The encoder now has type and
  format information for each column. `encode_field` no longer requires `Type`
  and `FieldFormat`. A new `encode_field_with_type_and_format` is provided for
  custom use-case.
- Updated `do_describe` API from `ExtendedQueryHandler` to include full
  information about requested `Statement` or `Portal`.
- `query_response` function is replaced by `QueryResponse::new`

## [0.11.1] - 2023-02-26

### Fixed

- lifetime binding in `on_query` functions if the implementation has its own
  lifetime specifier

## [0.11.0] - 2023-02-26

Further improve extended query support. Now the server can respond statement
describe correctly.

### Added

- Add some docs.
- Add `integer_datetimes` parameter on startup so clients like jdbc will parse
  time types as integer.
- `DescribeResponse` that contains information for both `ParameterDescription`
  and `RowDescription`.

### Changed

- Update `do_describe` of `ExtendedQueryHandler`. Add new bool argument
  `inference_parameters` to check if parameter types from statement is required
  to return.
- Updated resultset `Response::QueryResponse` lifetime from `'static` to portal
  or query string, this allows reference of portal in stream.

### Fixed

- The default implementation of `ExtendedQueryHandler` now correctly responds
  `Close` message with `CloseComplete`
- Correct SCRAM mechanism name in plain connection

## [0.10.0] - 2023-02-08

This release reverts design to use `MakeHandler` in `process_socket`, since it
stops shared implementation between `StartupHandler` and query handlers.

### Changed

- Update `process_socket` to accept `StartupHandler`, `SimpleQueryHandler` and
  `ExtendedQueryHandler`. `MakeHandler` should be called upon new socket
  incoming. Check our updated examples for usage.
- Removed `Password`, `SASLInitialResponse` and `SASLResponse` from frontend
  message enum to avoid confusion. `PasswordMessageFamily` is now an enum that
  contains all these message forms and a raw form. This is because in postgres
  protocol, password message can only be decoded into concrete type when there
  is a context. See our MD5 and SCRAM authenticator for usage.

## [0.9.1] - 2023-02-02

Fixes compatibility with rust-postgres client's prepared statement
implementation. It sends Parse-Describe-Sync on `Client::prepare_typed`.

### Fixed

- `Parse` and `Bind` completion response is now fixed
- Responding statement describe request with parameter description, and ready
  for query

## [0.9.0] - 2023-02-01

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
