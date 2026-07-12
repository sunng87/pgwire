//! # pgwire
//!
//! `pgwire` implements the PostgreSQL wire protocol as a library for building
//! PostgreSQL-compatible servers and clients. It's like
//! [hyper](https://github.com/hyperium/hyper/), but for the postgres wire
//! protocol. For general-purpose Postgres client use cases such as
//! application development, the [`rust-postgres`](https://crates.io/crates/postgres)
//! driver is usually a better fit; the client API here is designed for building
//! things like Postgres proxies and protocol-level tooling. Note that the client
//! API is still incomplete and in an early phase of development, so its design
//! may change between releases.
//!
//! ## About the Postgres Wire Protocol
//!
//! The Postgres Wire Protocol is a general-purpose Layer-7 protocol with six
//! parts:
//!
//! - **Startup**: client-server handshake and authentication.
//! - **Simple Query**: the legacy text-based query protocol. Queries are
//!   provided as a string and the server is allowed to stream data in response.
//! - **Extended Query**: a sub-protocol that caches a parsed query server-side
//!   and reuses it with new parameters. The response part is identical to
//!   Simple Query.
//! - **Copy**: a sub-protocol to bulk-transfer data in and out of the server.
//! - **Replication**: physical streaming replication.
//! - **Logical Replication**: logical streaming replication.
//!
//! pgwire currently implements startup (including authentication), simple
//! query, extended query and copy, on top of protocol versions 3.0 and 3.2
//! (PostgreSQL 18). Streaming replication is not yet supported.
//!
//! Note that the wire protocol has no semantics about SQL: you can use any
//! query language, data format, or even natural language to interact with the
//! backend. Responses are always encoded as data rows, preceded by a field
//! description header describing each column's name, type and format.
//!
//! ## Components
//!
//! The protocol is split into several components, each with its own handler
//! trait:
//!
//! - **Startup & authentication** — [`StartupHandler`](api::auth::StartupHandler)
//! - **Simple query** — [`SimpleQueryHandler`](api::query::SimpleQueryHandler)
//! - **Extended query** — [`ExtendedQueryHandler`](api::query::ExtendedQueryHandler)
//! - **Copy** — [`CopyHandler`](api::copy::CopyHandler)
//! - **Cancellation** — [`CancelHandler`](api::cancel::CancelHandler)
//!
//! On the server side these are aggregated by the
//! [`PgWireServerHandlers`](api::PgWireServerHandlers) trait: implement it on a
//! single struct to serve a full connection.
//!
//! ## Layered API
//!
//! pgwire provides three layers of abstraction that you can compose your
//! application from:
//!
//! - **Protocol layer**: just use the message definitions and codecs in the
//!   [`messages`] module. This layer is always available, even with no features
//!   enabled.
//! - **Message handler layer**: implement the `on_` prefixed methods of the
//!   handler traits above. The default `on_` implementations take care of
//!   protocol bookkeeping and dispatch to the `do_` prefixed methods, which
//!   you implement with your own query/auth logic.
//! - **High-level API layer**: authentication via [`AuthSource`](api::auth::AuthSource)
//!   and the built-in mechanisms (cleartext, MD5, SCRAM-SHA-256, OAuth), server
//!   parameters via [`ServerParameterProvider`](api::auth::ServerParameterProvider),
//!   [`QueryParser`](api::stmt::QueryParser)/[`PortalStore`](api::store::PortalStore)
//!   for extended query, and [`ConnectionManager`](api::ConnectionManager) for
//!   query cancellation.
//!
//! ## Features
//!
//! Server API (default):
//!
//! - `server-api-aws-lc-rs` *(enabled by default)*: the full server-side API
//!   with `aws-lc-rs` as the TLS/crypto backend.
//! - `server-api-ring`: same as above but using `ring` as the crypto backend.
//!
//! Client API:
//!
//! - `client-api-aws-lc-rs` / `client-api-ring`: the client-side API for
//!   building proxies and protocol-level tooling, with the matching crypto
//!   backend.
//!
//! Data types:
//!
//! - `pg-ext-types` *(enabled by default)*: enables `chrono`, `rust_decimal`
//!   and `serde_json` type support. The finer-grained `pg-type-*` features
//!   (including `pg-type-postgis`) allow individual control.
//!
//! Other:
//!
//! - `simple-oidc-validator`: JWT/OIDC validation support for OAuth
//!   authentication.
//!
//! Turn off default features if you only need the protocol layer.
//!
//! ## Examples
//!
//! The [`examples/`](https://github.com/sunng87/pgwire/tree/master/examples)
//! directory demonstrates API usage on both the server and client sides,
//! including a sqlite-backed server, SCRAM and OAuth authentication, TLS,
//! transactions, cursors, COPY, cancellation and a basic client.
//!

#[macro_use]
extern crate derive_new;

/// handler layer and high-level API layer.
#[cfg(any(feature = "server-api", feature = "client-api"))]
pub mod api;
/// error types.
pub mod error;
/// the protocol layer.
pub mod messages;
/// server entry-point for tokio based application.
#[cfg(any(feature = "server-api", feature = "client-api"))]
pub mod tokio;
/// types and encoding related helper
#[cfg(any(feature = "server-api", feature = "client-api"))]
pub mod types;
