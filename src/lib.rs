//! # pgwire
//!
//! `pgwire` provides the PostgreSQL wire protocol as a library for
//! implementing PostgreSQL-compatible servers and clients.
//! [`rust-postgres`](https://crates.io/crates/postgres) will be sufficient
//! for most Postgres client use-cases, so this library focuses on
//! server development.
//!
//! ## About Postgres Wire Protocol
//!
//! Postgres Wire Protocol is a relatively general-purpose Layer-7
//! protocol. There are 3 parts of the protocol:
//!
//! - Startup: client-server handshake and authentication.
//! - Simple Query: The legacy query protocol of postgresql. Query are provided
//!   as string, and server is allowed to stream data in response.
//! - Extended Query: A new sub-protocol for query which has ability to cache
//!   the query on server-side and reuse it with new parameters. The response part
//!   is identical to Simple Query.
//!
//! Also note that Postgres Wire Protocol has no semantics about SQL, so
//! literally you can use any query language, data formats or even natural
//! language to interact with the backend.
//!
//! The response are always encoded as data row format. And there is a field
//! description as header of the data to describe its name, type and format.
//!
//! ## Components
//!
//! There are two main components in postgresql wire protocol: **startup** and
//! **query**. In **query**, there are two subprotocols: the legacy text-based
//! **simple query** and binary **extended query**.
//!
//! ## Layered API
//!
//! pgwire provides three layers of abstractions that allows you to compose your
//! application from any level of abstraction. They are:
//!
//! - Protocol layer: Just use message definitions and codecs in `messages`
//!   module.
//! - Message handler layer: Implement `on_` prefixed methods in traits:
//!   - `StartupHandler`
//!   - `SimpleQueryHandler`
//!   - `ExtendedQueryHandler`
//! - High-level API layer
//!   - `AuthSource` and various authentication mechanisms
//!   - `do_` prefixed methods in handler traits
//!   - `QueryParser`/`PortalStore` for extended query support
//!
//! ## Features
//!
//! - `server-api-aws-lc-rs` is enabled by default, it includes all three layers
//!   of our API and uses `aws-lc-rs` as crypto backend.
//! - `server-api-ring` is almost same to `server-api-aws-lc-rs` except for it's
//!   using `ring` as crypto backend.
//! - `scram` for the SASL/SCRAM authenticator.
//! - Turn off default features if you just use our Protocol layer.
//!
//! ## Examples
//!
//! [Examples](https://github.com/sunng87/pgwire) are provided to demo API
//! usages.
//!

#[macro_use]
extern crate derive_new;

/// handler layer and high-level API layer.
#[cfg(feature = "server-api")]
pub mod api;
/// error types.
pub mod error;
/// the protocol layer.
pub mod messages;
/// server entry-point for tokio based application.
#[cfg(feature = "server-api")]
pub mod tokio;
/// types and encoding related helper
#[cfg(feature = "server-api")]
pub mod types;
