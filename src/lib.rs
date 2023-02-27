//! # pgwire
//!
//! pgwire is library that implements postgresql wire protocol and allows to
//! write postgresql protocol compatible servers and clients. Of course in most
//! cases you don't need a custom client, `postgres-rust` will be work for
//! postgresql comptiable servers. So at the moment, this library focus on
//! server development.
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
//! module.
//! - Message handler layer: Implement `on_` prefixed methods in traits:
//!   - `StartupHandler`
//!   - `SimpleQueryHandler`
//!   - `ExtendedQueryHandler`
//! - High-level API layer
//!   - `AuthSource` and various authentication mechanisms
//!   - `do_` prefixed methods in handler traits
//!   - `QueryParser`/`PortalStore` for extended query support
//!
//! ## Examples
//!
//! [Examples](https://github.com/sunng87/pgwire) are provided to demo API
//! usages.
//!

#[macro_use]
extern crate getset;
#[macro_use]
extern crate derive_new;

/// handler layer and high-level API layer.
pub mod api;
/// error types.
pub mod error;
/// the protocol layer.
pub mod messages;
/// server entry-point for tokio based application.
#[cfg(feature = "tokio_support")]
pub mod tokio;
/// types and encoding related helper
pub mod types;
