#[cfg(feature = "client-api")]
pub mod client;

#[cfg(feature = "server-api")]
pub mod server;

/// Re-export of the TCP socket processing entrypoint.
#[cfg(feature = "server-api")]
pub use server::process_socket;

/// Re-export of the Unix domain socket processing entrypoint.
#[cfg(all(feature = "server-api", unix))]
pub use server::process_socket_unix;

/// Re-export of `tokio_rustls` crate for TLS support.
pub use tokio_rustls;
/// TLS acceptor type for incoming connections.
pub type TlsAcceptor = tokio_rustls::TlsAcceptor;
/// TLS connector type for outgoing connections.
pub type TlsConnector = tokio_rustls::TlsConnector;

pub(super) const POSTGRESQL_ALPN_NAME: &[u8] = b"postgresql";
