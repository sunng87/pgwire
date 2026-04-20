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
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub use tokio_rustls;
/// TLS acceptor type for incoming connections.
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub type TlsAcceptor = tokio_rustls::TlsAcceptor;
/// TLS connector type for outgoing connections.
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub type TlsConnector = tokio_rustls::TlsConnector;

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub(super) const POSTGRESQL_ALPN_NAME: &[u8] = b"postgresql";

/// Placeholder TLS acceptor when no TLS backend is enabled.
#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub enum TlsAcceptor {}
/// Placeholder TLS connector when no TLS backend is enabled.
#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub enum TlsConnector {}
