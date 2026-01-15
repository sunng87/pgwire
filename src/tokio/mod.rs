#[cfg(feature = "client-api")]
pub mod client;

#[cfg(feature = "server-api")]
pub mod server;

#[cfg(feature = "server-api")]
pub use server::process_socket;

#[cfg(all(feature = "server-api", unix))]
pub use server::process_socket_unix;

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub use tokio_rustls;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub type TlsAcceptor = tokio_rustls::TlsAcceptor;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub type TlsConnector = tokio_rustls::TlsConnector;

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub(super) const POSTGRESQL_ALPN_NAME: &[u8] = b"postgresql";

#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub enum TlsAcceptor {}
#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub enum TlsConnector {}
