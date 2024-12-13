mod server;

pub use server::process_socket;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub use tokio_rustls;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub type TlsAcceptor = tokio_rustls::TlsAcceptor;

#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub enum TlsAcceptor {}
