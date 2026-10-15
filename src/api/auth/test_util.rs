//! (test-only) An in-memory client for testing startup flows.
//!
//! Records every backend message sent to it so tests can assert on the wire
//! behavior of [`crate::api::auth::StartupHandler`] implementations.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Sink;

use crate::api::{ClientInfo, DefaultClient, PgWireConnectionState, SessionExtensions};
use crate::error::PgWireError;
use crate::messages::response::TransactionStatus;
use crate::messages::startup::{SecretKey, Startup};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion};

pub(crate) struct TestClient {
    inner: DefaultClient<()>,
    pub(crate) sent: Vec<PgWireBackendMessage>,
}

impl TestClient {
    pub(crate) fn new(user: Option<&str>) -> Self {
        let mut inner = DefaultClient::new("127.0.0.1:5432".parse::<SocketAddr>().unwrap(), false);
        if let Some(user) = user {
            inner.metadata.insert("user".to_owned(), user.to_owned());
        }
        Self {
            inner,
            sent: vec![],
        }
    }
}

impl ClientInfo for TestClient {
    fn socket_addr(&self) -> SocketAddr {
        self.inner.socket_addr()
    }
    fn is_secure(&self) -> bool {
        self.inner.is_secure()
    }
    fn protocol_version(&self) -> ProtocolVersion {
        self.inner.protocol_version()
    }
    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.inner.set_protocol_version(version)
    }
    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        self.inner.pid_and_secret_key()
    }
    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
        self.inner.set_pid_and_secret_key(pid, secret_key)
    }
    fn state(&self) -> PgWireConnectionState {
        self.inner.state()
    }
    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.inner.set_state(new_state)
    }
    fn transaction_status(&self) -> TransactionStatus {
        self.inner.transaction_status()
    }
    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.inner.set_transaction_status(new_status)
    }
    fn metadata(&self) -> &HashMap<String, String> {
        self.inner.metadata()
    }
    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        self.inner.metadata_mut()
    }
    fn session_extensions(&self) -> &SessionExtensions {
        self.inner.session_extensions()
    }
    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn sni_server_name(&self) -> Option<&str> {
        None
    }
    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn client_certificates<'a>(&self) -> Option<&[rustls_pki_types::CertificateDer<'a>]> {
        None
    }
}

impl Sink<PgWireBackendMessage> for TestClient {
    type Error = PgWireError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(self: Pin<&mut Self>, item: PgWireBackendMessage) -> Result<(), Self::Error> {
        self.get_mut().sent.push(item);
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

pub(crate) fn startup_message(user: &str) -> PgWireFrontendMessage {
    let mut startup = Startup::new();
    startup
        .parameters
        .insert("user".to_owned(), user.to_owned());
    startup
        .parameters
        .insert("database".to_owned(), "db".to_owned());
    PgWireFrontendMessage::Startup(startup)
}
