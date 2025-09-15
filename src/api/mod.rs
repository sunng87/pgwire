//! APIs for building postgresql compatible servers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub use postgres_types::Type;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use rustls_pki_types::CertificateDer;

use crate::error::PgWireError;
use crate::messages::response::TransactionStatus;
use crate::messages::startup::SecretKey;
use crate::messages::ProtocolVersion;

pub mod auth;
pub mod cancel;
#[cfg(feature = "client-api")]
pub mod client;
pub mod copy;
pub mod portal;
pub mod query;
pub mod results;
pub mod stmt;
pub mod store;
pub mod transaction;

pub const DEFAULT_NAME: &str = "POSTGRESQL_DEFAULT_NAME";

#[derive(Debug, Clone, Copy, Default)]
pub enum PgWireConnectionState {
    #[default]
    AwaitingSslRequest,
    AwaitingStartup,
    AuthenticationInProgress,
    ReadyForQuery,
    QueryInProgress,
    CopyInProgress(bool),
    AwaitingSync,
}

/// Describe a client information holder
pub trait ClientInfo {
    fn socket_addr(&self) -> SocketAddr;

    fn is_secure(&self) -> bool;

    fn protocol_version(&self) -> ProtocolVersion;

    fn set_protocol_version(&mut self, version: ProtocolVersion);

    fn pid_and_secret_key(&self) -> (i32, SecretKey);

    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey);

    fn state(&self) -> PgWireConnectionState;

    fn set_state(&mut self, new_state: PgWireConnectionState);

    fn transaction_status(&self) -> TransactionStatus;

    fn set_transaction_status(&mut self, new_status: TransactionStatus);

    fn metadata(&self) -> &HashMap<String, String>;

    fn metadata_mut(&mut self) -> &mut HashMap<String, String>;

    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn client_certificates<'a>(&self) -> Option<&[CertificateDer<'a>]>;
}

/// Client Portal Store
pub trait ClientPortalStore {
    type PortalStore;
    type PortalSuspendedResult;

    fn portal_store(&self) -> &Self::PortalStore;

    fn portal_suspended_result(&self) -> &Self::PortalSuspendedResult;
}

pub const METADATA_USER: &str = "user";
pub const METADATA_DATABASE: &str = "database";

#[non_exhaustive]
#[derive(Debug)]
pub struct DefaultClient<S> {
    pub socket_addr: SocketAddr,
    pub is_secure: bool,
    pub protocol_version: ProtocolVersion,
    pub pid_secret_key: (i32, SecretKey),
    pub state: PgWireConnectionState,
    pub transaction_status: TransactionStatus,
    pub metadata: HashMap<String, String>,
    pub portal_store: store::MemPortalStore<S>,
    pub suspended_portal_results: store::MemPortalSuspendedResult,
}

impl<S> ClientInfo for DefaultClient<S> {
    fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    fn is_secure(&self) -> bool {
        self.is_secure
    }

    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        self.pid_secret_key.clone()
    }

    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
        self.pid_secret_key = (pid, secret_key);
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.protocol_version
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.protocol_version = version;
    }

    fn state(&self) -> PgWireConnectionState {
        self.state
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.state = new_state;
    }

    fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    fn metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.metadata
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.transaction_status = new_status
    }

    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn client_certificates<'a>(&self) -> Option<&[CertificateDer<'a>]> {
        None
    }
}

impl<S> DefaultClient<S> {
    pub fn new(socket_addr: SocketAddr, is_secure: bool) -> DefaultClient<S> {
        DefaultClient {
            socket_addr,
            is_secure,
            protocol_version: ProtocolVersion::default(),
            pid_secret_key: (0, SecretKey::default()),
            state: PgWireConnectionState::default(),
            transaction_status: TransactionStatus::Idle,
            metadata: HashMap::new(),
            portal_store: store::MemPortalStore::new(),
            suspended_portal_results: store::MemPortalSuspendedResult::new(),
        }
    }
}

impl<S> ClientPortalStore for DefaultClient<S> {
    type PortalStore = store::MemPortalStore<S>;
    type PortalSuspendedResult = store::MemPortalSuspendedResult;

    fn portal_store(&self) -> &Self::PortalStore {
        &self.portal_store
    }

    fn portal_suspended_result(&self) -> &Self::PortalSuspendedResult {
        &self.suspended_portal_results
    }
}

/// A centralized handler for all errors
///
/// This handler captures all errors produces by authentication, query and
/// copy. You can do logging, filtering or masking the error before it sent to
/// client.
pub trait ErrorHandler: Send + Sync {
    fn on_error<C>(&self, _client: &C, _error: &mut PgWireError)
    where
        C: ClientInfo,
    {
    }
}

/// A noop implementation for `ErrorHandler`.
#[derive(Debug)]
pub struct NoopHandler;

impl ErrorHandler for NoopHandler {}

pub trait PgWireServerHandlers {
    fn simple_query_handler(&self) -> Arc<impl query::SimpleQueryHandler> {
        Arc::new(NoopHandler)
    }

    fn extended_query_handler(&self) -> Arc<impl query::ExtendedQueryHandler> {
        Arc::new(NoopHandler)
    }

    fn startup_handler(&self) -> Arc<impl auth::StartupHandler> {
        Arc::new(NoopHandler)
    }

    fn copy_handler(&self) -> Arc<impl copy::CopyHandler> {
        Arc::new(NoopHandler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(NoopHandler)
    }

    fn cancel_handler(&self) -> Arc<impl cancel::CancelHandler> {
        Arc::new(NoopHandler)
    }
}

impl<T> PgWireServerHandlers for Arc<T>
where
    T: PgWireServerHandlers,
{
    fn simple_query_handler(&self) -> Arc<impl query::SimpleQueryHandler> {
        (**self).simple_query_handler()
    }

    fn extended_query_handler(&self) -> Arc<impl query::ExtendedQueryHandler> {
        (**self).extended_query_handler()
    }

    fn startup_handler(&self) -> Arc<impl auth::StartupHandler> {
        (**self).startup_handler()
    }

    fn copy_handler(&self) -> Arc<impl copy::CopyHandler> {
        (**self).copy_handler()
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        (**self).error_handler()
    }

    fn cancel_handler(&self) -> Arc<impl cancel::CancelHandler> {
        (**self).cancel_handler()
    }
}
