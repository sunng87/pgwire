//! APIs for building postgresql compatible servers.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

pub use postgres_types::Type;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use rustls_pki_types::CertificateDer;

use crate::error::PgWireError;
use crate::messages::ProtocolVersion;
use crate::messages::response::TransactionStatus;
use crate::messages::startup::SecretKey;

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

/// Per-connection typed extension store, keyed by `TypeId`.
///
/// Allows hooks and handlers to store arbitrary per-session state that is
/// automatically cleaned up when the connection closes. Uses interior
/// mutability so it can be accessed from `&dyn ClientInfo`.
#[derive(Default)]
pub struct SessionExtensions {
    map: RwLock<HashMap<TypeId, Arc<dyn Any + Send + Sync>>>,
}

impl std::fmt::Debug for SessionExtensions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let count = self.map.read().unwrap().len();
        f.debug_struct("SessionExtensions")
            .field("entries", &count)
            .finish()
    }
}

impl SessionExtensions {
    pub fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
        }
    }

    /// Get an extension by type, or insert a default value if not present.
    pub fn get_or_insert_with<T: Send + Sync + 'static>(&self, f: impl FnOnce() -> T) -> Arc<T> {
        let type_id = TypeId::of::<T>();

        // Fast path: read lock
        {
            let map = self.map.read().unwrap();
            if let Some(val) = map.get(&type_id) {
                return val.clone().downcast::<T>().unwrap();
            }
        }

        // Slow path: write lock
        let mut map = self.map.write().unwrap();
        let val = map.entry(type_id).or_insert_with(|| Arc::new(f()));
        val.clone().downcast::<T>().unwrap()
    }

    /// Get an extension by type, returning `None` if not present.
    pub fn get<T: Send + Sync + 'static>(&self) -> Option<Arc<T>> {
        let type_id = TypeId::of::<T>();
        let map = self.map.read().unwrap();
        map.get(&type_id)
            .map(|val| val.clone().downcast::<T>().unwrap())
    }

    /// Insert an extension by type, replacing any existing value.
    pub fn insert<T: Send + Sync + 'static>(&self, val: T) -> Option<Arc<T>> {
        let type_id = TypeId::of::<T>();
        let mut map = self.map.write().unwrap();
        map.insert(type_id, Arc::new(val))
            .map(|old| old.downcast::<T>().unwrap())
    }
}

// TODO: add oauth scope and issuer
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

    fn session_extensions(&self) -> &SessionExtensions;

    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn sni_server_name(&self) -> Option<&str>;

    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn client_certificates<'a>(&self) -> Option<&[CertificateDer<'a>]>;
}

/// Client Portal Store
pub trait ClientPortalStore {
    type PortalStore;

    fn portal_store(&self) -> &Self::PortalStore;
}

pub const METADATA_USER: &str = "user";
pub const METADATA_DATABASE: &str = "database";
pub const METADATA_CLIENT_ENCODING: &str = "client_encoding";
pub const METADATA_APPLICATION_NAME: &str = "application_name";

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
    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    pub sni_server_name: Option<String>,
    pub portal_store: store::MemPortalStore<S>,
    pub session_extensions: SessionExtensions,
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

    fn session_extensions(&self) -> &SessionExtensions {
        &self.session_extensions
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.transaction_status = new_status
    }

    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn sni_server_name(&self) -> Option<&str> {
        self.sni_server_name.as_deref()
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
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            sni_server_name: None,
            portal_store: store::MemPortalStore::new(),
            session_extensions: SessionExtensions::new(),
        }
    }
}

impl<S> ClientPortalStore for DefaultClient<S> {
    type PortalStore = store::MemPortalStore<S>;

    fn portal_store(&self) -> &Self::PortalStore {
        &self.portal_store
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::RwLock;

    #[test]
    fn session_extensions_get_or_insert_with() {
        let ext = SessionExtensions::new();
        let val: Arc<RwLock<Vec<i32>>> = ext.get_or_insert_with(|| RwLock::new(vec![1, 2, 3]));
        assert_eq!(*val.read().unwrap(), vec![1, 2, 3]);

        // Second call returns same instance, ignores closure
        let val2: Arc<RwLock<Vec<i32>>> = ext.get_or_insert_with(|| RwLock::new(vec![99]));
        assert_eq!(*val2.read().unwrap(), vec![1, 2, 3]);
        assert!(Arc::ptr_eq(&val, &val2));
    }

    #[test]
    fn session_extensions_different_types_are_independent() {
        let ext = SessionExtensions::new();
        ext.insert(42u32);
        ext.insert("hello".to_string());

        assert_eq!(*ext.get::<u32>().unwrap(), 42);
        assert_eq!(*ext.get::<String>().unwrap(), "hello");
    }

    #[test]
    fn session_extensions_get_returns_none_when_missing() {
        let ext = SessionExtensions::new();
        assert!(ext.get::<u64>().is_none());
    }

    #[test]
    fn session_extensions_insert_replaces_previous() {
        let ext = SessionExtensions::new();
        ext.insert(1u32);
        let old = ext.insert(2u32);
        assert_eq!(*old.unwrap(), 1);
        assert_eq!(*ext.get::<u32>().unwrap(), 2);
    }
}
