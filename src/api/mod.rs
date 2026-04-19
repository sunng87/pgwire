//! APIs for building postgresql compatible servers.

use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, RwLock};

use bytes::Bytes;
use futures::channel::oneshot;
use futures::lock::Mutex;
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

/// Generator for process ID and secret key pairs used to identify connections
/// for cancel requests.
///
/// Implement this trait to customize how PID and secret key values are
/// produced. The default implementation [`RandomPidSecretKeyGenerator`]
/// generates random values.
pub trait PidSecretKeyGenerator: Send + Sync {
    fn generate(&self, client: &dyn ClientInfo) -> (i32, SecretKey);
}

/// Default implementation of [`PidSecretKeyGenerator`] that produces random
/// values using the `rand` crate.
///
/// For protocol 3.0, generates a 4-byte `SecretKey::I32`. For protocol 3.2,
/// generates a 32-byte `SecretKey::Bytes`.
#[derive(Debug, Default)]
pub struct RandomPidSecretKeyGenerator;

impl PidSecretKeyGenerator for RandomPidSecretKeyGenerator {
    fn generate(&self, client: &dyn ClientInfo) -> (i32, SecretKey) {
        let pid = rand::random::<u32>() as i32;
        let secret_key = match client.protocol_version() {
            ProtocolVersion::PROTOCOL3_0 => SecretKey::I32(rand::random::<i32>()),
            ProtocolVersion::PROTOCOL3_2 => {
                let mut bytes = vec![0u8; 32];
                rand::fill(&mut bytes);
                SecretKey::Bytes(bytes.into())
            }
        };
        (pid, secret_key)
    }
}

/// Per-connection cancel handle.
///
/// Stores a replaceable `oneshot::Sender` that is swapped out before each
/// query via [`ConnectionHandle::start_query`]. When a cancel request arrives,
/// [`ConnectionHandle::cancel`] fires the current sender.
pub struct ConnectionHandle {
    cancel_tx: Mutex<Option<oneshot::Sender<()>>>,
}

impl std::fmt::Debug for ConnectionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionHandle").finish()
    }
}

impl Default for ConnectionHandle {
    fn default() -> Self {
        Self::new()
    }
}

impl ConnectionHandle {
    pub fn new() -> Self {
        Self {
            cancel_tx: Mutex::new(None),
        }
    }

    /// Install a fresh cancel sender and return the receiver.
    ///
    /// Call this before each query. The previous sender (if any) is dropped,
    /// causing its paired receiver to resolve with `Err(Canceled)`.
    pub async fn start_query(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        *self.cancel_tx.lock().await = Some(tx);
        rx
    }

    /// Fire the current cancel sender.
    ///
    /// Returns `true` if there was an active query to cancel, `false` if no
    /// query was in progress.
    pub async fn cancel(&self) -> bool {
        if let Some(tx) = self.cancel_tx.lock().await.take() {
            let _ = tx.send(());
            true
        } else {
            false
        }
    }
}

/// RAII guard that unregisters a connection from the [`ConnectionManager`] on
/// drop.
pub struct ConnectionGuard {
    pid: i32,
    secret_key: SecretKey,
    manager: Arc<ConnectionManager>,
}

impl std::fmt::Debug for ConnectionGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionGuard")
            .field("pid", &self.pid)
            .finish()
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.manager.unregister(self.pid, &self.secret_key);
    }
}

/// A registry mapping `(pid, secret_key)` to per-connection cancel handles.
///
/// Create one `ConnectionManager` at server startup, share it as
/// `Arc<ConnectionManager>` across all handler implementations.
///
/// ## Lifecycle
///
/// 1. In your `StartupHandler`, call [`register`](ConnectionManager::register)
///    to obtain an [`Arc<ConnectionHandle>`] and a [`ConnectionGuard`].
/// 2. Store both in [`SessionExtensions`] so they live as long as the
///    connection.
/// 3. Before each query, call
///    [`handle.start_query()`](ConnectionHandle::start_query) to get a
///    `oneshot::Receiver`. `select!` on it alongside your query.
/// 4. A cancel request fires the receiver, interrupting the current query.
/// 5. When the connection ends (task dropped), the `ConnectionGuard` drops and
///    unregisters from this manager.
#[derive(Debug)]
pub struct ConnectionManager {
    inner: RwLock<HashMap<(i32, Bytes), Arc<ConnectionHandle>>>,
}

impl ConnectionManager {
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
        }
    }

    pub fn register(
        self: &Arc<Self>,
        pid: i32,
        secret_key: SecretKey,
    ) -> (Arc<ConnectionHandle>, ConnectionGuard) {
        let key = (pid, secret_key.to_bytes());
        {
            let mut map = self.inner.write().unwrap();
            assert!(
                !map.contains_key(&key),
                "ConnectionManager: (pid, secret_key) already registered"
            );
            let handle = Arc::new(ConnectionHandle::new());
            map.insert(key, handle.clone());
            (
                handle,
                ConnectionGuard {
                    pid,
                    secret_key,
                    manager: Arc::clone(self),
                },
            )
        }
    }

    pub async fn cancel(&self, pid: i32, secret_key: &SecretKey) -> bool {
        let key = (pid, secret_key.to_bytes());
        let handle = self.inner.read().unwrap().get(&key).cloned();
        if let Some(handle) = handle {
            handle.cancel().await;
            true
        } else {
            false
        }
    }

    fn unregister(&self, pid: i32, secret_key: &SecretKey) {
        let key = (pid, secret_key.to_bytes());
        self.inner.write().unwrap().remove(&key);
    }
}

impl ConnectionGuard {
    pub fn pid(&self) -> i32 {
        self.pid
    }

    pub fn secret_key(&self) -> &SecretKey {
        &self.secret_key
    }
}

impl Default for ConnectionManager {
    fn default() -> Self {
        Self::new()
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

pub trait PgWireServerHandlers: 'static {
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

    #[tokio::test]
    async fn connection_manager_register_and_cancel() {
        let manager = Arc::new(ConnectionManager::new());
        let key = SecretKey::I32(12345);

        let (handle, _guard) = manager.register(1, key.clone());

        let mut rx = handle.start_query().await;
        assert!(manager.cancel(1, &key).await);
        assert_eq!(rx.try_recv(), Ok(Some(())));
    }

    #[tokio::test]
    async fn connection_manager_cancel_unknown_connection() {
        let manager = Arc::new(ConnectionManager::new());
        assert!(!manager.cancel(999, &SecretKey::I32(0)).await);
    }

    #[tokio::test]
    async fn connection_manager_guard_unregisters() {
        let manager = Arc::new(ConnectionManager::new());
        let key = SecretKey::I32(99);

        {
            let (_handle, _guard) = manager.register(42, key.clone());
        }

        assert!(!manager.cancel(42, &key).await);
    }

    #[tokio::test]
    async fn connection_handle_start_query_replaces_previous() {
        let handle = ConnectionHandle::new();

        let mut rx1 = handle.start_query().await;
        let mut rx2 = handle.start_query().await;

        assert!(rx1.try_recv().is_err());
        assert!(handle.cancel().await);
        assert_eq!(rx2.try_recv(), Ok(Some(())));
    }

    #[tokio::test]
    async fn connection_handle_cancel_no_active_query() {
        let handle = ConnectionHandle::new();
        assert!(!handle.cancel().await);
    }
}
