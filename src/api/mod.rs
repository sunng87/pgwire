//! APIs for building postgresql compatible servers.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub use postgres_types::Type;

pub mod auth;
pub mod copy;
pub mod portal;
pub mod query;
pub mod results;
pub mod stmt;
pub mod store;

pub const DEFAULT_NAME: &str = "POSTGRESQL_DEFAULT_NAME";

#[derive(Debug, Clone, Copy, Default)]
pub enum PgWireConnectionState {
    #[default]
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

    fn state(&self) -> PgWireConnectionState;

    fn set_state(&mut self, new_state: PgWireConnectionState);

    fn metadata(&self) -> &HashMap<String, String>;

    fn metadata_mut(&mut self) -> &mut HashMap<String, String>;
}

/// Client Portal Store
pub trait ClientPortalStore {
    type PortalStore;

    fn portal_store(&self) -> &Self::PortalStore;
}

pub const METADATA_USER: &str = "user";
pub const METADATA_DATABASE: &str = "database";

#[non_exhaustive]
#[derive(Debug)]
pub struct DefaultClient<S> {
    pub socket_addr: SocketAddr,
    pub is_secure: bool,
    pub state: PgWireConnectionState,
    pub metadata: HashMap<String, String>,
    pub portal_store: store::MemPortalStore<S>,
}

impl<S> ClientInfo for DefaultClient<S> {
    fn socket_addr(&self) -> SocketAddr {
        self.socket_addr
    }

    fn is_secure(&self) -> bool {
        self.is_secure
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
}

impl<S> DefaultClient<S> {
    pub fn new(socket_addr: SocketAddr, is_secure: bool) -> DefaultClient<S> {
        DefaultClient {
            socket_addr,
            is_secure,
            state: PgWireConnectionState::default(),
            metadata: HashMap::new(),
            portal_store: store::MemPortalStore::new(),
        }
    }
}

impl<S> ClientPortalStore for DefaultClient<S> {
    type PortalStore = store::MemPortalStore<S>;

    fn portal_store(&self) -> &Self::PortalStore {
        &self.portal_store
    }
}

pub trait PgWireHandlerFactory {
    type StartupHandler: auth::StartupHandler;
    type SimpleQueryHandler: query::SimpleQueryHandler;
    type ExtendedQueryHandler: query::ExtendedQueryHandler;
    type CopyHandler: copy::CopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler>;

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler>;

    fn startup_handler(&self) -> Arc<Self::StartupHandler>;

    fn copy_handler(&self) -> Arc<Self::CopyHandler>;
}

impl<T> PgWireHandlerFactory for Arc<T>
where
    T: PgWireHandlerFactory,
{
    type StartupHandler = T::StartupHandler;
    type SimpleQueryHandler = T::SimpleQueryHandler;
    type ExtendedQueryHandler = T::ExtendedQueryHandler;
    type CopyHandler = T::CopyHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        (**self).simple_query_handler()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        (**self).extended_query_handler()
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        (**self).startup_handler()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        (**self).copy_handler()
    }
}
