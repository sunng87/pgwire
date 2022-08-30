use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

pub use postgres_types::Type;

pub mod auth;
pub mod portal;
pub mod query;
pub mod results;
pub mod stmt;
pub mod store;

pub const DEFAULT_NAME: &str = "POSTGRESQL_DEFAULT_NAME";

#[derive(Debug)]
pub enum PgWireConnectionState {
    AwaitingSslRequest,
    AwaitingStartup,
    AuthenticationInProgress,
    ReadyForQuery,
    QueryInProgress,
}

impl Default for PgWireConnectionState {
    fn default() -> PgWireConnectionState {
        PgWireConnectionState::AwaitingSslRequest
    }
}

/// Describe a client infomation holder
pub trait ClientInfo {
    fn socket_addr(&self) -> &SocketAddr;

    fn state(&self) -> &PgWireConnectionState;

    fn set_state(&mut self, new_state: PgWireConnectionState);

    fn metadata(&self) -> &HashMap<String, String>;

    fn metadata_mut(&mut self) -> &mut HashMap<String, String>;

    fn stmt_store(&self) -> &dyn store::SessionStore<Arc<stmt::Statement>>;

    fn stmt_store_mut(&mut self) -> &mut dyn store::SessionStore<Arc<stmt::Statement>>;

    fn portal_store(&self) -> &dyn store::SessionStore<Arc<portal::Portal>>;

    fn portal_store_mut(&mut self) -> &mut dyn store::SessionStore<Arc<portal::Portal>>;
}

#[derive(Debug, new, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ClientInfoHolder {
    socket_addr: SocketAddr,
    #[new(default)]
    state: PgWireConnectionState,
    #[new(default)]
    metadata: HashMap<String, String>,
    #[new(default)]
    portal_store: store::MemSessionStore<portal::Portal>,
    #[new(default)]
    stmt_store: store::MemSessionStore<stmt::Statement>,
}
