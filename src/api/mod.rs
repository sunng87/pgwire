use std::collections::HashMap;
use std::net::SocketAddr;

pub mod auth;
pub mod portal;
pub mod query;
pub mod stmt;
pub mod store;

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
