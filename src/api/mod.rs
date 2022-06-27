use std::collections::HashMap;
use std::net::SocketAddr;

pub mod auth;

#[derive(Debug)]
pub enum PgWireConnectionState {
    AwaitingSslRequest,
    AwaitingStartup,
    AuthenticationInProgress,
    ReadyForQuery,
}

impl Default for PgWireConnectionState {
    fn default() -> PgWireConnectionState {
        PgWireConnectionState::AwaitingSslRequest
    }
}

#[derive(Debug, new, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct ClientInfo {
    addr: SocketAddr,
    #[new(default)]
    state: PgWireConnectionState,
    #[new(default)]
    metadata: HashMap<String, String>,
}
