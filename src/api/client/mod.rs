pub mod auth;
pub(crate) mod config;
pub mod query;
pub mod result;

use std::collections::BTreeMap;

/// Re-export of the connection configuration type.
pub use config::Config;

use crate::messages::ProtocolVersion;
use crate::messages::startup::SecretKey;

/// A trait for fetching necessary information from Client
pub trait ClientInfo {
    /// Returns configuration of this client
    fn config(&self) -> &Config;

    /// Returns server parameters received from server
    fn server_parameters(&self) -> &BTreeMap<String, String>;

    /// Returns process id received from server
    fn process_id(&self) -> i32;

    /// Returns the secret key received from the server's `BackendKeyData`.
    ///
    /// Together with [`ClientInfo::process_id`], this identifies the backend
    /// session so a `CancelRequest` can be issued against a running query.
    fn secret_key(&self) -> &SecretKey;

    /// Returns client protocol version
    fn protocol_version(&self) -> ProtocolVersion;

    /// Sets the protocol version in effect for this connection.
    ///
    /// Custom [`StartupHandler`](auth::StartupHandler) implementations should
    /// call this with the version they advertise in the `Startup` message, so
    /// that subsequent backend messages are decoded with the rules of that
    /// version. The default startup handler and the negotiation flow handle
    /// this automatically.
    fn set_protocol_version(&mut self, version: ProtocolVersion);

    // TODO: transaction state
}

/// Carries server provided information for current connection
#[derive(Debug, Default)]
pub struct ServerInformation {
    pub parameters: BTreeMap<String, String>,
    pub process_id: i32,
    pub secret_key: SecretKey,
}

/// Indicate the result of current request
pub enum ReadyState<D> {
    Pending,
    Ready(D),
}
