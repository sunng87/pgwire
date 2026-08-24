pub mod auth;
pub(crate) mod config;
pub mod query;
pub mod result;

use std::collections::BTreeMap;

/// Re-export of the connection configuration type.
pub use config::Config;

use crate::messages::ProtocolVersion;
use crate::messages::response::TransactionStatus;
use crate::messages::startup::SecretKey;

/// A trait for fetching necessary information from Client
pub trait ClientInfo {
    /// Returns configuration of this client
    fn config(&self) -> &Config;

    /// Returns server parameters received from server
    ///
    /// The cache is kept up to date as the server reports changes with
    /// `ParameterStatus` messages, both during startup and during query
    /// execution (for example after a `SET` statement).
    fn server_parameters(&self) -> &BTreeMap<String, String>;

    /// Updates a cached server parameter.
    ///
    /// Called when the server reports a parameter change with a
    /// `ParameterStatus` message outside of the startup phase, for example
    /// when a `SET` statement takes effect during query execution.
    /// Implementations should update the map returned by
    /// [`ClientInfo::server_parameters`] accordingly.
    fn set_server_parameter(&mut self, name: String, value: String);

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

    /// Returns the current transaction status, as last reported by the
    /// server in a `ReadyForQuery` message.
    ///
    /// The status starts as [`TransactionStatus::Idle`] after startup and
    /// is updated automatically whenever a `ReadyForQuery` message is
    /// consumed, on every code path: startup, simple query, extended query,
    /// and the error-recovery drains.
    fn transaction_status(&self) -> TransactionStatus;

    /// Updates the tracked transaction status.
    ///
    /// This is called automatically by the default message dispatch when a
    /// `ReadyForQuery` message arrives. Handlers that override the default
    /// `on_message` implementations and consume `ReadyForQuery` themselves
    /// should call this so the tracked status stays in sync.
    fn set_transaction_status(&mut self, status: TransactionStatus);
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
