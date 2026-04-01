pub mod auth;
pub(crate) mod config;
pub mod query;
pub mod result;

use std::collections::BTreeMap;

pub use config::Config;

use crate::messages::ProtocolVersion;

/// A trait for fetching necessary information from Client
pub trait ClientInfo {
    /// Returns configuration of this client
    fn config(&self) -> &Config;

    /// Returns server parameters received from server
    fn server_parameters(&self) -> &BTreeMap<String, String>;

    /// Returns process id received from server
    fn process_id(&self) -> i32;

    /// Returns client protocol version
    fn protocol_version(&self) -> ProtocolVersion;

    // TODO: transaction state
}

/// Carries server provided information for current connection
#[derive(Debug, Default)]
pub struct ServerInformation {
    pub parameters: BTreeMap<String, String>,
    pub process_id: i32,
}

/// Indicate the result of current request
pub enum ReadyState<D> {
    Pending,
    Ready(D),
}
