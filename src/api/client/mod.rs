pub mod auth;
pub(crate) mod config;

use std::collections::BTreeMap;

pub use config::Config;

/// A trait for fetching necessary information from Client
pub trait ClientInfo {
    /// Returns configuration of this client
    fn config(&self) -> &Config;

    fn server_parameters(&self, key: &str) -> Option<String>;

    fn process_id(&self) -> Option<i32>;
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
