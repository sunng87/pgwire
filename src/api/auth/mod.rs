use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;

use super::{ClientInfo, PgWireConnectionState, METADATA_DATABASE, METADATA_USER};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, TransactionStatus};
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus, Startup};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Handles startup process and frontend messages
#[async_trait]
pub trait StartupHandler: Send + Sync {
    /// A generic frontend message callback during startup phase.
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;
}

pub trait ServerParameterProvider: Send + Sync {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo;
}

/// Default noop parameter provider.
///
/// This provider responds frontend with default parameters:
///
/// - `DateStyle: ISO YMD`: the default text serialization in this library is
/// using `YMD` style date. If you override this, or use your own serialization
/// for date types, remember to update this as well.
/// - `server_encoding: UTF8`
/// - `client_encoding: UTF8`
/// - `integer_datetimes: on`:
///
#[non_exhaustive]
#[derive(Debug)]
pub struct DefaultServerParameterProvider {
    pub server_version: String,
    pub server_encoding: String,
    pub client_encoding: String,
    pub date_style: String,
    pub integer_datetimes: String,
}

impl Default for DefaultServerParameterProvider {
    fn default() -> Self {
        Self {
            server_version: env!("CARGO_PKG_VERSION").to_owned(),
            server_encoding: "UTF8".to_owned(),
            client_encoding: "UTF8".to_owned(),
            date_style: "ISO YMD".to_owned(),
            integer_datetimes: "on".to_owned(),
        }
    }
}

impl ServerParameterProvider for DefaultServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(5);
        params.insert("server_version".to_owned(), self.server_version.clone());
        params.insert("server_encoding".to_owned(), self.server_encoding.clone());
        params.insert("client_encoding".to_owned(), self.client_encoding.clone());
        params.insert("DateStyle".to_owned(), self.date_style.clone());
        params.insert(
            "integer_datetimes".to_owned(),
            self.integer_datetimes.clone(),
        );

        Some(params)
    }
}

#[derive(Debug, new, Clone)]
pub struct Password {
    salt: Option<Vec<u8>>,
    password: Vec<u8>,
}

impl Password {
    pub fn salt(&self) -> Option<&[u8]> {
        self.salt.as_deref()
    }

    pub fn password(&self) -> &[u8] {
        &self.password
    }
}

#[derive(Debug, new)]
pub struct LoginInfo<'a> {
    user: Option<&'a str>,
    database: Option<&'a str>,
    host: String,
}

impl<'a> LoginInfo<'a> {
    pub fn user(&self) -> Option<&str> {
        self.user
    }

    pub fn database(&self) -> Option<&str> {
        self.database
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn from_client_info<C>(client: &'a C) -> LoginInfo
    where
        C: ClientInfo,
    {
        LoginInfo {
            user: client.metadata().get(METADATA_USER).map(|s| s.as_str()),
            database: client.metadata().get(METADATA_DATABASE).map(|s| s.as_str()),
            host: client.socket_addr().ip().to_string(),
        }
    }
}

/// Represents auth source, the source returns password either in cleartext or
/// hashed with salt.
///
/// When using with different authentication mechanism, the developer can choose
/// specific implementation of `AuthSource`. For example, with cleartext
/// authentication, salt is not required, while in md5pass, a 4-byte salt is
/// needed.
#[async_trait]
pub trait AuthSource: Send + Sync {
    /// Get password from the `AuthSource`.
    ///
    /// `Password` has a an optional salt field when it's hashed.
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password>;
}

pub fn save_startup_parameters_to_metadata<C>(client: &mut C, startup_message: &Startup)
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
{
    client.metadata_mut().extend(
        startup_message
            .parameters
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned())),
    );
}

pub async fn finish_authentication<C, P>(client: &mut C, server_parameter_provider: &P)
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    P: ServerParameterProvider,
{
    let mut messages = vec![PgWireBackendMessage::Authentication(Authentication::Ok)];

    if let Some(parameters) = server_parameter_provider.server_parameters(client) {
        for (k, v) in parameters {
            messages.push(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                k, v,
            )));
        }
    }

    // TODO: store this backend key
    messages.push(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
        std::process::id() as i32,
        rand::random::<i32>(),
    )));
    messages.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
        TransactionStatus::Idle,
    )));
    let mut message_stream = stream::iter(messages.into_iter().map(Ok));
    client.send_all(&mut message_stream).await.unwrap();
    client.set_state(PgWireConnectionState::ReadyForQuery);
}

pub mod cleartext;
pub mod md5pass;
pub mod noop;
pub mod scram;
