use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use rand;

use super::{ClientInfo, PgWireConnectionState, METADATA_DATABASE, METADATA_USER};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus, Startup};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

/// Handles startup process and frontend messages
#[async_trait]
pub trait StartupHandler: Send + Sync {
    /// A generic frontend message callback during startup phase.
    async fn on_startup<C>(
        &mut self,
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

/// Default noop parameter provider
pub struct DefaultServerParameterProvider;

impl ServerParameterProvider for DefaultServerParameterProvider {
    fn server_parameters<C>(&self, _client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(4);
        params.insert(
            "server_version".to_owned(),
            env!("CARGO_PKG_VERSION").to_owned(),
        );
        params.insert("server_encoding".to_owned(), "UTF8".to_owned());
        params.insert("client_encoding".to_owned(), "UTF8".to_owned());
        params.insert("DateStyle".to_owned(), "ISO YMD".to_owned());
        params.insert("integer_datetimes".to_owned(), "on".to_owned());

        Some(params)
    }
}

#[derive(Debug, new, Getters, Clone)]
#[getset(get = "pub")]
pub struct Password {
    salt: Option<Vec<u8>>,
    password: Vec<u8>,
}

#[derive(Debug, new, Getters)]
#[getset(get = "pub")]
pub struct LoginInfo<'a> {
    user: Option<&'a String>,
    database: Option<&'a String>,
    host: String,
}

impl<'a> LoginInfo<'a> {
    pub fn from_client_info<C>(client: &'a C) -> LoginInfo
    where
        C: ClientInfo,
    {
        LoginInfo {
            user: client.metadata().get(METADATA_USER),
            database: client.metadata().get(METADATA_DATABASE),
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
            .parameters()
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
        READY_STATUS_IDLE,
    )));
    let mut message_stream = stream::iter(messages.into_iter().map(Ok));
    client.send_all(&mut message_stream).await.unwrap();
    client.set_state(PgWireConnectionState::ReadyForQuery);
}

pub mod cleartext;
pub mod md5pass;
pub mod noop;
pub mod scram;
