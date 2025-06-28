use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};

use super::{ClientInfo, PgWireConnectionState, METADATA_DATABASE, METADATA_USER};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, TransactionStatus};
use crate::messages::startup::{
    Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus, Startup,
};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion};

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
///   using `YMD` style date. If you override this, or use your own serialization
///   for date types, remember to update this as well.
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
            server_version: format!("16.6-pgwire-{}", env!("CARGO_PKG_VERSION").to_owned()),
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

impl LoginInfo<'_> {
    pub fn user(&self) -> Option<&str> {
        self.user
    }

    pub fn database(&self) -> Option<&str> {
        self.database
    }

    pub fn host(&self) -> &str {
        &self.host
    }

    pub fn from_client_info<C>(client: &C) -> LoginInfo<'_>
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

pub async fn protocol_negotiation<C>(client: &mut C, startup_message: &Startup) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
{
    if let Some(protocol_version) = ProtocolVersion::from_version_number(
        startup_message.protocol_number_major,
        startup_message.protocol_number_minor,
    ) {
        client.set_protocol_version(protocol_version);
        Ok(())
    } else {
        let newest_server_version = ProtocolVersion::default();
        let (newest_major_version, newest_minor_version) = newest_server_version.version_number();
        if newest_major_version == startup_message.protocol_number_major {
            client
                .send(PgWireBackendMessage::NegotiateProtocolVersion(
                    NegotiateProtocolVersion::new(newest_minor_version as i32, vec![]),
                ))
                .await?;
            client.set_protocol_version(newest_server_version);
            Ok(())
        } else {
            Err(PgWireError::UnsupportedProtocolVersion(
                startup_message.protocol_number_major,
                startup_message.protocol_number_minor,
            ))
        }
    }
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

pub(crate) async fn finish_authentication0<C, P>(
    client: &mut C,
    server_parameter_provider: &P,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    P: ServerParameterProvider,
{
    client
        .feed(PgWireBackendMessage::Authentication(Authentication::Ok))
        .await?;

    if let Some(parameters) = server_parameter_provider.server_parameters(client) {
        for (k, v) in parameters {
            client
                .feed(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                    k, v,
                )))
                .await?;
        }
    }

    let (pid, secret_key) = client.pid_and_secret_key();
    client
        .feed(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
            pid, secret_key,
        )))
        .await?;

    Ok(())
}

pub async fn finish_authentication<C, P>(
    client: &mut C,
    server_parameter_provider: &P,
) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
    C::Error: Debug,
    PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    P: ServerParameterProvider,
{
    finish_authentication0(client, server_parameter_provider).await?;

    client
        .send(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            TransactionStatus::Idle,
        )))
        .await?;

    client.set_state(PgWireConnectionState::ReadyForQuery);
    Ok(())
}

pub mod cleartext;
pub mod md5pass;
pub mod noop;
#[cfg(feature = "scram")]
pub mod scram;
