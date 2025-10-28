use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use sasl::scram::SCRAM_ITERATIONS;

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
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
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
    pub integer_datetimes: bool,
    pub in_hot_standby: bool,
    pub search_path: String,
    pub is_superuser: bool,
    pub default_transaction_read_only: bool,
    pub scram_iterations: usize,
    // format settings
    pub time_zone: String,
    pub date_style: String,
    pub interval_style: String,
    pub standard_conforming_strings: bool,
    // user name and application name, follow client data
    pub client_encoding: Option<String>,
    pub application_name: Option<String>,
    pub session_authorization: Option<String>,
}

impl Default for DefaultServerParameterProvider {
    fn default() -> Self {
        Self {
            server_version: format!("16.6-pgwire-{}", env!("CARGO_PKG_VERSION").to_owned()),
            server_encoding: "UTF8".to_owned(),
            integer_datetimes: true,
            in_hot_standby: false,
            search_path: "public".to_owned(),
            is_superuser: true,
            default_transaction_read_only: false,
            scram_iterations: SCRAM_ITERATIONS,

            time_zone: "Etc/UTC".to_owned(),
            date_style: "ISO YMD".to_owned(),
            interval_style: "postgres".to_owned(),
            standard_conforming_strings: true,

            client_encoding: None,
            application_name: None,
            session_authorization: None,
        }
    }
}

fn bool_to_string(v: bool) -> String {
    if v {
        "on".to_string()
    } else {
        "off".to_string()
    }
}

impl ServerParameterProvider for DefaultServerParameterProvider {
    fn server_parameters<C>(&self, client: &C) -> Option<HashMap<String, String>>
    where
        C: ClientInfo,
    {
        let mut params = HashMap::with_capacity(16);
        params.insert("server_version".to_owned(), self.server_version.clone());
        params.insert("server_encoding".to_owned(), self.server_encoding.clone());
        params.insert(
            "integer_datetimes".to_owned(),
            bool_to_string(self.integer_datetimes),
        );
        params.insert(
            "in_hot_standby".to_owned(),
            bool_to_string(self.in_hot_standby),
        );
        params.insert("search_path".to_owned(), self.search_path.clone());
        params.insert("is_superuser".to_owned(), bool_to_string(self.is_superuser));
        params.insert(
            "default_transaction_read_only".to_owned(),
            bool_to_string(self.default_transaction_read_only),
        );
        params.insert(
            "scram_iterations".to_owned(),
            self.scram_iterations.to_string(),
        );

        params.insert("TimeZone".to_owned(), self.time_zone.clone());
        params.insert("DateStyle".to_owned(), self.date_style.clone());
        params.insert("IntervalStyle".to_owned(), self.interval_style.clone());
        params.insert(
            "standard_conforming_strings".to_owned(),
            bool_to_string(self.standard_conforming_strings),
        );

        if let Some(client_encoding) = self
            .client_encoding
            .as_ref()
            .or_else(|| client.metadata().get("client_encoding"))
        {
            params.insert("client_encoding".to_owned(), client_encoding.clone());
        }
        if let Some(application_name) = self
            .application_name
            .as_ref()
            .or_else(|| client.metadata().get("application_name"))
        {
            params.insert("application_name".to_owned(), application_name.clone());
        }
        if let Some(user) = self
            .session_authorization
            .as_ref()
            .or_else(|| client.metadata().get("user"))
        {
            params.insert("session_authorization".to_owned(), user.clone());
        }

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
pub trait AuthSource: Send + Sync + Debug {
    /// Get password from the `AuthSource`.
    ///
    /// `Password` has a an optional salt field when it's hashed.
    async fn get_password(&self, login: &LoginInfo) -> PgWireResult<Password>;
}

pub async fn protocol_negotiation<C>(client: &mut C, startup_message: &Startup) -> PgWireResult<()>
where
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
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
    C: ClientInfo,
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
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
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
    C: ClientInfo + Sink<PgWireBackendMessage> + Unpin,
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
#[cfg(any(feature = "_aws-lc-rs", feature = "_ring"))]
pub mod sasl;
