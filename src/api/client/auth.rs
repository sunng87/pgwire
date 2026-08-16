use std::collections::BTreeMap;

use async_trait::async_trait;
use futures::{Sink, SinkExt, Stream, StreamExt};

use crate::api::auth::md5pass::hash_md5_password;
use crate::api::auth::sasl::SCRAM_SHA_256_METHOD;
use crate::api::auth::sasl::scram::ScramClientAuth;
use crate::error::{ErrorInfo, PgWireClientError, PgWireClientResult, PgWireResult};
use crate::messages::response::ReadyForQuery;
use crate::messages::startup::{
    Authentication, BackendKeyData, NegotiateProtocolVersion, ParameterStatus, Password,
    PasswordMessageFamily, SASLInitialResponse, SASLResponse, SecretKey, Startup,
};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion};

use super::{ClientInfo, ReadyState, ServerInformation};

/// Handler trait for the startup/authentication phase of a client connection.
#[async_trait]
pub trait StartupHandler: Send {
    /// Initiate the startup process by sending a startup message.
    async fn startup<C>(&mut self, client: &mut C) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Handle a single backend message during startup/authentication.
    async fn on_message<C>(
        &mut self,
        client: &mut C,
        message: PgWireBackendMessage,
    ) -> PgWireClientResult<ReadyState<ServerInformation>>
    where
        C: ClientInfo
            + Stream<Item = PgWireResult<PgWireBackendMessage>>
            + Sink<PgWireFrontendMessage>
            + Unpin
            + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match message {
            PgWireBackendMessage::Authentication(authentication) => {
                self.on_authentication(client, authentication).await?;
            }
            PgWireBackendMessage::NegotiateProtocolVersion(negotiation) => {
                self.on_negotiate_protocol_version(client, negotiation)
                    .await?;
            }
            PgWireBackendMessage::ParameterStatus(parameter_status) => {
                self.on_parameter_status(client, parameter_status).await?;
            }
            PgWireBackendMessage::BackendKeyData(backend_key_data) => {
                self.on_backend_key(client, backend_key_data).await?;
            }
            PgWireBackendMessage::ReadyForQuery(ready) => {
                let server_information = self.on_ready_for_query(client, ready).await?;
                return Ok(ReadyState::Ready(server_information));
            }
            PgWireBackendMessage::ErrorResponse(error) => {
                let error_info = ErrorInfo::from(error);
                return Err(error_info.into());
            }
            PgWireBackendMessage::NoticeResponse(_) => {}
            _ => return Err(PgWireClientError::UnexpectedMessage(Box::new(message))),
        }

        Ok(ReadyState::Pending)
    }

    /// Handle an authentication message from the server.
    async fn on_authentication<C>(
        &mut self,
        client: &mut C,
        message: Authentication,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo
            + Stream<Item = PgWireResult<PgWireBackendMessage>>
            + Sink<PgWireFrontendMessage>
            + Unpin
            + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Handle a `NegotiateProtocolVersion` message from the server.
    ///
    /// The default implementation adopts the negotiated version for the rest
    /// of the connection. Both the full 32-bit version number used by
    /// PostgreSQL 18+ and pgwire, and the minor-only form used by older
    /// servers, are understood. Startup parameters reported as unrecognized
    /// by the server are ignored, matching libpq's behavior for non-`_pq_`
    /// options.
    async fn on_negotiate_protocol_version<C>(
        &mut self,
        client: &mut C,
        message: NegotiateProtocolVersion,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match negotiated_version(&message) {
            Some(version) => {
                client.set_protocol_version(version);
                Ok(())
            }
            None => Err(PgWireClientError::UnexpectedMessage(Box::new(
                PgWireBackendMessage::NegotiateProtocolVersion(message),
            ))),
        }
    }

    /// Handle a parameter status message from the server.
    async fn on_parameter_status<C>(
        &mut self,
        client: &mut C,
        message: ParameterStatus,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Handle a backend key data message from the server.
    async fn on_backend_key<C>(
        &mut self,
        client: &mut C,
        message: BackendKeyData,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;

    /// Handle a `ReadyForQuery` message and return the collected server information.
    async fn on_ready_for_query<C>(
        &mut self,
        client: &mut C,
        message: ReadyForQuery,
    ) -> PgWireClientResult<ServerInformation>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>;
}

/// Default startup handler that supports cleartext, MD5, and SCRAM-SHA-256 authentication.
#[derive(new, Debug)]
pub struct DefaultStartupHandler {
    #[new(default)]
    server_parameters: BTreeMap<String, String>,
    #[new(default)]
    process_id: Option<i32>,
    #[new(default)]
    secret_key: Option<SecretKey>,
}

#[async_trait]
impl StartupHandler for DefaultStartupHandler {
    async fn startup<C>(&mut self, client: &mut C) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        let mut startup = Startup::new();

        let config = client.config();

        // Advertise the configured protocol version. The connection decodes
        // with the rules of the advertised version until the server lowers it
        // via NegotiateProtocolVersion.
        let (major, minor) = config.get_protocol_version().version_number();
        startup.protocol_number_major = major;
        startup.protocol_number_minor = minor;

        if let Some(application_name) = &config.application_name {
            startup
                .parameters
                .insert("application_name".to_string(), application_name.clone());
        }
        if let Some(user) = &config.user {
            startup.parameters.insert("user".to_string(), user.clone());
        }
        if let Some(dbname) = &config.dbname {
            startup
                .parameters
                .insert("database".to_string(), dbname.clone());
        }

        client.send(PgWireFrontendMessage::Startup(startup)).await?;
        Ok(())
    }

    async fn on_authentication<C>(
        &mut self,
        client: &mut C,
        message: Authentication,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo
            + Stream<Item = PgWireResult<PgWireBackendMessage>>
            + Sink<PgWireFrontendMessage>
            + Unpin
            + Send,
        PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
    {
        match message {
            Authentication::Ok => {}
            Authentication::CleartextPassword => {
                let pass = client
                    .config()
                    .password
                    .as_ref()
                    .map(|bs| String::from_utf8_lossy(bs).into_owned())
                    .unwrap_or_default();

                client
                    .send(PgWireFrontendMessage::PasswordMessageFamily(
                        PasswordMessageFamily::Password(Password::new(pass)),
                    ))
                    .await?;
            }
            Authentication::MD5Password(salt) => {
                let username = client.config().user.as_ref().map_or("", |s| s.as_str());

                let password = client
                    .config()
                    .password
                    .as_ref()
                    .map(|bs| String::from_utf8_lossy(bs).into_owned())
                    .unwrap_or_default();

                let hashed_password = hash_md5_password(username, &password, &salt);
                client
                    .send(PgWireFrontendMessage::PasswordMessageFamily(
                        PasswordMessageFamily::Password(Password::new(hashed_password)),
                    ))
                    .await?;
            }
            Authentication::SASL(auth_mechanisms) => {
                for auth_mechanism in &auth_mechanisms {
                    if auth_mechanism == SCRAM_SHA_256_METHOD {
                        do_scram_sha256_auth(client).await?;
                        return Ok(());
                    }
                }
                // No supported auth mechanism
                return Err(PgWireClientError::UnsupportedSASLAuthMethods(
                    auth_mechanisms,
                ));
            }
            _ => {
                return Err(PgWireClientError::UnexpectedMessage(Box::new(
                    PgWireBackendMessage::Authentication(message),
                )));
            }
        }

        Ok(())
    }

    async fn on_parameter_status<C>(
        &mut self,
        _client: &mut C,
        message: ParameterStatus,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        self.server_parameters.insert(message.name, message.value);
        Ok(())
    }

    async fn on_backend_key<C>(
        &mut self,
        _client: &mut C,
        message: BackendKeyData,
    ) -> PgWireClientResult<()>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        self.process_id = Some(message.pid);
        self.secret_key = Some(message.secret_key);
        Ok(())
    }

    async fn on_ready_for_query<C>(
        &mut self,
        _client: &mut C,
        _message: ReadyForQuery,
    ) -> PgWireClientResult<ServerInformation>
    where
        C: ClientInfo + Sink<PgWireFrontendMessage> + Unpin + Send,
    {
        Ok(ServerInformation {
            parameters: self.server_parameters.clone(),
            process_id: self.process_id.unwrap_or(-1),
            secret_key: self.secret_key.clone().unwrap_or_default(),
        })
    }
}

async fn do_scram_sha256_auth<C>(client: &mut C) -> PgWireClientResult<()>
where
    C: ClientInfo
        + Stream<Item = PgWireResult<PgWireBackendMessage>>
        + Sink<PgWireFrontendMessage>
        + Unpin
        + Send,
    PgWireClientError: From<<C as Sink<PgWireFrontendMessage>>::Error>,
{
    let username = client.config().user.clone().unwrap_or_default();
    let password = String::from_utf8(client.config().password.clone().unwrap_or_default())
        .map_err(|_| {
            PgWireClientError::ScramError("Only UTF-8 passwords are supported by SCRAM".into())
        })?;
    let auth_client = ScramClientAuth::new(username, password);

    // Client first message
    let (message, auth_client) = auth_client.build_client_first()?;
    client
        .send(PgWireFrontendMessage::PasswordMessageFamily(
            PasswordMessageFamily::SASLInitialResponse(SASLInitialResponse::new(
                SCRAM_SHA_256_METHOD.into(),
                Some(message.into()),
            )),
        ))
        .await?;

    // Server first message
    let Some(message) = client.next().await else {
        return Err(PgWireClientError::UnexpectedEOF);
    };
    let message = match message? {
        PgWireBackendMessage::Authentication(Authentication::SASLContinue(message)) => message,
        PgWireBackendMessage::ErrorResponse(error) => {
            let error_info = ErrorInfo::from(error);
            return Err(error_info.into());
        }
        message => return Err(PgWireClientError::UnexpectedMessage(Box::new(message))),
    };

    // Client final message
    let (message, auth_client) = auth_client.build_client_final(&message)?;
    client
        .send(PgWireFrontendMessage::PasswordMessageFamily(
            PasswordMessageFamily::SASLResponse(SASLResponse::new(message.into())),
        ))
        .await?;

    // Server final message
    let Some(message) = client.next().await else {
        return Err(PgWireClientError::UnexpectedEOF);
    };
    let message = match message? {
        PgWireBackendMessage::Authentication(Authentication::SASLFinal(message)) => message,
        PgWireBackendMessage::ErrorResponse(error) => {
            let error_info = ErrorInfo::from(error);
            return Err(error_info.into());
        }
        message => return Err(PgWireClientError::UnexpectedMessage(Box::new(message))),
    };
    auth_client.verify_server_final(&message)
}

/// Interpret the version number reported by a `NegotiateProtocolVersion`
/// message and return the protocol version to use for the rest of the
/// connection.
///
/// This is the client-side counterpart of the server's
/// `api::auth::protocol_negotiation`. Two wire dialects exist:
///
/// - PostgreSQL 18+ and pgwire report the **full 32-bit protocol version
///   number** (e.g. `196610` for 3.2), which is always greater than `65535`,
/// - older servers report only the **minor version** (e.g. `2`), leaving the
///   major version unchanged from the client's request.
///
/// Returns `None` if the reported version cannot be mapped to a version this
/// crate supports (e.g. a newer major version).
fn negotiated_version(message: &NegotiateProtocolVersion) -> Option<ProtocolVersion> {
    let value = message.newest_minor_protocol;
    if value < 0 {
        return None;
    }

    let (major, minor) = if value > i32::from(u16::MAX) {
        // full 32-bit protocol version number
        (((value >> 16) & 0xFFFF) as u16, (value & 0xFFFF) as u16)
    } else {
        // minor-only form, the major version is unchanged
        (3, value as u16)
    };

    match ProtocolVersion::from_version_number(major, minor) {
        Some(version) => Some(version),
        // A minor version we don't have a variant for: protocol 3.2 is what
        // changed the wire formats we care about (secret keys), so an unknown
        // minor below 2 behaves like 3.0, and one at or above 2 falls back to
        // our newest 3.x.
        None if major == 3 && minor >= 2 => Some(ProtocolVersion::PROTOCOL3_2),
        None if major == 3 => Some(ProtocolVersion::PROTOCOL3_0),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_negotiated_version() {
        // The same version can arrive in two wire dialects, and both forms
        // must negotiate to the same version:
        //
        // - the full 32-bit protocol version number (`i32::from(version)`),
        //   used by PostgreSQL 18+ and current pgwire,
        // - the minor version alone, used by older servers, which leaves the
        //   major version unchanged from the client's request.
        //
        // Realistic on the wire: full 3.2 (PostgreSQL 18+), bare 0 (older
        // PostgreSQL, which only supports 3.0) and bare 2 (pgwire before the
        // #439 fix sent the minor alone). Full-form 3.0 never occurs, but is
        // unambiguous.
        for (full_form, minor_form, expected) in [
            (
                i32::from(ProtocolVersion::PROTOCOL3_2),
                2,
                ProtocolVersion::PROTOCOL3_2,
            ),
            (
                i32::from(ProtocolVersion::PROTOCOL3_0),
                0,
                ProtocolVersion::PROTOCOL3_0,
            ),
        ] {
            assert_eq!(
                negotiated_version(&NegotiateProtocolVersion::new(full_form, vec![])),
                Some(expected)
            );
            assert_eq!(
                negotiated_version(&NegotiateProtocolVersion::new(minor_form, vec![])),
                Some(expected)
            );
        }

        // Unknown minors: below 2 behaves like 3.0, at or above 2 falls back
        // to our newest 3.x
        assert_eq!(
            negotiated_version(&NegotiateProtocolVersion::new(1, vec![])),
            Some(ProtocolVersion::PROTOCOL3_0)
        );
        assert_eq!(
            negotiated_version(&NegotiateProtocolVersion::new(5, vec![])),
            Some(ProtocolVersion::PROTOCOL3_2)
        );

        // An unknown major version cannot be mapped
        assert_eq!(
            negotiated_version(&NegotiateProtocolVersion::new((4 << 16) | 2, vec![])),
            None
        );
        // A negative value is invalid
        assert_eq!(
            negotiated_version(&NegotiateProtocolVersion::new(-1, vec![])),
            None
        );
    }
}
