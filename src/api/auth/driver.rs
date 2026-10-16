//! Reusable authentication driver.
//!
//! A [`StartupHandler`](super::StartupHandler) owns the startup protocol;
//! authentication logic
//! lives in [`AuthMechanism`] state machines. [`AuthDriver`] is the small shared core between the two: it
//! runs the mechanism state machine (`begin_authentication` /
//! `advance_authentication`) and completes the startup protocol, while the
//! handler keeps full control of everything around it — pre-auth
//! validation, per-connection mechanism selection, custom failure
//! responses. The built-in `*AuthStartupHandler` types are written against
//! it in exactly the same way a custom handler is:
//!
//! ```no_run
//! # use std::fmt::Debug;
//! # use std::sync::Arc;
//! # use async_trait::async_trait;
//! # use futures::sink::{Sink, SinkExt};
//! use pgwire::api::auth::AuthDriver;
//! use pgwire::api::auth::mechanism::AuthMechanismFactory;
//! use pgwire::api::auth::cleartext::CleartextPasswordAuth;
//! use pgwire::api::auth::md5pass::Md5PasswordAuth;
//! # use pgwire::api::auth::{AuthSource, LoginInfo, Password, ServerParameterProvider, StartupHandler, DefaultServerParameterProvider};
//! # use pgwire::api::ClientInfo;
//! # use pgwire::error::{PgWireError, PgWireResult};
//! # use pgwire::messages::PgWireBackendMessage;
//! # use pgwire::messages::PgWireFrontendMessage;
//! # #[derive(Debug)]
//! # struct MyAuthSource;
//! # #[async_trait]
//! # impl AuthSource for MyAuthSource {
//! #     async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> { unimplemented!() }
//! # }
//! # struct MyQueryHandlers;
//! # impl pgwire::api::PgWireServerHandlers for MyQueryHandlers {}
//!
//! struct MyStartupHandler {
//!     driver: AuthDriver<DefaultServerParameterProvider>,
//!     cleartext: Arc<dyn AuthMechanismFactory>,
//!     md5: Arc<dyn AuthMechanismFactory>,
//! }
//!
//! #[async_trait]
//! impl StartupHandler for MyStartupHandler {
//!     async fn on_startup<C>(
//!         &self,
//!         client: &mut C,
//!         message: PgWireFrontendMessage,
//!     ) -> PgWireResult<()>
//!     where
//!         C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
//!         C::Error: Debug,
//!         PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
//!     {
//!         match message {
//!             PgWireFrontendMessage::Startup(ref startup) => {
//!                 pgwire::api::auth::protocol_negotiation(client, startup).await?;
//!                 pgwire::api::auth::save_startup_parameters_to_metadata(client, startup);
//!
//!                 // custom pre-auth validation goes here (force TLS, resolve
//!                 // database, ...) with full `&mut C` access
//!
//!                 // per-connection mechanism selection:
//!                 let mechanism = if client.metadata().get("user").is_some_and(|u| u == "legacy") {
//!                     self.cleartext.create()
//!                 } else {
//!                     self.md5.create()
//!                 };
//!                 self.driver.begin_authentication(client, mechanism).await
//!             }
//!             PgWireFrontendMessage::PasswordMessageFamily(message) => {
//!                 // map or report auth failures your way by handling the error:
//!                 self.driver.advance_authentication(client, message).await
//!             }
//!             _ => Ok(()),
//!         }
//!     }
//! }
//!
//! let handler = MyStartupHandler {
//!     driver: AuthDriver::new(DefaultServerParameterProvider::default()),
//!     cleartext: Arc::new(CleartextPasswordAuth::new(Arc::new(MyAuthSource))),
//!     md5: Arc::new(Md5PasswordAuth::new(Arc::new(MyAuthSource))),
//! };
//! ```

use std::fmt::Debug;
use std::sync::Arc;

use futures::sink::{Sink, SinkExt};

use super::mechanism::AuthMechanism;
use super::mechanism::AuthState;
use super::{ClientInfo, PgWireConnectionState, ServerParameterProvider};
use crate::api::{ConnectionManager, PidSecretKeyGenerator, RandomPidSecretKeyGenerator};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::PgWireBackendMessage;
use crate::messages::startup::{Authentication, PasswordMessageFamily};

/// Reusable authentication driver: runs a per-connection
/// [`AuthMechanism`] state machine to
/// completion and finishes the startup protocol around it.
///
/// This is the building block for custom
/// [`StartupHandler`](super::StartupHandler)
/// implementations. Create one (it is stateless and can be shared across
/// connections), then:
///
/// - after your pre-auth logic, call [`AuthDriver::begin_authentication`]
///   with a fresh mechanism,
/// - feed every password-family message to
///   [`AuthDriver::advance_authentication`],
/// - for connections that skip authentication, call
///   [`AuthDriver::complete_authentication`] instead.
///
/// The built-in `*AuthStartupHandler` types embed this driver alongside a
/// fixed [`AuthMechanismFactory`](super::mechanism::AuthMechanismFactory);
/// custom handlers embed it the same way.
pub struct AuthDriver<P> {
    parameter_provider: P,
    pid_secret_key_generator: Arc<dyn PidSecretKeyGenerator>,
    connection_manager: Option<Arc<ConnectionManager>>,
}

impl<P> AuthDriver<P> {
    /// Creates a driver with the given server parameter provider.
    pub fn new(parameter_provider: P) -> Self {
        Self {
            parameter_provider,
            pid_secret_key_generator: Arc::new(RandomPidSecretKeyGenerator::default()),
            connection_manager: None,
        }
    }

    /// Sets a custom PID/secret key generator.
    pub fn with_pid_secret_key_generator(
        mut self,
        generator: Arc<dyn PidSecretKeyGenerator>,
    ) -> Self {
        self.pid_secret_key_generator = generator;
        self
    }

    /// Sets a connection manager.
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.connection_manager = Some(manager);
        self
    }
}

impl<P: ServerParameterProvider> AuthDriver<P> {
    /// Begins authenticating a connection with a fresh mechanism: calls
    /// [`AuthMechanism::begin`], sends the initial `Authentication` request
    /// and keeps the mechanism for the following password-family messages.
    ///
    /// If the mechanism completes immediately (`Ok` with or without a final
    /// message), the startup is completed right away.
    ///
    /// Authentication failures are returned as `Err`; handle them in your
    /// `StartupHandler` to send custom error responses, or let the server
    /// turn them into an `ErrorResponse`.
    pub async fn begin_authentication<C>(
        &self,
        client: &mut C,
        mut mechanism: Box<dyn AuthMechanism>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let state = mechanism.begin(client).await;
        self.handle_state(client, mechanism, state).await
    }

    /// Feeds one password-family message to the mechanism started by
    /// [`AuthDriver::begin_authentication`], sending whatever the mechanism
    /// returns; on success the startup protocol is completed
    /// (`AuthenticationOk`, server parameters, `BackendKeyData`,
    /// `ReadyForQuery`).
    ///
    /// Returns `Err` when authentication fails, or
    /// [`PgWireError::NoAuthenticationInProgress`] when a password-family
    /// message arrives while no authentication handshake is running (before
    /// one began or after it already finished).
    pub async fn advance_authentication<C>(
        &self,
        client: &mut C,
        message: PasswordMessageFamily,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let Some(mut mechanism) = take_mechanism(client) else {
            return Err(PgWireError::NoAuthenticationInProgress);
        };

        let state = mechanism.next(client, message).await;
        self.handle_state(client, mechanism, state).await
    }

    /// Applies one step of the mechanism state machine: on
    /// [`AuthState::InProgress`] the message is sent and the mechanism kept
    /// for the next round; on [`AuthState::Ok`] the startup protocol is
    /// completed; failures are propagated to the caller.
    async fn handle_state<C>(
        &self,
        client: &mut C,
        mechanism: Box<dyn AuthMechanism>,
        state: PgWireResult<AuthState>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match state {
            Ok(AuthState::InProgress(next)) => {
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                store_mechanism(client, mechanism);
                client
                    .send(PgWireBackendMessage::Authentication(next))
                    .await?;
                Ok(())
            }
            Ok(AuthState::Ok(final_message)) => self.finish_with(client, final_message).await,
            Ok(AuthState::Failed(error)) | Err(error) => Err(error),
        }
    }

    /// Completes the startup protocol without authentication: generates the
    /// pid/secret key, registers the connection, sends server parameters,
    /// `BackendKeyData` and `ReadyForQuery`.
    pub async fn complete_authentication<C>(&self, client: &mut C) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let (pid, secret_key) = self.pid_secret_key_generator.generate(client);
        client.set_pid_and_secret_key(pid, secret_key);
        if let Some(manager) = &self.connection_manager {
            super::register_connection(client, manager);
        }

        super::finish_authentication(client, &self.parameter_provider).await
    }

    /// Sends the mechanism's optional final message (e.g. SCRAM's
    /// `SASLFinal`), then completes the startup.
    async fn finish_with<C>(
        &self,
        client: &mut C,
        final_message: Option<Authentication>,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        if let Some(message) = final_message {
            client
                .send(PgWireBackendMessage::Authentication(message))
                .await?;
        }
        self.complete_authentication(client).await
    }
}

/// Per-connection storage for the running auth mechanism.
///
/// Stored in [`ClientInfo::session_extensions`] so a single shared
/// `AuthDriver` can drive any number of concurrent connections.
#[derive(Default)]
struct MechanismSlot {
    mechanism: std::sync::Mutex<Option<Box<dyn AuthMechanism>>>,
}

fn take_mechanism(client: &impl ClientInfo) -> Option<Box<dyn AuthMechanism>> {
    client
        .session_extensions()
        .get_or_insert_with(MechanismSlot::default)
        .mechanism
        .lock()
        .unwrap()
        .take()
}

fn store_mechanism(client: &impl ClientInfo, mechanism: Box<dyn AuthMechanism>) {
    *client
        .session_extensions()
        .get_or_insert_with(MechanismSlot::default)
        .mechanism
        .lock()
        .unwrap() = Some(mechanism);
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::messages::PgWireFrontendMessage;
    use async_trait::async_trait;

    use std::sync::Arc;

    use crate::api::PgWireConnectionState;
    use crate::api::auth::DefaultServerParameterProvider;
    use crate::api::auth::StartupHandler;
    use crate::api::auth::cleartext::CleartextPasswordAuth;
    use crate::api::auth::mechanism::AuthMechanism;
    use crate::api::auth::mechanism::AuthMechanismFactory;
    use crate::api::auth::test_util::{TestClient, startup_message};
    use crate::api::auth::{AuthSource, LoginInfo, Password};
    use crate::error::PgWireError;
    use crate::messages::PgWireBackendMessage;
    use crate::messages::startup::{
        Authentication, Password as PasswordMessage, PasswordMessageFamily,
    };

    #[derive(Debug, Default)]
    struct FixedAuthSource {
        password: String,
    }

    #[async_trait]
    impl AuthSource for FixedAuthSource {
        async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
            Ok(Password::new(None, self.password.as_bytes().to_vec()))
        }
    }

    /// A mechanism that authenticates instantly, exercising the
    /// `begin -> Ok` path.
    struct TrustMechanism;

    #[async_trait]
    impl AuthMechanism for TrustMechanism {
        async fn begin(
            &mut self,
            _client: &(dyn ClientInfo + Send + Sync),
        ) -> PgWireResult<AuthState> {
            Ok(AuthState::Ok(None))
        }

        async fn next(
            &mut self,
            _client: &(dyn ClientInfo + Send + Sync),
            _message: PasswordMessageFamily,
        ) -> PgWireResult<AuthState> {
            Err(PgWireError::NoAuthenticationInProgress)
        }
    }

    /// A mechanism that rejects instantly, exercising the fail-fast path.
    struct RejectMechanism;

    #[async_trait]
    impl AuthMechanism for RejectMechanism {
        async fn begin(
            &mut self,
            _client: &(dyn ClientInfo + Send + Sync),
        ) -> PgWireResult<AuthState> {
            Ok(AuthState::Failed(PgWireError::UserNameRequired))
        }

        async fn next(
            &mut self,
            _client: &(dyn ClientInfo + Send + Sync),
            _message: PasswordMessageFamily,
        ) -> PgWireResult<AuthState> {
            Err(PgWireError::NoAuthenticationInProgress)
        }
    }

    fn cleartext_driver_and_factory() -> (
        AuthDriver<DefaultServerParameterProvider>,
        CleartextPasswordAuth<FixedAuthSource>,
    ) {
        (
            AuthDriver::new(DefaultServerParameterProvider::default()),
            CleartextPasswordAuth::new(Arc::new(FixedAuthSource {
                password: "secret".to_owned(),
            })),
        )
    }

    #[tokio::test]
    async fn test_driver_cleartext_success() {
        let (driver, factory) = cleartext_driver_and_factory();
        let mut client = TestClient::new(Some("alice"));

        driver
            .begin_authentication(&mut client, factory.create())
            .await
            .unwrap();

        assert!(matches!(
            client.state(),
            PgWireConnectionState::AuthenticationInProgress
        ));
        assert!(matches!(
            client.sent.last(),
            Some(PgWireBackendMessage::Authentication(
                Authentication::CleartextPassword
            ))
        ));

        driver
            .advance_authentication(
                &mut client,
                PasswordMessageFamily::Password(PasswordMessage::new("secret".to_owned())),
            )
            .await
            .unwrap();

        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));
        assert!(client.sent.iter().any(|msg| {
            matches!(
                msg,
                PgWireBackendMessage::Authentication(Authentication::Ok)
            )
        }));
        assert!(matches!(
            client.sent.last(),
            Some(PgWireBackendMessage::ReadyForQuery(_))
        ));
    }

    #[tokio::test]
    async fn test_driver_cleartext_failure() {
        let (driver, factory) = cleartext_driver_and_factory();
        let mut client = TestClient::new(Some("alice"));

        driver
            .begin_authentication(&mut client, factory.create())
            .await
            .unwrap();

        let result = driver
            .advance_authentication(
                &mut client,
                PasswordMessageFamily::Password(PasswordMessage::new("wrong".to_owned())),
            )
            .await;

        assert!(matches!(result, Err(PgWireError::InvalidPassword(_))));
        assert!(matches!(
            client.state(),
            PgWireConnectionState::AuthenticationInProgress
        ));
    }

    #[tokio::test]
    async fn test_driver_instant_ok_mechanism() {
        let driver = AuthDriver::new(DefaultServerParameterProvider::default());
        let mut client = TestClient::new(None);

        driver
            .begin_authentication(&mut client, Box::new(TrustMechanism))
            .await
            .unwrap();

        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));
        assert!(matches!(
            client.sent.first(),
            Some(PgWireBackendMessage::Authentication(Authentication::Ok))
        ));
    }

    #[tokio::test]
    async fn test_driver_instant_failure_mechanism() {
        let driver = AuthDriver::new(DefaultServerParameterProvider::default());
        let mut client = TestClient::new(None);

        let result = driver
            .begin_authentication(&mut client, Box::new(RejectMechanism))
            .await;

        assert!(matches!(result, Err(PgWireError::UserNameRequired)));
    }

    #[tokio::test]
    async fn test_driver_password_message_without_mechanism() {
        let driver = AuthDriver::new(DefaultServerParameterProvider::default());
        let mut client = TestClient::new(None);

        let result = driver
            .advance_authentication(
                &mut client,
                PasswordMessageFamily::Password(PasswordMessage::new("secret".to_owned())),
            )
            .await;

        assert!(matches!(
            result,
            Err(PgWireError::NoAuthenticationInProgress)
        ));
    }

    /// A custom StartupHandler built on an embedded [`AuthDriver`], with
    /// per-connection mechanism selection and pre-auth validation — the
    /// pattern recommended for database servers.
    struct CustomHandler {
        driver: AuthDriver<DefaultServerParameterProvider>,
        cleartext: Arc<dyn AuthMechanismFactory>,
    }

    #[async_trait]
    impl StartupHandler for CustomHandler {
        async fn on_startup<C>(
            &self,
            client: &mut C,
            message: PgWireFrontendMessage,
        ) -> PgWireResult<()>
        where
            C: ClientInfo + futures::sink::Sink<PgWireBackendMessage> + Unpin + Send + Sync,
            C::Error: std::fmt::Debug,
            PgWireError: From<<C as futures::sink::Sink<PgWireBackendMessage>>::Error>,
        {
            match message {
                PgWireFrontendMessage::Startup(ref startup) => {
                    crate::api::auth::protocol_negotiation(client, startup).await?;
                    crate::api::auth::save_startup_parameters_to_metadata(client, startup);

                    // custom pre-auth validation
                    if client.metadata().get("database").is_none() {
                        return Err(PgWireError::InvalidStartupMessage);
                    }

                    // per-connection mechanism selection
                    let mechanism = if client.metadata().get("user").is_some_and(|u| u == "trust") {
                        Box::new(TrustMechanism)
                    } else {
                        self.cleartext.create()
                    };
                    self.driver.begin_authentication(client, mechanism).await
                }
                PgWireFrontendMessage::PasswordMessageFamily(message) => {
                    self.driver.advance_authentication(client, message).await
                }
                _ => Ok(()),
            }
        }
    }

    #[tokio::test]
    async fn test_custom_handler_with_driver() {
        let handler = CustomHandler {
            driver: AuthDriver::new(DefaultServerParameterProvider::default()),
            cleartext: Arc::new(CleartextPasswordAuth::new(Arc::new(FixedAuthSource {
                password: "secret".to_owned(),
            }))),
        };

        // "trust" user: instant auth via custom mechanism selection
        let mut client = TestClient::new(Some("trust"));
        client.metadata_mut().remove("user");
        client
            .metadata_mut()
            .insert("user".to_owned(), "trust".to_owned());
        handler
            .on_startup(&mut client, startup_message("trust"))
            .await
            .unwrap();
        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));

        // other users: cleartext via driver
        let mut client = TestClient::new(Some("alice"));
        handler
            .on_startup(&mut client, startup_message("alice"))
            .await
            .unwrap();
        assert!(matches!(
            client.state(),
            PgWireConnectionState::AuthenticationInProgress
        ));
        handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(PasswordMessageFamily::Password(
                    PasswordMessage::new("secret".to_owned()),
                )),
            )
            .await
            .unwrap();
        assert!(matches!(
            client.state(),
            PgWireConnectionState::ReadyForQuery
        ));
    }

    #[cfg(all(any(feature = "_aws-lc-rs", feature = "_ring"), feature = "client-api"))]
    mod scram_tests {
        use super::*;

        use crate::api::auth::sasl::scram::ScramAuth;
        use crate::api::auth::sasl::scram::ScramClientAuth;
        use crate::api::auth::sasl::scram::{ScramSecret, gen_salted_password};
        use crate::messages::startup::{SASLInitialResponse, SASLResponse};

        const SALT: &[u8] = b"0123456789abcdef";
        const ITERATIONS: usize = 4096;

        /// A source that only stores a postgres-style verifier.
        #[derive(Debug)]
        struct VerifierAuthSource {
            password: String,
        }

        #[async_trait]
        impl AuthSource for VerifierAuthSource {
            async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
                Err(PgWireError::ApiError("must not be called".into()))
            }

            async fn get_scram_secret(
                &self,
                _login: &LoginInfo,
            ) -> PgWireResult<crate::api::auth::sasl::scram::ScramSecret> {
                let salted_password = gen_salted_password(&self.password, SALT, ITERATIONS);
                Ok(ScramSecret::verifier_from_salted_password(
                    SALT.to_vec(),
                    salted_password,
                    ITERATIONS,
                ))
            }
        }

        #[tokio::test]
        async fn test_driver_scram_with_verifier_secret() {
            let scram = ScramAuth::new(Arc::new(VerifierAuthSource {
                password: "password".to_owned(),
            }));
            let driver = AuthDriver::new(DefaultServerParameterProvider::default());
            let mut client = TestClient::new(Some("alice"));

            let scram_client = ScramClientAuth::new("alice".to_owned(), "password".to_owned());

            driver
                .begin_authentication(&mut client, scram.create())
                .await
                .unwrap();
            assert!(matches!(
                client.state(),
                PgWireConnectionState::AuthenticationInProgress
            ));

            // step 1: SASL advertisement -> client first
            assert!(matches!(
                client.sent.last(),
                Some(PgWireBackendMessage::Authentication(Authentication::SASL(
                    _
                )))
            ));
            let (client_first, scram_client) = scram_client.build_client_first().unwrap();
            driver
                .advance_authentication(
                    &mut client,
                    PasswordMessageFamily::SASLInitialResponse(SASLInitialResponse::new(
                        "SCRAM-SHA-256".to_owned(),
                        Some(client_first.into()),
                    )),
                )
                .await
                .unwrap();

            // step 2: server first -> client final
            let server_first = match client.sent.last() {
                Some(PgWireBackendMessage::Authentication(Authentication::SASLContinue(data))) => {
                    data.clone()
                }
                other => panic!("unexpected message: {other:?}"),
            };
            let (client_final, scram_client) =
                scram_client.build_client_final(&server_first).unwrap();
            driver
                .advance_authentication(
                    &mut client,
                    PasswordMessageFamily::SASLResponse(SASLResponse::new(client_final.into())),
                )
                .await
                .unwrap();

            // step 3: server final -> verified, connection ready
            let server_final = client
                .sent
                .iter()
                .find_map(|msg| match msg {
                    PgWireBackendMessage::Authentication(Authentication::SASLFinal(data)) => {
                        Some(data.clone())
                    }
                    _ => None,
                })
                .expect("SASLFinal should have been sent");
            scram_client.verify_server_final(&server_final).unwrap();

            assert!(matches!(
                client.state(),
                PgWireConnectionState::ReadyForQuery
            ));
        }

        #[tokio::test]
        async fn test_driver_scram_wrong_password() {
            let scram = ScramAuth::new(Arc::new(VerifierAuthSource {
                password: "password".to_owned(),
            }));
            let driver = AuthDriver::new(DefaultServerParameterProvider::default());
            let mut client = TestClient::new(Some("alice"));

            let scram_client = ScramClientAuth::new("alice".to_owned(), "wrong".to_owned());

            driver
                .begin_authentication(&mut client, scram.create())
                .await
                .unwrap();

            let (client_first, scram_client) = scram_client.build_client_first().unwrap();
            driver
                .advance_authentication(
                    &mut client,
                    PasswordMessageFamily::SASLInitialResponse(SASLInitialResponse::new(
                        "SCRAM-SHA-256".to_owned(),
                        Some(client_first.into()),
                    )),
                )
                .await
                .unwrap();

            let server_first = match client.sent.last() {
                Some(PgWireBackendMessage::Authentication(Authentication::SASLContinue(data))) => {
                    data.clone()
                }
                other => panic!("unexpected message: {other:?}"),
            };
            let (client_final, _) = scram_client.build_client_final(&server_first).unwrap();
            let result = driver
                .advance_authentication(
                    &mut client,
                    PasswordMessageFamily::SASLResponse(SASLResponse::new(client_final.into())),
                )
                .await;

            assert!(matches!(result, Err(PgWireError::InvalidPassword(_))));
        }
    }
}
