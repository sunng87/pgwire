use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::sink::Sink;

use crate::api::{ClientInfo, ConnectionManager, PidSecretKeyGenerator};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{Authentication, PasswordMessageFamily};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::AuthDriver;
use super::ServerParameterProvider;
use super::StartupHandler;
use super::mechanism::AuthMechanism;
use super::mechanism::AuthMechanismFactory;
use super::mechanism::AuthState;

pub mod oauth;
pub mod scram;

/// SASL mechanism name for SCRAM-SHA-256.
pub const SCRAM_SHA_256_METHOD: &str = "SCRAM-SHA-256";
/// SASL mechanism name for SCRAM-SHA-256-PLUS with channel binding.
pub const SCRAM_SHA_256_PLUS_METHOD: &str = "SCRAM-SHA-256-PLUS";
/// SASL mechanism name for OAUTHBEARER.
pub const OAUTHBEARER_METHOD: &str = "OAUTHBEARER";

/// States of the SASL authentication state machine.
#[derive(Debug)]
pub enum SASLState {
    Initial,
    // scram authentication method selected
    ScramClientFirstReceived,
    // cached password, channel_binding and partial auth-message
    ScramServerFirstSent(Box<scram::ScramServerAuthWaitingForClientFinal>),
    // oauth authentication method selected
    OauthStateInit,
    // failure during authentication
    OauthStateError,
    // finished
    Finished,
}

/// A SASL-family [`AuthMechanismFactory`] that can be registered in a
/// [`SaslMechanisms`] registry.
///
/// The names returned by [`SaslMechanismFactory::sasl_mechanism_names`] are
/// advertised to the client (as `Authentication::SASL([...])`) and used to
/// dispatch the mechanism the client selects.
pub trait SaslMechanismFactory: AuthMechanismFactory {
    /// The SASL mechanism names offered by this mechanism, for example
    /// `["SCRAM-SHA-256", "SCRAM-SHA-256-PLUS"]`.
    fn sasl_mechanism_names(&self) -> Vec<String>;
}

/// A composite SASL mechanism factory: advertises the union of its
/// children's SASL mechanisms and lets each client pick one; the handshake
/// is then dispatched to the selected child.
///
/// Children are [`SaslMechanismFactory`]s (for example
/// [`scram::ScramAuth`] and [`oauth::Oauth`]), provided at construction.
pub struct SaslMechanisms {
    children: Vec<Arc<dyn SaslMechanismFactory>>,
}

impl std::fmt::Debug for SaslMechanisms {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SaslMechanisms")
            .field("children", &self.children.len())
            .finish()
    }
}

impl SaslMechanisms {
    /// Creates a registry from the given child mechanisms.
    pub fn new(mechanisms: Vec<Arc<dyn SaslMechanismFactory>>) -> Self {
        Self {
            children: mechanisms,
        }
    }

    /// Builder-style constructor appending one child mechanism.
    pub fn with_mechanism(mut self, mechanism: Arc<dyn SaslMechanismFactory>) -> Self {
        self.children.push(mechanism);
        self
    }
}

impl AuthMechanismFactory for SaslMechanisms {
    fn create(&self) -> Box<dyn AuthMechanism> {
        Box::new(SaslMechanism {
            children: self.children.clone(),
            selected: None,
        })
    }
}

/// Stateful composite SASL mechanism ([`SaslMechanisms`] is its factory).
struct SaslMechanism {
    children: Vec<Arc<dyn SaslMechanismFactory>>,
    selected: Option<Box<dyn AuthMechanism>>,
}

#[async_trait]
impl AuthMechanism for SaslMechanism {
    async fn begin(&mut self, _client: &(dyn ClientInfo + Send + Sync)) -> PgWireResult<AuthState> {
        // advertise the union of all children's mechanisms; the child's own
        // `begin` is skipped, its handshake starts at the client's selection
        Ok(AuthState::InProgress(Authentication::SASL(
            self.children
                .iter()
                .flat_map(|child| child.sasl_mechanism_names())
                .collect(),
        )))
    }

    async fn next(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
        message: PasswordMessageFamily,
    ) -> PgWireResult<AuthState> {
        if let Some(mechanism) = self.selected.as_mut() {
            return mechanism.next(client, message).await;
        }

        // First message: the client picks a mechanism from the advertised
        // list; dispatch to the matching child.
        let initial = message.into_sasl_initial_response()?;
        let child = self
            .children
            .iter()
            .find(|child| child.sasl_mechanism_names().contains(&initial.auth_method))
            .ok_or_else(|| PgWireError::UnsupportedSASLAuthMethod(initial.auth_method.clone()))?;

        let mut mechanism = child.create();
        let state = mechanism
            .next(client, PasswordMessageFamily::SASLInitialResponse(initial))
            .await?;
        self.selected = Some(mechanism);
        Ok(state)
    }
}

/// Startup handler for SASL-based authentication (SCRAM and OAUTHBEARER).
///
/// Implements [`StartupHandler`] over an [`AuthDriver`] and a
/// [`SaslMechanisms`] registry: it advertises the union of the configured
/// mechanisms ([`with_scram`](Self::with_scram) /
/// [`with_oauth`](Self::with_oauth)) and lets each client pick one.
///
/// The per-connection SASL state lives in the driver's per-connection
/// mechanism storage, so a single handler can be shared across concurrent
/// connections.
pub struct SASLAuthStartupHandler<P> {
    driver: AuthDriver<Arc<P>>,
    mechanisms: SaslMechanisms,
}

impl<P> SASLAuthStartupHandler<P>
where
    P: ServerParameterProvider,
{
    /// Creates a new SASL auth handler.
    pub fn new(parameter_provider: Arc<P>) -> Self {
        Self {
            driver: AuthDriver::new(parameter_provider),
            mechanisms: SaslMechanisms::new(vec![]),
        }
    }

    /// Configures SCRAM authentication.
    pub fn with_scram(self, scram_auth: scram::ScramAuth) -> Self {
        Self {
            driver: self.driver,
            mechanisms: self.mechanisms.with_mechanism(Arc::new(scram_auth)),
        }
    }

    /// Configures OAuth authentication.
    pub fn with_oauth(self, oauth: oauth::Oauth) -> Self {
        Self {
            driver: self.driver,
            mechanisms: self.mechanisms.with_mechanism(Arc::new(oauth)),
        }
    }

    /// Sets a custom PID/secret key generator.
    pub fn with_pid_secret_key_generator(
        mut self,
        generator: Arc<dyn PidSecretKeyGenerator>,
    ) -> Self {
        self.driver = self.driver.with_pid_secret_key_generator(generator);
        self
    }

    /// Sets a connection manager.
    pub fn with_connection_manager(mut self, manager: Arc<ConnectionManager>) -> Self {
        self.driver = self.driver.with_connection_manager(manager);
        self
    }
}

#[async_trait]
impl<P> StartupHandler for SASLAuthStartupHandler<P>
where
    P: ServerParameterProvider,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                super::protocol_negotiation(client, startup).await?;
                super::save_startup_parameters_to_metadata(client, startup);

                let mechanism = self.mechanisms.create();
                self.driver
                    .begin_authentication(client, mechanism)
                    .await
                    .map(|_| ())
            }
            PgWireFrontendMessage::PasswordMessageFamily(message) => self
                .driver
                .advance_authentication(client, message)
                .await
                .map(|_| ()),
            _ => Ok(()),
        }
    }
}

#[cfg(all(test, feature = "client-api"))]
mod tests {
    use super::*;
    use crate::api::PgWireConnectionState;
    use crate::api::auth::DefaultServerParameterProvider;
    use crate::api::auth::sasl::SCRAM_SHA_256_METHOD;
    use crate::api::auth::sasl::scram::ScramClientAuth;
    use crate::api::auth::sasl::scram::gen_salted_password;
    use crate::api::auth::test_util::{TestClient, startup_message};
    use crate::api::auth::{AuthSource, LoginInfo, Password};
    use crate::error::PgWireResult;
    use crate::messages::startup::{
        Authentication, PasswordMessageFamily, SASLInitialResponse, SASLResponse,
    };

    const SALT: &[u8] = b"0123456789abcdef";
    const ITERATIONS: usize = 4096;

    #[derive(Debug)]
    struct SaltedSource;

    #[async_trait]
    impl AuthSource for SaltedSource {
        async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
            Ok(Password::new(
                Some(SALT.to_vec()),
                gen_salted_password("password", SALT, ITERATIONS),
            ))
        }
    }

    #[tokio::test]
    async fn test_sasl_handler_scram_roundtrip() {
        let scram = scram::ScramAuth::new(Arc::new(SaltedSource));
        let handler =
            SASLAuthStartupHandler::new(Arc::new(DefaultServerParameterProvider::default()))
                .with_scram(scram);

        let mut client = TestClient::new(Some("alice"));
        let scram_client = ScramClientAuth::new("alice".to_owned(), "password".to_owned());

        // Startup -> SASL advertisement
        handler
            .on_startup(&mut client, startup_message("alice"))
            .await
            .unwrap();
        assert!(matches!(
            client.sent.last(),
            Some(PgWireBackendMessage::Authentication(Authentication::SASL(methods)))
                if methods == &vec![SCRAM_SHA_256_METHOD.to_owned()]
        ));

        // client-first -> server-first
        let (client_first, scram_client) = scram_client.build_client_first().unwrap();
        handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(
                    PasswordMessageFamily::SASLInitialResponse(SASLInitialResponse::new(
                        SCRAM_SHA_256_METHOD.to_owned(),
                        Some(client_first.into()),
                    )),
                ),
            )
            .await
            .unwrap();
        let server_first = match client.sent.last() {
            Some(PgWireBackendMessage::Authentication(Authentication::SASLContinue(data))) => {
                data.clone()
            }
            other => panic!("unexpected message: {other:?}"),
        };

        // client-final -> server-final + AuthenticationOk + ReadyForQuery
        let (client_final, scram_client) = scram_client.build_client_final(&server_first).unwrap();
        handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(PasswordMessageFamily::SASLResponse(
                    SASLResponse::new(client_final.into()),
                )),
            )
            .await
            .unwrap();

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
    async fn test_sasl_handler_unsupported_method() {
        let handler =
            SASLAuthStartupHandler::new(Arc::new(DefaultServerParameterProvider::default()));

        let mut client = TestClient::new(Some("alice"));
        handler
            .on_startup(&mut client, startup_message("alice"))
            .await
            .unwrap();

        let result = handler
            .on_startup(
                &mut client,
                PgWireFrontendMessage::PasswordMessageFamily(
                    PasswordMessageFamily::SASLInitialResponse(SASLInitialResponse::new(
                        "ANONYMOUS".to_owned(),
                        None,
                    )),
                ),
            )
            .await;

        assert!(matches!(
            result,
            Err(PgWireError::UnsupportedSASLAuthMethod(method)) if method == "ANONYMOUS"
        ));
    }
}
