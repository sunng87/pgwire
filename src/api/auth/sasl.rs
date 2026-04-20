use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use tokio::sync::Mutex;

use crate::api::auth::sasl::scram::ScramServerAuthWaitingForClientFinal;
use crate::api::{
    ClientInfo, ConnectionManager, PgWireConnectionState, PidSecretKeyGenerator,
    RandomPidSecretKeyGenerator,
};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{Authentication, PasswordMessageFamily};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::{ServerParameterProvider, StartupHandler};

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
    ScramServerFirstSent(Box<ScramServerAuthWaitingForClientFinal>),
    // oauth authentication method selected
    OauthStateInit,
    // failure during authentication
    OauthStateError,
    // finished
    Finished,
}

impl SASLState {
    fn is_scram(&self) -> bool {
        matches!(
            self,
            SASLState::ScramClientFirstReceived | SASLState::ScramServerFirstSent(_)
        )
    }

    fn is_oauth(&self) -> bool {
        matches!(self, SASLState::OauthStateInit | SASLState::OauthStateError)
    }
}

/// Startup handler for SASL-based authentication (SCRAM and OAUTHBEARER).
pub struct SASLAuthStartupHandler<P> {
    parameter_provider: Arc<P>,
    pid_secret_key_generator: Arc<dyn PidSecretKeyGenerator>,
    connection_manager: Option<Arc<ConnectionManager>>,
    /// state of the SASL auth
    state: Mutex<SASLState>,
    /// scram configuration
    scram: Option<scram::ScramAuth>,
    /// oauth configuration
    oauth: Option<oauth::Oauth>,
}

impl<P> SASLAuthStartupHandler<P> {
    /// Creates a new SASL auth handler.
    pub fn new(parameter_provider: Arc<P>) -> Self {
        SASLAuthStartupHandler {
            parameter_provider,
            pid_secret_key_generator: Arc::new(RandomPidSecretKeyGenerator),
            connection_manager: None,
            state: Mutex::new(SASLState::Initial),
            scram: None,
            oauth: None,
        }
    }

    /// Configures SCRAM authentication.
    pub fn with_scram(mut self, scram_auth: scram::ScramAuth) -> Self {
        self.scram = Some(scram_auth);
        self
    }

    /// Configures OAuth authentication.
    pub fn with_oauth(mut self, oauth: oauth::Oauth) -> Self {
        self.oauth = Some(oauth);
        self
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

    fn supported_mechanisms(&self) -> Vec<String> {
        let mut mechanisms = vec![];

        if let Some(scram) = &self.scram {
            mechanisms.push(SCRAM_SHA_256_METHOD.to_owned());

            if scram.supports_channel_binding() {
                mechanisms.push(SCRAM_SHA_256_PLUS_METHOD.to_owned());
            }
        }

        if self.oauth.is_some() {
            mechanisms.push(OAUTHBEARER_METHOD.to_owned());
        }

        mechanisms
    }
}

#[async_trait]
impl<P: ServerParameterProvider> StartupHandler for SASLAuthStartupHandler<P> {
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
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                let supported_mechanisms = self.supported_mechanisms();
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        supported_mechanisms,
                    )))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(mut msg) => {
                let mut state = self.state.lock().await;

                msg = if let SASLState::Initial = *state {
                    let sasl_initial_response = msg.into_sasl_initial_response()?;
                    let selected_mechanism = sasl_initial_response.auth_method.as_str();

                    *state = if [SCRAM_SHA_256_METHOD, SCRAM_SHA_256_PLUS_METHOD]
                        .contains(&selected_mechanism)
                    {
                        SASLState::ScramClientFirstReceived
                    } else if OAUTHBEARER_METHOD == selected_mechanism {
                        SASLState::OauthStateInit
                    } else {
                        return Err(PgWireError::UnsupportedSASLAuthMethod(
                            selected_mechanism.to_string(),
                        ));
                    };

                    PasswordMessageFamily::SASLInitialResponse(sasl_initial_response)
                } else {
                    let sasl_response = msg.into_sasl_response()?;
                    PasswordMessageFamily::SASLResponse(sasl_response)
                };

                if state.is_scram() {
                    let scram = self.scram.as_ref().ok_or_else(|| {
                        PgWireError::UnsupportedSASLAuthMethod("SCRAM".to_string())
                    })?;
                    let (res, new_state) = scram.process_scram_message(client, msg, &state).await?;
                    client
                        .send(PgWireBackendMessage::Authentication(res))
                        .await?;
                    *state = new_state;
                } else if state.is_oauth() {
                    let oauth = self.oauth.as_ref().ok_or_else(|| {
                        PgWireError::UnsupportedSASLAuthMethod("OAUTHBEARER".to_string())
                    })?;
                    let (res, new_state) = oauth.process_oauth_message(client, msg, &state).await?;
                    if let Some(res) = res {
                        client
                            .send(PgWireBackendMessage::Authentication(res))
                            .await?;
                    }
                    *state = new_state;
                } else {
                    return Err(PgWireError::InvalidSASLState);
                };

                if matches!(*state, SASLState::Finished) {
                    let (pid, secret_key) = self.pid_secret_key_generator.generate(client);
                    client.set_pid_and_secret_key(pid, secret_key);
                    if let Some(manager) = &self.connection_manager {
                        super::register_connection(client, manager);
                    }
                    super::finish_authentication(client, self.parameter_provider.as_ref()).await?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}
