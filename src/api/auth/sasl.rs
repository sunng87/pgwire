use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{Sink, SinkExt};
use tokio::sync::Mutex;

use crate::api::auth::Password;
use crate::api::{ClientInfo, PgWireConnectionState};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{Authentication, PasswordMessageFamily};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::{ServerParameterProvider, StartupHandler};

pub mod scram;
pub mod oauth;

#[derive(Debug)]
pub enum SASLState {
    Initial,
    // scram authentication method selected
    ScramClientFirstReceived,
    // cached password, channel_binding and partial auth-message
    ScramServerFirstSent(Password, String, String),
    // finished
    Finished,
}

impl SASLState {
    fn is_scram(&self) -> bool {
        matches!(
            self,
            SASLState::ScramClientFirstReceived | SASLState::ScramServerFirstSent(_, _, _)
        )
    }
}

#[derive(Debug)]
pub struct SASLAuthStartupHandler<P> {
    parameter_provider: Arc<P>,
    /// state of the SASL auth
    state: Mutex<SASLState>,
    /// scram configuration
    scram: Option<scram::ScramAuth>,
    /// oauth configuration
    // TODO
    oauth: Option<String>,
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
                if let SASLState::Initial = *state {
                    let sasl_initial_response = msg.into_sasl_initial_response()?;
                    let selected_mechanism = sasl_initial_response.auth_method.as_str();

                    // TODO: include the oauth mechanism, but I am not sure if the state should still be
                    // ScramClientFirstReceived. when I am respomding with the SASLInitialResponse, I have to include the `auth`
                    // field. that means I have to check if the selected mechanism is oauth, then construct the SASLInitialResponse.
                    // then, I'll handle the  AuthenticationSASLContinue message type (add it to the PasswordMessageFamily enum)
                    if [Self::SCRAM_SHA_256, Self::SCRAM_SHA_256_PLUS].contains(&selected_mechanism)
                    {
                        *state = SASLState::ScramClientFirstReceived;
                    } else {
                        return Err(PgWireError::UnsupportedSASLAuthMethod(
                            selected_mechanism.to_string(),
                        ));
                    }

                    msg = PasswordMessageFamily::SASLInitialResponse(sasl_initial_response);
                } else {
                    let sasl_response = msg.into_sasl_response()?;
                    msg = PasswordMessageFamily::SASLResponse(sasl_response);
                }

                // SCRAM authentication
                if state.is_scram() {
                    if let Some(scram) = &self.scram {
                        let (resp, new_state) =
                            scram.process_scram_message(client, msg, &state).await?;
                        client
                            .send(PgWireBackendMessage::Authentication(resp))
                            .await?;
                        *state = new_state;
                    } else {
                        // scram is not configured
                        return Err(PgWireError::UnsupportedSASLAuthMethod("SCRAM".to_string()));
                    }
                }

                if matches!(*state, SASLState::Finished) {
                    super::finish_authentication(client, self.parameter_provider.as_ref()).await?;
                }
            }
            _ => {}
        }

        Ok(())
    }
}

impl<P> SASLAuthStartupHandler<P> {
    pub fn new(parameter_provider: Arc<P>) -> Self {
        SASLAuthStartupHandler {
            parameter_provider,
            state: Mutex::new(SASLState::Initial),
            scram: None,
            oauth: None,
        }
    }

    pub fn with_scram(mut self, scram_auth: scram::ScramAuth) -> Self {
        self.scram = Some(scram_auth);
        self
    }

    const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
    const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";
    const OAUTHBEARER: &str = "OAUTHBEARER";

    fn supported_mechanisms(&self) -> Vec<String> {
        let mut mechanisms = vec![];

        if let Some(scram) = &self.scram {
            mechanisms.push(Self::SCRAM_SHA_256.to_owned());

            if scram.server_cert_sig.is_some() {
                mechanisms.push(Self::SCRAM_SHA_256_PLUS.to_owned());
            }
        }

        if let Some(oauth) = &self.oauth {
            mechanisms.push(Self::OAUTHBEARER.to_owned());
        }

        mechanisms
    }
}
