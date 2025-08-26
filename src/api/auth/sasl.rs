use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use base64::engine::general_purpose::STANDARD;
use base64::Engine;
use futures::{Sink, SinkExt};
use tokio::sync::Mutex;

use crate::api::auth::{AuthSource, Password};
use crate::api::{ClientInfo, PgWireConnectionState};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{Authentication, PasswordMessageFamily};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

use super::{ServerParameterProvider, StartupHandler};

pub mod scram;

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
pub struct SASLAuthStartupHandler<A, P> {
    parameter_provider: Arc<P>,
    /// state of the SASL auth
    state: Mutex<SASLState>,
    /// scram configuration
    scram: scram::ScramAuth<A>,
}

impl<A, P> SASLAuthStartupHandler<A, P> {
    const SCRAM_SHA_256: &str = "SCRAM-SHA-256";
    const SCRAM_SHA_256_PLUS: &str = "SCRAM-SHA-256-PLUS";

    fn supported_mechanisms(&self) -> Vec<String> {
        if self.scram.server_cert_sig.is_some() {
            vec![
                Self::SCRAM_SHA_256.to_owned(),
                Self::SCRAM_SHA_256_PLUS.to_owned(),
            ]
        } else {
            vec![Self::SCRAM_SHA_256.to_owned()]
        }
    }
}

#[async_trait]
impl<A: AuthSource, P: ServerParameterProvider> StartupHandler for SASLAuthStartupHandler<A, P> {
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
                    let (resp, new_state) = self
                        .scram
                        .process_scram_message(client, msg, &state)
                        .await?;
                    client
                        .send(PgWireBackendMessage::Authentication(resp))
                        .await?;
                    *state = new_state;
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

impl<A, P> SASLAuthStartupHandler<A, P> {
    pub fn new(auth_db: Arc<A>, parameter_provider: Arc<P>) -> Self {
        SASLAuthStartupHandler {
            parameter_provider,
            state: Mutex::new(SASLState::Initial),
            scram: scram::ScramAuth {
                auth_db,
                server_cert_sig: None,
                iterations: 4096,
            },
        }
    }

    /// enable channel binding (SCRAM-SHA-256-PLUS) by configuring server
    /// certificate.
    ///
    /// Original pem data is required here. We will decode pem and use the first
    /// certificate as server certificate.
    pub fn configure_certificate(&mut self, certs_pem: &[u8]) -> PgWireResult<()> {
        let sig = scram::compute_cert_signature(certs_pem)?;
        self.scram.server_cert_sig = Some(Arc::new(STANDARD.encode(sig)));
        Ok(())
    }

    /// Set password hash iteration count, according to SCRAM RFC, a minimal of
    /// 4096 is required.
    ///
    /// Note that this implementation does not hash password, it just tells
    /// client to hash with this iteration count. You have to implement password
    /// hashing in your `AuthSource` implementation, either after fetching
    /// cleartext password, or before storing hashed password. And this number
    /// should be identical to your `AuthSource` implementation.
    pub fn set_iterations(&mut self, iterations: usize) {
        self.scram.iterations = iterations;
    }
}
