use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Sink, SinkExt};

use crate::messages::startup::Authentication;
use crate::{
    api::ClientInfo,
    error::{PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage},
};

use super::{ServerParameterProvider, StartupHandler};

pub enum ScramState {
    Initial,
    // cache client-first-message-bare
    ClientFirstReceived(String),
    // cache salted-password and server-first-message
    ServerFirstSent(Vec<u8>, String),
    ClientFinalReceived,
    ServerFinalSent,
}

#[derive(new)]
pub struct SASLScramAuthStartupHandler<A, P> {
    auth_db: Arc<A>,
    parameter_provider: P,
    /// state of the client-server communication
    state: Mutex<ScramState>,
}

/// This trait abstracts an authentication database for SCRAM authentication
/// mechanism.
#[async_trait]
pub trait AuthDB: Send + Sync {
    /// Fetch password and add salt, this step is defined in
    /// [RFC5802](https://www.rfc-editor.org/rfc/rfc5802#section-3)
    ///
    /// ```text
    /// SaltedPassword  := Hi(Normalize(password), salt, i)
    /// ```
    ///
    /// The implementation should first retrieve password from its storage and
    /// compute it into SaltedPassword
    async fn get_salted_password(&self, username: &str, salt: &[u8], iterations: usize) -> Vec<u8>;
}

/// compute salted password from raw password
pub fn salt_password(password: &[u8], salt: &[u8], iters: usize) -> Vec<u8> {
    todo!()
}

#[async_trait]
impl<A: AuthDB, P: ServerParameterProvider> StartupHandler for SASLScramAuthStartupHandler<A, P> {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                super::save_startup_parameters_to_metadata(client, startup);
                client
                    .send(PgWireBackendMessage::Authentication(Authentication::SASL(
                        vec!["SCRAM-SHA-256".to_owned()],
                    )))
                    .await?;
            }
            PgWireFrontendMessage::PasswordMessageFamily(msg) => {
                // this should never block
                let state = self.state.lock().unwrap();
                if matches!(*state, ScramState::Initial) {
                    // initial response, client_first
                    let resp = msg.into_sasl_initial_response()?;
                    let method = resp.auth_method();
                    // parse into client_first
                    let client_first = resp
                        .data()
                        .ok_or_else(|| {
                            PgWireError::InvalidScramMessage("Empty client-first".to_owned())
                        })
                        .and_then(|data| {
                            ClientFirst::try_new(String::from_utf8_lossy(data.as_ref()).as_ref())
                        })?;
                    *state = ScramState::ClientFirstReceived(client_first.bare());

                    // create server_first and send
                } else if let ScramState::ServerFirstSent(ref salted_pass, ref server_first) =
                    *state
                {
                    // second response, client_final
                    let resp = msg.into_sasl_response()?;
                    let client_final = resp.data();

                    self.state = ScramState::ClientFinalReceived;
                }
            }

            _ => {}
        }
        Ok(())
    }
}

#[derive(Debug)]
struct ClientFirst {
    cbind_flag: char,
    auth_zid: String,
    username: String,
    nonce: String,
}

impl ClientFirst {
    fn try_new(s: &str) -> PgWireResult<ClientFirst> {
        let parts: Vec<&str> = s.splitn(4, ',').collect();
        if parts.len() != 4
            || parts[0].len() != 1
            || !parts[2].starts_with("n=")
            || !parts[3].starts_with("r=")
        {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        }
        // now it's safe to unwrap
        let cbind_flag = parts[0].chars().nth(0).unwrap();
        let auth_zid = parts[1].to_owned();
        let username = parts[2].strip_prefix("n=").unwrap().to_owned();
        let nonce = parts[3].strip_prefix("r=").unwrap().to_owned();

        Ok(ClientFirst {
            cbind_flag,
            auth_zid,
            username,
            nonce,
        })
    }

    fn bare(&self) -> String {
        format!("n={},r={}", self.username, self.nonce)
    }
}

#[derive(Debug, new)]
struct ServerFirst {
    nonce: String,
    salt: String,
    iteration: usize,
}

impl ServerFirst {
    fn server_first_message(&self) -> String {
        format!("r={},s={},i={}", self.nonce, self.salt, self.iteration)
    }
}

#[derive(Debug)]
struct ClientFinal {
    channel_binding: String,
    nonce: String,
    proof: String,
}

impl ClientFinal {
    fn try_new(s: &str) -> PgWireResult<ClientFinal> {
        let parts: Vec<&str> = s.splitn(3, ',').collect();
        if parts.len() != 3
            || !parts[0].starts_with("c=")
            || !parts[1].starts_with("r=")
            || !parts[2].starts_with("p=")
        {
            return Err(PgWireError::InvalidScramMessage(s.to_owned()));
        }

        // safe to unwrap after check

        let channel_binding = parts[0].strip_prefix("c=").unwrap().to_owned();
        let nonce = parts[1].strip_prefix("r=").unwrap().to_owned();
        let proof = parts[2].strip_prefix("p=").unwrap().to_owned();

        Ok(ClientFinal {
            channel_binding,
            nonce,
            proof,
        })
    }

    fn with_proof(&self) -> String {
        format!("c={},r={}", self.channel_binding, self.nonce)
    }
}

#[derive(Debug)]
struct ServerFinal {
    verifier: String,
}

impl ServerFinal {
    fn server_final_message(&self) -> String {
        format!("v={}", self.verifier)
    }
}
