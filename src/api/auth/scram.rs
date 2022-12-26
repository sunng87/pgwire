use std::fmt::Debug;
use std::sync::atomic::{AtomicU8, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use futures::{Sink, SinkExt};

use crate::messages::startup::Authentication;
use crate::{
    api::ClientInfo,
    error::{PgWireError, PgWireResult},
    messages::{PgWireBackendMessage, PgWireFrontendMessage},
};

use super::{PasswordVerifier, ServerParameterProvider, StartupHandler};

#[derive(new)]
pub struct SASLScramAuthStartupHandler<A, P> {
    auth_db: Arc<A>,
    parameter_provider: P,
    state: AtomicU8,

    /// cached salted_password
    salted_password: Option<Vec<u8>>,
    /// client-first-message-bare
    client_first: Option<String>,
    /// server-first-message
    server_first: Option<String>,
    /// client-final-message-without-proof
    client_final: Option<String>,
}

#[async_trait]
pub trait AuthDB {
    /// Fetch password and add salt, this step is defined in RFC5802
    ///
    /// ```text
    /// SaltedPassword  := Hi(Normalize(password), salt, i)
    /// ```
    ///
    /// The implementation should first retrieve password from its storage and
    /// compute it into SaltedPassword
    async fn get_salted_password(&self, username: &str, salt: &[u8], iterations: usize) -> Vec<u8>;
}

///
pub fn salt_password(password: &[u8], salt: &[u8], iters: usize) -> Vec<u8> {}

#[async_trait]
impl<V: PasswordVerifier, P: ServerParameterProvider> StartupHandler
    for SASLScramAuthStartupHandler<V, P>
{
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
                if self.state.load(Ordering::Relaxed) == 0 {
                    // initial response, client_first
                    let resp = msg.into_sasl_initial_response()?;
                    let method = resp.auth_method();
                    let client_first = resp.data();

                    self.state.fetch_add(Ordering::Relaxed, 1);
                } else {
                    // second response, client_final
                    let resp = msg.into_sasl_response()?;
                    let client_final = resp.data();
                }
            }

            _ => {}
        }
        Ok(())
    }
}
