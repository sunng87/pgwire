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
pub struct SASLScramAuthStartupHandler<V, P> {
    verifier: V,
    parameter_provider: P,
    state: AtomicU8,
}

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
