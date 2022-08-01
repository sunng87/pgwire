use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use rand;

use super::{ClientInfo, PgWireConnectionState};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ErrorResponse, ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus, Startup};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

// Alternative design: pass PgWireMessage into the trait and allow the
// implementation to track and define state within itself. This allows better
// support for other auth type like sasl.
#[async_trait]
pub trait StartupHandler: Send + Sync {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>;

    fn handle_startup_parameters<C>(&self, client: &mut C, startup_message: &Startup)
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
    {
        client.metadata_mut().extend(
            startup_message
                .parameters()
                .iter()
                .map(|(k, v)| (k.to_owned(), v.to_owned())),
        );
    }

    async fn finish_authentication<C>(&self, client: &mut C)
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
    {
        client.set_state(PgWireConnectionState::ReadyForQuery);
        let mut messages = Vec::new();
        messages.push(PgWireBackendMessage::Authentication(Authentication::Ok));
        for (k, v) in self.server_parameters(client) {
            messages.push(PgWireBackendMessage::ParameterStatus(ParameterStatus::new(
                k, v,
            )));
        }

        // TODO: store this backend key
        messages.push(PgWireBackendMessage::BackendKeyData(BackendKeyData::new(
            std::process::id() as i32,
            rand::random::<i32>(),
        )));
        messages.push(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
            READY_STATUS_IDLE,
        )));
        let mut message_stream = stream::iter(messages.into_iter().map(Ok));
        client.send_all(&mut message_stream).await.unwrap();
    }

    fn server_parameters<C>(&self, _client: &C) -> HashMap<String, String>
    where
        C: ClientInfo;
}

#[async_trait]
pub trait CleartextPasswordAuthStartupHandler: StartupHandler {
    async fn verify_password(&self, password: &str) -> PgWireResult<bool>;

    fn server_parameters<C>(&self, _client: &C) -> HashMap<String, String>
    where
        C: ClientInfo;
}

#[async_trait]
impl<T> StartupHandler for T
where
    T: CleartextPasswordAuthStartupHandler,
{
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: &PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match message {
            PgWireFrontendMessage::Startup(ref startup) => {
                self.handle_startup_parameters(client, startup);
                client.set_state(PgWireConnectionState::AuthenticationInProgress);
                client
                    .send(PgWireBackendMessage::Authentication(
                        Authentication::CleartextPassword,
                    ))
                    .await?;
            }
            PgWireFrontendMessage::Password(ref pwd) => {
                if let Ok(true) = self.verify_password(pwd.password()).await {
                    self.finish_authentication(client).await
                } else {
                    let info = vec![
                        (b'L', "FATAL".to_owned()),
                        (b'T', "FATAL".to_owned()),
                        (b'C', "28P01".to_owned()),
                        (b'M', "Password authentication failed".to_owned()),
                        (b'R', "auth_failed".to_owned()),
                    ];
                    let error = ErrorResponse::new(info);

                    client
                        .feed(PgWireBackendMessage::ErrorResponse(error))
                        .await?;
                    client.close().await?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn server_parameters<C>(&self, _client: &C) -> HashMap<String, String>
    where
        C: ClientInfo,
    {
        CleartextPasswordAuthStartupHandler::server_parameters(self, _client)
    }
}

// TODO: md5, scram-sha-256(sasl)
