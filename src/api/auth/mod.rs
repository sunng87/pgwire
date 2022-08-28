use std::collections::HashMap;
use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::{Sink, SinkExt};
use futures::stream;
use rand;

use super::{ClientInfo, PgWireConnectionState};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::response::{ReadyForQuery, READY_STATUS_IDLE};
use crate::messages::startup::{Authentication, BackendKeyData, ParameterStatus, Startup};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

// Alternative design: pass PgWireMessage into the trait and allow the
// implementation to track and define state within itself. This allows better
// support for other auth type like sasl.
#[async_trait]
pub trait StartupHandler: Send + Sync {
    /// A generic frontend message callback during startup phase.
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
        let mut messages = vec![PgWireBackendMessage::Authentication(Authentication::Ok)];
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

pub mod cleartext;

// TODO: md5, scram-sha-256(sasl)
