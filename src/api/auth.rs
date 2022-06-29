use std::fmt::Debug;

use async_trait::async_trait;
use futures::sink::Sink;

use super::ClientInfo;
use crate::messages::PgWireMessage;

// Alternative design: pass PgWireMessage into the trait and allow the
// implementation to track and define state within itself. This allows better
// support for other auth type like sasl.

#[async_trait]
pub trait Authenticator {
    // TODO: move connection state into ClientInfo
    async fn on_startup<C>(&self, client: &mut C, message: &PgWireMessage) -> Result<(), std::io::Error>
    where
        C: ClientInfo + Sink<PgWireMessage> + Unpin + Debug + Send, C::Error: Debug;
}

// TODO: password handler
