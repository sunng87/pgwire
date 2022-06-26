use tokio_util::codec::{Decoder, Encoder};

use crate::messages::startup::{SslRequest, Startup};
use crate::messages::{Message, PgWireMessage};

#[derive(Debug)]
pub enum PgWireConnectionState {
    AwaitingSslRequest,
    AwaitingStartup,
    AuthenticationInProgress,
    ReadyForQuery,
}

#[derive(Debug, new)]
pub struct PgWireMessageServerCodec {
    #[new(value = "PgWireConnectionState::AwaitingSslRequest")]
    state: PgWireConnectionState,
}

impl PgWireMessageServerCodec {
    // user are responsible for updating this state as startup progressing
    pub fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.state = new_state;
    }
}

impl Decoder for PgWireMessageServerCodec {
    type Item = PgWireMessage;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.state {
            PgWireConnectionState::AwaitingSslRequest => {
                if let Some(ssl_request) = SslRequest::decode(src)? {
                    Ok(Some(PgWireMessage::SslRequest(ssl_request)))
                } else {
                    Ok(None)
                }
            }
            PgWireConnectionState::AwaitingStartup => {
                if let Some(startup) = Startup::decode(src)? {
                    Ok(Some(PgWireMessage::Startup(startup)))
                } else {
                    Ok(None)
                }
            }
            _ => PgWireMessage::decode(src),
        }
    }
}

impl Encoder<PgWireMessage> for PgWireMessageServerCodec {
    type Error = std::io::Error;

    fn encode(
        &mut self,
        item: PgWireMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst)
    }
}
