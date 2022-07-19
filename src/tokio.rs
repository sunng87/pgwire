use std::net::SocketAddr;
use std::sync::Arc;

use futures::SinkExt;
use futures::StreamExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::auth::StartupHandler;
use crate::api::query::SimpleQueryHandler;
use crate::api::{ClientInfo, ClientInfoHolder, PgWireConnectionState};
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{SslRequest, Startup};
use crate::messages::{Message, PgWireBackendMessage, PgWireFrontendMessage};

#[derive(Debug, new, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct PgWireMessageServerCodec {
    client_info: ClientInfoHolder,
}

impl Decoder for PgWireMessageServerCodec {
    type Item = PgWireFrontendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.client_info.state() {
            PgWireConnectionState::AwaitingSslRequest => {
                if let Some(ssl_request) = SslRequest::decode(src)? {
                    Ok(Some(PgWireFrontendMessage::SslRequest(ssl_request)))
                } else {
                    Ok(None)
                }
            }
            PgWireConnectionState::AwaitingStartup => {
                if let Some(startup) = Startup::decode(src)? {
                    Ok(Some(PgWireFrontendMessage::Startup(startup)))
                } else {
                    Ok(None)
                }
            }
            _ => PgWireFrontendMessage::decode(src),
        }
    }
}

impl Encoder<PgWireBackendMessage> for PgWireMessageServerCodec {
    type Error = PgWireError;

    fn encode(
        &mut self,
        item: PgWireBackendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst)
    }
}

impl<T> ClientInfo for Framed<T, PgWireMessageServerCodec> {
    fn socket_addr(&self) -> &std::net::SocketAddr {
        self.codec().client_info().socket_addr()
    }

    fn state(&self) -> &PgWireConnectionState {
        self.codec().client_info().state()
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.codec_mut().client_info_mut().set_state(new_state);
    }

    fn metadata(&self) -> &std::collections::HashMap<String, String> {
        self.codec().client_info().metadata()
    }

    fn metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
        self.codec_mut().client_info_mut().metadata_mut()
    }
}

async fn process_message<A, Q>(
    message: PgWireFrontendMessage,
    socket: &mut Framed<TcpStream, PgWireMessageServerCodec>,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
) -> PgWireResult<()>
where
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
{
    println!("{:?}", message);
    match socket.codec().client_info().state() {
        PgWireConnectionState::AwaitingSslRequest => {
            if matches!(message, PgWireFrontendMessage::SslRequest(_)) {
                socket
                    .codec_mut()
                    .client_info_mut()
                    .set_state(PgWireConnectionState::AwaitingStartup);
                socket.send(PgWireBackendMessage::SslResponse(b'N')).await?;
            } else {
                // TODO: raise error here for invalid packet read
                socket.close().await?;
            }
        }
        PgWireConnectionState::AwaitingStartup
        | PgWireConnectionState::AuthenticationInProgress => {
            authenticator.on_startup(socket, &message).await?;
        }
        _ => {
            if let PgWireFrontendMessage::Query(ref query) = message {
                query_handler.on_query(socket, query).await?;
            } else {
                //todo:
            }
        }
    }
    Ok(())
}

pub fn process_socket<A, Q>(
    incoming_socket: (TcpStream, SocketAddr),
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
) where
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
{
    let (raw_socket, addr) = incoming_socket;
    tokio::spawn(async move {
        let client_info = ClientInfoHolder::new(addr);
        let mut socket = Framed::new(raw_socket, PgWireMessageServerCodec::new(client_info));

        loop {
            match socket.next().await {
                Some(Ok(msg)) => {
                    if let Err(e) = process_message(
                        msg,
                        &mut socket,
                        authenticator.clone(),
                        query_handler.clone(),
                    )
                    .await
                    {
                        // TODO: error processing
                        println!("{:?}", e);
                        break;
                    }
                }
                Some(Err(e)) => {
                    // TODO: logging
                    println!("{:?}", e);
                    break;
                }
                None => break,
            }
        }
    });
}
