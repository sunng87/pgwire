use std::sync::Arc;

use bytes::Buf;
use futures::SinkExt;
use futures::StreamExt;
use tokio::net::TcpStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::auth::StartupHandler;
use crate::api::portal::Portal;
use crate::api::query::ExtendedQueryHandler;
use crate::api::query::SimpleQueryHandler;
use crate::api::stmt::Statement;
use crate::api::store::SessionStore;
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
                    // the packet is longer than 8 bytes and is not a
                    // `SslRequest`, so it should be a client without SSL
                    // capability, goto next state directly and try to decode it
                    // as a `Startup` message
                    if src.remaining() >= 8 {
                        *self.client_info.state_mut() = PgWireConnectionState::AwaitingStartup;
                        // re-attempt to decode immediately
                        return self.decode(src);
                    }
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

    fn stmt_store(&self) -> &dyn SessionStore<Arc<Statement>> {
        self.codec().client_info().stmt_store()
    }

    fn stmt_store_mut(&mut self) -> &mut dyn SessionStore<Arc<Statement>> {
        self.codec_mut().client_info_mut().stmt_store_mut()
    }

    fn portal_store(&self) -> &dyn SessionStore<Arc<Portal>> {
        self.codec().client_info().portal_store()
    }

    fn portal_store_mut(&mut self) -> &mut dyn SessionStore<Arc<Portal>> {
        self.codec_mut().client_info_mut().portal_store_mut()
    }
}

async fn process_message<A, Q, EQ>(
    message: PgWireFrontendMessage,
    socket: &mut Framed<TcpStream, PgWireMessageServerCodec>,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
) -> PgWireResult<()>
where
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
    EQ: ExtendedQueryHandler + 'static,
{
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
                debug!("invalid packet received, expected sslrequest");
                socket.close().await?;
            }
        }
        PgWireConnectionState::AwaitingStartup
        | PgWireConnectionState::AuthenticationInProgress => {
            authenticator.on_startup(socket, &message).await?;
        }
        _ => {
            // query or query in progress
            match message {
                PgWireFrontendMessage::Query(ref query) => {
                    query_handler.on_query(socket, query).await?;
                }
                PgWireFrontendMessage::Parse(ref parse) => {
                    extended_query_handler.on_parse(socket, parse).await?;
                }
                PgWireFrontendMessage::Bind(ref bind) => {
                    extended_query_handler.on_bind(socket, bind).await?;
                }
                PgWireFrontendMessage::Execute(ref execute) => {
                    extended_query_handler.on_execute(socket, execute).await?;
                }
                PgWireFrontendMessage::Describe(ref describe) => {
                    extended_query_handler.on_describe(socket, describe).await?;
                }
                PgWireFrontendMessage::Sync(ref sync) => {
                    extended_query_handler.on_sync(socket, sync).await?;
                }
                PgWireFrontendMessage::Close(ref close) => {
                    extended_query_handler.on_close(socket, close).await?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

pub async fn process_socket<A, Q, EQ>(
    tcp_socket: TcpStream,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
) where
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
    EQ: ExtendedQueryHandler + 'static,
{
    if let Ok(addr) = tcp_socket.peer_addr() {
        let client_info = ClientInfoHolder::new(addr);
        let mut socket = Framed::new(tcp_socket, PgWireMessageServerCodec::new(client_info));

        loop {
            match socket.next().await {
                Some(Ok(msg)) => {
                    if let Err(_e) = process_message(
                        msg,
                        &mut socket,
                        authenticator.clone(),
                        query_handler.clone(),
                        extended_query_handler.clone(),
                    )
                    .await
                    {
                        // TODO: error processing
                        // println!("{:?}", e);
                        break;
                    }
                }
                Some(Err(_e)) => {
                    // TODO: logging
                    // println!("{:?}", e);
                    break;
                }
                None => break,
            }
        }
    }
}
