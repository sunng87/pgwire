use std::sync::Arc;

use bytes::Buf;
use futures::SinkExt;
use futures::StreamExt;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::auth::StartupHandler;
use crate::api::portal::Portal;
use crate::api::query::ExtendedQueryHandler;
use crate::api::query::SimpleQueryHandler;
use crate::api::stmt::Statement;
use crate::api::store::SessionStore;
use crate::api::{ClientInfo, ClientInfoHolder, PgWireConnectionState};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::ReadyForQuery;
use crate::messages::response::READY_STATUS_IDLE;
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

    fn is_secure(&self) -> bool {
        *self.codec().client_info().is_secure()
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

async fn process_message<S, A, Q, EQ>(
    message: PgWireFrontendMessage,
    socket: &mut Framed<S, PgWireMessageServerCodec>,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
) -> PgWireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
    EQ: ExtendedQueryHandler + 'static,
{
    match socket.codec().client_info().state() {
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

async fn process_error<S>(socket: &mut Framed<S, PgWireMessageServerCodec>, error: PgWireError)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    match error {
        PgWireError::UserError(error_info) => {
            let _ = socket
                .feed(PgWireBackendMessage::ErrorResponse((*error_info).into()))
                .await;
            let _ = socket
                .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    READY_STATUS_IDLE,
                )))
                .await;
            let _ = socket.flush().await;
        }
        PgWireError::ApiError(e) => {
            let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
            let _ = socket
                .feed(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await;
            let _ = socket
                .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                    READY_STATUS_IDLE,
                )))
                .await;
            let _ = socket.flush().await;
        }
        _ => {
            // Internal error
            let error_info =
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string());
            let _ = socket
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await;
            let _ = socket.close().await;
        }
    }
}

pub async fn process_socket<A, Q, EQ>(
    mut tcp_socket: TcpStream,
    tls_acceptor: Option<TlsAcceptor>,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
) where
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
    EQ: ExtendedQueryHandler + 'static,
{
    if let Ok(addr) = tcp_socket.peer_addr() {
        let mut ssl = false;
        let mut buf = [0u8; SslRequest::BODY_SIZE];
        loop {
            if let Ok(size) = tcp_socket.peek(&mut buf).await {
                if size == SslRequest::BODY_SIZE {
                    let mut buf_ref = buf.as_ref();
                    // skip first 4 bytes
                    let _ = buf_ref.get_i32();
                    if buf_ref.get_i32() == SslRequest::BODY_MAGIC_NUMBER {
                        // the socket is sending sslrequest, read the first 8 bytes
                        // skip first 8 bytes
                        let _ = tcp_socket.read(&mut [0u8; SslRequest::BODY_SIZE]).await;
                        // ssl supported
                        if tls_acceptor.is_some() {
                            ssl = true;
                            let _ = tcp_socket.write(b"S").await;
                        } else {
                            let _ = tcp_socket.write(b"N").await;
                        }
                    }
                    break;
                }
            } else {
                return;
            }
        }

        if ssl {
            // safe to unwrap tls_acceptor here
            if let Ok(ssl_socket) = tls_acceptor.unwrap().accept(tcp_socket).await {
                let client_info = ClientInfoHolder::new(addr, true);
                let mut socket =
                    Framed::new(ssl_socket, PgWireMessageServerCodec::new(client_info));

                while let Some(Ok(msg)) = socket.next().await {
                    if let Err(e) = process_message(
                        msg,
                        &mut socket,
                        authenticator.clone(),
                        query_handler.clone(),
                        extended_query_handler.clone(),
                    )
                    .await
                    {
                        process_error(&mut socket, e).await;
                    }
                }
            }
        } else {
            let client_info = ClientInfoHolder::new(addr, false);
            let mut socket = Framed::new(tcp_socket, PgWireMessageServerCodec::new(client_info));

            while let Some(Ok(msg)) = socket.next().await {
                if let Err(e) = process_message(
                    msg,
                    &mut socket,
                    authenticator.clone(),
                    query_handler.clone(),
                    extended_query_handler.clone(),
                )
                .await
                {
                    process_error(&mut socket, e).await;
                }
            }
        }
    }
}
