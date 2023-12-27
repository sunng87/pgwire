use std::io::Error as IOError;
use std::sync::Arc;

use bytes::BytesMut;
use futures::future::poll_fn;
use futures::{SinkExt, StreamExt};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::auth::StartupHandler;
use crate::api::query::ExtendedQueryHandler;
use crate::api::query::SimpleQueryHandler;
use crate::api::{ClientInfo, ClientPortalStore, DefaultClient, PgWireConnectionState};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::SslResponse;
use crate::messages::startup::{SslRequest, Startup};
use crate::messages::{Message, PgWireBackendMessage, PgWireFrontendMessage};

#[derive(Debug, new, Getters, Setters, MutGetters)]
#[getset(get = "pub", set = "pub", get_mut = "pub")]
pub struct PgWireMessageServerCodec<S> {
    client_info: DefaultClient<S>,
}

impl<S> Decoder for PgWireMessageServerCodec<S> {
    type Item = PgWireFrontendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match self.client_info.state() {
            PgWireConnectionState::AwaitingStartup => {
                if let Some(request) = SslRequest::decode(src)? {
                    return Ok(Some(PgWireFrontendMessage::SslRequest(request)));
                }

                if let Some(startup) = Startup::decode(src)? {
                    return Ok(Some(PgWireFrontendMessage::Startup(startup)));
                }

                Ok(None)
            }
            _ => PgWireFrontendMessage::decode(src),
        }
    }
}

impl<S> Encoder<PgWireBackendMessage> for PgWireMessageServerCodec<S> {
    type Error = IOError;

    fn encode(
        &mut self,
        item: PgWireBackendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst).map_err(Into::into)
    }
}

impl<T, S> ClientInfo for Framed<T, PgWireMessageServerCodec<S>> {
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
}

impl<T, S> ClientPortalStore for Framed<T, PgWireMessageServerCodec<S>> {
    type PortalStore = <DefaultClient<S> as ClientPortalStore>::PortalStore;

    fn portal_store(&self) -> &Self::PortalStore {
        self.codec().client_info().portal_store()
    }
}

async fn process_message<S, A, Q, EQ>(
    message: PgWireFrontendMessage,
    socket: &mut Framed<S, PgWireMessageServerCodec<EQ::Statement>>,
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
            authenticator.on_startup(socket, message).await?;
        }
        // From Postgres docs:
        // When an error is detected while processing any extended-query
        // message, the backend issues ErrorResponse, then reads and discards
        // messages until a Sync is reached, then issues ReadyForQuery and
        // returns to normal message processing.
        PgWireConnectionState::AwaitingSync => match message {
            PgWireFrontendMessage::Sync(sync) => {
                extended_query_handler.on_sync(socket, sync).await?;
                socket.set_state(PgWireConnectionState::ReadyForQuery);
            }
            _ => {}
        },
        _ => {
            // query or query in progress
            match message {
                PgWireFrontendMessage::Query(query) => {
                    query_handler.on_query(socket, query).await?;
                }
                PgWireFrontendMessage::Parse(parse) => {
                    extended_query_handler.on_parse(socket, parse).await?;
                }
                PgWireFrontendMessage::Bind(bind) => {
                    extended_query_handler.on_bind(socket, bind).await?;
                }
                PgWireFrontendMessage::Execute(execute) => {
                    extended_query_handler.on_execute(socket, execute).await?;
                }
                PgWireFrontendMessage::Describe(describe) => {
                    extended_query_handler.on_describe(socket, describe).await?;
                }
                PgWireFrontendMessage::Sync(sync) => {
                    extended_query_handler.on_sync(socket, sync).await?;
                }
                PgWireFrontendMessage::Close(close) => {
                    extended_query_handler.on_close(socket, close).await?;
                }
                _ => {}
            }
        }
    }
    Ok(())
}

async fn process_error<S, ST>(
    socket: &mut Framed<S, PgWireMessageServerCodec<ST>>,
    error: PgWireError,
    wait_for_sync: bool,
) -> Result<(), IOError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
{
    match error {
        PgWireError::UserError(error_info) => {
            socket
                .feed(PgWireBackendMessage::ErrorResponse((*error_info).into()))
                .await?;
            socket.flush().await?;
        }
        PgWireError::ApiError(e) => {
            let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
            socket
                .feed(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            socket.flush().await?;
        }
        _ => {
            // Internal error
            let error_info =
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string());
            socket
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            socket.close().await?;
        }
    }
    if wait_for_sync {
        socket.set_state(PgWireConnectionState::AwaitingSync);
    }
    Ok(())
}

async fn is_sslrequest_pending(tcp_socket: &TcpStream) -> Result<bool, IOError> {
    let mut buf = [0u8; SslRequest::BODY_SIZE];
    let mut buf = ReadBuf::new(&mut buf);
    while buf.filled().len() < SslRequest::BODY_SIZE {
        if poll_fn(|cx| tcp_socket.poll_peek(cx, &mut buf)).await? == 0 {
            // the tcp_stream has ended
            return Ok(false);
        }
    }

    let mut buf = BytesMut::from(buf.filled());
    if let Ok(Some(_)) = SslRequest::decode(&mut buf) {
        return Ok(true);
    }
    Ok(false)
}

async fn peek_for_sslrequest<ST>(
    socket: &mut Framed<TcpStream, PgWireMessageServerCodec<ST>>,
    ssl_supported: bool,
) -> Result<bool, IOError> {
    let mut ssl = false;
    if is_sslrequest_pending(socket.get_ref()).await? {
        // consume request
        socket.next().await;

        let response = if ssl_supported {
            ssl = true;
            PgWireBackendMessage::SslResponse(SslResponse::Accept)
        } else {
            PgWireBackendMessage::SslResponse(SslResponse::Refuse)
        };
        socket.send(response).await?;
    }
    Ok(ssl)
}

pub async fn process_socket<A, Q, EQ>(
    tcp_socket: TcpStream,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    startup_handler: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
) -> Result<(), IOError>
where
    A: StartupHandler + 'static,
    Q: SimpleQueryHandler + 'static,
    EQ: ExtendedQueryHandler + 'static,
{
    let addr = tcp_socket.peer_addr()?;
    tcp_socket.set_nodelay(true)?;

    let client_info = DefaultClient::new(addr, false);
    let mut tcp_socket = Framed::new(tcp_socket, PgWireMessageServerCodec::new(client_info));
    let ssl = peek_for_sslrequest(&mut tcp_socket, tls_acceptor.is_some()).await?;

    if !ssl {
        // use an already configured socket.
        let mut socket = tcp_socket;

        while let Some(Ok(msg)) = socket.next().await {
            let is_extended_query = msg.is_extended_query();
            if let Err(e) = process_message(
                msg,
                &mut socket,
                startup_handler.clone(),
                query_handler.clone(),
                extended_query_handler.clone(),
            )
            .await
            {
                process_error(&mut socket, e, is_extended_query).await?;
            }
        }
    } else {
        // mention the use of ssl
        let client_info = DefaultClient::new(addr, true);
        // safe to unwrap tls_acceptor here
        let ssl_socket = tls_acceptor
            .unwrap()
            .accept(tcp_socket.into_inner())
            .await?;
        let mut socket = Framed::new(ssl_socket, PgWireMessageServerCodec::new(client_info));

        while let Some(Ok(msg)) = socket.next().await {
            let is_extended_query = msg.is_extended_query();
            if let Err(e) = process_message(
                msg,
                &mut socket,
                startup_handler.clone(),
                query_handler.clone(),
                extended_query_handler.clone(),
            )
            .await
            {
                process_error(&mut socket, e, is_extended_query).await?;
            }
        }
    }

    Ok(())
}
