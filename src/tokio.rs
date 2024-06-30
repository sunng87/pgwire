use std::io::{Error as IOError, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Poll};

use bytes::BytesMut;
use futures::future::poll_fn;
use futures::{pin_mut, FutureExt, Sink, SinkExt, Stream, StreamExt};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tokio_rustls::TlsAcceptor;

use crate::api::auth::StartupHandler;
use crate::api::copy::CopyHandler;
use crate::api::query::SimpleQueryHandler;
use crate::api::query::{send_ready_for_query, ExtendedQueryHandler};
use crate::api::{
    ClientInfo, ClientPortalStore, DefaultClient, PgWireConnectionState, PgWireHandlerFactory,
};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::ReadyForQuery;
use crate::messages::response::{SslResponse, TransactionStatus};
use crate::messages::startup::{SslRequest, Startup};
use crate::messages::{Message, PgWireBackendMessage, PgWireFrontendMessage};

const FLUSH_THRESHOLD: usize = 10;
const READ_BUFFER_SIZE: usize = 8192;

#[non_exhaustive]
#[derive(Debug)]
pub struct TokioTcpFrontendConnection<S, W> {
    pub client_info: DefaultClient<S>,
    pub socket: W,
    read_buffer: BytesMut,
    error: bool,
    outbound_sender: UnboundedSender<PgWireBackendMessage>,
    outbound_receiver: UnboundedReceiver<PgWireBackendMessage>,
}

impl<S, W> TokioTcpFrontendConnection<S, W> {
    fn new(client_info: DefaultClient<S>, socket: W) -> TokioTcpFrontendConnection<S, W> {
        let (outbound_sender, outbound_receiver) = unbounded_channel::<PgWireBackendMessage>();
        TokioTcpFrontendConnection {
            client_info,
            socket,
            read_buffer: BytesMut::with_capacity(READ_BUFFER_SIZE),
            error: false,
            outbound_sender,
            outbound_receiver,
        }
    }
}

impl<S, W: AsyncRead + Unpin> TokioTcpFrontendConnection<S, W> {
    async fn poll_socket(&mut self) -> std::io::Result<usize> {
        self.socket.read_buf(&mut self.read_buffer).await
    }

    fn decode(&mut self) -> Result<Option<PgWireFrontendMessage>, PgWireError> {
        match self.client_info.state() {
            PgWireConnectionState::AwaitingStartup => {
                if let Some(request) = SslRequest::decode(&mut self.read_buffer)? {
                    return Ok(Some(PgWireFrontendMessage::SslRequest(request)));
                }

                if let Some(startup) = Startup::decode(&mut self.read_buffer)? {
                    return Ok(Some(PgWireFrontendMessage::Startup(startup)));
                }

                Ok(None)
            }
            _ => PgWireFrontendMessage::decode(&mut self.read_buffer),
        }
    }
}

impl<S, W: AsyncRead + Unpin> Stream for TokioTcpFrontendConnection<S, W> {
    type Item = Result<PgWireFrontendMessage, PgWireError>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.error {
            return Poll::Ready(None);
        }
        let mut conn = self.as_mut();

        let result = {
            let f = conn.poll_socket();
            pin_mut!(f);
            f.poll_unpin(cx)
        };

        match result {
            Poll::Ready(re) => match re {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        // EOF
                        Poll::Ready(None)
                    } else {
                        let decode_result = conn.decode();
                        match decode_result {
                            Ok(Some(msg)) => Poll::Ready(Some(Ok(msg))),
                            Ok(None) => Poll::Pending,
                            Err(e) => {
                                conn.error = true;
                                Poll::Ready(Some(Err(e)))
                            }
                        }
                    }
                }
                Err(e) => {
                    conn.error = true;
                    Poll::Ready(Some(Err(PgWireError::IoError(e))))
                }
            },
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<S, W: AsyncWrite + Unpin + Send> Sink<PgWireBackendMessage>
    for TokioTcpFrontendConnection<S, W>
{
    type Error = IOError;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let pending = self.outbound_receiver.len();
        if pending > FLUSH_THRESHOLD {
            self.poll_flush(cx)
        } else {
            Poll::Ready(Ok(()))
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: PgWireBackendMessage) -> Result<(), Self::Error> {
        self.as_mut()
            .outbound_sender
            .send(item)
            .map_err(|e| IOError::new(ErrorKind::Other, e))
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        let mut conn = self.as_mut();

        // actually write messages to the socket
        while let Some(msg) = ready!(conn.outbound_receiver.poll_recv(cx)) {
            let f = msg.write(&mut conn.socket);
            pin_mut!(f);

            ready!(f.poll_unpin(cx))?;
        }

        Pin::new(&mut conn.socket).poll_flush(cx)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        ready!(Pin::new(&mut self.socket).poll_shutdown(cx)?);

        Poll::Ready(Ok(()))
    }
}

impl<S, W> ClientInfo for TokioTcpFrontendConnection<S, W> {
    fn socket_addr(&self) -> std::net::SocketAddr {
        self.client_info.socket_addr
    }

    fn is_secure(&self) -> bool {
        self.client_info.is_secure
    }

    fn state(&self) -> PgWireConnectionState {
        self.client_info.state
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.client_info.set_state(new_state);
    }

    fn metadata(&self) -> &std::collections::HashMap<String, String> {
        self.client_info.metadata()
    }

    fn metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
        self.client_info.metadata_mut()
    }
}

impl<S, W> ClientPortalStore for TokioTcpFrontendConnection<S, W> {
    type PortalStore = <DefaultClient<S> as ClientPortalStore>::PortalStore;

    fn portal_store(&self) -> &Self::PortalStore {
        self.client_info.portal_store()
    }
}

async fn process_message<S, A, Q, EQ, C>(
    message: PgWireFrontendMessage,
    socket: &mut TokioTcpFrontendConnection<EQ::Statement, S>,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
    copy_handler: Arc<C>,
) -> PgWireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
    A: StartupHandler,
    Q: SimpleQueryHandler,
    EQ: ExtendedQueryHandler,
    C: CopyHandler,
{
    match socket.state() {
        PgWireConnectionState::AwaitingStartup
        | PgWireConnectionState::AuthenticationInProgress => {
            authenticator.on_startup(socket, message).await?;
        }
        // From Postgres docs:
        // When an error is detected while processing any extended-query
        // message, the backend issues ErrorResponse, then reads and discards
        // messages until a Sync is reached, then issues ReadyForQuery and
        // returns to normal message processing.
        PgWireConnectionState::AwaitingSync => {
            if let PgWireFrontendMessage::Sync(sync) = message {
                extended_query_handler.on_sync(socket, sync).await?;
                socket.set_state(PgWireConnectionState::ReadyForQuery);
            }
        }
        PgWireConnectionState::CopyInProgress(is_extended_query) => {
            // query or query in progress
            match message {
                PgWireFrontendMessage::CopyData(copy_data) => {
                    copy_handler.on_copy_data(socket, copy_data).await?;
                }
                PgWireFrontendMessage::CopyDone(copy_done) => {
                    let result = copy_handler.on_copy_done(socket, copy_done).await;
                    if !is_extended_query {
                        // If the copy was initiated from a simple protocol
                        // query, we should leave the CopyInProgress state
                        // before returning the error in order to resume normal
                        // operation after handling it in process_error.
                        socket.set_state(PgWireConnectionState::ReadyForQuery);
                    }
                    match result {
                        Ok(_) => {
                            if !is_extended_query {
                                // If the copy was initiated from a simple protocol
                                // query, notify the client that we are not ready
                                // for the next query.
                                send_ready_for_query(socket, TransactionStatus::Idle).await?
                            } else {
                                // In the extended protocol (at least as
                                // implemented by rust-postgres) we get a Sync
                                // after the CopyDone, so we should let the
                                // on_sync handler send the ReadyForQuery.
                            }
                        }
                        err => return err,
                    }
                }
                PgWireFrontendMessage::CopyFail(copy_fail) => {
                    let error = copy_handler.on_copy_fail(socket, copy_fail).await;
                    if !is_extended_query {
                        // If the copy was initiated from a simple protocol query,
                        // we should leave the CopyInProgress state
                        // before returning the error in order to resume normal
                        // operation after handling it in process_error.
                        socket.set_state(PgWireConnectionState::ReadyForQuery);
                    }
                    return Err(error);
                }
                _ => {}
            }
        }
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
    socket: &mut TokioTcpFrontendConnection<ST, S>,
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
        }
        PgWireError::ApiError(e) => {
            let error_info = ErrorInfo::new("ERROR".to_owned(), "XX000".to_owned(), e.to_string());
            socket
                .feed(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
        }
        _ => {
            // Internal error
            let error_info =
                ErrorInfo::new("FATAL".to_owned(), "XX000".to_owned(), error.to_string());
            socket
                .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
                .await?;
            return socket.close().await;
        }
    }

    if wait_for_sync {
        socket.set_state(PgWireConnectionState::AwaitingSync);
    } else {
        socket
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                TransactionStatus::Idle,
            )))
            .await?;
    }
    socket.flush().await?;

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
    client: &mut TokioTcpFrontendConnection<ST, TcpStream>,
    ssl_supported: bool,
) -> Result<bool, IOError> {
    let mut ssl = false;
    if is_sslrequest_pending(&client.socket).await? {
        // consume the request
        client.next().await;

        let response = if ssl_supported {
            ssl = true;
            PgWireBackendMessage::SslResponse(SslResponse::Accept)
        } else {
            PgWireBackendMessage::SslResponse(SslResponse::Refuse)
        };
        client.send(response).await?;
    }
    Ok(ssl)
}

pub async fn process_socket<H>(
    tcp_socket: TcpStream,
    tls_acceptor: Option<Arc<TlsAcceptor>>,
    handlers: Arc<H>,
) -> Result<(), IOError>
where
    H: PgWireHandlerFactory,
{
    let addr = tcp_socket.peer_addr()?;
    tcp_socket.set_nodelay(true)?;

    let client_info = DefaultClient::new(addr, false);
    let mut client = TokioTcpFrontendConnection::new(client_info, tcp_socket);
    let ssl = peek_for_sslrequest(&mut client, tls_acceptor.is_some()).await?;

    let startup_handler = handlers.startup_handler();
    let simple_query_handler = handlers.simple_query_handler();
    let extended_query_handler = handlers.extended_query_handler();
    let copy_handler = handlers.copy_handler();

    if !ssl {
        while let Some(Ok(msg)) = client.next().await {
            let is_extended_query = match client.state() {
                PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                _ => msg.is_extended_query(),
            };
            if let Err(e) = process_message(
                msg,
                &mut client,
                startup_handler.clone(),
                simple_query_handler.clone(),
                extended_query_handler.clone(),
                copy_handler.clone(),
            )
            .await
            {
                process_error(&mut client, e, is_extended_query).await?;
            }
        }
    } else {
        // mention the use of ssl
        let client_info = DefaultClient::new(addr, true);
        // safe to unwrap tls_acceptor here
        let ssl_socket = tls_acceptor.unwrap().accept(client.socket).await?;
        let mut client = TokioTcpFrontendConnection::new(client_info, ssl_socket);

        while let Some(Ok(msg)) = client.next().await {
            let is_extended_query = match client.state() {
                PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                _ => msg.is_extended_query(),
            };
            if let Err(e) = process_message(
                msg,
                &mut client,
                startup_handler.clone(),
                simple_query_handler.clone(),
                extended_query_handler.clone(),
                copy_handler.clone(),
            )
            .await
            {
                process_error(&mut client, e, is_extended_query).await?;
            }
        }
    }

    Ok(())
}
