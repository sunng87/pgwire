use std::collections::BTreeMap;
use std::io::{Error as IOError, ErrorKind};
use std::net::SocketAddr;
#[cfg(unix)]
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Sink, SinkExt, Stream, StreamExt};
use pin_project::pin_project;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use tokio_rustls::client::TlsStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::TlsConnector;
use crate::api::client::auth::StartupHandler;
use crate::api::client::config::Host;
use crate::api::client::query::SimpleQueryHandler;
use crate::api::client::{ClientInfo, Config, ReadyState, ServerInformation};
use crate::error::{PgWireClientError, PgWireClientResult, PgWireError};
use crate::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
    SslNegotiationMetaMessage,
};

#[non_exhaustive]
#[derive(Debug, Default)]
pub struct PgWireMessageClientCodec {
    decode_context: DecodeContext,
}

impl Decoder for PgWireMessageClientCodec {
    type Item = PgWireBackendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        PgWireBackendMessage::decode(src, &self.decode_context)
    }
}

impl Encoder<PgWireFrontendMessage> for PgWireMessageClientCodec {
    type Error = PgWireError;

    fn encode(
        &mut self,
        item: PgWireFrontendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            // check special messages for decoding state
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresSsl(_)) => {
                self.decode_context.awaiting_backend_ssl_response = true;
            }
            PgWireFrontendMessage::SslNegotiation(SslNegotiationMetaMessage::PostgresGss(_)) => {
                self.decode_context.awaiting_backend_gss_response = true;
            }
            _ => {
                self.decode_context.awaiting_backend_ssl_response = false;
                self.decode_context.awaiting_backend_gss_response = false;
            }
        }

        item.encode(dst)
    }
}

#[pin_project]
pub struct PgWireClient {
    socket: Framed<ClientSocket, PgWireMessageClientCodec>,
    config: Arc<Config>,
    server_information: ServerInformation,
}

impl ClientInfo for PgWireClient {
    fn config(&self) -> &Config {
        &self.config
    }

    fn server_parameters(&self) -> &BTreeMap<String, String> {
        &self.server_information.parameters
    }

    fn process_id(&self) -> i32 {
        self.server_information.process_id
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.socket.codec().decode_context.protocol_version
    }
}

impl Sink<PgWireFrontendMessage> for PgWireClient {
    type Error =
        <Framed<ClientSocket, PgWireMessageClientCodec> as Sink<PgWireFrontendMessage>>::Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(self.project().socket).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: PgWireFrontendMessage) -> Result<(), Self::Error> {
        Pin::new(self.project().socket).start_send(item)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(self.project().socket).poll_flush(cx)
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Pin::new(self.project().socket).poll_close(cx)
    }
}

impl PgWireClient {
    /// Connect to server via TCP, optional TLS, and performance Postgres
    /// startup process.
    pub async fn connect<S>(
        config: Arc<Config>,
        mut startup_handler: S,
        tls_connector: Option<TlsConnector>,
    ) -> PgWireClientResult<PgWireClient>
    where
        S: StartupHandler,
    {
        // tcp connect
        let mut socket = match get_addr(&config)? {
            PgSocketAddr::Ip(socket_addr) => {
                ClientSocket::Plain(TcpStream::connect(socket_addr).await?)
            }
            PgSocketAddr::Host(socket_addr) => {
                ClientSocket::Plain(TcpStream::connect(socket_addr).await?)
            }
            #[cfg(unix)]
            PgSocketAddr::Unix(socket_addr) => {
                ClientSocket::Unix(UnixStream::connect(socket_addr).await?)
            }
        };
        if let ClientSocket::Plain(tcp_socket) = socket {
            // perform ssl handshake based on postgres configuration
            // if tls is not enabled, just return the socket and perform startup
            // directly
            socket = ssl_handshake(tcp_socket, &config, tls_connector).await?;
        };
        let socket = Framed::new(socket, PgWireMessageClientCodec::default());

        let mut client = PgWireClient {
            socket,
            config: config.clone(),
            server_information: ServerInformation::default(),
        };

        startup_handler.startup(&mut client).await?;
        // loop until finished
        while let Some(message_result) = client.socket.next().await {
            let message = message_result?;

            if let ReadyState::Ready(server_info) =
                startup_handler.on_message(&mut client, message).await?
            {
                client.server_information = server_info;
                return Ok(client);
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Start a query with simple query subprotocol
    pub async fn simple_query<H>(
        &mut self,
        mut simple_query_handler: H,
        query: &str,
    ) -> PgWireClientResult<Vec<H::QueryResponse>>
    where
        H: SimpleQueryHandler,
    {
        simple_query_handler.simple_query(self, query).await?;

        while let Some(message_result) = self.next().await {
            let message = message_result?;

            if let ReadyState::Ready(responses) =
                simple_query_handler.on_message(self, message).await?
            {
                return Ok(responses);
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Parse a query to server side
    pub async fn parse<H>(
        &mut self,
        mut extended_query_handler: H,
        name: Option<&str>,
        query: &str,
    ) -> PgWireClientResult<()> {
        todo!()
    }

    /// Execute the query
    pub async fn execute<H>(
        &mut self,
        mut extended_query_handler: H,
        name: Option<&str>,
        parameters: Vec<String>, //TODO
    ) -> PgWireClientResult<()> {
        todo!()
    }
}

impl Stream for PgWireClient {
    type Item = Result<PgWireBackendMessage, PgWireError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(self.project().socket).poll_next(cx)
    }
}

#[pin_project(project = ClientSocketProj)]
pub enum ClientSocket {
    Plain(#[pin] TcpStream),
    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    Secure(#[pin] Box<TlsStream<TcpStream>>),
    #[cfg(unix)]
    Unix(#[pin] UnixStream),
}

impl AsyncRead for ClientSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_read(cx, buf),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_read(cx, buf),
            #[cfg(unix)]
            ClientSocketProj::Unix(socket) => socket.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientSocket {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_write(cx, buf),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_write(cx, buf),
            #[cfg(unix)]
            ClientSocketProj::Unix(tls_socket) => tls_socket.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_flush(cx),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_flush(cx),
            #[cfg(unix)]
            ClientSocketProj::Unix(tls_socket) => tls_socket.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_shutdown(cx),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_shutdown(cx),
            #[cfg(unix)]
            ClientSocketProj::Unix(tls_socket) => tls_socket.poll_shutdown(cx),
        }
    }
}

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
async fn connect_tls(
    socket: TcpStream,
    config: &Config,
    tls_connector: TlsConnector,
) -> PgWireClientResult<ClientSocket> {
    use crate::api::client::config::SslNegotiation;
    // alpn check for direct connect
    if config.ssl_negotiation == SslNegotiation::Direct {
        let config = tls_connector.config();

        // make sure postgresql is the only alpn protocol from client
        if !config.alpn_protocols.len() == 1
            || config.alpn_protocols[0] == super::POSTGRESQL_ALPN_NAME
        {
            return Err(PgWireClientError::AlpnRequired);
        }
    }

    let hostname = config.host[0].get_hostname().unwrap_or("".to_owned());
    let server_name =
        ServerName::try_from(hostname).map_err(|e| IOError::new(ErrorKind::InvalidInput, e))?;
    let tls_stream = tls_connector.connect(server_name, socket).await?;
    Ok(ClientSocket::Secure(Box::new(tls_stream)))
}

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub(crate) async fn ssl_handshake(
    socket: TcpStream,
    config: &Config,
    tls_connector: Option<TlsConnector>,
) -> PgWireClientResult<ClientSocket> {
    use crate::api::client::config::{SslMode, SslNegotiation};
    use crate::messages::response::SslResponse;

    // ssl is disabled on client side
    if config.ssl_mode == SslMode::Disable {
        return Ok(ClientSocket::Plain(socket));
    }

    if let Some(tls_connector) = tls_connector {
        if config.ssl_negotiation == SslNegotiation::Direct {
            connect_tls(socket, config, tls_connector).await
        } else {
            let mut socket = Framed::new(socket, PgWireMessageClientCodec::default());
            // postgres ssl handshake
            socket
                .send(PgWireFrontendMessage::SslNegotiation(
                    SslNegotiationMetaMessage::PostgresSsl(crate::messages::startup::SslRequest),
                ))
                .await?;

            if let Some(Ok(PgWireBackendMessage::SslResponse(ssl_resp))) = socket.next().await {
                match ssl_resp {
                    SslResponse::Accept => {
                        connect_tls(socket.into_inner(), config, tls_connector).await
                    }
                    SslResponse::Refuse => {
                        if config.ssl_mode == SslMode::Require {
                            Err(IOError::new(
                                ErrorKind::ConnectionAborted,
                                "TLS is not enabled on server ",
                            )
                            .into())
                        } else {
                            Ok(ClientSocket::Plain(socket.into_inner()))
                        }
                    }
                }
            } else {
                // connection closed
                Err(IOError::new(ErrorKind::ConnectionAborted, "Expect SslResponse").into())
            }
        }
    } else {
        Ok(ClientSocket::Plain(socket))
    }
}

#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub(crate) async fn ssl_handshake(
    socket: ClientSocket,
    _config: &Config,
    _tls_connector: Option<TlsConnector>,
) -> PgWireClientResult<ClientSocket> {
    Ok(socket)
}

enum PgSocketAddr {
    Ip(SocketAddr),
    Host((String, u16)),
    Unix(PathBuf),
}

fn get_addr(config: &Config) -> Result<PgSocketAddr, PgWireClientError> {
    let port = config.get_ports().first().cloned().unwrap_or(5432);

    if let Some(hostaddr) = config.get_hostaddrs().first() {
        return Ok(PgSocketAddr::Ip(SocketAddr::new(*hostaddr, port)));
    }

    if let Some(host) = config.get_hosts().first() {
        return Ok(match host {
            Host::Tcp(host) => PgSocketAddr::Host((host.clone(), port)),
            Host::Unix(path) => PgSocketAddr::Unix(path.join(format!(".s.PGSQL.{}", port))),
        });
    }

    Err(PgWireClientError::InvalidConfig("host".to_string()))
}
