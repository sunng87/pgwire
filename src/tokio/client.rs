use std::collections::BTreeMap;
use std::io::{Error as IOError, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;

use futures::{Sink, SinkExt, StreamExt};
use pin_project::pin_project;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use rustls_pki_types::ServerName;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use tokio_rustls::client::TlsStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::TlsConnector;
use crate::api::client::auth::StartupHandler;
use crate::api::client::config::Host;
use crate::api::client::query::{Response, SimpleQueryHandler};
use crate::api::client::{ClientInfo, Config, ReadyState, ServerInformation};
use crate::error::{PgWireClientError, PgWireClientResult, PgWireError};
use crate::messages::{PgWireBackendMessage, PgWireFrontendMessage};

#[non_exhaustive]
#[derive(Debug)]
pub struct PgWireMessageClientCodec;

impl Decoder for PgWireMessageClientCodec {
    type Item = PgWireBackendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        PgWireBackendMessage::decode(src)
    }
}

impl Encoder<PgWireFrontendMessage> for PgWireMessageClientCodec {
    type Error = PgWireError;

    fn encode(
        &mut self,
        item: PgWireFrontendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst).map_err(Into::into)
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
}

impl Sink<PgWireFrontendMessage> for PgWireClient {
    type Error =
        <Framed<ClientSocket, PgWireMessageClientCodec> as Sink<PgWireFrontendMessage>>::Error;

    fn poll_ready(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(self.project().socket).poll_ready(cx)
    }

    fn start_send(self: Pin<&mut Self>, item: PgWireFrontendMessage) -> Result<(), Self::Error> {
        Pin::new(self.project().socket).start_send(item)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Pin::new(self.project().socket).poll_flush(cx)
    }

    fn poll_close(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
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
        let socket = TcpStream::connect(get_addr(&config)?).await?;
        let socket = Framed::new(socket, PgWireMessageClientCodec);
        // perform ssl handshake based on postgres configuration
        // if tls is not enabled, just return the socket and perform startup
        // directly
        let socket = ssl_handshake(socket, &config, tls_connector).await?;
        let socket = Framed::new(socket, PgWireMessageClientCodec);

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
    ) -> PgWireClientResult<Vec<Response>>
    where
        H: SimpleQueryHandler,
    {
        simple_query_handler.simple_query(self, query).await?;

        while let Some(message_result) = self.socket.next().await {
            let message = message_result?;

            if let ReadyState::Ready(responses) =
                simple_query_handler.on_message(self, message).await?
            {
                return Ok(responses);
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }
}

#[pin_project(project = ClientSocketProj)]
pub enum ClientSocket {
    Plain(#[pin] TcpStream),
    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    Secure(#[pin] TlsStream<TcpStream>),
}

impl AsyncRead for ClientSocket {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_read(cx, buf),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for ClientSocket {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<Result<usize, std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_write(cx, buf),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_flush(cx),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_shutdown(cx),
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_shutdown(cx),
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
        if !config
            .alpn_protocols
            .iter()
            .any(|alpn| alpn.as_slice() == b"postgresql")
        {
            return Err(PgWireClientError::AlpnRequired);
        }
    }

    let hostname = config.host[0].get_hostname().unwrap_or("".to_owned());
    let server_name =
        ServerName::try_from(hostname).map_err(|e| IOError::new(ErrorKind::InvalidInput, e))?;
    let tls_stream = tls_connector.connect(server_name, socket).await?;
    Ok(ClientSocket::Secure(tls_stream))
}

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub(crate) async fn ssl_handshake(
    mut socket: Framed<TcpStream, PgWireMessageClientCodec>,
    config: &Config,
    tls_connector: Option<TlsConnector>,
) -> PgWireClientResult<ClientSocket> {
    use crate::{
        api::client::config::{SslMode, SslNegotiation},
        messages::response::SslResponse,
    };

    // ssl is disabled on client side
    if config.ssl_mode == SslMode::Disable {
        return Ok(ClientSocket::Plain(socket.into_inner()));
    }

    if let Some(tls_connector) = tls_connector {
        if config.ssl_negotiation == SslNegotiation::Direct {
            connect_tls(socket.into_inner(), config, tls_connector).await
        } else {
            // postgres ssl handshake
            socket
                .send(PgWireFrontendMessage::SslRequest(Some(
                    crate::messages::startup::SslRequest,
                )))
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
        Ok(ClientSocket::Plain(socket.into_inner()))
    }
}

#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub(crate) async fn ssl_handshake(
    socket: Framed<TcpStream, PgWireMessageClientCodec>,
    _config: &Config,
    _tls_connector: Option<TlsConnector>,
) -> PgWireClientResult<ClientSocket> {
    Ok(ClientSocket::Plain(socket.into_inner()))
}

fn get_addr(config: &Config) -> Result<String, PgWireClientError> {
    if !config.get_hostaddrs().is_empty() {
        return Ok(format!(
            "{}:{}",
            config.get_hostaddrs()[0],
            config.get_ports().first().cloned().unwrap_or(5432u16)
        ));
    }

    if !config.get_hosts().is_empty() {
        match &config.get_hosts()[0] {
            Host::Tcp(host) => {
                return Ok(format!(
                    "{}:{}",
                    host,
                    config.get_ports().first().cloned().unwrap_or(5432u16)
                ))
            }
            _ => {
                return Err(PgWireClientError::InvalidConfig("host".to_string()));
            }
        }
    }

    Err(PgWireClientError::InvalidConfig("host".to_string()))
}
