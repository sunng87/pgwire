use std::io::{Error as IOError, ErrorKind};
use std::pin::Pin;
use std::sync::Arc;

use bytes::Buf;
use futures::stream::SplitSink;
use futures::{SinkExt, StreamExt};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use tokio_rustls::TlsConnector;
use tokio_rustls::TlsStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::client::config::Host;
use crate::api::client::{Config, PgWireClientHandlers};
use crate::error::{PgWireError, PgWireResult};
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

pub struct PgWireClient<
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync,
    H: PgWireClientHandlers + Send + Sync,
> {
    transport: SplitSink<Framed<S, PgWireMessageClientCodec>, PgWireFrontendMessage>,
    handlers: H,
    config: Config,
}

impl<H: PgWireClientHandlers + Send + Sync + 'static> PgWireClient<ClientSocket, H> {
    pub async fn connect(
        config: Config,
        handlers: H,
    ) -> Result<Arc<PgWireClient<ClientSocket, H>>, IOError> {
        // tcp connect
        let socket = TcpStream::connect(get_addr(&config)?).await?;
        let socket = Framed::new(socket, PgWireMessageClientCodec);
        // perform ssl handshake based on postgres configuration
        // if tls is not enabled, just return the socket and perform startup
        // directly
        let socket = ssl_handshake(socket, &config).await?;
        let socket = Framed::new(socket, PgWireMessageClientCodec);

        let (sender, mut receiver) = socket.split();
        let client = Arc::new(PgWireClient {
            transport: sender,
            handlers,
            config,
        });
        let handle_client = client.clone();

        let handle = async move {
            while let Some(msg) = receiver.next().await {
                if let Ok(msg) = msg {
                    if let Err(e) = handle_client.process_message(msg).await {
                        if let Err(_e) = handle_client.process_error(e).await {
                            break;
                        }
                    }
                } else {
                    break;
                }
            }
        };

        tokio::spawn(handle);

        Ok(client)
    }

    async fn process_message(&self, message: PgWireBackendMessage) -> PgWireResult<()> {
        todo!();
        Ok(())
    }

    async fn process_error(&self, error: PgWireError) -> Result<(), IOError> {
        todo!()
    }
}

#[pin_project(project = ClientSocketProj)]
pub enum ClientSocket {
    Plain(#[pin] TcpStream),
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
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_flush(cx),
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), std::io::Error>> {
        match self.project() {
            ClientSocketProj::Plain(socket) => socket.poll_shutdown(cx),
            ClientSocketProj::Secure(tls_socket) => tls_socket.poll_shutdown(cx),
        }
    }
}

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
pub(crate) async fn ssl_handshake(
    mut socket: Framed<TcpStream, PgWireMessageClientCodec>,
    config: &Config,
) -> Result<ClientSocket, IOError> {
    use crate::{
        api::client::config::{SslMode, SslNegotiation},
        messages::response::SslResponse,
    };

    // ssl is disabled on client side
    if config.ssl_mode == SslMode::Disable {
        return Ok(ClientSocket::Plain(socket.into_inner()));
    }

    if config.ssl_negotiation == SslNegotiation::Direct {
        // TODO: more tls configuration
        // let tls_stream = TlsConnector::connect(socket.into_inner(), )
        todo!();
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
                    todo!();
                }
                SslResponse::Refuse => Ok(ClientSocket::Plain(socket.into_inner())),
            }
        } else {
            // connection closed
            Err(IOError::new(
                ErrorKind::ConnectionAborted,
                "Expect SslResponse",
            ))
        }
    }
}

#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub(crate) async fn ssl_handshake(socket: TcpStream) -> Result<ClientSocket, IOError> {
    Ok(socket)
}

fn get_addr(config: &Config) -> Result<String, IOError> {
    if config.get_hostaddrs().len() > 0 {
        return Ok(format!(
            "{}:{}",
            config.get_hostaddrs()[0].to_string(),
            config.get_ports().get(0).cloned().unwrap_or(5432u16)
        ));
    }

    if config.get_hosts().len() > 0 {
        match &config.get_hosts()[0] {
            Host::Tcp(host) => {
                return Ok(format!(
                    "{}:{}",
                    host,
                    config.get_ports().get(0).cloned().unwrap_or(5432u16)
                ))
            }
            _ => {
                return Err(IOError::new(ErrorKind::InvalidData, "Invalid host"));
            }
        }
    }

    Err(IOError::new(ErrorKind::InvalidData, "Invalid host"))
}
