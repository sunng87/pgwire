use std::collections::BTreeMap;
use std::io::{Error as IOError, ErrorKind};
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use futures::{SinkExt, StreamExt};
use pin_project::pin_project;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};
use tokio_rustls::client::TlsStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use super::TlsConnector;
use crate::api::client::auth::StartupHandler;
use crate::api::client::config::Host;
use crate::api::client::{ClientInfo, Config, PgWireClientHandlers};
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

#[derive(Debug, Default)]
pub enum PgWireClientConnectionState {
    #[default]
    Disconnected,
    Connected,
    ReadyForQuery(ServerInformation),
}

#[derive(Debug, Default)]
pub struct ServerInformation {
    pub parameters: BTreeMap<String, String>,
    pub process_id: i32,
    pub finalized: bool,
}

#[derive(Debug)]
pub struct PgWireClient<H> {
    sender: UnboundedSender<PgWireFrontendMessage>,
    handlers: Arc<H>,
    config: Arc<Config>,
    state: Mutex<PgWireClientConnectionState>,
}

impl<H> PgWireClient<H> {
    pub fn send(&self, msg: PgWireFrontendMessage) -> PgWireResult<()> {
        self.sender
            .send(msg)
            .map_err(|e| PgWireError::ClientMessageSendError(e))
    }

    pub fn update_state(&self, state: PgWireClientConnectionState) {
        *self.state.lock().unwrap() = state;
    }
}

impl<H> ClientInfo for PgWireClient<H> {
    fn config(&self) -> &Config {
        &self.config
    }

    fn server_parameters(&self, key: &str) -> Option<String> {
        if let PgWireClientConnectionState::ReadyForQuery(server_info) =
            &*self.state.lock().unwrap()
        {
            server_info.parameters.get(key).map(|s| s.to_string())
        } else {
            None
        }
    }

    fn process_id(&self) -> Option<i32> {
        if let PgWireClientConnectionState::ReadyForQuery(server_info) =
            &*self.state.lock().unwrap()
        {
            Some(server_info.process_id)
        } else {
            None
        }
    }
}

impl<H> PgWireClient<H>
where
    H: PgWireClientHandlers + Send + Sync + 'static,
{
    pub async fn connect(
        config: Arc<Config>,
        handlers: Arc<H>,
        tls_connector: Option<TlsConnector>,
    ) -> Result<Arc<PgWireClient<H>>, IOError> {
        // tcp connect
        let socket = TcpStream::connect(get_addr(&config)?).await?;
        let socket = Framed::new(socket, PgWireMessageClientCodec);
        // perform ssl handshake based on postgres configuration
        // if tls is not enabled, just return the socket and perform startup
        // directly
        let socket = ssl_handshake(socket, &config, tls_connector).await?;
        let socket = Framed::new(socket, PgWireMessageClientCodec);
        let (mut socket_sender, mut socket_receiver) = socket.split();

        let (tx, mut rx) = unbounded_channel();
        let client = Arc::new(PgWireClient {
            sender: tx,
            handlers,
            config: config.clone(),
            state: Mutex::new(PgWireClientConnectionState::Connected),
        });
        let c2 = client.clone();

        let handle = async move {
            loop {
                tokio::select! {
                    send_msg = rx.recv() => {
                        if let Some(msg) = send_msg {
                            let r = socket_sender.send(msg).await;
                            if r.is_err() {
                                break;
                            }
                        } else {
                            break;
                        }
                    }

                    recv_msg = socket_receiver.next() => {
                        if let Some(msg) = recv_msg {
                            if let Ok(msg) = msg {
                                if let Err(e) = c2.process_message(msg).await {
                                    if let Err(_e) = c2.process_error(e).await {
                                        break;
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        };
        tokio::spawn(handle);

        client
            .handlers
            .startup_handler()
            .startup(client.as_ref())
            .await?;

        Ok(client)
    }

    async fn process_message(&self, message: PgWireBackendMessage) -> PgWireResult<()> {
        match message {
            PgWireBackendMessage::Authentication(authentication) => {
                self.handlers
                    .startup_handler()
                    .on_authentication(&self, authentication)
                    .await?;
            }
            PgWireBackendMessage::ParameterStatus(parameter_status) => {
                self.handlers
                    .startup_handler()
                    .on_parameter_status(&self, parameter_status)
                    .await?;
            }
            PgWireBackendMessage::BackendKeyData(backend_key_data) => {
                self.handlers
                    .startup_handler()
                    .on_backend_key(&self, backend_key_data)
                    .await?;
            }
            PgWireBackendMessage::ReadyForQuery(ready) => {
                self.handlers
                    .startup_handler()
                    .on_ready_for_query(&self, ready)
                    .await?;
            }
            _ => {}
        }
        Ok(())
    }

    async fn process_error(&self, _error: PgWireError) -> Result<(), IOError> {
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
async fn connect_tls(
    socket: TcpStream,
    config: &Config,
    tls_connector: TlsConnector,
) -> Result<ClientSocket, IOError> {
    // TODO: set ALPN correctly
    use rustls_pki_types::ServerName;

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
) -> Result<ClientSocket, IOError> {
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
                            ))
                        } else {
                            Ok(ClientSocket::Plain(socket.into_inner()))
                        }
                    }
                }
            } else {
                // connection closed
                Err(IOError::new(
                    ErrorKind::ConnectionAborted,
                    "Expect SslResponse",
                ))
            }
        }
    } else {
        return Ok(ClientSocket::Plain(socket.into_inner()));
    }
}

#[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
pub(crate) async fn ssl_handshake(
    socket: TcpStream,
    _config: &Config,
    _tls_connector: Option<TlsConnector>,
) -> Result<ClientSocket, IOError> {
    Ok(ClientSocket::Plain(socket))
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
