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
use crate::api::client::query::{ExtendedQueryClient, ExtendedQueryHandler, SimpleQueryHandler};
use crate::api::client::{ClientInfo, Config, ReadyState, ServerInformation};
use crate::error::{PgWireClientError, PgWireClientResult, PgWireError};
use crate::messages::cancel::CancelRequest;
use crate::messages::response::TransactionStatus;
use crate::messages::startup::SecretKey;
use crate::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
    SslNegotiationMetaMessage,
};

/// Codec for encoding and decoding PostgreSQL wire protocol messages on the client side.
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

/// A PostgreSQL client connection using the wire protocol.
#[pin_project]
pub struct PgWireClient {
    socket: Framed<ClientSocket, PgWireMessageClientCodec>,
    config: Arc<Config>,
    server_information: ServerInformation,
    /// Transaction status as last reported by the server in a
    /// `ReadyForQuery` message.
    transaction_status: TransactionStatus,
    /// TLS connector retained so [`PgWireClient::cancel`] can open a second
    /// secured connection to the same server.
    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    tls_connector: Option<TlsConnector>,
}

impl ClientInfo for PgWireClient {
    fn config(&self) -> &Config {
        &self.config
    }

    fn server_parameters(&self) -> &BTreeMap<String, String> {
        &self.server_information.parameters
    }

    fn set_server_parameter(&mut self, name: String, value: String) {
        self.server_information.parameters.insert(name, value);
    }

    fn process_id(&self) -> i32 {
        self.server_information.process_id
    }

    fn secret_key(&self) -> &SecretKey {
        &self.server_information.secret_key
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.socket.codec().decode_context.protocol_version
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.socket.codec_mut().decode_context.protocol_version = version;
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.transaction_status
    }

    fn set_transaction_status(&mut self, status: TransactionStatus) {
        self.transaction_status = status;
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
        // The TLS connector is retained so `cancel` can open a second secured
        // connection later. When TLS is disabled there is no field to store.
        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        let tls_connector_for_cancel = tls_connector.clone();

        let socket = connect_socket(&config, tls_connector).await?;

        let mut client = PgWireClient {
            socket,
            config: config.clone(),
            server_information: ServerInformation::default(),
            transaction_status: TransactionStatus::Idle,
            #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
            tls_connector: tls_connector_for_cancel,
        };

        // Decode backend messages with the rules of the protocol version we
        // are about to advertise, until the server negotiates it down.
        client.set_protocol_version(config.get_protocol_version());

        startup_handler.startup(&mut client).await?;
        // loop until finished
        while let Some(message_result) = client.socket.next().await {
            let message = message_result?;

            if let ReadyState::Ready(server_info) =
                startup_handler.on_message(&mut client, message).await?
            {
                let ServerInformation {
                    parameters,
                    process_id,
                    secret_key,
                } = server_info;
                // Parameters reported by the handler are merged over the ones
                // already cached as they arrived during startup (the default
                // `on_parameter_status` stores them on the client), so the
                // cache is complete regardless of how the handler builds its
                // `ServerInformation`.
                client.server_information.parameters.extend(parameters);
                client.server_information.process_id = process_id;
                client.server_information.secret_key = secret_key;
                return Ok(client);
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Cancel the currently running query on this connection.
    ///
    /// Per the PostgreSQL wire protocol, a cancel request must be sent on a
    /// **separate** connection to the same server — it carries the `pid` and
    /// `secret_key` from the original connection's `BackendKeyData`. This
    /// method opens that second connection (reusing this client's [`Config`]
    /// and TLS connector), sends the `CancelRequest`, and closes it.
    ///
    /// The server sends no reply on the cancel connection. Whether the cancel
    /// succeeded is observed on the original connection: the interrupted query
    /// returns an error (typically `57014` / `query_canceled`).
    ///
    /// Returns an error only if the second connection itself could not be
    /// established or the cancel message could not be written.
    pub async fn cancel(&self) -> PgWireClientResult<()> {
        // TLS connector is only stored when a TLS backend is enabled; without
        // TLS the cancel connection is always plaintext.
        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        let tls_connector = self.tls_connector.clone();
        #[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
        let tls_connector: Option<TlsConnector> = None;

        let mut socket = connect_socket(&self.config, tls_connector).await?;

        socket
            .send(PgWireFrontendMessage::CancelRequest(CancelRequest::new(
                self.server_information.process_id,
                self.server_information.secret_key.clone(),
            )))
            .await?;
        socket.close().await?;

        Ok(())
    }

    /// Start a query with simple query subprotocol
    ///
    /// If the query fails, the trailing `ReadyForQuery` of the failed query
    /// is consumed before the error is returned, so the connection remains
    /// usable for further queries.
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

            match simple_query_handler.on_message(self, message).await {
                Ok(ReadyState::Ready(responses)) => return Ok(responses),
                Ok(ReadyState::Pending) => {}
                Err(error) => {
                    // drain until ReadyForQuery so the connection is left in
                    // a reusable state; the server always sends it as the
                    // last message of a simple query
                    while let Some(message_result) = self.next().await {
                        match message_result? {
                            PgWireBackendMessage::ReadyForQuery(ready) => {
                                self.set_transaction_status(ready.status);
                                break;
                            }
                            PgWireBackendMessage::ParameterStatus(parameter_status) => {
                                self.set_server_parameter(
                                    parameter_status.name,
                                    parameter_status.value,
                                );
                            }
                            _ => continue,
                        }
                    }
                    return Err(error);
                }
            }
        }

        Err(PgWireClientError::UnexpectedEOF)
    }

    /// Create an extended query client for extended query subprotocol
    pub fn extended_query<'a, H>(
        &'a mut self,
        handler: &'a mut H,
    ) -> ExtendedQueryClient<'a, Self, H>
    where
        H: ExtendedQueryHandler,
    {
        ExtendedQueryClient::new(self, handler)
    }
}

impl Stream for PgWireClient {
    type Item = Result<PgWireBackendMessage, PgWireError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(self.project().socket).poll_next(cx)
    }
}

/// Abstraction over plain TCP, TLS, or Unix domain socket connections.
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

/// Establish a framed connection to the server: TCP (optionually upgraded to
/// TLS) or Unix domain socket. Shared by [`PgWireClient::connect`] (which then
/// runs startup) and [`PgWireClient::cancel`] (which sends a `CancelRequest`
/// instead of a `Startup`).
async fn connect_socket(
    config: &Config,
    tls_connector: Option<TlsConnector>,
) -> PgWireClientResult<Framed<ClientSocket, PgWireMessageClientCodec>> {
    let mut socket = match get_addr(config)? {
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
        // Perform the ssl handshake based on postgres configuration; when TLS
        // is disabled `ssl_handshake` returns the plain socket unchanged.
        socket = ssl_handshake(tcp_socket, config, tls_connector).await?;
    }
    Ok(Framed::new(socket, PgWireMessageClientCodec::default()))
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

#[cfg(all(test, feature = "server-api"))]
mod tests {
    use std::sync::Arc;

    use async_trait::async_trait;
    use tokio::net::TcpListener;

    use super::PgWireClient;
    use crate::api::PgWireServerHandlers;
    use crate::api::client::ClientInfo;
    use crate::api::client::auth::DefaultStartupHandler;
    use crate::api::client::config::Config;
    use crate::api::client::query::DefaultSimpleQueryHandler;
    use crate::api::query::SimpleQueryHandler as ServerSimpleQueryHandler;
    use crate::api::results::{Response, Tag};
    use crate::api::store::PortalStore;
    use crate::api::{ClientInfo as ServerClientInfo, ClientPortalStore};
    use crate::error::{ErrorInfo, PgWireResult};
    use crate::messages::ProtocolVersion;
    use crate::messages::response::TransactionStatus;
    use crate::messages::startup::SecretKey;
    use crate::tokio::server::process_socket;

    struct TestHandlers;

    impl PgWireServerHandlers for TestHandlers {}

    /// Simple-query handler that models transaction control statements the
    /// way a real backend does, for exercising client-side status tracking.
    struct TxHandlers;

    impl PgWireServerHandlers for TxHandlers {
        fn simple_query_handler(&self) -> Arc<impl ServerSimpleQueryHandler> {
            Arc::new(TxSimpleQueryHandler)
        }
    }

    struct TxSimpleQueryHandler;

    #[async_trait]
    impl ServerSimpleQueryHandler for TxSimpleQueryHandler {
        async fn do_query<C>(&self, _client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
        where
            C: ServerClientInfo + ClientPortalStore + Unpin + Send + Sync,
            C::PortalStore: PortalStore,
        {
            match query.trim().to_uppercase().as_str() {
                "BEGIN" => Ok(vec![Response::TransactionStart(Tag::new("BEGIN"))]),
                "COMMIT" | "ROLLBACK" => Ok(vec![Response::TransactionEnd(Tag::new("COMMIT"))]),
                "FAIL" => Ok(vec![Response::Error(Box::new(ErrorInfo::new(
                    "ERROR".to_owned(),
                    "XX000".to_owned(),
                    "boom".to_owned(),
                )))]),
                _ => Ok(vec![Response::Execution(Tag::new("OK"))]),
            }
        }
    }

    async fn spawn_test_server<H>(handlers: Arc<H>) -> u16
    where
        H: PgWireServerHandlers + Send + Sync,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let handlers = handlers.clone();
                tokio::spawn(async move {
                    let _ = process_socket(socket, None, handlers).await;
                });
            }
        });
        port
    }

    #[tokio::test]
    async fn client_negotiates_3_9999_down_to_3_2() {
        let port = spawn_test_server(Arc::new(TestHandlers)).await;

        let mut config = Config::new();
        config.host("127.0.0.1");
        config.port(port);
        config.user("pgwire");
        config.protocol_version(ProtocolVersion::PROTOCOL3_9999);

        let client = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
            .await
            .unwrap();

        // The server did not accept 3.9999 as-is; it negotiated down to its
        // newest supported version.
        assert_eq!(client.protocol_version(), ProtocolVersion::PROTOCOL3_2);
        // Protocol 3.2 backend keys are 32 bytes long.
        assert!(matches!(client.secret_key(), SecretKey::Bytes(key) if key.len() == 32));
    }

    #[tokio::test]
    async fn client_default_3_0_keeps_i32_secret_key() {
        let port = spawn_test_server(Arc::new(TestHandlers)).await;

        let mut config = Config::new();
        config.host("127.0.0.1");
        config.port(port);
        config.user("pgwire");

        let client = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
            .await
            .unwrap();

        assert_eq!(client.protocol_version(), ProtocolVersion::PROTOCOL3_0);
        // A protocol 3.0 cancel key is decoded as a 4-byte i32, not as bytes.
        assert!(matches!(client.secret_key(), SecretKey::I32(_)));
    }

    #[tokio::test]
    async fn client_tracks_transaction_status() {
        let port = spawn_test_server(Arc::new(TxHandlers)).await;

        let mut config = Config::new();
        config.host("127.0.0.1");
        config.port(port);
        config.user("pgwire");

        let mut client =
            PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
                .await
                .unwrap();

        // a fresh connection is idle
        assert_eq!(client.transaction_status(), TransactionStatus::Idle);

        client
            .simple_query(DefaultSimpleQueryHandler::new(), "BEGIN")
            .await
            .unwrap();
        assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

        // statements inside the transaction keep the status
        client
            .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1")
            .await
            .unwrap();
        assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

        // a failed statement puts the connection into the failed-transaction
        // state; the error is returned but the connection stays usable
        assert!(
            client
                .simple_query(DefaultSimpleQueryHandler::new(), "FAIL")
                .await
                .is_err()
        );
        assert_eq!(client.transaction_status(), TransactionStatus::Error);

        client
            .simple_query(DefaultSimpleQueryHandler::new(), "ROLLBACK")
            .await
            .unwrap();
        assert_eq!(client.transaction_status(), TransactionStatus::Idle);
    }
}
