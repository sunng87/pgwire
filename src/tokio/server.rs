use std::io;
use std::pin::Pin;
use std::sync::Arc;

use futures::{SinkExt, StreamExt};
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use rustls_pki_types::CertificateDer;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufStream};
use tokio::net::TcpStream;
use tokio::time::{sleep, Duration, Sleep};
#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
use tokio_rustls::server::TlsStream;
use tokio_util::codec::{Decoder, Encoder, Framed};

use crate::api::auth::StartupHandler;
use crate::api::cancel::CancelHandler;
use crate::api::copy::CopyHandler;
use crate::api::query::{send_ready_for_query, ExtendedQueryHandler, SimpleQueryHandler};
use crate::api::{
    ClientInfo, ClientPortalStore, DefaultClient, ErrorHandler, PgWireConnectionState,
    PgWireServerHandlers,
};
use crate::error::{ErrorInfo, PgWireError, PgWireResult};
use crate::messages::response::{GssEncResponse, ReadyForQuery, SslResponse, TransactionStatus};
use crate::messages::startup::{GssEncRequest, SecretKey, SslRequest};
use crate::messages::{
    DecodeContext, PgWireBackendMessage, PgWireFrontendMessage, ProtocolVersion,
};

/// startup timeout
const STARTUP_TIMEOUT_MILLIS: u64 = 60_000;

#[non_exhaustive]
#[derive(Debug, new)]
pub struct PgWireMessageServerCodec<S> {
    pub client_info: DefaultClient<S>,
    #[new(default)]
    decode_context: DecodeContext,
}

impl<S> Decoder for PgWireMessageServerCodec<S> {
    type Item = PgWireFrontendMessage;
    type Error = PgWireError;

    fn decode(&mut self, src: &mut bytes::BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        self.decode_context.protocol_version = self.client_info.protocol_version;

        match self.client_info.state() {
            PgWireConnectionState::AwaitingSslRequest => {}

            PgWireConnectionState::AwaitingStartup => {
                self.decode_context.awaiting_ssl = false;
            }

            _ => {
                self.decode_context.awaiting_startup = false;
            }
        }

        PgWireFrontendMessage::decode(src, &self.decode_context)
    }
}

impl<S> Encoder<PgWireBackendMessage> for PgWireMessageServerCodec<S> {
    type Error = io::Error;

    fn encode(
        &mut self,
        item: PgWireBackendMessage,
        dst: &mut bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        item.encode(dst).map_err(Into::into)
    }
}

impl<T: 'static, S> ClientInfo for Framed<T, PgWireMessageServerCodec<S>> {
    fn socket_addr(&self) -> std::net::SocketAddr {
        self.codec().client_info.socket_addr
    }

    fn is_secure(&self) -> bool {
        self.codec().client_info.is_secure
    }

    fn pid_and_secret_key(&self) -> (i32, SecretKey) {
        self.codec().client_info.pid_and_secret_key()
    }

    fn set_pid_and_secret_key(&mut self, pid: i32, secret_key: SecretKey) {
        self.codec_mut()
            .client_info
            .set_pid_and_secret_key(pid, secret_key);
    }

    fn protocol_version(&self) -> ProtocolVersion {
        self.codec().client_info.protocol_version()
    }

    fn set_protocol_version(&mut self, version: ProtocolVersion) {
        self.codec_mut().client_info.set_protocol_version(version);
    }

    fn state(&self) -> PgWireConnectionState {
        self.codec().client_info.state
    }

    fn set_state(&mut self, new_state: PgWireConnectionState) {
        self.codec_mut().client_info.set_state(new_state);
    }

    fn metadata(&self) -> &std::collections::HashMap<String, String> {
        self.codec().client_info.metadata()
    }

    fn metadata_mut(&mut self) -> &mut std::collections::HashMap<String, String> {
        self.codec_mut().client_info.metadata_mut()
    }

    fn transaction_status(&self) -> TransactionStatus {
        self.codec().client_info.transaction_status()
    }

    fn set_transaction_status(&mut self, new_status: TransactionStatus) {
        self.codec_mut()
            .client_info
            .set_transaction_status(new_status);
    }

    #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
    fn client_certificates<'a>(&self) -> Option<&[CertificateDer<'a>]> {
        if !self.is_secure() {
            None
        } else {
            let socket =
                <dyn std::any::Any>::downcast_ref::<TlsStream<TcpStream>>(self.get_ref()).unwrap();
            let (_, tls_session) = socket.get_ref();
            tls_session.peer_certificates()
        }
    }
}

impl<T, S> ClientPortalStore for Framed<T, PgWireMessageServerCodec<S>> {
    type PortalStore = <DefaultClient<S> as ClientPortalStore>::PortalStore;

    fn portal_store(&self) -> &Self::PortalStore {
        self.codec().client_info.portal_store()
    }
}

async fn process_message<S, A, Q, EQ, C, CR>(
    message: PgWireFrontendMessage,
    socket: &mut Framed<S, PgWireMessageServerCodec<EQ::Statement>>,
    authenticator: Arc<A>,
    query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
    copy_handler: Arc<C>,
    cancel_handler: Arc<CR>,
) -> PgWireResult<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    A: StartupHandler,
    Q: SimpleQueryHandler,
    EQ: ExtendedQueryHandler,
    C: CopyHandler,
    CR: CancelHandler,
{
    // CancelRequest is from a dedicated connection, process it and close it.
    if let PgWireFrontendMessage::CancelRequest(cancel) = message {
        cancel_handler.on_cancel_request(cancel).await;
        socket.close().await?;
        return Ok(());
    }

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
                // TODO: confirm if we need to track transaction state there
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
                PgWireFrontendMessage::Flush(flush) => {
                    extended_query_handler.on_flush(socket, flush).await?;
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
) -> Result<(), io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
{
    let error_info: ErrorInfo = error.into();
    let is_fatal = error_info.is_fatal();
    socket
        .send(PgWireBackendMessage::ErrorResponse(error_info.into()))
        .await?;

    let transaction_status = socket.transaction_status().to_error_state();
    socket.set_transaction_status(transaction_status);

    if wait_for_sync {
        socket.set_state(PgWireConnectionState::AwaitingSync);
    } else {
        socket.set_state(PgWireConnectionState::ReadyForQuery);
        socket
            .feed(PgWireBackendMessage::ReadyForQuery(ReadyForQuery::new(
                transaction_status,
            )))
            .await?;
    }
    socket.flush().await?;

    if is_fatal {
        return socket.close().await;
    }

    Ok(())
}

#[derive(Debug, PartialEq, Eq)]
enum SslNegotiationType {
    Postgres,
    Direct,
    None,
}

async fn check_ssl_direct_negotiation(
    tcp_socket: &mut BufStream<TcpStream>,
) -> Result<bool, io::Error> {
    let buf = tcp_socket.fill_buf().await?;

    Ok(!buf.is_empty() && buf[0] == 0x16)
}

async fn peek_for_sslrequest<ST>(
    socket: &mut Framed<BufStream<TcpStream>, PgWireMessageServerCodec<ST>>,
    ssl_supported: bool,
) -> Result<SslNegotiationType, io::Error> {
    if check_ssl_direct_negotiation(socket.get_mut()).await? {
        Ok(SslNegotiationType::Direct)
    } else {
        let mut ssl_done = false;
        let mut gss_done = false;

        loop {
            let buf = socket.get_mut().fill_buf().await?;
            let n = buf.len();

            // already EOF
            if n == 0 {
                return Ok(SslNegotiationType::None);
            }

            if n >= 8 {
                if SslRequest::is_ssl_request_packet(buf) {
                    // consume SslRequest
                    let _ = socket.next().await;
                    // ssl request
                    if ssl_supported {
                        socket
                            .send(PgWireBackendMessage::SslResponse(SslResponse::Accept))
                            .await?;
                        return Ok(SslNegotiationType::Postgres);
                    } else {
                        socket
                            .send(PgWireBackendMessage::SslResponse(SslResponse::Refuse))
                            .await?;
                        ssl_done = true;

                        if gss_done {
                            return Ok(SslNegotiationType::None);
                        } else {
                            // Continue to check for more requests (e.g., GssEncRequest after SSL refuse)
                            continue;
                        }
                    }
                } else if GssEncRequest::is_gss_enc_request_packet(buf) {
                    let _ = socket.next().await;
                    socket
                        .send(PgWireBackendMessage::GssEncResponse(GssEncResponse::Refuse))
                        .await?;
                    gss_done = true;

                    if ssl_done {
                        return Ok(SslNegotiationType::None);
                    } else {
                        // Continue to check for more requests (e.g., SSL request after GSSAPI refuse)
                        continue;
                    }
                } else {
                    // startup or cancel
                    return Ok(SslNegotiationType::None);
                }
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn do_process_socket<S, A, Q, EQ, C, CR, E>(
    socket: &mut Framed<S, PgWireMessageServerCodec<EQ::Statement>>,
    mut startup_timeout: Pin<&mut Sleep>,
    startup_handler: Arc<A>,
    simple_query_handler: Arc<Q>,
    extended_query_handler: Arc<EQ>,
    copy_handler: Arc<C>,
    cancel_handler: Arc<CR>,
    error_handler: Arc<E>,
) -> Result<(), io::Error>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + Sync + 'static,
    A: StartupHandler,
    Q: SimpleQueryHandler,
    EQ: ExtendedQueryHandler,
    C: CopyHandler,
    CR: CancelHandler,
    E: ErrorHandler,
{
    // for those steps without ssl negotiation
    socket.set_state(PgWireConnectionState::AwaitingStartup);

    loop {
        let msg = if matches!(
            socket.state(),
            PgWireConnectionState::AwaitingStartup
                | PgWireConnectionState::AuthenticationInProgress
        ) {
            tokio::select! {
                _ = &mut startup_timeout => {
                    if matches!(
                        socket.state(),
                        PgWireConnectionState::AwaitingStartup
                        | PgWireConnectionState::AuthenticationInProgress
                    ) {
                        None
                    } else {
                        continue;
                    }
                },
                msg = socket.next() => {
                    msg
                },
            }
        } else {
            socket.next().await
        };

        if let Some(Ok(msg)) = msg {
            let is_extended_query = match socket.state() {
                PgWireConnectionState::CopyInProgress(is_extended_query) => is_extended_query,
                _ => msg.is_extended_query(),
            };
            if let Err(mut e) = process_message(
                msg,
                socket,
                startup_handler.clone(),
                simple_query_handler.clone(),
                extended_query_handler.clone(),
                copy_handler.clone(),
                cancel_handler.clone(),
            )
            .await
            {
                error_handler.on_error(socket, &mut e);
                process_error(socket, e, is_extended_query).await?;
            }
        } else {
            break;
        }
    }

    Ok(())
}

#[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
fn check_alpn_for_direct_ssl<IO>(tls_socket: &TlsStream<IO>) -> Result<(), io::Error> {
    let (_, the_conn) = tls_socket.get_ref();
    let mut accept = false;

    if let Some(alpn) = the_conn.alpn_protocol() {
        if alpn == super::POSTGRESQL_ALPN_NAME {
            accept = true;
        }
    }

    if !accept {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "received direct SSL connection request without ALPN protocol negotiation extension",
        ))
    } else {
        Ok(())
    }
}

pub async fn process_socket<H>(
    tcp_socket: TcpStream,
    tls_acceptor: Option<crate::tokio::TlsAcceptor>,
    handlers: H,
) -> Result<(), io::Error>
where
    H: PgWireServerHandlers,
{
    let addr = tcp_socket.peer_addr()?;
    tcp_socket.set_nodelay(true)?;

    let client_info = DefaultClient::new(addr, false);
    let mut tcp_socket = Framed::new(
        BufStream::new(tcp_socket),
        PgWireMessageServerCodec::new(client_info),
    );

    // start a timer for startup process, if the client couldn't finish startup
    // within the timeout, it has to be dropped.
    let startup_timeout = sleep(Duration::from_millis(STARTUP_TIMEOUT_MILLIS));
    tokio::pin!(startup_timeout);

    // this function will process postgres ssl negotiation and consume the first
    // SslRequest packet if detected.
    let ssl = tokio::select! {
        _ = &mut startup_timeout => {
            return Ok(())
        },
        ssl = peek_for_sslrequest(&mut tcp_socket, tls_acceptor.is_some()) => {
            ssl?
        }
    };

    let startup_handler = handlers.startup_handler();
    let simple_query_handler = handlers.simple_query_handler();
    let extended_query_handler = handlers.extended_query_handler();
    let copy_handler = handlers.copy_handler();
    let cancel_handler = handlers.cancel_handler();
    let error_handler = handlers.error_handler();

    if ssl == SslNegotiationType::None {
        // use an already configured socket.
        let mut socket = tcp_socket;

        do_process_socket(
            &mut socket,
            startup_timeout,
            startup_handler,
            simple_query_handler,
            extended_query_handler,
            copy_handler,
            cancel_handler,
            error_handler,
        )
        .await
    } else {
        #[cfg(any(feature = "_ring", feature = "_aws-lc-rs"))]
        {
            if let Some(tls_acceptor) = tls_acceptor {
                // mention the use of ssl
                let mut client_info = DefaultClient::new(addr, true);

                let ssl_socket = tokio::select! {
                    _ = &mut startup_timeout => {
                        return Ok(())
                    },
                    // safe to unwrap tls_acceptor here
                    ssl_socket_result = tls_acceptor.accept(tcp_socket.into_inner()) => {
                        ssl_socket_result?
                    }
                };

                // check alpn for direct ssl connection
                if ssl == SslNegotiationType::Direct {
                    check_alpn_for_direct_ssl(&ssl_socket)?;
                }

                // capture SNI (server name) from the underlying TLS connection and store in metadata
                let sni = {
                    let (_, conn) = ssl_socket.get_ref();
                    conn.server_name().map(|s| s.to_string())
                };
                if let Some(s) = sni {
                    client_info
                        .metadata_mut()
                        .insert("server_name".to_string(), s);
                }

                let mut socket = Framed::new(
                    BufStream::new(ssl_socket),
                    PgWireMessageServerCodec::new(client_info),
                );

                do_process_socket(
                    &mut socket,
                    startup_timeout,
                    startup_handler,
                    simple_query_handler,
                    extended_query_handler,
                    copy_handler,
                    cancel_handler,
                    error_handler,
                )
                .await
            } else {
                // no tls_acceptor configured. But the client sends direct tls
                // negotiation. this is typically an invalid connection
                Ok(())
            }
        }

        #[cfg(not(any(feature = "_ring", feature = "_aws-lc-rs")))]
        Ok(())
    }
}

#[cfg(all(test, any(feature = "_ring", feature = "_aws-lc-rs")))]
mod tests {
    use super::*;
    use std::fs::File;
    use std::io::{BufReader, Error as IOError};
    use std::sync::Arc;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_rustls::rustls;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::TlsConnector;

    fn load_test_server_config() -> Result<rustls::ServerConfig, IOError> {
        use rustls_pemfile::{certs, pkcs8_private_keys};
        use rustls_pki_types::{CertificateDer, PrivateKeyDer};

        let certs = certs(&mut BufReader::new(File::open("examples/ssl/server.crt")?))
            .collect::<Result<Vec<CertificateDer>, _>>()?;
        let key = pkcs8_private_keys(&mut BufReader::new(File::open("examples/ssl/server.key")?))
            .map(|key| key.map(PrivateKeyDer::from))
            .collect::<Result<Vec<PrivateKeyDer>, _>>()?
            .remove(0);

        let mut cfg = rustls::ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidInput, e))?;
        // ALPN is optional for this test; SNI extraction doesn't depend on it.
        cfg.alpn_protocols = vec![crate::tokio::POSTGRESQL_ALPN_NAME.to_vec()];
        Ok(cfg)
    }

    fn make_test_client_connector() -> Result<TlsConnector, IOError> {
        // For this unit test we are only validating SNI plumbing, not cert validation.
        // Use a custom verifier that accepts any certificate.
        struct NoCertVerifier;
        impl rustls::client::danger::ServerCertVerifier for NoCertVerifier {
            fn verify_server_cert(
                &self,
                _end_entity: &rustls::pki_types::CertificateDer<'_>,
                _intermediates: &[rustls::pki_types::CertificateDer<'_>],
                _server_name: &rustls::pki_types::ServerName<'_>,
                _ocsp_response: &[u8],
                _now: rustls::pki_types::UnixTime,
            ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
                Ok(rustls::client::danger::ServerCertVerified::assertion())
            }
        }

        let cfg = rustls::ClientConfig::builder()
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(NoCertVerifier))
            .with_no_client_auth();
        Ok(TlsConnector::from(Arc::new(cfg)))
    }

    #[tokio::test]
    async fn server_name_metadata_is_set_from_tls_sni() {
        // set up TLS server
        let server_cfg = load_test_server_config().expect("server config");
        let acceptor = TlsAcceptor::from(Arc::new(server_cfg));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let (tx, rx) = oneshot::channel::<Option<String>>();

        // spawn server task
        tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let tls = acceptor.accept(tcp).await.unwrap();

            // mimic production path: capture SNI then add to client_info metadata as `server_name`
            let sni = {
                let (_, conn) = tls.get_ref();
                conn.server_name().map(|s| s.to_string())
            };
            let peer = tls.get_ref().0.peer_addr().unwrap();
            let mut ci: DefaultClient<()> = DefaultClient::new(peer, true);
            if let Some(s) = sni {
                ci.metadata_mut().insert("server_name".to_string(), s);
            }
            let framed = Framed::new(BufStream::new(tls), PgWireMessageServerCodec::new(ci));
            let server_name = framed.metadata().get("server_name").cloned();
            let _ = tx.send(server_name);
        });

        // connect as TLS client with SNI=localhost
        let connector = make_test_client_connector().expect("client connector");
        let tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = rustls_pki_types::ServerName::try_from("localhost").unwrap();
        let _ = connector.connect(server_name, tcp).await.unwrap();

        // verify server observed SNI and stored as `server_name`
        let observed = rx.await.expect("server_name from server");
        assert_eq!(observed.as_deref(), Some("localhost"));
    }
}
