use std::fs::{self, File};
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use async_trait::async_trait;

use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use pgwire::api::auth::scram::{gen_salted_password, SASLScramAuthStartupHandler};
use pgwire::api::auth::{AuthSource, DefaultServerParameterProvider, LoginInfo, Password};
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{Response, Tag};

use pgwire::api::ClientInfo;
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        _query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(vec![Response::Execution(Tag::new("OK").with_rows(1))])
    }
}

pub fn random_salt() -> Vec<u8> {
    Vec::from(rand::random::<[u8; 10]>())
}

const ITERATIONS: usize = 4096;

struct DummyAuthDB;

#[async_trait]
impl AuthSource for DummyAuthDB {
    async fn get_password(&self, _login: &LoginInfo) -> PgWireResult<Password> {
        let password = "pencil";
        let salt = random_salt();

        let hash_password = gen_salted_password(password, salt.as_ref(), ITERATIONS);
        Ok(Password::new(Some(salt), hash_password))
    }
}

/// configure TlsAcceptor and get server cert for SCRAM channel binding
fn setup_tls() -> Result<TlsAcceptor, IOError> {
    let cert = certs(&mut BufReader::new(File::open("examples/ssl/server.crt")?))
        .collect::<Result<Vec<CertificateDer>, IOError>>()?;

    let key = pkcs8_private_keys(&mut BufReader::new(File::open("examples/ssl/server.key")?))
        .map(|key| key.map(PrivateKeyDer::from))
        .collect::<Result<Vec<PrivateKeyDer>, IOError>>()?
        .remove(0);

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(DummyProcessor);
    // We have not implemented extended query in this server, use placeholder instead
    let placeholder = Arc::new(PlaceholderExtendedQueryHandler);
    let noop_copy_handler = Arc::new(NoopCopyHandler);

    let cert = fs::read("examples/ssl/server.crt").unwrap();

    let server_addr = "127.0.0.1:5432";
    let tls_acceptor = Arc::new(setup_tls().unwrap());
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let tls_acceptor_ref = tls_acceptor.clone();
        let authenticator = Arc::new(SASLScramAuthStartupHandler::new(
            Arc::new(DummyAuthDB),
            Arc::new(DefaultServerParameterProvider::default()),
        ));
        authenticator.set_iterations(ITERATIONS);
        authenticator.configure_certificate(cert.as_ref()).unwrap();

        let processor_ref = processor.clone();
        let placeholder_ref = placeholder.clone();
        let copy_handler_ref = noop_copy_handler.clone();

        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                Some(tls_acceptor_ref),
                authenticator.clone(),
                processor_ref,
                placeholder_ref,
                copy_handler_ref,
            )
            .await
        });
    }
}
