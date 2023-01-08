use std::fs::{self, File};
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use async_trait::async_trait;

use rustls_pemfile::{certs, pkcs8_private_keys};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{Certificate, PrivateKey, ServerConfig};
use tokio_rustls::TlsAcceptor;

use pgwire::api::auth::scram::{gen_salted_password, AuthDB, MakeSASLScramAuthStartupHandler};
use pgwire::api::auth::NoopServerParameterProvider;
use pgwire::api::portal::Portal;
use pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{Response, Tag};
use pgwire::api::{ClientInfo, StatelessMakeHandler};
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub struct DummyProcessor;

#[async_trait]
impl SimpleQueryHandler for DummyProcessor {
    async fn do_query<C>(&self, _client: &C, _query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(vec![Response::Execution(Tag::new_for_execution(
            "OK",
            Some(1),
        ))])
    }
}

#[async_trait]
impl ExtendedQueryHandler for DummyProcessor {
    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        Ok(Response::Execution(Tag::new_for_execution("OK", Some(1))))
    }
}

struct DummyAuthDB;

#[async_trait]
impl AuthDB for DummyAuthDB {
    async fn get_salted_password(
        &self,
        _username: &str,
        salt: &[u8],
        iterations: usize,
    ) -> PgWireResult<Vec<u8>> {
        let password = "pencil";
        Ok(gen_salted_password(password, salt, iterations))
    }
}

/// configure TlsAcceptor and get server cert for SCRAM channel binding
fn setup_tls() -> Result<TlsAcceptor, IOError> {
    let cert: Vec<Certificate> = certs(&mut BufReader::new(File::open("examples/ssl/server.crt")?))
        .map_err(|_| IOError::new(ErrorKind::InvalidInput, "invalid cert"))
        .map(|mut certs| certs.drain(..).map(Certificate).collect())?;
    let key = pkcs8_private_keys(&mut BufReader::new(File::open("examples/ssl/server.key")?))
        .map_err(|_| IOError::new(ErrorKind::InvalidInput, "invalid key"))
        .map(|mut keys| keys.drain(..).map(PrivateKey).next().unwrap())?;

    let config = ServerConfig::builder()
        .with_safe_defaults()
        .with_no_client_auth()
        .with_single_cert(cert, key)
        .map_err(|err| IOError::new(ErrorKind::InvalidInput, err))?;

    Ok(TlsAcceptor::from(Arc::new(config)))
}

#[tokio::main]
pub async fn main() {
    let processor = Arc::new(StatelessMakeHandler::new(Arc::new(DummyProcessor)));
    let mut authenticator = MakeSASLScramAuthStartupHandler::new(
        Arc::new(DummyAuthDB),
        Arc::new(NoopServerParameterProvider),
    );

    let cert = fs::read("examples/ssl/server.crt").unwrap();
    authenticator.configure_certificate(cert.as_ref()).unwrap();
    let authenticator = Arc::new(authenticator);

    let server_addr = "127.0.0.1:5432";
    let tls_acceptor = Arc::new(setup_tls().unwrap());
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let tls_acceptor_ref = tls_acceptor.clone();
        let authenticator_ref = authenticator.clone();
        let processor_ref = processor.clone();
        tokio::spawn(async move {
            process_socket(
                incoming_socket.0,
                Some(tls_acceptor_ref),
                authenticator_ref,
                processor_ref.clone(),
                processor_ref,
            )
            .await
        });
    }
}
