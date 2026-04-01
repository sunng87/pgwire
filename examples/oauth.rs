/// This example shows how to use pgwire with OAuth.
/// To connect with psql:
/// 1. Install libpq-oauth: sudo apt-get install libpq-oauth
/// 2. Execute: psql "postgres://postgres@localhost:5432/db?oauth_issuer=https://auth.example.com&oauth_client_id=my-app-client-id"
use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use pgwire::api::PgWireServerHandlers;
use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::sasl::oauth::Oauth;
use pgwire::api::auth::simple_oidc_validator::SimpleOidcValidator;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::tokio::process_socket;
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::rustls::ServerConfig;

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

struct DummyProcessorFactory {
    startup_handler: Arc<SASLAuthStartupHandler<DefaultServerParameterProvider>>,
}
impl PgWireServerHandlers for DummyProcessorFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        self.startup_handler.clone()
    }
}

#[tokio::main]
pub async fn main() {
    let iss = "http://localhost:8080/realms/postgres-realm";
    let validator = SimpleOidcValidator::new(iss)
        .await
        .expect("Failed to create OIDC validator");

    let oauth = Oauth::new(
        iss.to_string(),
        "openid email".to_string(),
        Arc::new(validator),
    );

    let startup_handler = Arc::new(
        SASLAuthStartupHandler::new(Arc::new(DefaultServerParameterProvider::default()))
            .with_oauth(oauth),
    );

    let factory = Arc::new(DummyProcessorFactory { startup_handler });

    let server_addr = "127.0.0.1:5432";
    let tls_acceptor = setup_tls().unwrap();
    let listener = TcpListener::bind(server_addr).await.unwrap();
    println!("Listening to {}", server_addr);
    loop {
        let incoming_socket = listener.accept().await.unwrap();
        let tls_acceptor_ref = tls_acceptor.clone();

        let factory_ref = factory.clone();

        tokio::spawn(async move {
            process_socket(incoming_socket.0, Some(tls_acceptor_ref), factory_ref).await
        });
    }
}
