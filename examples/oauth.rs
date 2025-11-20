/// This example shows how to use pgwire with OAuth.
/// To connect with psql:
/// 1. Install libq-oauth: sudo apt-get install libpq-oauth
/// 2. Execute: psql "postgres://postgres@localhost:5432/db?oauth_issuer=https://auth.example.com&oauth_client_id=my-app-client-id"
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use async_trait::async_trait;

use pgwire::api::auth::sasl::oauth::{Oauth, OauthValidator, ValidatorModuleResult};
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::PgWireServerHandlers;
use pgwire::error::PgWireResult;
use pgwire::tokio::process_socket;

pub fn random_salt() -> Vec<u8> {
    Vec::from(rand::random::<[u8; 10]>())
}

// TODO: change to JSON web token validator, this is just for validating the current code
#[derive(Debug)]
struct SimpleTokenValidator {
    valid_tokens: HashMap<String, String>,
}

impl SimpleTokenValidator {
    pub fn new() -> Self {
        let mut valid_tokens = HashMap::new();
        valid_tokens.insert(
            "secret_token_123".to_string(),
            "user@example.com".to_string(),
        );
        valid_tokens.insert(
            "admin_token_456".to_string(),
            "admin@example.com".to_string(),
        );

        Self { valid_tokens }
    }
}

#[async_trait]
impl OauthValidator for SimpleTokenValidator {
    async fn validate(
        &self,
        token: &str,
        username: &str,
        issuer: &str,
        required_scopes: &str,
    ) -> PgWireResult<ValidatorModuleResult> {
        println!("tokennnnn: {}", token);
        println!("Validating token for user: {}", username);
        println!("Expected issuer: {}", issuer);
        println!("Required scopes: {}", required_scopes);

        if let Some(authenticated_user) = self.valid_tokens.get(token) {
            Ok(ValidatorModuleResult {
                authorized: true,
                authn_id: Some(authenticated_user.clone()),
                metadata: None,
            })
        } else {
            Ok(ValidatorModuleResult {
                authorized: false,
                authn_id: None,
                metadata: None,
            })
        }
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

struct DummyProcessorFactory;

impl PgWireServerHandlers for DummyProcessorFactory {
    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        let validator = SimpleTokenValidator::new();
        let oauth = Oauth::new(
            "https://accounts.google.com".to_string(),
            "openid profile email".to_string(),
            Arc::new(validator),
        );

        let authenticator =
            SASLAuthStartupHandler::new(Arc::new(DefaultServerParameterProvider::default()))
                .with_oauth(oauth);

        Arc::new(authenticator)
    }
}

#[tokio::main]
pub async fn main() {
    let factory = Arc::new(DummyProcessorFactory);

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
