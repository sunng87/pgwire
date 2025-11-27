use base64::prelude::BASE64_URL_SAFE_NO_PAD;
use base64::Engine;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
/// This example shows how to use pgwire with Keycloak OAuth.
/// To connect with psql:
/// 1. Install libq-oauth: sudo apt-get install libpq-oauth
/// 2. Setup keycloak. check this for more details: https://habr.com/en/companies/tantor/articles/959776/
/// 2. Execute: psql "postgres://postgres@localhost:5432/postgres?oauth_issuer=http://localhost:8080/realms/postgres-realm&oauth_client_id=postgres-client&oauth_client_secret=<my-client-secret>"
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error as IOError, ErrorKind};
use std::sync::Arc;

use async_trait::async_trait;

use pgwire::api::auth::sasl::oauth::{Oauth, OauthValidator, ValidatorModuleResult};
use rustls_pemfile::{certs, pkcs8_private_keys};
use rustls_pki_types::{CertificateDer, PrivateKeyDer};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use pgwire::api::auth::sasl::SASLAuthStartupHandler;
use pgwire::api::auth::{DefaultServerParameterProvider, StartupHandler};
use pgwire::api::PgWireServerHandlers;
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::tokio::process_socket;

pub fn random_salt() -> Vec<u8> {
    Vec::from(rand::random::<[u8; 10]>())
}

#[derive(Debug, Serialize, Deserialize, Default)]
struct RealmAccess {
    #[serde(default)]
    roles: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct KeyCloakClaims {
    sub: String,
    scope: Option<String>,
    #[serde(default)]
    realm_access: RealmAccess,
    preferred_username: Option<String>,
    email: Option<String>,
    exp: usize,
    iat: usize,
    iss: usize,
}

/// ODIC discovery doc
#[derive(Debug, Deserialize)]
struct OidcDiscovery {
    issuer: String,
    jwks_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct Jwk {
    kid: String,
    kty: String,
    #[serde(rename = "use")]
    key_use: Option<String>,
    // modulus for the rsa algo
    n: String,
    // exponent for yhe rsa algo
    e: String,
    alg: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Debug, Clone)]
struct KeyCloakValidator {
    issuer: String,
    client: reqwest::Client,
    jwks_cache: Arc<RwLock<HashMap<String, String>>>,
}

impl KeyCloakValidator {
    pub fn new(issuer: String) -> Self {
        Self {
            issuer,
            client: reqwest::Client::new(),
            jwks_cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    async fn fetch_oidc_discovery(&self) -> Result<OidcDiscovery, Box<dyn std::error::Error>> {
        let discovery_url = format!("{}/.well-known/openid-configuration", self.issuer);
        let response = self.client.get(&discovery_url).send().await?;
        let discovery: OidcDiscovery = response.json().await?;
        Ok(discovery)
    }

    async fn fetch_jwks(&self) -> Result<Jwks, Box<dyn std::error::Error>> {
        let discovery = self.fetch_oidc_discovery().await?;
        let response = self.client.get(&discovery.jwks_uri).send().await?;
        let jwks: Jwks = response.json().await?;
        Ok(jwks)
    }

    /// this gets the public key for a given key ID (kid)
    async fn get_public_key(&self, kid: &str) -> Result<DecodingKey, Box<dyn std::error::Error>> {
        {
            let cache = self.jwks_cache.read().await;
            if let Some(pem) = cache.get(kid) {
                return Ok(DecodingKey::from_rsa_pem(pem.as_bytes())?);
            }
        }

        let jwks = self.fetch_jwks().await?;
        let jwk = jwks
            .keys
            .iter()
            .find(|k| k.kid == kid)
            .ok_or("Key ID not found in JWKS")?;

        let pem = self.jwk_to_pem(jwk)?;
        {
            let mut cache = self.jwks_cache.write().await;
            cache.insert(kid.to_string(), pem.clone());
        }
        Ok(DecodingKey::from_rsa_pem(pem.as_bytes())?)
    }

    fn jwk_to_pem(&self, jwk: &Jwk) -> Result<String, Box<dyn std::error::Error>> {
        let n_bytes = BASE64_URL_SAFE_NO_PAD.decode(&jwk.n)?;
        let e_bytes = BASE64_URL_SAFE_NO_PAD.decode(&jwk.e)?;

        use rsa::BigUint;
        use rsa::RsaPublicKey;

        let n = BigUint::from_bytes_be(&n_bytes);
        let e = BigUint::from_bytes_be(&e_bytes);

        let public_key = RsaPublicKey::new(n, e)?;

        use rsa::pkcs8::EncodePublicKey;
        let pem = public_key.to_public_key_pem(rsa::pkcs8::LineEnding::LF)?;

        Ok(pem)
    }

    fn split_scopes(scope_str: &str) -> Vec<String> {
        scope_str
            .split_whitespace()
            .map(|s| s.to_string())
            .collect()
    }

    fn check_scopes(granted: &[String], required: &[String]) -> bool {
        required.iter().all(|req| granted.contains(req))
    }
}

#[async_trait]
impl OauthValidator for KeyCloakValidator {
    async fn validate(
        &self,
        token: &str,
        username: &str,
        issuer: &str,
        required_scopes: &str,
    ) -> PgWireResult<ValidatorModuleResult> {
        println!("Validating Keycloak token for user: {}", username);
        println!("Expected issuer: {}", issuer);
        println!("Required scopes: {}", required_scopes);

        //get kid from header
        let header = decode_header(token).map_err(|e| {
            PgWireError::OAuthValidationError(format!("Invalid token header: {}", e))
        })?;

        let kid = header.kid.ok_or_else(|| {
            PgWireError::OAuthValidationError("Missing 'kid' in token header".to_string())
        })?;

        // public key for the specified kid
        let decoding_key = self.get_public_key(&kid).await.map_err(|e| {
            PgWireError::OAuthValidationError(format!("Failed to get public key: {}", e))
        })?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);

        let token_data =
            decode::<KeyCloakClaims>(token, &decoding_key, &validation).map_err(|e| {
                PgWireError::OAuthValidationError(format!("Token validation failed: {}", e))
            })?;

        let claims = token_data.claims;

        // get 'sub' (user ID)
        let authn_id = claims.sub.clone();

        // get scopes and validate them
        let granted_scopes = if let Some(scope) = &claims.scope {
            Self::split_scopes(scope)
        } else {
            // use the realm roles if we can't ffind scopes
            claims.realm_access.roles.clone()
        };

        let required_scopes_list = Self::split_scopes(required_scopes);

        let scopes_match = Self::check_scopes(&granted_scopes, &required_scopes_list);

        if !scopes_match {
            println!(
                "Scope mismatch. Granted: {:?}, Required: {:?}",
                granted_scopes, required_scopes_list
            );
            return Ok(ValidatorModuleResult {
                authorized: false,
                authn_id: Some(authn_id),
                metadata: None,
            });
        }

        Ok(ValidatorModuleResult {
            authorized: true,
            authn_id: Some(authn_id),
            metadata: Some({
                let mut meta = HashMap::new();
                if let Some(email) = claims.email {
                    meta.insert("email".to_string(), email);
                }
                if let Some(username) = claims.preferred_username {
                    meta.insert("preferred_username".to_string(), username);
                }
                meta
            }),
        })
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
        let validator =
            KeyCloakValidator::new("http://localhost:8080/realms/postgres-realm".to_string());

        let oauth = Oauth::new(
            "http://localhost:8080/realms/postgres-realm".to_string(),
            "openid postgres".to_string(),
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
