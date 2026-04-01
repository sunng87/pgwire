use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use base64::{Engine, prelude::BASE64_URL_SAFE_NO_PAD};
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use rsa::{
    BigUint, RsaPublicKey,
    pkcs8::{EncodePublicKey, LineEnding},
};
use serde::Deserialize;
use tokio::sync::RwLock;

use crate::{
    api::auth::sasl::oauth::{OauthValidator, ValidatorModuleResult},
    error::{PgWireError, PgWireResult},
};

#[derive(Debug, Deserialize)]
struct SimpleOidcDiscovery {
    jwks_uri: String,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

#[derive(Debug, Deserialize)]
struct Jwk {
    /// jey id
    kid: String,
    /// key type. we only support RSA for this
    kty: String,
    n: Option<String>,
    e: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Claims {
    sub: String,
    #[serde(default)]
    scope: Option<String>,
    #[serde(default)]
    preferred_username: Option<String>,
    #[serde(default)]
    email: Option<String>,
}

/// A simple OIDC validator that works with most identity providers
#[derive(Debug)]
pub struct SimpleOidcValidator {
    issuer: String,
    client: reqwest::Client,
    key_cache: Arc<RwLock<HashMap<String, String>>>,
    jwks_uri: Arc<RwLock<Option<String>>>,
}

impl SimpleOidcValidator {
    pub async fn new(issuer: impl Into<String>) -> Result<Self, PgWireError> {
        let issuer = issuer.into();
        let client = reqwest::Client::new();

        let uri = Self::fetch_jwks_uri(&client, &issuer).await?;

        Ok(Self {
            issuer,
            client,
            key_cache: Arc::new(RwLock::new(HashMap::new())),
            jwks_uri: Arc::new(RwLock::new(Some(uri))),
        })
    }

    async fn fetch_jwks_uri(client: &reqwest::Client, issuer: &str) -> Result<String, PgWireError> {
        let url = format!(
            "{}/.well-known/openid-configuration",
            issuer.trim_end_matches('/')
        );

        let discovery: SimpleOidcDiscovery = client
            .get(&url)
            .send()
            .await
            .map_err(|e| PgWireError::OAuthValidationError(format!("Discovery failed: {}", e)))?
            .json()
            .await
            .map_err(|e| PgWireError::OAuthValidationError(format!("Invalid discovery: {}", e)))?;

        Ok(discovery.jwks_uri)
    }

    /// retrieve uri from the cache
    async fn get_uri(&self) -> Result<String, PgWireError> {
        let cache = self.jwks_uri.read().await;
        if let Some(uri) = cache.as_ref() {
            return Ok(uri.clone());
        }

        // incase the uri is empty during startup
        let uri = Self::fetch_jwks_uri(&self.client, &self.issuer).await?;
        let mut cache = self.jwks_uri.write().await;
        *cache = Some(uri.clone());
        Ok(uri)
    }

    fn jwk_to_pem(&self, jwk: &Jwk) -> Result<String, PgWireError> {
        if jwk.kty != "RSA" {
            return Err(PgWireError::OAuthValidationError(format!(
                "only RSA key type is supported. Got: {}",
                jwk.kty
            )));
        }

        let n = jwk
            .n
            .as_ref()
            .ok_or_else(|| PgWireError::OAuthValidationError("modulus is missing".to_string()))?;
        let e = jwk
            .e
            .as_ref()
            .ok_or_else(|| PgWireError::OAuthValidationError("exponent is missing".to_string()))?;

        let n_bytes = BASE64_URL_SAFE_NO_PAD
            .decode(n)
            .map_err(|e| PgWireError::OAuthValidationError(e.to_string()))?;
        let e_bytes = BASE64_URL_SAFE_NO_PAD
            .decode(e)
            .map_err(|e| PgWireError::OAuthValidationError(e.to_string()))?;

        let public_key = RsaPublicKey::new(
            BigUint::from_bytes_be(&n_bytes),
            BigUint::from_bytes_be(&e_bytes),
        )
        .map_err(|e| PgWireError::OAuthValidationError(e.to_string()))?;

        public_key
            .to_public_key_pem(LineEnding::LF)
            .map_err(|e| PgWireError::OAuthValidationError(e.to_string()))
    }

    fn check_scopes(granted: Option<&str>, required: &str) -> bool {
        if required.is_empty() {
            return true;
        }
        let granted: Vec<&str> = granted.unwrap_or("").split_whitespace().collect();
        required.split_whitespace().all(|r| granted.contains(&r))
    }

    // get the public keu
    async fn get_pk(&self, kid: &str) -> Result<DecodingKey, PgWireError> {
        // check if it is present in the cache first
        {
            let cache = self.key_cache.read().await;
            if let Some(pem) = cache.get(kid) {
                return DecodingKey::from_rsa_pem(pem.as_bytes())
                    .map_err(|err| PgWireError::OAuthValidationError(err.to_string()));
            }
        }

        let uri = self.get_uri().await?;
        let jwks: Jwks = self
            .client
            .get(&uri)
            .send()
            .await
            .map_err(|err| {
                PgWireError::OAuthValidationError(format!(
                    "failed to fetch jwks from uri: {uri}. Err: {}",
                    err
                ))
            })?
            .json()
            .await
            .map_err(|err| {
                PgWireError::OAuthValidationError(format!("invalid jwks format. Err {}", err))
            })?;

        let jwk: Jwk = jwks
            .keys
            .into_iter()
            .find(|k| k.kid == kid)
            .ok_or_else(|| PgWireError::OAuthValidationError(format!("key not found: {}", kid)))?;

        let pem = self.jwk_to_pem(&jwk)?;
        // keep it in the cache
        {
            let mut cache = self.key_cache.write().await;
            cache.insert(kid.to_string(), pem.clone());
        }

        DecodingKey::from_rsa_pem(pem.as_bytes())
            .map_err(|err| PgWireError::OAuthValidationError(err.to_string()))
    }
}

#[async_trait]
impl OauthValidator for SimpleOidcValidator {
    async fn validate(
        &self,
        token: &str,
        username: &str,
        _issuer: &str,
        required_scopes: &str,
    ) -> PgWireResult<ValidatorModuleResult> {
        let header = decode_header(token).map_err(|e| {
            PgWireError::OAuthValidationError(format!("Invalid token header: {}", e))
        })?;

        let kid = header.kid.ok_or_else(|| {
            PgWireError::OAuthValidationError("Missing 'kid' in token".to_string())
        })?;

        let key = self.get_pk(&kid).await?;

        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_issuer(&[&self.issuer]);
        validation.validate_aud = false;

        let token_data = decode::<Claims>(token, &key, &validation).map_err(|e| {
            PgWireError::OAuthValidationError(format!("Token validation failed: {}", e))
        })?;

        let claims = token_data.claims;

        // the OIDC says to prefer email or usernmae over sub
        let authn_id = claims
            .preferred_username
            .or(claims.email)
            .unwrap_or(claims.sub.clone());

        // confirm username
        if username != authn_id && username != claims.sub {
            return Ok(ValidatorModuleResult {
                authorized: false,
                authn_id: Some(authn_id),
                metadata: None,
            });
        }

        // confirm scopes too
        if !Self::check_scopes(claims.scope.as_deref(), required_scopes) {
            return Ok(ValidatorModuleResult {
                authorized: false,
                authn_id: Some(authn_id),
                metadata: None,
            });
        }

        Ok(ValidatorModuleResult {
            authorized: true,
            authn_id: Some(authn_id),
            metadata: None,
        })
    }
}
