// ref: https://github.com/postgres/postgres/blob/e7ccb247b38fff342c13aa7bdf61ce5ab45b2a85/src/backend/libpq/auth-oauth
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    api::{auth::sasl::SASLState, ClientInfo},
    error::{PgWireError, PgWireResult},
    messages::startup::{Authentication, PasswordMessageFamily},
};

#[derive(Debug)]
pub struct Oauth {
    pub issuer: String,
    pub scope: String,
    pub validator: Arc<dyn OauthValidator>,
    /// Whether to skip user mapping (validator is authoritative)
    pub skip_usermap: bool,
}

/// Constants seen in an OAUTHBEARER client initial response.
///
/// required header scheme (case-insensitive!)
const BEARER_SCHEME: &str = "Bearer ";
/// key containing the Authorization header
const AUTH_KEY: &str = "auth";
/// separator byte for key/value pairs
const KVSEP: u8 = 0x01;

/// the result of validating a token from a validator module (in this case, validator trait)
#[derive(Debug, Clone)]
pub struct ValidatorModuleResult {
    /// whether the token is authorized for the requested access
    pub authorized: bool,
    pub authn_id: Option<String>,
    /// optional: additional claims or metadata for state
    pub metadata: Option<HashMap<String, String>>,
}

/// validate a bearer tokjn for a specific user
#[async_trait]
pub trait OauthValidator: Send + Sync + Debug {
    async fn validate(
        &self,
        token: &str,
        username: &str,
        issuer: &str,
        required_scopes: &str,
    ) -> PgWireResult<ValidatorModuleResult>;
}

impl Oauth {
    /// initialize the oauth context.
    pub fn new(issuer: String, scope: String, validator: Arc<dyn OauthValidator>) -> Self {
        Self {
            issuer,
            scope,
            validator,
            skip_usermap: false,
        }
    }

    pub fn with_skip_usermapping(mut self, skip: bool) -> Self {
        self.skip_usermap = skip;
        self
    }

    /// * Builds the JSON response for failed authentication (RFC 7628, Sec. 3.2.2).
    /// * This contains the required scopes for entry and a pointer to the OAuth/OpenID
    /// * discovery document, which the client may use to conduct its OAuth flow.
    fn generate_error_response(&self) -> String {
        // * Build a default .well-known URI based on our issuer, unless the HBA has
        // * already provided one.
        let config = if self.issuer.contains("/.well-known/") {
            self.issuer.clone()
        } else {
            format!("{}/.well-known/openid-configuration", self.issuer)
        };

        let config = config.replace('\\', "\\\\").replace('"', "\\\"");
        let scope = self.scope.replace('\\', "\\\\").replace('"', "\\\"");

        format!(
            r#"{{"status":"invalid_token","openid-configuration":"{}","scope":"{}"}}"#,
            config, scope
        )
    }

    fn parse_client_initial_response(&self, data: &[u8]) -> PgWireResult<Option<String>> {
        // discovery connection
        if data.is_empty() {
            return Ok(None);
        }

        let s = str::from_utf8(data)
            .map_err(|err| PgWireError::InvalidOauthMessage(format!("Invalid UTF-8: {err}")))?;

        // from the docs, it says:
        // The client initial response consists of the standard "GS2" header used by SCRAM, followed by a list of key=value pairs
        let mut chars = s.chars();

        // so, we have to parse the GS2 header
        let cbind_flag = chars
            .next()
            .ok_or_else(|| PgWireError::InvalidOauthMessage("Empty message".to_string()))?;
        match cbind_flag {
            'n' | 'y' => {
                if chars.next() != Some(',') {
                    return Err(PgWireError::InvalidOauthMessage(
                        "Expected comma after channel binding flag".to_string(),
                    ));
                }
            }
            'p' => {
                return Err(PgWireError::InvalidOauthMessage(
                    "Channel binding not supported for oauth".to_string(),
                ))
            }
            _ => {
                return Err(PgWireError::InvalidOauthMessage(format!(
                    "Invalid channel binding flag: {cbind_flag}"
                )))
            }
        }

        // get authzid, we expect it to be empty too, according to the docs
        if chars.next() != Some(',') {
            return Err(PgWireError::InvalidOauthMessage(
                "authzid not supported".to_string(),
            ));
        }

        // then, we exoect the separator
        if chars.next() != Some('\x01') {
            return Err(PgWireError::InvalidOauthMessage(
                "Expected kvsep after GS2 header".to_string(),
            ));
        }

        let remnant = chars.as_str();
        self.parse_kvpairs(remnant)
    }

    ///  * Performs syntactic validation of a key and value from the initial client
    /// * response. (Semantic validation of interesting values must be performed
    /// * later.)
    fn parse_kvpairs(&self, data: &str) -> PgWireResult<Option<String>> {
        let mut auth = None;
        for kv in data.split('\x01') {
            // that is, we've come to the end of the key value pairs
            if kv.is_empty() {
                break;
            }

            let parts: Vec<&str> = kv.splitn(2, '=').collect();
            if parts.len() != 2 {
                return Err(PgWireError::InvalidOauthMessage(
                    "Malformed key-value pair".to_owned(),
                ));
            }

            let key = parts[0];
            let value = parts[1];

            if !key.chars().all(|c| c.is_ascii_alphabetic()) {
                return Err(PgWireError::InvalidOauthMessage(
                    "Invalid key name".to_owned(),
                ));
            }

            // Validate value (VCHAR / SP / HTAB / CR / LF)
            if !value
                .chars()
                .all(|c| matches!(c, '\x21'..='\x7E' | ' ' | '\t' | '\r' | '\n'))
            {
                return Err(PgWireError::InvalidOauthMessage(
                    "Invalid value characters".to_owned(),
                ));
            }

            if key == AUTH_KEY {
                if auth.is_some() {
                    return Err(PgWireError::InvalidOauthMessage(
                        "Multiple oauth values".to_string(),
                    ));
                }
                auth = Some(value.to_string())
            }
        }

        Ok(auth)
    }

    pub async fn process_oauth_message<C>(
        &self,
        client: &C,
        msg: PasswordMessageFamily,
        state: &SASLState,
    ) -> PgWireResult<(Authentication, SASLState)>
    where
        C: ClientInfo + Unpin + Send,
    {
        // decode the message, if there's no auth in the SASL data field, return Authentication::SASLContinue
        // to make the client respond with the Initial Client Response (still figuring out what this is exactly, but I think it is just the auth field as descibed in the docs)
        // then handle authentication based on the states, validate token and bla bla bla
        match state {
            SASLState::OauthStateInit => {
                let res = msg.into_sasl_initial_response()?;
                let data = res.data.as_deref().unwrap_or(&[]);

                // if dtata is empty, that means it is for discovery
                if data.is_empty() {
                    let error_res = self.generate_error_response();
                    return Ok((
                        Authentication::SASLContinue(Bytes::from(error_res)),
                        SASLState::OauthStateError,
                    ));
                }

                todo!()
            }
            SASLState::OauthStateError => {
                let res = msg.into_sasl_response()?;
                if res.data.len() != 1 || res.data[0] != KVSEP {
                    return Err(PgWireError::InvalidOauthMessage(
                        "Expected single kvsep byte in error response".to_string(),
                    ));
                }

                Err(PgWireError::OAuthAuthenticationFailed(
                    "Token validation failed".to_string(),
                ))
            }
            _ => Err(PgWireError::InvalidSASLState),
        }
    }
}
