// ref: https://github.com/postgres/postgres/blob/e7ccb247b38fff342c13aa7bdf61ce5ab45b2a85/src/backend/libpq/auth-oauth
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;
use bytes::Bytes;

use crate::{
    api::{
        auth::{sasl::SASLState, LoginInfo},
        ClientInfo,
    },
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

    /*-----
     * Validates the provided Authorization header and returns the token from
     * within it. NULL is returned on validation failure.
     *
     * Only Bearer tokens are accepted. The ABNF is defined in RFC 6750, Sec.
     * 2.1:
     *
     *      b64token    = 1*( ALPHA / DIGIT /
     *                        "-" / "." / "_" / "~" / "+" / "/" ) *"="
     *      credentials = "Bearer" 1*SP b64token
     *
     * The "credentials" construction is what we receive in our auth value.
     *
     * Since that spec is subordinate to HTTP (i.e. the HTTP Authorization
     * header format; RFC 9110 Sec. 11), the "Bearer" scheme string must be
     * compared case-insensitively. (This is not mentioned in RFC 6750, but the
     * OAUTHBEARER spec points it out: RFC 7628 Sec. 4.)
     */
    fn validate_token_format<'a>(&self, value: &'a str) -> Option<&'a str> {
        if value.is_empty() {
            return None;
        }

        // validate case insensitive bearer scheme
        if !value
            .to_ascii_lowercase()
            .starts_with(&BEARER_SCHEME.to_ascii_lowercase())
        {
            return None;
        }

        let token = value[BEARER_SCHEME.len()..].trim_start();

        if token.is_empty() {
            return None;
        }

        let valid_chars = token.chars().all(|c| {
            c.is_ascii_alphanumeric()
                || c == '-'
                || c == '.'
                || c == '_'
                || c == '~'
                || c == '+'
                || c == '/'
                || c == '='
        });

        if !valid_chars {
            return None;
        }

        Some(token)
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

                let auth = match self.parse_client_initial_response(data) {
                    Ok(Some(auth)) => auth,
                    Ok(None) => {
                        let err = self.generate_error_response();
                        return Ok((
                            Authentication::SASLContinue(Bytes::from(err)),
                            SASLState::OauthStateError,
                        ));
                    }
                    Err(err) => return Err(err),
                };

                let token = match self.validate_token_format(&auth) {
                    Some(t) => t,
                    None => {
                        let err = self.generate_error_response();
                        return Ok((
                            Authentication::SASLContinue(Bytes::from(err)),
                            SASLState::OauthStateError,
                        ));
                    }
                };

                let login_info = LoginInfo::from_client_info(client);
                let username = login_info
                    .user()
                    .ok_or_else(|| PgWireError::UserNameRequired)?;

                let validation_result = self
                    .validator
                    .validate(token, username, &self.issuer, &self.scope)
                    .await?;

                if !validation_result.authorized {
                    let err = self.generate_error_response();
                    return Ok((
                        Authentication::SASLContinue(Bytes::from(err)),
                        SASLState::OauthStateError,
                    ));
                }

                // TODO: handle user mapping with skip_usermap

                Ok((Authentication::Ok, SASLState::Finished))
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
#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MockValidator;

    #[async_trait]
    impl OauthValidator for MockValidator {
        async fn validate(
            &self,
            _token: &str,
            _username: &str,
            _issuer: &str,
            _required_scopes: &str,
        ) -> PgWireResult<ValidatorModuleResult> {
            Ok(ValidatorModuleResult {
                authorized: true,
                authn_id: Some("test@example.com".to_string()),
                metadata: None,
            })
        }
    }

    #[test]
    fn test_parse_kvpairs() {
        let oauth = Oauth::new(
            "https://example.com".to_string(),
            "openid".to_string(),
            Arc::new(MockValidator),
        );

        // valid
        let data = "auth=Bearer token123\x01\x01";
        let result = oauth.parse_kvpairs(data).unwrap();
        assert_eq!(result, Some("Bearer token123".to_string()));

        // multiple keys
        let data = "host=localhost\x01auth=Bearer token123\x01port=5432\x01\x01";
        let result = oauth.parse_kvpairs(data).unwrap();
        assert_eq!(result, Some("Bearer token123".to_string()));
    }

    #[test]
    fn test_validate_token_format() {
        let oauth = Oauth::new(
            "https://example.com".to_string(),
            "openid".to_string(),
            Arc::new(MockValidator),
        );

        assert!(oauth.validate_token_format("Bearer abc123").is_some());
        assert!(oauth
            .validate_token_format("Bearer abc.123_def-ghi+jkl/mno===")
            .is_some());

        assert!(oauth.validate_token_format("").is_none());
        assert!(oauth.validate_token_format("Bearer ").is_none());
        assert!(oauth.validate_token_format("Basic abc123").is_none());
    }
}
