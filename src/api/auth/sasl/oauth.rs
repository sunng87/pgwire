// ref: https://github.com/postgres/postgres/blob/e7ccb247b38fff342c13aa7bdf61ce5ab45b2a85/src/backend/libpq/auth-oauth
use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_trait::async_trait;

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
            SASLState::OauthStateInit => Ok(()),
            SASLState::OauthStateError => {
                let resp = msg.into_sasl_response()?;
                if resp.data.len() != 1 || resp.data[0] != KVSEP {
                    return Err(PgWireError::Invalid);
                }
            }
            _ => Err(PgWireError::InvalidSASLState),
        }
    }
}
