use std::{fmt::Debug, sync::Arc};

use crate::{
    api::{auth::sasl::SASLState, ClientInfo},
    error::PgWireResult,
    messages::startup::{Authentication, PasswordMessageFamily},
};

#[derive(Debug)]
// TODO: find a way to get the issuer and scope, i think it'll be gotten from the client?
// as per: https://github.com/postgres/postgres/blob/e7ccb247b38fff342c13aa7bdf61ce5ab45b2a85/src/backend/libpq/auth-oauth.c#L102
pub struct Oauth {
    pub issuer: String,
    pub scope: String,
    pub validator: Arc<dyn OauthValidator>,
}

// TODO: move this out
pub trait OauthValidator: Send + Sync + Debug {}

impl Oauth {
    /// initialize the oauth context.
    pub fn init() -> Self {
        todo!()
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
        todo!()
    }
}
