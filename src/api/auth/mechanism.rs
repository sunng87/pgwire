//! Authentication mechanism traits.
//!
//! An [`AuthMechanism`] is a **stateful, per-connection** authentication
//! state machine: the startup handler feeds it client messages and calls it
//! until it reports [`AuthState::Ok`] or [`AuthState::Failed`].
//!
//! ```text
//!            begin()                next(msg)              next(msg)
//!   create() ───────▶ InProgress ─────────────▶ InProgress ─────────▶ Ok/Failed
//!   (or straight to Ok/Failed)
//! ```
//!
//! - [`AuthMechanism::begin`] is called once when authentication starts. It
//!   returns the first `Authentication` request to send (or a terminal
//!   state — useful for fail-fast rejects).
//! - [`AuthMechanism::next`] consumes one password-family message from the
//!   client and returns the next state.
//!
//! Mechanisms are created per connection by [`AuthMechanismFactory`], which
//! holds the shared configuration (auth source, certificates, ...). The
//! surrounding startup protocol (protocol negotiation, parameter exchange,
//! `BackendKeyData`, `ReadyForQuery`) is owned by
//! [`crate::api::auth::AuthDriver`], which drives mechanisms to completion
//! inside a [`StartupHandler`](crate::api::auth::StartupHandler).
//!
//! Built-in mechanisms, each in its own module next to the matching
//! startup handler:
//!
//! - cleartext: [`CleartextPasswordAuth`](crate::api::auth::cleartext::CleartextPasswordAuth)
//! - md5: [`Md5PasswordAuth`](crate::api::auth::md5pass::Md5PasswordAuth)
//! - SASL family (feature `_ring`/`_aws-lc-rs`):
//!   [`ScramAuth`](crate::api::auth::sasl::scram::ScramAuth),
//!   [`Oauth`](crate::api::auth::sasl::oauth::Oauth) and the
//!   [`SaslMechanisms`](crate::api::auth::sasl::SaslMechanisms) registry
//!   that lets the client choose between them

use std::sync::Arc;

use async_trait::async_trait;

use super::ClientInfo;
use crate::error::{PgWireError, PgWireResult};
use crate::messages::startup::{Authentication, PasswordMessageFamily};

/// The state of an authentication handshake after one step.
#[derive(Debug)]
pub enum AuthState<I = ()> {
    /// Handshake in progress: send this message to the client, then feed the
    /// next password-family message into [`AuthMechanism::next`].
    InProgress(Authentication),
    /// Authentication succeeded. If `final_message` is present, send it to
    /// the client before completing the startup (for example SCRAM's
    /// `Authentication::SASLFinal`). `identity` is the authenticated
    /// principal the mechanism resolved, if any; the driver hands it back
    /// to the startup handler on completion.
    Ok {
        final_message: Option<Authentication>,
        identity: Option<I>,
    },
    /// Authentication failed with the given reason; the driver reports it
    /// to the client as an `ErrorResponse` (or the enclosing startup
    /// handler handles it).
    Failed(PgWireError),
}

/// A stateful, per-connection authentication state machine.
///
/// Created by [`AuthMechanismFactory::create`], then driven by
/// [`crate::api::auth::AuthDriver`]: `begin` once, `next` for every
/// password-family message, until [`AuthState::Ok`] or
/// [`AuthState::Failed`].
#[async_trait]
pub trait AuthMechanism<I = ()>: Send {
    /// Called once when authentication begins, before any client
    /// password-family message arrives.
    ///
    /// Returns the first `Authentication` request to send to the client
    /// ([`AuthState::InProgress`]), or a terminal state: [`AuthState::Ok`]
    /// for mechanisms that authenticate without client interaction, or
    /// [`AuthState::Failed`] to reject the connection without a round-trip.
    async fn begin(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> PgWireResult<AuthState<I>>;

    /// Consume one password-family message from the client and advance the
    /// handshake.
    ///
    /// Returning [`AuthState::Failed`] or an `Err` both end the handshake
    /// and are reported to the driver as an error. Prefer `Failed` for
    /// authentication outcomes (wrong password, unsupported method,
    /// malformed message) and `Err` for unexpected internal errors.
    async fn next(
        &mut self,
        client: &(dyn ClientInfo + Send + Sync),
        message: PasswordMessageFamily,
    ) -> PgWireResult<AuthState<I>>;
}

/// Shared configuration of an authentication method, able to create a fresh
/// stateful [`AuthMechanism`] for every connection.
pub trait AuthMechanismFactory<I = ()>: Send + Sync {
    /// Creates a new per-connection mechanism.
    fn create(&self) -> Box<dyn AuthMechanism<I>>;
}

impl<F: AuthMechanismFactory<I> + ?Sized, I> AuthMechanismFactory<I> for Arc<F> {
    fn create(&self) -> Box<dyn AuthMechanism<I>> {
        (**self).create()
    }
}
