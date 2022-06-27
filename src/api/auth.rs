use super::ClientInfo;
use crate::messages::startup::{Password, Startup};
use crate::messages::PgWireMessage;

/// Server api for a password authticator implementation
pub trait PasswordAuthenticator {
    fn on_startup(&self, client_info: ClientInfo, msg: &Startup) -> Result<(), std::io::Error>;

    fn on_password(&self, client_info: ClientInfo, msg: &Password) -> Result<(), std::io::Error>;
}

// Alternative design: pass PgWireMessage into the trait and allow the
// implementation to track and define state within itself. This allows better
// support for other auth type like sasl.

pub trait Authenticator {
    // TODO: move connection state into ClientInfo
    fn on_startup(
        &self,
        client_info: &mut ClientInfo,
        message: &PgWireMessage,
    ) -> Result<(), std::io::Error>;
}

pub struct DummyAuthenticator;

impl Authenticator for DummyAuthenticator {
    fn on_startup(
        &self,
        client_info: &mut ClientInfo,
        message: &PgWireMessage,
    ) -> Result<(), std::io::Error> {
        println!("{:?}, {:?}", client_info, message);
        Ok(())
    }
}
