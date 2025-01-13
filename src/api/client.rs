mod auth;
pub(crate) mod config;

use std::sync::Arc;

pub use config::Config;

/// The collection of all client handlers
pub trait PgWireClientHandlers {
    type StartupHandler: auth::StartupHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler>;
}

impl<T> PgWireClientHandlers for Arc<T>
where
    T: PgWireClientHandlers,
{
    type StartupHandler = T::StartupHandler;

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        (**self).startup_handler()
    }
}
