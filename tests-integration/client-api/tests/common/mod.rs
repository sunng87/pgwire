//! Shared helpers for the client API integration tests.
//!
//! The tests talk to a real PostgreSQL server, by default the one started by
//! `run.sh` in this directory (podman, PostgreSQL 18, port 54329).
//!
//! Connection settings can be overridden with environment variables so the
//! tests can run against any PostgreSQL:
//!
//! - `PGWIRE_ITEST_HOST`     (default `127.0.0.1`)
//! - `PGWIRE_ITEST_PORT`     (default `54329`)
//! - `PGWIRE_ITEST_USER`     (default `postgres`)
//! - `PGWIRE_ITEST_PASSWORD` (default `postgres`)
//! - `PGWIRE_ITEST_DB`       (default `postgres`)

use std::sync::Arc;

use pgwire::api::client::Config;
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::tokio::client::PgWireClient;

pub fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

/// A [`Config`] pointing at the test PostgreSQL server.
pub fn test_config() -> Config {
    let mut config = Config::new();

    config.host(env_or("PGWIRE_ITEST_HOST", "127.0.0.1"));
    config.port(
        env_or("PGWIRE_ITEST_PORT", "54329")
            .parse()
            .expect("PGWIRE_ITEST_PORT must be a port number"),
    );
    config.user(env_or("PGWIRE_ITEST_USER", "postgres"));
    config.password(env_or("PGWIRE_ITEST_PASSWORD", "postgres"));
    config.dbname(env_or("PGWIRE_ITEST_DB", "postgres"));

    config
}

/// Connect to the test server with the default [`DefaultStartupHandler`]
/// (cleartext/MD5/SCRAM authentication).
#[allow(dead_code)] // not every test binary uses this helper
pub async fn connect() -> PgWireClient {
    connect_with(test_config()).await
}

/// Connect to the test server with a custom [`Config`].
pub async fn connect_with(config: Config) -> PgWireClient {
    PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .expect(
            "failed to connect to the test PostgreSQL server; \
             start one with tests-integration/client-api/run.sh \
             or point PGWIRE_ITEST_HOST/PGWIRE_ITEST_PORT at a running server",
        )
}
