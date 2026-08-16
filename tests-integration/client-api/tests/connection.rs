//! Connection/startup integration tests: authentication (SCRAM against a real
//! server), server parameter reporting, backend key data, and protocol
//! version negotiation.
//!
//! The protocol 3.2 tests require PostgreSQL 18+, which is what `run.sh`
//! starts. Against an older server they will fail because the server
//! negotiates down to 3.0.

mod common;

use std::sync::Arc;

use common::{connect, connect_with, test_config};
use pgwire::api::client::auth::DefaultStartupHandler;
use pgwire::api::client::{ClientInfo, Config};
use pgwire::error::PgWireClientError;
use pgwire::messages::ProtocolVersion;
use pgwire::messages::startup::SecretKey;
use pgwire::tokio::client::PgWireClient;

#[tokio::test]
async fn connects_with_scram_and_reports_server_information() {
    let client = connect().await;

    // ParameterStatus messages received during startup are collected
    let parameters = client.server_parameters();
    assert!(
        parameters.contains_key("server_version"),
        "expected server_version in {parameters:?}"
    );
    assert_eq!(
        parameters.get("client_encoding").map(String::as_str),
        Some("UTF8")
    );
    assert_eq!(
        parameters
            .get("standard_conforming_strings")
            .map(String::as_str),
        Some("on")
    );

    // BackendKeyData: a usable pid and a 3.0-style i32 secret key
    assert!(client.process_id() > 0);
    assert!(matches!(client.secret_key(), SecretKey::I32(_)));

    // The default protocol version stays 3.0 unless we ask for more
    assert_eq!(client.protocol_version(), ProtocolVersion::PROTOCOL3_0);
}

#[tokio::test]
async fn protocol_3_2_is_accepted_by_postgresql_18() {
    let mut config = test_config();
    config.protocol_version(ProtocolVersion::PROTOCOL3_2);

    let client = connect_with(config).await;

    // accepted as-is, no negotiation
    assert_eq!(client.protocol_version(), ProtocolVersion::PROTOCOL3_2);
    // protocol 3.2 backend keys are 32 random bytes
    assert!(matches!(client.secret_key(), SecretKey::Bytes(key) if key.len() == 32));
}

#[tokio::test]
async fn protocol_3_9999_is_negotiated_down_to_3_2() {
    let mut config = test_config();
    config.protocol_version(ProtocolVersion::PROTOCOL3_9999);

    let client = connect_with(config).await;

    // the server answers NegotiateProtocolVersion with its newest minor
    assert_eq!(client.protocol_version(), ProtocolVersion::PROTOCOL3_2);
    assert!(matches!(client.secret_key(), SecretKey::Bytes(_)));
}

#[tokio::test]
async fn wrong_password_is_rejected() {
    let mut config = test_config();
    config.password("definitely-the-wrong-password");

    let error = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .err()
        .expect("connecting with a wrong password must fail");

    match error {
        PgWireClientError::RemoteError(info) => {
            assert_eq!(info.code, "28P01"); // invalid_password
            assert!(info.message.contains("password authentication failed"));
        }
        error => panic!("expected RemoteError, got: {error:?}"),
    }
}

#[tokio::test]
async fn unknown_database_is_rejected() {
    let mut config = test_config();
    config.dbname("pgwire_no_such_database");

    let error = PgWireClient::connect(Arc::new(config), DefaultStartupHandler::new(), None)
        .await
        .err()
        .expect("connecting to an unknown database must fail");

    match error {
        PgWireClientError::RemoteError(info) => {
            assert_eq!(info.code, "3D000"); // invalid_catalog_name
        }
        error => panic!("expected RemoteError, got: {error:?}"),
    }
}

#[tokio::test]
async fn conninfo_string_is_parsed_by_config() {
    // The Config used by all the other tests is built programmatically; this
    // test verifies the libpq-style conninfo parser produces the same
    // connection.
    let conninfo = format!(
        "host={} port={} user={} password={} dbname={}",
        common::env_or("PGWIRE_ITEST_HOST", "127.0.0.1"),
        common::env_or("PGWIRE_ITEST_PORT", "54329"),
        common::env_or("PGWIRE_ITEST_USER", "postgres"),
        common::env_or("PGWIRE_ITEST_PASSWORD", "postgres"),
        common::env_or("PGWIRE_ITEST_DB", "postgres"),
    );
    let config: Config = conninfo.parse().expect("conninfo should parse");

    let client = connect_with(config).await;
    assert_eq!(client.protocol_version(), ProtocolVersion::PROTOCOL3_0);
    assert!(client.process_id() > 0);
}
