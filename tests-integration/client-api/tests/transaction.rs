//! Transaction status tracking integration tests against a real PostgreSQL:
//! the status carried by `ReadyForQuery` must be reflected in
//! `PgWireClient::transaction_status()` across simple query, extended query
//! and error paths.

mod common;

use common::connect;
use pgwire::api::client::ClientInfo;
use pgwire::api::client::query::{DefaultExtendedQueryHandler, DefaultSimpleQueryHandler};
use pgwire::messages::response::TransactionStatus;

#[tokio::test]
async fn simple_query_tracks_transaction_status() {
    let mut client = connect().await;

    // a fresh connection is idle
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);

    client
        .simple_query(DefaultSimpleQueryHandler::new(), "BEGIN")
        .await
        .expect("BEGIN failed");
    assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

    // statements inside the transaction keep the status
    client
        .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1")
        .await
        .expect("SELECT in transaction failed");
    assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

    client
        .simple_query(DefaultSimpleQueryHandler::new(), "COMMIT")
        .await
        .expect("COMMIT failed");
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn failed_statement_inside_transaction_reports_error_status() {
    let mut client = connect().await;

    client
        .simple_query(DefaultSimpleQueryHandler::new(), "BEGIN")
        .await
        .expect("BEGIN failed");

    // 1/0 fails inside the transaction; the query returns an error but the
    // connection survives in the failed-transaction state
    assert!(
        client
            .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1/0")
            .await
            .is_err()
    );
    assert_eq!(client.transaction_status(), TransactionStatus::Error);

    // further statements are rejected while in the failed block, and the
    // status stays Error
    assert!(
        client
            .simple_query(DefaultSimpleQueryHandler::new(), "SELECT 1")
            .await
            .is_err()
    );
    assert_eq!(client.transaction_status(), TransactionStatus::Error);

    client
        .simple_query(DefaultSimpleQueryHandler::new(), "ROLLBACK")
        .await
        .expect("ROLLBACK failed");
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
}

#[tokio::test]
async fn extended_query_tracks_transaction_status() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();

    {
        let mut eqc = client.extended_query(&mut handler);
        eqc.query("BEGIN", &[], vec![])
            .await
            .expect("extended BEGIN failed");
    }
    assert_eq!(client.transaction_status(), TransactionStatus::Transaction);

    // a failing extended query inside the transaction also lands in the
    // failed-transaction state
    {
        let mut eqc = client.extended_query(&mut handler);
        assert!(eqc.query("SELECT 1/0", &[], vec![]).await.is_err());
    }
    assert_eq!(client.transaction_status(), TransactionStatus::Error);

    {
        let mut eqc = client.extended_query(&mut handler);
        eqc.query("ROLLBACK", &[], vec![])
            .await
            .expect("extended ROLLBACK failed");
    }
    assert_eq!(client.transaction_status(), TransactionStatus::Idle);
}
