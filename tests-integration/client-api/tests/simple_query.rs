//! Simple query subprotocol integration tests: `Query` message round-trip,
//! command tags, row decoding (text format), empty query and error handling.

mod common;

use common::connect;
use pgwire::api::client::ClientInfo;
use pgwire::api::client::query::{DefaultSimpleQueryHandler, Response};
use pgwire::api::results::Tag;
use pgwire::error::PgWireClientError;
use pgwire::tokio::client::PgWireClient;

async fn simple_query(client: &mut PgWireClient, query: &str) -> Vec<Response> {
    client
        .simple_query(DefaultSimpleQueryHandler::new(), query)
        .await
        .expect("simple query failed")
}

/// Assert a response is `Response::Execution` and return its tag.
fn execution_tag(response: &Response) -> &Tag {
    let Response::Execution(tag) = response else {
        panic!("expected Response::Execution, got {response:?}")
    };
    tag
}

#[tokio::test]
async fn selects_and_decodes_rows() {
    let mut client = connect().await;

    let responses = simple_query(
        &mut client,
        "SELECT 1 AS one, 'hello' AS greeting, true AS flag, NULL::int4 AS nothing",
    )
    .await;

    assert_eq!(responses.len(), 1);
    let Response::Query((tag, fields, _rows)) = &responses[0] else {
        panic!("expected Response::Query, got {:?}", responses[0]);
    };
    assert_eq!(*tag, Tag::new("SELECT").with_rows(1));
    assert_eq!(fields.len(), 4);
    assert_eq!(fields[0].name(), "one");
    assert_eq!(fields[1].name(), "greeting");

    let mut reader = responses
        .into_iter()
        .next()
        .unwrap()
        .into_data_rows_reader();
    let mut row = reader.next_row().expect("expected one row");
    assert_eq!(row.len(), 4);
    assert_eq!(row.next_value::<i32>().unwrap(), Some(1));
    assert_eq!(
        row.next_value::<String>().unwrap(),
        Some("hello".to_owned())
    );
    assert_eq!(row.next_value::<bool>().unwrap(), Some(true));
    assert_eq!(row.next_value::<i32>().unwrap(), None);
    assert!(reader.next_row().is_none(), "expected no more rows");
}

#[tokio::test]
async fn multi_statement_query_returns_response_per_statement() {
    let mut client = connect().await;

    let responses = simple_query(
        &mut client,
        "DROP TABLE IF EXISTS pgwire_itest_simple;
         CREATE TABLE pgwire_itest_simple (id INT4, name TEXT);
         INSERT INTO pgwire_itest_simple VALUES (1, 'a'), (2, 'b');
         UPDATE pgwire_itest_simple SET name = 'c' WHERE id = 2;
         DELETE FROM pgwire_itest_simple WHERE id = 1;",
    )
    .await;

    assert_eq!(responses.len(), 5);
    assert_eq!(execution_tag(&responses[0]), &Tag::new("DROP TABLE"));
    assert_eq!(execution_tag(&responses[1]), &Tag::new("CREATE TABLE"));
    // "INSERT 0 2" is `INSERT <oid> <rows>`: oid 0 (unused) and 2 rows
    assert_eq!(
        execution_tag(&responses[2]),
        &Tag::new("INSERT").with_oid(0).with_rows(2)
    );
    assert_eq!(
        execution_tag(&responses[3]),
        &Tag::new("UPDATE").with_rows(1)
    );
    assert_eq!(
        execution_tag(&responses[4]),
        &Tag::new("DELETE").with_rows(1)
    );
}

#[tokio::test]
async fn decodes_various_types_in_text_format() {
    let mut client = connect().await;

    let responses = simple_query(
        &mut client,
        "SELECT 2147483647::int4,
                9223372036854775807::int8,
                3.5::float8,
                'text value'::text,
                false::bool,
                '2025-01-15 10:30:00'::timestamp::text",
    )
    .await;

    let mut reader = responses
        .into_iter()
        .next()
        .unwrap()
        .into_data_rows_reader();
    let mut row = reader.next_row().expect("expected one row");
    assert_eq!(row.next_value::<i32>().unwrap(), Some(i32::MAX));
    assert_eq!(row.next_value::<i64>().unwrap(), Some(i64::MAX));
    assert_eq!(row.next_value::<f64>().unwrap(), Some(3.5));
    assert_eq!(
        row.next_value::<String>().unwrap(),
        Some("text value".to_owned())
    );
    assert_eq!(row.next_value::<bool>().unwrap(), Some(false));
    assert_eq!(
        row.next_value::<String>().unwrap(),
        Some("2025-01-15 10:30:00".to_owned())
    );
}

#[tokio::test]
async fn empty_query_returns_empty_query_response() {
    let mut client = connect().await;

    let responses = simple_query(&mut client, "").await;
    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], Response::EmptyQuery));

    // comment-only queries are empty queries as well
    let responses = simple_query(&mut client, "-- just a comment").await;
    assert_eq!(responses.len(), 1);
    assert!(matches!(responses[0], Response::EmptyQuery));
}

#[tokio::test]
async fn server_error_is_reported_and_connection_recovers() {
    let mut client = connect().await;

    let error = client
        .simple_query(
            DefaultSimpleQueryHandler::new(),
            "SELECT * FROM pgwire_itest_no_such_table",
        )
        .await
        .err()
        .expect("querying a missing table must fail");

    match error {
        PgWireClientError::RemoteError(info) => {
            assert_eq!(info.code, "42P01"); // undefined_table
            assert!(info.message.contains("pgwire_itest_no_such_table"));
        }
        error => panic!("expected RemoteError, got: {error:?}"),
    }

    // the trailing ReadyForQuery of the failed query is consumed by
    // simple_query, so the connection is immediately reusable
    let responses = simple_query(&mut client, "SELECT 1").await;
    assert_eq!(responses.len(), 1);
    let mut reader = responses
        .into_iter()
        .next()
        .unwrap()
        .into_data_rows_reader();
    let mut row = reader.next_row().expect("expected one row");
    assert_eq!(row.next_value::<i32>().unwrap(), Some(1));
}

#[tokio::test]
async fn notices_do_not_break_the_response_stream() {
    let mut client = connect().await;

    // Dropping a table that does not exist makes the server emit a
    // NoticeResponse before the CommandComplete of the DROP.
    let responses = simple_query(
        &mut client,
        "DROP TABLE IF EXISTS pgwire_itest_never_created",
    )
    .await;
    assert_eq!(responses.len(), 1);
    assert_eq!(execution_tag(&responses[0]), &Tag::new("DROP TABLE"));
}

#[tokio::test]
async fn application_name_from_config_is_visible_to_the_server() {
    let mut config = common::test_config();
    config.application_name("pgwire-client-api-itest");
    let mut client = common::connect_with(config).await;

    let responses = simple_query(&mut client, "SHOW application_name").await;
    let mut reader = responses
        .into_iter()
        .next()
        .unwrap()
        .into_data_rows_reader();
    let mut row = reader.next_row().expect("expected one row");
    assert_eq!(
        row.next_value::<String>().unwrap(),
        Some("pgwire-client-api-itest".to_owned())
    );
}

// A `SET` statement makes the server send a ParameterStatus message inside
// the query response; the handler must tolerate it and the client's cached
// server parameters must reflect the new value.
#[tokio::test]
async fn set_parameter_status_does_not_break_simple_query() {
    let mut client = connect().await;

    let responses = simple_query(&mut client, "SET application_name = 'renamed'").await;
    assert_eq!(responses.len(), 1);
    assert_eq!(execution_tag(&responses[0]), &Tag::new("SET"));

    assert_eq!(
        client.server_parameters().get("application_name"),
        Some(&"renamed".to_owned())
    );
}
