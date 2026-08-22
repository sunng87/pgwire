//! Extended query subprotocol integration tests: Parse/Bind/Execute/Describe/
//! Close round-trips, typed parameters, text and binary result formats, and
//! portal suspension with incremental fetch.

mod common;

use bytes::Bytes;
use common::connect;
use pgwire::api::Type;
use pgwire::api::client::ClientInfo;
use pgwire::api::client::query::{DefaultExtendedQueryHandler, DescribeTarget, ExecuteResult};
use pgwire::api::client::result::DataRowDecoder;
use pgwire::api::results::{FieldFormat, FieldInfo};
use pgwire::error::PgWireClientError;

fn text_param(value: &str) -> Option<Bytes> {
    Some(Bytes::copy_from_slice(value.as_bytes()))
}

fn text_fields(types: &[(&str, Type)]) -> Vec<FieldInfo> {
    types
        .iter()
        .map(|(name, ty)| FieldInfo::new((*name).into(), None, None, ty.clone(), FieldFormat::Text))
        .collect()
}

#[tokio::test]
async fn prepare_reports_parameter_and_result_types() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    let prepared = eqc
        .prepare(
            Some("st_types"),
            "SELECT $1::int4 + 1 AS added, $2::text AS label",
            &[],
        )
        .await
        .expect("prepare failed");

    assert_eq!(prepared.name.as_deref(), Some("st_types"));
    // parameter types inferred by the server
    assert_eq!(prepared.param_types, vec![Type::INT4, Type::TEXT]);

    // describing the statement gives the same information plus result fields
    let described = eqc
        .describe(DescribeTarget::Statement(Some("st_types")))
        .await
        .expect("describe statement failed");
    assert_eq!(described.param_types, vec![Type::INT4, Type::TEXT]);
    assert_eq!(described.fields.len(), 2);
    assert_eq!(described.fields[0].name(), "added");
    assert_eq!(described.fields[0].datatype(), &Type::INT4);
    assert_eq!(described.fields[1].name(), "label");
    assert_eq!(described.fields[1].datatype(), &Type::TEXT);

    eqc.close(DescribeTarget::Statement(Some("st_types")))
        .await
        .expect("close failed");
}

#[tokio::test]
async fn client_specified_parameter_types_are_accepted() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    // `$1` alone has no inferable type; the client must specify it
    let prepared = eqc
        .prepare(Some("st_untyped"), "SELECT $1", &[Type::INT4.oid()])
        .await
        .expect("prepare with explicit parameter type failed");
    assert_eq!(prepared.param_types, vec![Type::INT4]);

    eqc.close(DescribeTarget::Statement(Some("st_untyped")))
        .await
        .expect("close failed");
}

#[tokio::test]
async fn command_statement_describes_to_no_data() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    let prepared = eqc
        .prepare(
            Some("st_cmd"),
            "CREATE TEMP TABLE pgwire_itest_ext (id INT4)",
            &[],
        )
        .await
        .expect("prepare of a command failed");
    assert!(prepared.param_types.is_empty());

    // commands have no result set: the server answers NoData
    let described = eqc
        .describe(DescribeTarget::Statement(Some("st_cmd")))
        .await
        .expect("describe failed");
    assert!(described.fields.is_empty());

    // execute the prepared command through a portal
    eqc.bind(Some("p_cmd"), Some("st_cmd"), vec![], vec![])
        .await
        .expect("bind failed");
    let result = eqc.execute(Some("p_cmd"), 0).await.expect("execute failed");
    assert!(matches!(result, ExecuteResult::Complete(rows) if rows.is_empty()));
}

#[tokio::test]
async fn bind_execute_with_parameters_and_text_decoding() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    eqc.prepare(
        Some("st_params"),
        "SELECT $1::int4 AS n, $2::text AS t, $1::int4 + 1 AS next",
        &[],
    )
    .await
    .expect("prepare failed");

    eqc.bind(
        Some("p_params"),
        Some("st_params"),
        vec![text_param("41"), text_param("hello")],
        vec![], // text result format
    )
    .await
    .expect("bind failed");

    // describing the portal reflects the bound result format
    let described = eqc
        .describe(DescribeTarget::Portal(Some("p_params")))
        .await
        .expect("describe portal failed");
    assert_eq!(described.fields.len(), 3);
    assert_eq!(described.fields[0].name(), "n");
    assert_eq!(described.fields[0].format(), FieldFormat::Text);

    let ExecuteResult::Complete(rows) = eqc
        .execute(Some("p_params"), 0)
        .await
        .expect("execute failed")
    else {
        panic!("expected complete execution");
    };
    assert_eq!(rows.len(), 1);

    let fields = described.fields.clone();
    let mut decoder = DataRowDecoder::new(&fields, rows.into_iter().next().unwrap());
    assert_eq!(decoder.next_value::<i32>().unwrap(), Some(41));
    assert_eq!(
        decoder.next_value::<String>().unwrap(),
        Some("hello".to_owned())
    );
    assert_eq!(decoder.next_value::<i32>().unwrap(), Some(42));
}

#[tokio::test]
async fn one_shot_query_round_trips() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    let rows = eqc
        .query(
            "SELECT $1::int4 * 2 AS doubled",
            &[],
            vec![text_param("21")],
        )
        .await
        .expect("one-shot query failed");
    assert_eq!(rows.len(), 1);

    let fields = text_fields(&[("doubled", Type::INT4)]);
    let mut decoder = DataRowDecoder::new(&fields, rows.into_iter().next().unwrap());
    assert_eq!(decoder.next_value::<i32>().unwrap(), Some(42));
}

#[tokio::test]
async fn binary_result_format_decoding() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    eqc.prepare(
        Some("st_binary"),
        "SELECT 1::int4 AS n, 'hello'::text AS t, true AS f",
        &[],
    )
    .await
    .expect("prepare failed");

    eqc.bind(Some("p_binary"), Some("st_binary"), vec![], vec![1]) // binary result format
        .await
        .expect("bind failed");

    // describing the portal reports the bound result format
    let described = eqc
        .describe(DescribeTarget::Portal(Some("p_binary")))
        .await
        .expect("describe portal failed");
    assert_eq!(described.fields[0].format(), FieldFormat::Binary);
    assert_eq!(described.fields[1].format(), FieldFormat::Binary);
    assert_eq!(described.fields[2].format(), FieldFormat::Binary);

    let ExecuteResult::Complete(rows) = eqc
        .execute(Some("p_binary"), 0)
        .await
        .expect("execute failed")
    else {
        panic!("expected complete execution");
    };
    assert_eq!(rows.len(), 1);

    let fields = described.fields.clone();
    let mut decoder = DataRowDecoder::new(&fields, rows.into_iter().next().unwrap());
    assert_eq!(decoder.next_value::<i32>().unwrap(), Some(1));
    assert_eq!(
        decoder.next_value::<String>().unwrap(),
        Some("hello".to_owned())
    );
    assert_eq!(decoder.next_value::<bool>().unwrap(), Some(true));
}

#[tokio::test]
async fn portal_suspension_fetches_in_batches() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    eqc.prepare(Some("st_series"), "SELECT generate_series(1, 7) AS n", &[])
        .await
        .expect("prepare failed");

    eqc.bind(Some("p_series"), Some("st_series"), vec![], vec![])
        .await
        .expect("bind failed");

    let described = eqc
        .describe(DescribeTarget::Portal(Some("p_series")))
        .await
        .expect("describe portal failed");
    let fields = described.fields.clone();

    let mut all = Vec::new();
    loop {
        match eqc
            .execute(Some("p_series"), 3)
            .await
            .expect("execute failed")
        {
            ExecuteResult::Suspended(rows) => {
                assert_eq!(rows.len(), 3, "expected a full batch before suspending");
                for row in rows {
                    let mut decoder = DataRowDecoder::new(&fields, row);
                    all.push(decoder.next_value::<i32>().unwrap());
                }
            }
            ExecuteResult::Complete(rows) => {
                for row in rows {
                    let mut decoder = DataRowDecoder::new(&fields, row);
                    all.push(decoder.next_value::<i32>().unwrap());
                }
                break;
            }
        }
    }

    assert_eq!(
        all,
        vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
            Some(6),
            Some(7)
        ]
    );
}

#[tokio::test]
async fn prepared_statement_is_reused_across_executions() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    eqc.prepare(Some("st_reuse"), "SELECT $1::int4 AS n", &[])
        .await
        .expect("prepare failed");

    let fields = text_fields(&[("n", Type::INT4)]);
    for expected in [10, 20, 30] {
        eqc.bind(
            Some("p_reuse"),
            Some("st_reuse"),
            vec![text_param(&expected.to_string())],
            vec![],
        )
        .await
        .expect("bind failed");
        let ExecuteResult::Complete(rows) = eqc
            .execute(Some("p_reuse"), 0)
            .await
            .expect("execute failed")
        else {
            panic!("expected complete execution");
        };
        assert_eq!(rows.len(), 1);
        let mut decoder = DataRowDecoder::new(&fields, rows.into_iter().next().unwrap());
        assert_eq!(decoder.next_value::<i32>().unwrap(), Some(expected));
    }
}

#[tokio::test]
async fn server_error_is_reported_and_connection_recovers() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    let error = eqc
        .prepare(Some("st_broken"), "SELEC 1", &[])
        .await
        .err()
        .expect("preparing a syntax error must fail");

    match error {
        PgWireClientError::RemoteError(info) => {
            assert_eq!(info.code, "42601"); // syntax_error
        }
        error => panic!("expected RemoteError, got: {error:?}"),
    }

    // the error recovery already consumed ReadyForQuery, so the connection
    // is immediately reusable through the same client
    let rows = eqc
        .query("SELECT $1::int4", &[], vec![text_param("7")])
        .await
        .expect("connection should be reusable after an error");
    let fields = text_fields(&[("?column?", Type::INT4)]);
    let mut decoder = DataRowDecoder::new(&fields, rows.into_iter().next().unwrap());
    assert_eq!(decoder.next_value::<i32>().unwrap(), Some(7));
}

#[tokio::test]
async fn parameter_status_during_extended_query_updates_cache() {
    let mut client = connect().await;
    let mut handler = DefaultExtendedQueryHandler::new();
    let mut eqc = client.extended_query(&mut handler);

    eqc.query("SET application_name = 'eq-renamed'", &[], vec![])
        .await
        .expect("SET via extended query failed");

    drop(eqc);
    assert_eq!(
        client.server_parameters().get("application_name"),
        Some(&"eq-renamed".to_owned())
    );
}
