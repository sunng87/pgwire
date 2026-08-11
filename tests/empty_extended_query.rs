use std::fmt::Debug;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use async_trait::async_trait;
use bytes::BytesMut;
use futures::Sink;
use pgwire::api::portal::Portal;
use pgwire::api::query::ExtendedQueryHandler;
use pgwire::api::results::{DescribePortalResponse, DescribeStatementResponse, Response};
use pgwire::api::stmt::{NoopQueryParser, StoredStatement};
use pgwire::api::{ClientInfo, DefaultClient, PgWireConnectionState};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::extendedquery::{
    Bind, Describe, Execute, Parse, Sync as PgSync, TARGET_TYPE_BYTE_PORTAL,
    TARGET_TYPE_BYTE_STATEMENT,
};
use pgwire::messages::{DecodeContext, PgWireBackendMessage};
use pgwire::tokio::server::PgWireMessageServerCodec;
use tokio::io::{AsyncReadExt, duplex};
use tokio_util::codec::Framed;

struct TestExtendedQueryHandler;

#[async_trait]
impl ExtendedQueryHandler for TestExtendedQueryHandler {
    type Statement = String;
    type QueryParser = NoopQueryParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        Arc::new(NoopQueryParser)
    }

    async fn do_describe_statement<C>(
        &self,
        _client: &mut C,
        _target: &StoredStatement<Self::Statement>,
    ) -> PgWireResult<DescribeStatementResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        panic!("empty query reached statement description")
    }

    async fn do_describe_portal<C>(
        &self,
        _client: &mut C,
        _target: &Portal<Self::Statement>,
    ) -> PgWireResult<DescribePortalResponse>
    where
        C: ClientInfo + Unpin + Send + Sync,
    {
        panic!("empty query reached portal description")
    }

    async fn do_query<C>(
        &self,
        _client: &mut C,
        _portal: &Portal<Self::Statement>,
        _max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        panic!("empty query reached query execution")
    }
}

#[tokio::test]
async fn empty_extended_query() {
    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5432);
    let mut client_info = DefaultClient::<String>::new(address, false);
    client_info.set_state(PgWireConnectionState::ReadyForQuery);
    let codec = PgWireMessageServerCodec::new(client_info);
    let (server_stream, mut client_stream) = duplex(1024);
    let mut server = Framed::new(server_stream, codec);
    let handler = TestExtendedQueryHandler;

    handler
        .on_parse(&mut server, Parse::new(None, String::new(), vec![]))
        .await
        .unwrap();
    handler
        .on_describe(&mut server, Describe::new(TARGET_TYPE_BYTE_STATEMENT, None))
        .await
        .unwrap();
    handler
        .on_bind(&mut server, Bind::new(None, None, vec![], vec![], vec![]))
        .await
        .unwrap();
    handler
        .on_describe(&mut server, Describe::new(TARGET_TYPE_BYTE_PORTAL, None))
        .await
        .unwrap();
    handler
        .on_execute(&mut server, Execute::new(None, 0))
        .await
        .unwrap();
    handler.on_sync(&mut server, PgSync::new()).await.unwrap();

    let mut buffer = BytesMut::new();
    let mut messages = Vec::new();
    while messages.len() < 7 {
        assert_ne!(client_stream.read_buf(&mut buffer).await.unwrap(), 0);
        while let Some(message) =
            PgWireBackendMessage::decode(&mut buffer, &DecodeContext::default()).unwrap()
        {
            messages.push(message);
        }
    }

    assert_eq!(messages.len(), 7);
    assert!(matches!(
        messages[0],
        PgWireBackendMessage::ParseComplete(_)
    ));
    match &messages[1] {
        PgWireBackendMessage::ParameterDescription(description) => {
            assert!(description.types.is_empty());
        }
        message => panic!("unexpected message: {message:?}"),
    }
    assert!(matches!(messages[2], PgWireBackendMessage::NoData(_)));
    assert!(matches!(messages[3], PgWireBackendMessage::BindComplete(_)));
    assert!(matches!(messages[4], PgWireBackendMessage::NoData(_)));
    assert!(matches!(
        messages[5],
        PgWireBackendMessage::EmptyQueryResponse(_)
    ));
    assert!(matches!(
        messages[6],
        PgWireBackendMessage::ReadyForQuery(_)
    ));
}
